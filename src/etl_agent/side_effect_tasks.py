"""副作用执行 Celery 任务 — Worker 侧的副作用消费者（第 12 章）。

服务层（``SideEffectHarnessService``）在 Commit 时签发一次性能力令牌，并在同一事务
写入 ``harness_command_outbox``，``target = worker.side_effect.<operation 小写>``。本模块
补齐那半段闭环：Worker 从 Outbox 领取已 Commit 的副作用命令并落地执行结果，让
"审批 + Commit 成功" 之后的外部副作用真正被执行，而不是永远停在待投递。

执行纪律：

- **至少一次 + 租约**：``claim_committed_side_effect`` 用行锁 + 执行租约领取命令，
  重复投递被租约挡住，同一副作用不会被两个 Worker 同时执行。
- **失败关闭**：本机没有 Doris / SeaTunnel / 外部密钥系统，凡需要这些外部系统的
  操作一律调用 ``fail_side_effect`` 落下固定脱敏文案，绝不假装成功、也绝不落异常原文。
- **绝不崩 Worker**：任何异常都被兜底捕获并尽量回执失败，任务只返回状态字典。
"""

import socket
import uuid
from typing import Any

from etl_agent.application.execution import ExecutionService
from etl_agent.application.side_effect_harness import (
    claim_committed_side_effect,
    complete_side_effect,
    fail_side_effect,
)
from etl_agent.asyncio_compat import run_async
from etl_agent.domain.enums import ExecutionStatus
from etl_agent.domain.errors import DomainError
from etl_agent.domain.side_effects import SideEffectOperation
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.models import ExecutionRun
from etl_agent.worker import celery_app

# 固定脱敏文案：外部系统未启用属于"失败关闭"，文案不暴露环境细节与异常原文。
_EXTERNAL_UNAVAILABLE_MESSAGE = "本地/测试环境未启用该副作用所需的外部系统，已按失败关闭处理。"
_CANCEL_TARGET_MISSING_MESSAGE = "无法定位待取消的执行记录，取消操作已按失败关闭处理。"
_CANCEL_ILLEGAL_TRANSITION_MESSAGE = "目标执行记录当前状态不允许取消，取消操作已按失败关闭处理。"
_UNEXPECTED_MESSAGE = "副作用执行过程中发生未预期错误，已按失败关闭处理。"

# 已到终态的执行记录：取消请求视为无需再取消，直接跳过并回执成功（幂等）。
_TERMINAL_EXECUTION_STATUSES = frozenset(
    {ExecutionStatus.SUCCEEDED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED}
)


class _SideEffectFailure(Exception):
    """副作用确定性失败：携带可安全落库的固定脱敏文案。"""

    def __init__(self, message: str) -> None:
        """保存固定文案，供 ``fail_side_effect`` 原样落库（不含异常原文）。"""
        super().__init__(message)
        self.message = message


def _worker_id() -> str:
    """返回 1..128 字符的 Worker 标识；优先主机名，缺失时退化为常量。"""
    hostname = socket.gethostname().strip()
    if not hostname:
        return "celery-worker"
    return hostname[:128]


def _parse_uuid(raw: object) -> uuid.UUID | None:
    """把任意入参解析为 UUID；无法解析时返回 ``None``，由调用方决定失败关闭文案。"""
    if not raw:
        return None
    try:
        return uuid.UUID(str(raw))
    except (ValueError, AttributeError):
        return None


async def _claim_committed(worker_id: str) -> dict[str, Any] | None:
    """在独立会话/事务里领取一条已 Commit 的副作用命令并占用执行租约。"""
    async with SessionFactory() as session:
        async with session.begin():
            return await claim_committed_side_effect(session, worker_id)


async def _complete(command_id: str, result: dict[str, Any]) -> None:
    """用新的会话/事务回执成功；绝不复用领取阶段已关闭的会话。"""
    async with SessionFactory() as session:
        async with session.begin():
            await complete_side_effect(session, command_id, result)


async def _fail(command_id: str, message: str) -> None:
    """用新的会话/事务回执失败；文案已脱敏，重复回执幂等。"""
    async with SessionFactory() as session:
        async with session.begin():
            await fail_side_effect(session, command_id, message)


async def _safe_complete(command_id: str, result: dict[str, Any]) -> bool:
    """回执成功；回执本身失败时返回 ``False`` 而不抛出，交由调用方兜底。"""
    try:
        await _complete(command_id, result)
    except Exception:
        return False
    return True


async def _safe_fail(command_id: str, message: str) -> None:
    """回执失败；回执本身失败时静默吞掉，保证任务永不抛出。"""
    try:
        await _fail(command_id, message)
    except Exception:
        return


async def _cancel_execution(claimed: dict[str, Any]) -> dict[str, Any]:
    """真正执行 CANCEL_EXECUTION：把目标 ExecutionRun 推进到 CANCELLED。

    目标 run 优先取 ``command_payload["run_id"]``，缺失时退化为该操作的
    ``aggregate_id``；已到终态的 run 直接跳过（幂等），无法定位或非法转换则失败关闭。
    """
    payload = claimed.get("command_payload")
    payload = payload if isinstance(payload, dict) else {}
    run_id = _parse_uuid(payload.get("run_id") or claimed.get("aggregate_id"))
    if run_id is None:
        raise _SideEffectFailure(_CANCEL_TARGET_MISSING_MESSAGE)
    async with SessionFactory() as session:
        async with session.begin():
            run = await session.get(ExecutionRun, run_id)
            if run is None:
                raise _SideEffectFailure(_CANCEL_TARGET_MISSING_MESSAGE)
            if run.status in _TERMINAL_EXECUTION_STATUSES:
                # 已到终态：不再尝试取消，按"无需再取消"回执成功。
                skipped = True
            else:
                try:
                    await ExecutionService().mark_cancelled(session, run_id)
                except DomainError as exc:
                    raise _SideEffectFailure(_CANCEL_ILLEGAL_TRANSITION_MESSAGE) from exc
                skipped = False
    return {"cancelled_run_id": str(run_id), "cancellation_skipped": skipped}


async def _execute_claimed(claimed: dict[str, Any]) -> dict[str, Any]:
    """按 ``operation`` 确定性执行；需要外部系统的操作一律失败关闭。

    9 种操作里只有 ``CANCEL_EXECUTION`` 属控制面动作可真正执行；其余 8 种
    （ROLLBACK/CLEANUP_DORIS、MIGRATE/STAGE/VALIDATE/ACTIVATE/ROLLBACK/DESTROY_SECRET）
    都依赖本机没有的外部系统，连同任何未知 operation 一起失败关闭。
    """
    raw_operation = str(claimed.get("operation") or "")
    if raw_operation == SideEffectOperation.CANCEL_EXECUTION.value:
        return await _cancel_execution(claimed)
    raise _SideEffectFailure(_EXTERNAL_UNAVAILABLE_MESSAGE)


async def _execute_side_effect(command_id: str) -> dict[str, Any]:
    """副作用任务主体：领取 → 执行 → 用新会话回执；任何异常都兜底不崩。

    Args:
        command_id: 触发本次执行的 Outbox 命令 ID；实际领取由
            ``claim_committed_side_effect`` 选取最早的已提交命令并加执行租约。
    """
    claimed = await _claim_or_none(_worker_id())
    if isinstance(claimed, str):
        # 领取阶段失败：还没有可回执的命令，只报告失败不抛出。
        return {"status": "FAILED", "reason": claimed}
    if claimed is None:
        return {"status": "NOOP", "reason": "当前没有可领取的已提交副作用命令"}
    raw_command_id = claimed.get("command_id")
    if not raw_command_id:
        return {"status": "NOOP", "reason": "副作用命令缺少可回执的 command_id，未执行"}
    command = str(raw_command_id)
    try:
        result = await _execute_claimed(claimed)
    except _SideEffectFailure as failure:
        await _safe_fail(command, failure.message)
        return {"status": "FAILED", "command_id": command, "reason": failure.message}
    except Exception:
        await _safe_fail(command, _UNEXPECTED_MESSAGE)
        return {"status": "FAILED", "command_id": command, "reason": _UNEXPECTED_MESSAGE}
    if not await _safe_complete(command, result):
        await _safe_fail(command, _UNEXPECTED_MESSAGE)
        return {"status": "FAILED", "command_id": command, "reason": _UNEXPECTED_MESSAGE}
    return {"status": "SUCCEEDED", "command_id": command, **result}


async def _claim_or_none(worker_id: str) -> dict[str, Any] | str | None:
    """领取命令；成功返回领取结果，无命令返回 ``None``，领取失败返回固定文案。"""
    try:
        return await _claim_committed(worker_id)
    except Exception:
        return _UNEXPECTED_MESSAGE


@celery_app.task(name="etl_agent.execute_side_effect")  # type: ignore[untyped-decorator]
def execute_side_effect(command_id: str) -> dict[str, Any]:
    """由 Celery 执行一次副作用命令；参数为 Outbox 命令 ID，任务自身绝不抛出。"""
    return run_async(_execute_side_effect(command_id))
