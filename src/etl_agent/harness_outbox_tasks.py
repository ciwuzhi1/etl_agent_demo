"""Harness Outbox Celery 任务 — 可靠命令投递。

第 11 章：从 `harness_command_outbox` 表读取待处理命令，分发到对应的 Worker 任务执行。

可靠投递的最小闭环：

1. `scan_outbox` 用 ``SELECT ... FOR UPDATE SKIP LOCKED`` 认领到期命令，把状态从
   PENDING 推到 SENDING 并记租约（`locked_at`）。SKIP LOCKED 让多个 Beat/Worker 并发
   扫描时各拿各的行，不会互相阻塞或重复认领。
2. `dispatch_outbox_command` 读命令、按 `target`/`command_payload` 分发；成功置
   SENT + sent_at，失败置 FAILED + 脱敏 last_error 并 attempts += 1（等待下次重投）。
3. `recover_stale_leases` 把租约超时仍停在 SENDING 的行复原为 PENDING，覆盖
   "分发进程崩溃"这一窗口——至少一次投递，绝不丢命令。
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from etl_agent.application.execution_diagnostics import redact_execution_error
from etl_agent.application.harness_outbox import HarnessOutboxService
from etl_agent.asyncio_compat import run_async
from etl_agent.domain.errors import DomainError
from etl_agent.domain.harness import HarnessOutboxStatus
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.models import HarnessCommandOutbox
from etl_agent.worker import celery_app

# 命令类型到 Celery 任务的映射（作为 target 未命中时的兜底路由）。
COMMAND_TYPE_TO_TASK: dict[str, str] = {
    "EXECUTION_SUBMIT": "etl_agent.submit_execution",
    "EXECUTION_MONITOR": "etl_agent.monitor_execution",
    "SIDE_EFFECT_EXECUTE": "etl_agent.execute_side_effect",
}

# 目标标识到 Celery 任务的映射：target 是命令的显式路由键，优先命中。
_TARGET_TASKS: dict[str, str] = {
    "worker.execution": "etl_agent.submit_execution",
    "worker.execution.monitor": "etl_agent.monitor_execution",
}

# 副作用命令的 target 前缀：commit 写入 `worker.side_effect.<operation 小写>`
# （9 种操作各有后缀），因此用前缀匹配统一路由到同一个副作用执行任务。
_SIDE_EFFECT_TARGET_PREFIX = "worker.side_effect."
_SIDE_EFFECT_TASK_NAME = "etl_agent.execute_side_effect"

_DEFAULT_SCAN_LIMIT = 50


def _resolve_task_name(target: str, payload: dict[str, Any]) -> str:
    """按 target 前缀/精确匹配优先、command_type 兜底解析任务名；无法解析报领域错误。"""
    if target.startswith(_SIDE_EFFECT_TARGET_PREFIX):
        return _SIDE_EFFECT_TASK_NAME
    if target in _TARGET_TASKS:
        return _TARGET_TASKS[target]
    command_type = str(payload.get("command_type") or "").strip().upper()
    task_name = COMMAND_TYPE_TO_TASK.get(command_type)
    if not task_name:
        raise DomainError("Outbox 命令目标未注册对应任务")
    return task_name


def _task_argument(task_name: str, payload: dict[str, Any], command_id: uuid.UUID) -> str:
    """按目标任务签名挑选唯一入参：执行任务取 run_id，副作用任务取命令 ID。

    副作用命令的 payload（``side_effect_preparation_id``/``operation``/``aggregate_id``/
    ``command_payload``/``token_id``/``trace_id``）里没有单独的命令 ID 字段，命令身份就是
    Outbox 行自身的 ``id`` —— 这正是 commit 时写入 ``capability grant.execution_command_id``
    的那个值，也是 ``complete_side_effect``/``fail_side_effect`` 用来回执的 command_id。
    若 payload 里显式带了 ``command_id`` 则优先使用它。
    """
    if task_name == _SIDE_EFFECT_TASK_NAME:
        raw_command_id = payload.get("command_id")
        return str(raw_command_id) if raw_command_id else str(command_id)
    execution_run_id = payload.get("execution_run_id")
    if not execution_run_id:
        raise DomainError("Outbox 命令缺少执行记录标识")
    return str(execution_run_id)


def _dispatch_to_task(target: str, payload: dict[str, Any], command_id: uuid.UUID) -> None:
    """把一条命令投递到目标 Celery 任务；每个任务只接一个身份参数，拒绝整包透传。

    按解析出的任务名决定参数：``etl_agent.submit_execution``/``etl_agent.monitor_execution``
    只接 ExecutionRun UUID，副作用任务 ``etl_agent.execute_side_effect`` 只接命令 ID，
    避免把整份 payload 作为 kwargs 透传给不匹配的签名。
    """
    task_name = _resolve_task_name(target, payload)
    celery_app.send_task(task_name, args=[_task_argument(task_name, payload, command_id)])


async def _claim_pending(
    session: Any,
    *,
    limit: int = _DEFAULT_SCAN_LIMIT,
) -> list[HarnessCommandOutbox]:
    """认领 `status=PENDING 且 available_at<=now` 的命令：置 SENDING 并记租约。"""
    now = datetime.now(UTC)
    rows = list(
        (
            await session.scalars(
                select(HarnessCommandOutbox)
                .where(
                    HarnessCommandOutbox.status == HarnessOutboxStatus.PENDING,
                    HarnessCommandOutbox.available_at <= now,
                )
                .order_by(HarnessCommandOutbox.available_at, HarnessCommandOutbox.created_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).all()
    )
    for row in rows:
        row.status = HarnessOutboxStatus.SENDING
        row.locked_at = now
    await session.flush()
    return rows


async def _scan_outbox() -> int:
    """认领一批到期命令并逐条分发，返回本轮认领数量。"""
    async with SessionFactory() as session:
        async with session.begin():
            claimed = await _claim_pending(session)
            command_ids = [row.id for row in claimed]
    for command_id in command_ids:
        dispatch_outbox_command.delay(str(command_id))
    return len(command_ids)


async def _dispatch_outbox_command(command_id: uuid.UUID) -> None:
    """读命令 → 分发 → 成功置 SENT，失败置 FAILED（脱敏）并累加 attempts。"""
    async with SessionFactory() as session:
        row = await session.get(HarnessCommandOutbox, command_id, with_for_update=True)
        if row is None or row.status is HarnessOutboxStatus.SENT:
            return
        target = row.target
        payload = dict(row.command_payload or {})

    try:
        _dispatch_to_task(target, payload, command_id)
    except Exception as exc:
        error = redact_execution_error(str(exc)) or "Outbox 命令分发失败"
        async with SessionFactory() as session:
            async with session.begin():
                # 失败即累加尝试次数（mark_failed 的退避延时按 attempts 计算），
                # 同时用脱敏文案覆盖 last_error，绝不落异常原文。
                row = await session.get(HarnessCommandOutbox, command_id, with_for_update=True)
                if row is not None and row.status is not HarnessOutboxStatus.SENT:
                    row.attempts += 1
                await HarnessOutboxService().mark_failed(session, outbox_id=command_id, error=error)
        return

    async with SessionFactory() as session:
        async with session.begin():
            await HarnessOutboxService().mark_sent(session, outbox_id=command_id)


async def _recover_stale_leases(lease_timeout_seconds: int) -> int:
    """把租约超时的 SENDING 行复原为 PENDING，返回复原数量。"""
    timeout = max(int(lease_timeout_seconds), 0)
    cutoff = datetime.now(UTC) - timedelta(seconds=timeout)
    async with SessionFactory() as session:
        async with session.begin():
            rows = (
                await session.scalars(
                    select(HarnessCommandOutbox).where(
                        HarnessCommandOutbox.status == HarnessOutboxStatus.SENDING,
                        HarnessCommandOutbox.locked_at.is_not(None),
                        HarnessCommandOutbox.locked_at <= cutoff,
                    )
                )
            ).all()
            recovered = 0
            for row in rows:
                row.status = HarnessOutboxStatus.PENDING
                row.locked_at = None
                recovered += 1
            await session.flush()
    return recovered


@celery_app.task(name="harness.scan-outbox")  # type: ignore[untyped-decorator]
def scan_outbox() -> int:
    """Beat 周期任务：认领到期 Outbox 命令并逐条分发，返回本轮认领数量。"""
    return run_async(_scan_outbox())


@celery_app.task(name="harness.dispatch-outbox-command")  # type: ignore[untyped-decorator]
def dispatch_outbox_command(command_id: str) -> None:
    """从 outbox 读取命令并分发到目标 Worker 任务。

    Args:
        command_id: HarnessCommandOutbox 记录的 ID
    """
    run_async(_dispatch_outbox_command(uuid.UUID(command_id)))


@celery_app.task(name="harness.recover-stale-leases")  # type: ignore[untyped-decorator]
def recover_stale_leases(lease_timeout_seconds: int = 300) -> int:
    """恢复超时的租约（处理崩溃的 Worker）。

    Args:
        lease_timeout_seconds: 租约超时阈值（默认 300 秒）

    Returns:
        恢复的命令数量
    """
    return run_async(_recover_stale_leases(lease_timeout_seconds))
