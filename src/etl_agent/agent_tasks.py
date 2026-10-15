"""Agent 生成任务：把自然语言需求经 LangGraph 状态机转化为候选 HOCON。

第 8 章核心落库入口。任务只接 AgentRun UUID，读取脱敏上下文后在 Worker 内跑图：

- 生成端口由 `infrastructure/generation_factory.create_generation_adapter()` 提供，
  切换后端不改变本任务的语义。
- 图的 `clarify` / `wait_for_review` 节点靠 `interrupt` 暂停，而 `interrupt` 必须有
  Checkpoint 才能工作；因此这里优先用第 8 章 PostgreSQL Checkpoint，未实现时容错
  退回 `MemorySaver`，保证任务真正能跑起来而不是崩在基础设施未就绪上。
- 写回纪律：`candidate_hocon` 只在门禁通过、进入人工审批时落库；失败只写固定脱敏
  文案，`generation_*` 统计只保留次数/耗时/非敏感诊断，不保存 Prompt、模型正文或凭据。
"""

import time
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.agent.graph import build_generation_graph
from etl_agent.asyncio_compat import run_async
from etl_agent.config import Settings, get_settings
from etl_agent.domain.enums import AgentRunStatus
from etl_agent.domain.errors import DomainError
from etl_agent.domain.state_machine import require_agent_run_transition
from etl_agent.infrastructure.agent_run_model import AgentRun
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.generation_factory import create_generation_adapter
from etl_agent.worker import celery_app

# 失败落库的固定脱敏文案：绝不把异常原文、模型正文、门店路径或凭据写进 agent_runs。
_GENERATION_FAILURE_MESSAGE = "生成任务未通过确定性门禁或未完成，请结合生成诊断人工排查。"
_TERMINAL_AGENT_STATUSES = {
    AgentRunStatus.WAITING_APPROVAL,
    AgentRunStatus.WAITING_INPUT,
    AgentRunStatus.FAILED,
}


def _create_checkpointer(settings: Settings) -> Any | None:
    """优先用 PostgreSQL Checkpoint 后端；未实现/不可用时退回内存 Checkpoint。

    `interrupt` 依赖 Checkpoint 才能暂停图；缺失 saver 时图会在审批/澄清节点直接
    抛错。`NotImplementedError` 是当前 `agent/checkpoint.py` 的既定事实，因此这里
    容错退回 `MemorySaver`——单进程内可正常暂停与恢复，足够让生成任务真正跑起来。
    """
    try:
        from etl_agent.agent.checkpoint import create_checkpoint_saver

        saver = create_checkpoint_saver(settings.checkpoint_database_url)
        if saver is not None:
            return saver
    except NotImplementedError:
        pass
    except Exception:
        pass
    try:
        from langgraph.checkpoint.memory import MemorySaver

        return MemorySaver()
    except Exception:
        return None


def _safe_generation_diagnostics(generator: Any) -> dict[str, Any]:
    """读取生成适配器的非敏感诊断摘要；适配器不支持或抛错时返回空字典。"""
    method = getattr(generator, "generation_diagnostics", None)
    if not callable(method):
        return {}
    try:
        value = method()
    except Exception:
        return {}
    return dict(value) if isinstance(value, dict) else {}


async def _load_for_update(session: AsyncSession, run_id: uuid.UUID) -> AgentRun:
    """带行锁读取 AgentRun，缺失时抛领域错误。"""
    run = await session.scalar(select(AgentRun).where(AgentRun.id == run_id).with_for_update())
    if run is None:
        raise DomainError(f"AgentRun {run_id} 不存在")
    return run


async def _write_outcome(
    run_id: uuid.UUID,
    *,
    status: AgentRunStatus,
    candidate_hocon: str | None,
    error_message: str | None,
    settings: Settings,
    generator: Any,
    generation: dict[str, Any],
    repairs: int,
    phase: str,
    generation_started_at: datetime,
    elapsed_ms: int,
    diagnostics_extra: dict[str, Any] | None = None,
    assistant_question: str | None = None,
) -> None:
    """把生成结果、终态与 `generation_*` 统计写回 agent_runs。

    第一个事务已把状态推进到 RUNNING，因此这里只在 RUNNING 上前进；若状态已不是
    RUNNING（并发重复投递或人工介入），静默跳过，避免非法状态转换把任务打崩。

    ``assistant_question``：进入 WAITING_INPUT 时，把模型提出的澄清问题追加进
    ``metadata_context["conversation"]``，与用户答案一起构成可恢复的问答上下文。
    """
    diagnostics = _safe_generation_diagnostics(generator)
    if diagnostics_extra:
        diagnostics.update(diagnostics_extra)
    async with SessionFactory() as session:
        async with session.begin():
            run = await _load_for_update(session, run_id)
            # 注意：agent_runs.status 是纯 String 列，读回来是普通字符串，必须用
            # 相等比较（StrEnum 与同值字符串相等），不能用 `is`。
            if run.status != AgentRunStatus.RUNNING:
                return
            require_agent_run_transition(run.status, status)
            run.status = status
            run.generation_backend = settings.generation_backend
            run.generation_model = settings.llm_model
            upstream_version = generation.get("upstream_version")
            run.generation_upstream_version = str(upstream_version) if upstream_version else None
            run.generation_provider_calls = int(diagnostics.get("provider_calls") or 0)
            run.generation_repairs = repairs
            run.generation_candidates = repairs + 1
            run.generation_elapsed_ms = elapsed_ms
            run.generation_phase = phase or None
            run.generation_diagnostics = diagnostics
            run.generation_started_at = generation_started_at
            run.generation_finished_at = datetime.now(UTC)
            if status is AgentRunStatus.WAITING_APPROVAL:
                run.candidate_hocon = candidate_hocon
            elif status is AgentRunStatus.WAITING_INPUT:
                if assistant_question:
                    metadata_context = dict(run.metadata_context or {})
                    conversation = list(metadata_context.get("conversation", []))
                    conversation.append(
                        {
                            "role": "assistant",
                            "content": [{"type": "text", "text": assistant_question}],
                        }
                    )
                    metadata_context["conversation"] = conversation
                    # JSON 列只在整体赋值时才被 SQLAlchemy 记为变更，必须重新绑定新 dict。
                    run.metadata_context = metadata_context
            elif status is AgentRunStatus.FAILED:
                run.error_message = error_message or _GENERATION_FAILURE_MESSAGE
                run.failed_at = datetime.now(UTC)
            await session.flush()


async def _run_agent_generation(run_id: uuid.UUID) -> dict[str, object]:
    """读取 AgentRun → 构建并运行生成图 → 按结果推进状态并写回统计。"""
    settings = get_settings()
    async with SessionFactory() as session:
        async with session.begin():
            run = await _load_for_update(session, run_id)
            if run.status in _TERMINAL_AGENT_STATUSES:
                # 已处于终态：重复投递直接返回，不做任何状态变更。
                return {"run_id": str(run_id), "status": str(run.status)}
            require_agent_run_transition(run.status, AgentRunStatus.RUNNING)
            run.status = AgentRunStatus.RUNNING
            run.started_at = run.started_at or datetime.now(UTC)
            requirement = run.requirement
            metadata_context = dict(run.metadata_context or {})
            thread_id = str(run.thread_id)
            generation_started_at = datetime.now(UTC)

    generator = create_generation_adapter()
    checkpointer = _create_checkpointer(settings)
    graph = build_generation_graph(generator, checkpointer)

    started = time.perf_counter()
    try:
        # 会话历史从 metadata_context 读取：澄清恢复端点会把"模型追问 + 用户回答"
        # 追加进 metadata_context["conversation"]，重新投递后模型因此能接着往下生成。
        state: dict[str, Any] = await graph.ainvoke(
            {
                "requirement": requirement,
                "metadata_context": metadata_context,
                "conversation_history": list(metadata_context.get("conversation", [])),
            },
            config={"configurable": {"thread_id": thread_id}},
        )
    except Exception:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        await _write_outcome(
            run_id,
            status=AgentRunStatus.FAILED,
            candidate_hocon=None,
            error_message=_GENERATION_FAILURE_MESSAGE,
            settings=settings,
            generator=generator,
            generation={},
            repairs=0,
            phase="failed",
            generation_started_at=generation_started_at,
            elapsed_ms=elapsed_ms,
            diagnostics_extra={"outcome": "pipeline_error"},
        )
        return {"run_id": str(run_id), "status": AgentRunStatus.FAILED.value}

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    generation: dict[str, Any] = dict(state.get("generation") or {})
    phase = str(state.get("phase") or "")
    repairs = int(state.get("repair_count") or 0)
    hocon = generation.get("hocon")

    if phase == "waiting_enterprise_approval" and isinstance(hocon, str) and hocon:
        await _write_outcome(
            run_id,
            status=AgentRunStatus.WAITING_APPROVAL,
            candidate_hocon=hocon,
            error_message=None,
            settings=settings,
            generator=generator,
            generation=generation,
            repairs=repairs,
            phase=phase,
            generation_started_at=generation_started_at,
            elapsed_ms=elapsed_ms,
            diagnostics_extra={"outcome": "waiting_approval"},
        )
        return {"run_id": str(run_id), "status": AgentRunStatus.WAITING_APPROVAL.value}

    if phase == "question":
        # 澄清问题本身照旧不当作模型正文暴露给统计，但要追加进会话历史：否则人补了答案
        # 再重跑时，模型看不到自己问过什么，无法把回答接回上下文。
        question = generation.get("question")
        await _write_outcome(
            run_id,
            status=AgentRunStatus.WAITING_INPUT,
            candidate_hocon=None,
            error_message=None,
            settings=settings,
            generator=generator,
            generation=generation,
            repairs=repairs,
            phase=phase,
            generation_started_at=generation_started_at,
            elapsed_ms=elapsed_ms,
            diagnostics_extra={"outcome": "waiting_input"},
            assistant_question=question if isinstance(question, str) and question else None,
        )
        return {"run_id": str(run_id), "status": AgentRunStatus.WAITING_INPUT.value}

    await _write_outcome(
        run_id,
        status=AgentRunStatus.FAILED,
        candidate_hocon=None,
        error_message=_GENERATION_FAILURE_MESSAGE,
        settings=settings,
        generator=generator,
        generation=generation,
        repairs=repairs,
        phase=phase or "failed",
        generation_started_at=generation_started_at,
        elapsed_ms=elapsed_ms,
        diagnostics_extra={"outcome": "rejected"},
    )
    return {"run_id": str(run_id), "status": AgentRunStatus.FAILED.value}


@celery_app.task(name="etl_agent.run_agent_generation")  # type: ignore[untyped-decorator]
def run_agent_generation(run_id: str) -> dict[str, object]:
    """由 Celery 执行一次 Agent 生成，任务参数只携带 AgentRun UUID。"""
    return run_async(_run_agent_generation(uuid.UUID(run_id)))
