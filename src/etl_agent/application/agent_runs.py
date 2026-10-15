"""AgentRun 业务服务：管理生成任务的生命周期。

第 8 章核心：将自然语言需求转化为 HOCON 配置，并通过 LangGraph 状态机管理
从 QUEUED → RUNNING → WAITING_INPUT/WAITING_APPROVAL → COMPLETED/FAILED 的完整流程。
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.generation import PipelineGenerationPort
from etl_agent.domain.enums import AgentRunStatus
from etl_agent.domain.errors import DomainError
from etl_agent.domain.state_machine import require_agent_run_transition
from etl_agent.infrastructure.agent_run_model import AgentRun


class AgentRunService:
    """管理 AgentRun 生命周期。"""

    def __init__(self, generation_port: PipelineGenerationPort) -> None:
        self._generation_port = generation_port

    async def start(
        self,
        session: AsyncSession,
        project_id: uuid.UUID,
        requirement: str,
        metadata_context: dict[str, Any],
        created_by: uuid.UUID,
    ) -> AgentRun:
        """创建新的 AgentRun 记录（初始状态为 QUEUED）。"""
        run = AgentRun(
            project_id=project_id,
            status=AgentRunStatus.QUEUED,
            requirement=requirement,
            metadata_context=metadata_context,
            created_by=created_by,
        )
        session.add(run)
        await session.flush()
        return run

    async def mark_running(self, session: AsyncSession, run_id: uuid.UUID) -> AgentRun:
        """将 AgentRun 从 QUEUED 推进到 RUNNING 状态。"""
        run = await self._get_for_update(session, run_id)
        if run.status is AgentRunStatus.RUNNING:
            return run
        require_agent_run_transition(run.status, AgentRunStatus.RUNNING)
        run.status = AgentRunStatus.RUNNING
        run.started_at = datetime.now(UTC)
        await session.flush()
        return run

    async def wait_for_approval(
        self, session: AsyncSession, run_id: uuid.UUID, candidate_hocon: str
    ) -> AgentRun:
        """将 AgentRun 从 RUNNING 推进到 WAITING_APPROVAL 状态，等待人工审批。"""
        run = await self._get_for_update(session, run_id)
        require_agent_run_transition(run.status, AgentRunStatus.WAITING_APPROVAL)
        run.status = AgentRunStatus.WAITING_APPROVAL
        run.candidate_hocon = candidate_hocon
        await session.flush()
        return run

    async def fail(self, session: AsyncSession, run_id: uuid.UUID, error_message: str) -> AgentRun:
        """标记 AgentRun 为 FAILED 状态（可从任何非终态转换）。"""
        run = await self._get_for_update(session, run_id)
        require_agent_run_transition(run.status, AgentRunStatus.FAILED)
        run.status = AgentRunStatus.FAILED
        run.error_message = error_message
        run.failed_at = datetime.now(UTC)
        await session.flush()
        return run

    async def get_run(self, session: AsyncSession, run_id: uuid.UUID) -> AgentRun:
        """查询 AgentRun。"""
        result = await session.execute(select(AgentRun).where(AgentRun.id == run_id))
        run = result.scalar_one_or_none()
        if not run:
            raise DomainError(f"AgentRun {run_id} 不存在")
        return run

    async def _get_for_update(self, session: AsyncSession, run_id: uuid.UUID) -> AgentRun:
        """带行锁查询 AgentRun，防止并发修改。"""
        result = await session.execute(
            select(AgentRun).where(AgentRun.id == run_id).with_for_update()
        )
        run = result.scalar_one_or_none()
        if not run:
            raise DomainError(f"AgentRun {run_id} 不存在")
        return run
