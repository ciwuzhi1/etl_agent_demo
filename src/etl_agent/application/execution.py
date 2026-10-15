"""ExecutionService：管理 ExecutionRun 的生命周期状态转换。

第 10 章核心：负责执行记录从 QUEUED → SUBMITTED → RUNNING → SUCCEEDED/FAILED/CANCELLED
的状态流转，并记录 SeaTunnel JobId、指标、错误诊断等关键信息。

注意：当前使用 ExecutionRun 模型（来自 infrastructure.models），该模型已在 models.py 中定义。
如果后续需要扩展 ExecutionRun 字段（如 source_rows/sink_rows 等指标字段），需同步更新模型和迁移。
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.runtime_supervision import (
    RuntimeSupervisionService,
    build_snapshot_row,
    next_sequence_and_previous_hash,
)
from etl_agent.domain.enums import ExecutionStatus
from etl_agent.domain.errors import DomainError
from etl_agent.domain.state_machine import require_execution_transition
from etl_agent.domain.supervision import RuntimeSupervisionDecision
from etl_agent.infrastructure.models import (
    ExecutionRun,
    RuntimeSupervisionSnapshot,
)


class ExecutionService:
    """管理 ExecutionRun 生命周期状态转换的服务。

    所有状态变更方法都遵循以下模式：
    1. 使用 SELECT FOR UPDATE 获取行锁，防止并发修改
    2. 校验状态转换的合法性（通过 state_machine.require_execution_transition）
    3. 更新状态和相关字段
    4. flush 到数据库（不 commit，由调用方决定事务边界）
    """

    async def mark_submitted(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
        seatunnel_job_id: str,
    ) -> None:
        """将执行从 QUEUED 推进到 SUBMITTED 状态，并记录 SeaTunnel JobId。

        Args:
            session: 数据库会话
            run_id: 执行记录 ID
            seatunnel_job_id: SeaTunnel 引擎返回的作业 ID

        Raises:
            DomainError: 如果状态转换非法或记录不存在
        """
        run = await self._get_for_update(session, run_id)

        # 幂等性检查：如果已经是 SUBMITTED 且 JobId 相同，直接返回
        if run.status is ExecutionStatus.SUBMITTED and run.seatunnel_job_id == seatunnel_job_id:
            return

        require_execution_transition(run.status, ExecutionStatus.SUBMITTED)
        run.status = ExecutionStatus.SUBMITTED
        run.seatunnel_job_id = seatunnel_job_id
        run.submitted_at = run.submitted_at or datetime.now(UTC)
        await session.flush()

    async def mark_running(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
    ) -> None:
        """将执行从 SUBMITTED 推进到 RUNNING 状态，并设置开始时间。

        Args:
            session: 数据库会话
            run_id: 执行记录 ID

        Raises:
            DomainError: 如果状态转换非法或记录不存在
        """
        run = await self._get_for_update(session, run_id)

        # 幂等性检查：如果已经在运行中或更后的状态，直接返回
        if run.status in {
            ExecutionStatus.RUNNING,
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }:
            return

        require_execution_transition(run.status, ExecutionStatus.RUNNING)
        run.status = ExecutionStatus.RUNNING
        run.started_at = run.started_at or datetime.now(UTC)
        await session.flush()

    async def mark_succeeded(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
        source_rows: int,
        sink_rows: int,
        source_bytes: int,
        sink_bytes: int,
    ) -> None:
        """将执行从 RUNNING 推进到 SUCCEEDED 状态，并记录指标数据。

        Args:
            session: 数据库会话
            run_id: 执行记录 ID
            source_rows: 源端读取的行数
            sink_rows: 目标端写入的行数
            source_bytes: 源端读取的字节数
            sink_bytes: 目标端写入的字节数

        Raises:
            DomainError: 如果状态转换非法或记录不存在

        Note:
            指标字段已在 ExecutionRun 模型中定义（第 10 章）。
        """
        run = await self._get_for_update(session, run_id)

        # 如果已经成功，直接返回（幂等性）
        if run.status is ExecutionStatus.SUCCEEDED:
            return

        # 如果还在 SUBMITTED 状态，先推进到 RUNNING
        if run.status is ExecutionStatus.SUBMITTED:
            await self.mark_running(session, run_id)
            run = await self._get_for_update(session, run_id)

        require_execution_transition(run.status, ExecutionStatus.SUCCEEDED)
        run.status = ExecutionStatus.SUCCEEDED
        run.finished_at = run.finished_at or datetime.now(UTC)
        run.source_rows = source_rows
        run.sink_rows = sink_rows
        run.source_bytes = source_bytes
        run.sink_bytes = sink_bytes
        await session.flush()

    async def mark_failed(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
        error_message: str,
        failure_category: str | None = None,
        failure_stage: str | None = None,
        retryable: bool = True,
    ) -> None:
        """将执行从 RUNNING 推进到 FAILED 状态，并记录错误详情。

        Args:
            session: 数据库会话
            run_id: 执行记录 ID
            error_message: 错误消息（应已脱敏）
            failure_category: 失败分类（如 CONFIGURATION/CREDENTIALS/CONNECTIVITY 等）
            failure_stage: 失败阶段（如 SUBMISSION/SOURCE/TRANSFORM/SINK 等）
            retryable: 是否可重试

        Raises:
            DomainError: 如果状态转换非法或记录不存在
        """
        run = await self._get_for_update(session, run_id)

        # 如果已经是终态（FAILED/SUCCEEDED/CANCELLED），直接返回（防止迟到错误覆盖终态）
        if run.status in {
            ExecutionStatus.FAILED,
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.CANCELLED,
        }:
            return

        require_execution_transition(run.status, ExecutionStatus.FAILED)
        run.status = ExecutionStatus.FAILED
        run.error = error_message
        run.failure_category = failure_category
        run.failure_stage = failure_stage
        run.retryable = retryable
        run.failed_at = run.failed_at or datetime.now(UTC)
        await session.flush()

    async def mark_cancelled(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
    ) -> None:
        """将执行从 RUNNING 或 CANCELLING 推进到 CANCELLED 状态。

        Args:
            session: 数据库会话
            run_id: 执行记录 ID

        Raises:
            DomainError: 如果状态转换非法或记录不存在
        """
        run = await self._get_for_update(session, run_id)

        # 如果已经取消，直接返回（幂等性）
        if run.status is ExecutionStatus.CANCELLED:
            return

        # 只允许从 RUNNING 或 CANCELLING 转换到 CANCELLED
        if run.status not in {ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING}:
            raise DomainError(
                f"只有 RUNNING 或 CANCELLING 状态的执行可以取消，当前状态: {run.status.value}"
            )

        require_execution_transition(run.status, ExecutionStatus.CANCELLED)
        run.status = ExecutionStatus.CANCELLED
        run.cancelled_at = run.cancelled_at or datetime.now(UTC)
        await session.flush()

    async def record_supervision(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
        decision: RuntimeSupervisionDecision,
        *,
        observed_at: datetime | None = None,
    ) -> RuntimeSupervisionSnapshot:
        """把一次监督决定落库为哈希链快照，并把最新动作回写到运行记录。

        执行流程在每次引擎轮询后调用本方法：它复用 `build_snapshot_row` 保证哈希链
        口径与 `RuntimeSupervisionService.observe` 一致，并更新 ExecutionRun 的
        `supervision_action`；强动作（block_publish）同时置隔离标记，供发布门禁阻断。
        """
        run = await self._get_for_update(session, run_id)
        timestamp = observed_at or datetime.now(UTC)
        sequence, previous_hash = await next_sequence_and_previous_hash(session, run_id)
        snapshot = build_snapshot_row(
            decision,
            run_id=run_id,
            sequence=sequence,
            previous_hash=previous_hash,
            observed_at=timestamp,
        )
        session.add(snapshot)
        run.supervision_action = decision.action.value
        if decision.contract_sha256:
            run.supervision_contract_sha256 = decision.contract_sha256
        if decision.block_publish:
            run.quarantined_at = run.quarantined_at or timestamp
            run.quarantine_reason = "; ".join(match.reason for match in decision.matches)[:2000]
        await session.flush()
        return snapshot

    async def assert_publish_allowed(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
    ) -> None:
        """原子发布前的监督门禁：委托 `RuntimeSupervisionService` 阻断被隔离的运行。"""
        await RuntimeSupervisionService().require_publish_allowed(session, run_id)

    async def _get_for_update(self, session: AsyncSession, run_id: uuid.UUID) -> ExecutionRun:
        """带行锁查询 ExecutionRun，防止并发修改。

        Args:
            session: 数据库会话
            run_id: 执行记录 ID

        Returns:
            ExecutionRun: 执行记录对象

        Raises:
            DomainError: 如果记录不存在
        """
        result = await session.execute(
            select(ExecutionRun).where(ExecutionRun.id == run_id).with_for_update()
        )
        run = result.scalar_one_or_none()
        if not run:
            raise DomainError(f"ExecutionRun {run_id} 不存在")
        return run
