import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.domain.enums import (
    ApprovalDecisionValue,
    ApprovalStatus,
    PipelineVersionStatus,
    ProjectRole,
)
from etl_agent.domain.errors import PermissionDenied, ResourceNotFound, VersionConflict
from etl_agent.domain.state_machine import require_pipeline_transition
from etl_agent.infrastructure.models import (
    ApprovalRequest,
    AuditEvent,
    Pipeline,
    PipelineVersion,
    ProjectMembership,
)


class ApprovalService:
    async def latest_for_version(
        self,
        session: AsyncSession,
        *,
        version_id: uuid.UUID,
        actor_id: uuid.UUID,
    ) -> ApprovalRequest | None:
        """读取版本最近一张审批单，支持页面刷新后恢复审批上下文。"""
        version = await session.get(PipelineVersion, version_id)
        if version is None:
            raise ResourceNotFound("Pipeline 版本不存在")
        project_id = await self._project_id(session, version.pipeline_id)
        await self._require_role(
            session,
            project_id,
            actor_id,
            {
                ProjectRole.ADMIN,
                ProjectRole.DATA_ENGINEER,
                ProjectRole.APPROVER,
                ProjectRole.VIEWER,
            },
        )
        approval: ApprovalRequest | None = await session.scalar(
            select(ApprovalRequest)
            .where(ApprovalRequest.pipeline_version_id == version_id)
            .order_by(ApprovalRequest.created_at.desc(), ApprovalRequest.id.desc())
            .limit(1)
        )
        return approval

    async def submit(
        self,
        session: AsyncSession,
        *,
        version_id: uuid.UUID,
        actor_id: uuid.UUID,
        reason: str | None = None,
        trace_id: str | None = None,
    ) -> ApprovalRequest:
        """在审批应用服务中提交已经冻结的业务对象；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        version = await self._get_version_for_update(session, version_id)
        project_id = await self._project_id(session, version.pipeline_id)
        await self._require_role(
            session,
            project_id,
            actor_id,
            {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER},
        )
        if not version.artifact_sha256:
            raise VersionConflict("版本尚无可审批的确定性产物哈希")

        require_pipeline_transition(version.status, PipelineVersionStatus.WAITING_APPROVAL)
        before = {"status": version.status.value, "lock_version": version.lock_version}
        version.status = PipelineVersionStatus.WAITING_APPROVAL
        version.lock_version += 1

        approval = ApprovalRequest(
            pipeline_version_id=version.id,
            artifact_sha256=version.artifact_sha256,
            requested_by=actor_id,
        )
        session.add_all(
            [
                approval,
                AuditEvent(
                    actor_id=actor_id,
                    project_id=project_id,
                    action="pipeline_version.submitted_for_approval",
                    resource_type="PipelineVersion",
                    resource_id=version.id,
                    before=before,
                    after={
                        "status": version.status.value,
                        "lock_version": version.lock_version,
                        "artifact_sha256": version.artifact_sha256,
                    },
                    reason=reason,
                    trace_id=trace_id,
                ),
            ]
        )
        await session.flush()
        return approval

    async def decide(
        self,
        session: AsyncSession,
        *,
        approval_id: uuid.UUID,
        actor_id: uuid.UUID,
        decision: ApprovalDecisionValue,
        reason: str,
        trace_id: str | None = None,
    ) -> ApprovalRequest:
        """在审批应用服务中执行权限隔离的审批决策；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        statement = (
            select(ApprovalRequest).where(ApprovalRequest.id == approval_id).with_for_update()
        )
        approval = (await session.execute(statement)).scalar_one_or_none()
        if approval is None:
            raise ResourceNotFound("审批单不存在")
        if approval.status is not ApprovalStatus.PENDING:
            raise VersionConflict("审批单已经处理，不能重复决策")
        if approval.requested_by == actor_id:
            raise PermissionDenied("审批人不能与提交人相同，请由独立审批人处理")

        version = await self._get_version_for_update(session, approval.pipeline_version_id)
        project_id = await self._project_id(session, version.pipeline_id)
        await self._require_role(
            session, project_id, actor_id, {ProjectRole.ADMIN, ProjectRole.APPROVER}
        )

        # 审批绑定提交时的产物哈希，防止审核页面打开后配置被静默替换。
        if version.artifact_sha256 != approval.artifact_sha256:
            raise VersionConflict("版本产物已变化，请作废当前审批并重新提交")

        target = (
            PipelineVersionStatus.APPROVED
            if decision is ApprovalDecisionValue.APPROVE
            else PipelineVersionStatus.REJECTED
        )
        require_pipeline_transition(version.status, target)
        before = {"status": version.status.value, "lock_version": version.lock_version}
        version.status = target
        version.lock_version += 1
        approval.status = (
            ApprovalStatus.APPROVED
            if decision is ApprovalDecisionValue.APPROVE
            else ApprovalStatus.REJECTED
        )
        approval.decided_by = actor_id
        approval.reason = reason
        approval.decided_at = datetime.now(UTC)

        session.add(
            AuditEvent(
                actor_id=actor_id,
                project_id=project_id,
                action=f"approval.{approval.status.value.lower()}",
                resource_type="ApprovalRequest",
                resource_id=approval.id,
                before=before,
                after={
                    "status": version.status.value,
                    "lock_version": version.lock_version,
                    "artifact_sha256": version.artifact_sha256,
                },
                reason=reason,
                trace_id=trace_id,
            )
        )
        await session.flush()
        return approval

    @staticmethod
    async def _get_version_for_update(
        session: AsyncSession, version_id: uuid.UUID
    ) -> PipelineVersion:
        """加行锁读取待审批版本，防止审批决策与版本变化并发穿透。"""
        statement = (
            select(PipelineVersion).where(PipelineVersion.id == version_id).with_for_update()
        )
        version = (await session.execute(statement)).scalar_one_or_none()
        if version is None:
            raise ResourceNotFound("Pipeline 版本不存在")
        return version

    @staticmethod
    async def _project_id(session: AsyncSession, pipeline_id: uuid.UUID) -> uuid.UUID:
        """在审批应用服务中执行"project_id"对应的内部步骤；作为内部辅助函数，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        project_id = await session.scalar(
            select(Pipeline.project_id).where(Pipeline.id == pipeline_id)
        )
        if project_id is None:
            raise ResourceNotFound("Pipeline 不存在")
        return project_id

    @staticmethod
    async def _require_role(
        session: AsyncSession,
        project_id: uuid.UUID,
        actor_id: uuid.UUID,
        allowed: set[ProjectRole],
    ) -> None:
        """校验用户是否具备操作所需项目角色；不满足时在修改状态前拒绝。"""
        role = await session.scalar(
            select(ProjectMembership.role).where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.user_id == actor_id,
            )
        )
        if role not in allowed:
            raise PermissionDenied("当前用户没有执行此操作的项目角色")
