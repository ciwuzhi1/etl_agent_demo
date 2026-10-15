"""Execution Harness Service for Chapter 11.

Coordinates execution Prepare, role-based approval, and Commit phases,
serving as the unified security gateway for all execution sources.
"""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.evidence_ledger import EvidenceLedgerService
from etl_agent.application.harness_access import HarnessAccessService
from etl_agent.application.harness_policy import (
    EmbeddedPolicyDecisionPoint,
    PolicyDecisionService,
)
from etl_agent.domain.enums import ExecutionStatus, PipelineVersionStatus
from etl_agent.domain.errors import DomainError, PermissionDenied, ResourceNotFound, VersionConflict
from etl_agent.domain.harness import (
    CapabilityClaims,
    ExecutionPreparationStatus,
    HarnessApprovalValue,
    HarnessOutboxStatus,
    HarnessRole,
    PolicyEvaluationContext,
    PolicyEvaluationRequest,
    ToolEffect,
    ToolIntent,
    canonical_json_bytes,
    canonical_sha256,
)
from etl_agent.infrastructure.harness_signing import CapabilityTokenIssuer
from etl_agent.infrastructure.models import (
    ExecutionPreparation,
    ExecutionRun,
    HarnessApprovalDecision,
    HarnessCapabilityGrant,
    HarnessCommandOutbox,
    Pipeline,
    PipelineArtifact,
    PipelineVersion,
)

_EXECUTION_TOOL_NAME = "seatunnel.execute_pipeline"
_EXECUTION_TOOL_VERSION = "2.0.0"
_EXECUTION_EFFECTS = [ToolEffect.SECRET_USE, ToolEffect.WRITE]
_READ_ROLES = {
    HarnessRole.DATA_ENGINEER,
    HarnessRole.DATA_APPROVER,
    HarnessRole.SECURITY_APPROVER,
    HarnessRole.OPERATOR,
    HarnessRole.AUDITOR,
}


class _PersistentTokenReplayGuard:
    """使用能力授权行锁实现跨进程一次性消费，取代仅适合测试的内存集合。"""

    def __init__(self, session: AsyncSession, *, consumed_by: uuid.UUID) -> None:
        """绑定 Commit 事务和真实操作人，消费事实与 Run 创建原子提交。"""
        self._session = session
        self._consumed_by = consumed_by

    async def consume(self, token_id: uuid.UUID) -> bool:
        """锁定授权记录并只允许首次消费；未知 Token 也按重放风险拒绝。"""
        grant = await self._session.scalar(
            select(HarnessCapabilityGrant)
            .where(HarnessCapabilityGrant.token_id == token_id)
            .with_for_update()
        )
        if grant is None or grant.consumed_at is not None:
            return False
        grant.consumed_at = datetime.now(UTC)
        grant.consumed_by = self._consumed_by
        await self._session.flush()
        return True


class _ExecutionCommitHandler:
    """在 Broker 授权上下文内创建 Run 和 Outbox，不接收客户端扩权参数。"""

    def __init__(
        self,
        *,
        session: AsyncSession,
        preparation: ExecutionPreparation,
        trace_id: str | None = None,
    ) -> None:
        """绑定不可变 Preparation；所有业务参数只从已验证输入读取。"""
        self._session = session
        self._preparation = preparation
        self._trace_id = trace_id

    async def handle_commit(
        self,
        claims: CapabilityClaims,
    ) -> tuple[ExecutionRun, HarnessCommandOutbox]:
        """创建唯一执行记录和事务 Outbox；缺少已验签能力声明时失败关闭。"""
        # Create ExecutionRun record
        now = datetime.now(UTC)
        run = ExecutionRun(
            pipeline_version_id=self._preparation.pipeline_version_id,
            status=ExecutionStatus.QUEUED,
            idempotency_key=f"exec-{uuid.uuid4().hex[:16]}",
            requested_by=self._preparation.requested_by,
            submitted_at=now,
        )
        self._session.add(run)
        await self._session.flush()

        # Create outbox command for worker dispatch
        outbox = HarnessCommandOutbox(
            project_id=self._preparation.project_id,
            command_payload={
                "execution_run_id": str(run.id),
                "preparation_id": str(self._preparation.id),
                "trace_id": self._trace_id,
            },
            target="worker.execution",
            status=HarnessOutboxStatus.PENDING,
            available_at=now,
            created_by=self._preparation.requested_by,
        )
        self._session.add(outbox)
        await self._session.flush()

        return run, outbox


def _freeze_preparation_facts(preparation_context: dict[str, Any]) -> str:
    """Serialize and hash preparation context to create immutable fingerprint."""
    return canonical_sha256(preparation_context)


def _require_all_approvals(
    approvals: list[HarnessApprovalDecision],
    required_roles: list[str],
    required_approvals: int,
) -> bool:
    """Check if all required roles have approved with APPROVE decision."""
    if not required_roles:
        return True

    approved_roles = {
        approval.role
        for approval in approvals
        if approval.decision == HarnessApprovalValue.APPROVE.value
    }

    # Check if we have enough approvals from required roles
    matching_approvals = sum(
        1
        for approval in approvals
        if approval.decision == HarnessApprovalValue.APPROVE.value
        and approval.role in required_roles
    )

    has_required = len(approved_roles.intersection(set(required_roles))) > 0
    return matching_approvals >= required_approvals and has_required


class ExecutionHarnessService:
    """协调执行 Prepare、职责审批与 Commit，成为所有执行来源的统一安全入口。"""

    def __init__(
        self,
        pdp: EmbeddedPolicyDecisionPoint,
        token_issuer: CapabilityTokenIssuer,
        ledger: EvidenceLedgerService | None = None,
    ) -> None:
        """注入 PDP、令牌签发器和证据账本服务。"""
        self._pdp = pdp
        self._token_issuer = token_issuer
        self._ledger = ledger or EvidenceLedgerService()
        self._access = HarnessAccessService()

    async def prepare(
        self,
        session: AsyncSession,
        project_id: str,
        pipeline_version_id: str,
        intent: ToolIntent,
        requested_by: str,
    ) -> str:
        """Validate pipeline version exists, invoke PDP, and create ExecutionPreparation record."""
        # Convert string IDs to UUID
        try:
            project_uuid = uuid.UUID(project_id)
            version_uuid = uuid.UUID(pipeline_version_id)
            actor_uuid = uuid.UUID(requested_by)
        except ValueError as exc:
            raise DomainError("Invalid UUID format") from exc

        # Validate pipeline version exists
        row = await session.execute(
            select(PipelineVersion, Pipeline.project_id)
            .join(Pipeline, Pipeline.id == PipelineVersion.pipeline_id)
            .where(PipelineVersion.id == version_uuid)
            .with_for_update()
        )
        result = row.one_or_none()

        if result is None:
            raise ResourceNotFound("Pipeline 版本不存在")

        version: PipelineVersion = result[0]
        actual_project_id: uuid.UUID = result[1]

        if actual_project_id != project_uuid:
            raise PermissionDenied("Pipeline 版本不属于指定项目")

        # Check version status
        if version.status is not PipelineVersionStatus.APPROVED:
            raise VersionConflict("只有已审批的 Pipeline 版本才能执行")

        # Require artifact SHA-256
        if not version.artifact_sha256:
            raise VersionConflict("已审批 Pipeline 版本缺少 Artifact 摘要")

        # Verify artifact exists
        artifact = await session.scalar(
            select(PipelineArtifact).where(
                PipelineArtifact.pipeline_version_id == version.id,
                PipelineArtifact.sha256 == version.artifact_sha256,
            )
        )
        if artifact is None:
            raise VersionConflict("审批版本对应的 SeaTunnel Artifact 不存在")

        # Invoke PDP to evaluate intent
        now = datetime.now(UTC)
        context = PolicyEvaluationContext(
            environment="production",
            actor_roles=[],
            policy_bundle_version="v1.0.0",
        )

        request = PolicyEvaluationRequest(
            intent=intent,
            context=context,
        )

        # 先持久化决策父行：execution_preparations.policy_decision_id 是指向
        # harness_policy_decisions.id 的外键，缺父行时在 PostgreSQL 下会外键失败；
        # evaluate_and_record 同时完成评估与落库，返回的决策 id 即父行 UUID。
        policy_decision = await PolicyDecisionService(self._pdp).evaluate_and_record(
            session, request=request
        )

        # If DENY: raise DomainError
        if policy_decision.outcome.value == "DENY":
            raise DomainError(f"策略拒绝执行: {policy_decision.risk_level}")

        # Create unique idempotency key
        idempotency_key = f"prep-{uuid.uuid4().hex[:16]}"

        # Compute artifact SHA-256 fingerprint
        input_fingerprint = canonical_sha256(
            {
                "tool_name": intent.tool_name,
                "action": intent.action,
                "project_id": str(intent.project_id),
                "actor_id": str(intent.actor_id),
            }
        )

        resource_fingerprint = canonical_sha256(
            {
                "resource_scopes": [
                    {
                        "project_id": str(scope.project_id),
                        "resource_type": scope.resource_type,
                        "resource_id": scope.resource_id,
                    }
                    for scope in intent.resource_scopes
                ]
            }
        )

        # Create ExecutionPreparation record with frozen facts
        preparation = ExecutionPreparation(
            id=uuid.uuid4(),
            project_id=project_uuid,
            pipeline_version_id=version_uuid,
            policy_decision_id=policy_decision.id,
            status=ExecutionPreparationStatus.WAITING_APPROVAL.value,
            risk_level=policy_decision.risk_level.value,
            policy_bundle_version=policy_decision.policy_bundle_version,
            environment=context.environment,
            tool_name=intent.tool_name,
            tool_version=_EXECUTION_TOOL_VERSION,
            artifact_sha256=artifact.sha256,
            input_fingerprint=input_fingerprint,
            resource_fingerprint=resource_fingerprint,
            intent={
                "tool_name": intent.tool_name,
                "action": intent.action,
                "project_id": str(intent.project_id),
                "actor_id": str(intent.actor_id),
                "effects": [effect.value for effect in intent.effects],
                "bounded": intent.bounded,
            },
            policy_context=context.model_dump(mode="json"),
            prepared_input={},
            impact_report={
                "pipeline_version_id": str(version_uuid),
                "artifact_sha256": artifact.sha256,
                "risk_level": policy_decision.risk_level.value,
            },
            rollback_plan={},
            required_roles=[role.value for role in policy_decision.required_roles],
            required_approvals=policy_decision.required_approvals,
            idempotency_key=idempotency_key,
            requested_by=actor_uuid,
            expires_at=policy_decision.expires_at or (now + timedelta(days=1)),
        )

        session.add(preparation)
        await session.flush()

        # Create audit event
        await self._ledger.append(
            session,
            project_id=project_uuid,
            stream_type="execution-preparation",
            stream_key=str(preparation.id),
            event_type="PREPARED",
            risk_level=policy_decision.risk_level.value,
            payload={
                "preparation_id": str(preparation.id),
                "pipeline_version_id": str(version_uuid),
                "artifact_sha256": artifact.sha256,
                "input_fingerprint": input_fingerprint,
                "resource_fingerprint": resource_fingerprint,
                "policy_bundle_version": policy_decision.policy_bundle_version,
                "policy_outcome": policy_decision.outcome.value,
                "required_roles": preparation.required_roles,
                "required_approvals": preparation.required_approvals,
                "status": preparation.status,
            },
            actor_id=actor_uuid,
        )

        return str(preparation.id)

    async def decide(
        self,
        session: AsyncSession,
        preparation_id: str,
        actor_id: str,
        role: str,
        decision: str,
        reason: str | None = None,
    ) -> None:
        """Validate actor has required role, enforce separation of duties, and record approval."""
        # Convert string IDs to UUID
        try:
            prep_uuid = uuid.UUID(preparation_id)
            actor_uuid = uuid.UUID(actor_id)
        except ValueError as exc:
            raise DomainError("Invalid UUID format") from exc

        # Get preparation with lock
        preparation = await session.scalar(
            select(ExecutionPreparation)
            .where(ExecutionPreparation.id == prep_uuid)
            .with_for_update()
        )

        if preparation is None:
            raise ResourceNotFound("ExecutionPreparation 不存在")

        # Check preparation status
        if preparation.status == ExecutionPreparationStatus.EXPIRED.value:
            raise VersionConflict("Preparation 已过期，请重新发起")

        if preparation.status not in [
            ExecutionPreparationStatus.WAITING_APPROVAL.value,
            ExecutionPreparationStatus.READY.value,
        ]:
            raise VersionConflict("当前 Preparation 不处于待审批状态")

        # Validate actor is not the requester (separation of duties)
        if actor_uuid == preparation.requested_by:
            raise PermissionDenied("申请人不能审批自己的 Preparation")

        # Validate role is required
        harness_role = HarnessRole(role)
        if harness_role.value not in preparation.required_roles:
            raise PermissionDenied("该职责不是当前 Preparation 要求的审批槽位")

        # Validate decision value
        approval_decision = HarnessApprovalValue(decision)

        # Check if this role already has a decision
        existing_role_decision = await session.scalar(
            select(HarnessApprovalDecision).where(
                HarnessApprovalDecision.preparation_id == prep_uuid,
                HarnessApprovalDecision.role == harness_role.value,
            )
        )

        if existing_role_decision is not None:
            raise VersionConflict("该审批职责已经由其他决定占用")

        # Enforce separation of duties: same actor cannot approve twice
        existing_actor_decision = await session.scalar(
            select(HarnessApprovalDecision.id).where(
                HarnessApprovalDecision.preparation_id == prep_uuid,
                HarnessApprovalDecision.decided_by == actor_uuid,
            )
        )

        if existing_actor_decision is not None:
            raise PermissionDenied("同一自然人不能覆盖多个审批职责槽位")

        # Record approval decision
        decided_at = datetime.now(UTC)
        evidence_document = {
            "preparation_id": str(prep_uuid),
            "artifact_sha256": preparation.artifact_sha256,
            "policy_bundle_version": preparation.policy_bundle_version,
            "role": harness_role.value,
            "decision": approval_decision.value,
            "decided_by": str(actor_uuid),
            "reason": reason or "",
            "decided_at": decided_at.isoformat(),
        }

        approval = HarnessApprovalDecision(
            preparation_id=prep_uuid,
            role=harness_role.value,
            decision=approval_decision.value,
            decided_by=actor_uuid,
            reason=reason or "",
            evidence_sha256=canonical_sha256(evidence_document),
            decided_at=decided_at,
        )

        session.add(approval)
        await session.flush()

        # Update preparation status based on decision
        if approval_decision is HarnessApprovalValue.REJECT:
            preparation.status = ExecutionPreparationStatus.REJECTED.value
        else:
            # Check if all required approvals received
            approvals = list(
                (
                    await session.scalars(
                        select(HarnessApprovalDecision).where(
                            HarnessApprovalDecision.preparation_id == prep_uuid
                        )
                    )
                ).all()
            )

            if _require_all_approvals(
                approvals,
                preparation.required_roles,
                preparation.required_approvals,
            ):
                preparation.status = ExecutionPreparationStatus.READY.value

        preparation.lock_version += 1
        await session.flush()

    async def commit(
        self,
        session: AsyncSession,
        preparation_id: str,
        committed_by: str,
    ) -> str:
        """Verify preparation is fully approved, issue capability token, and create ExecutionRun."""
        # Convert string IDs to UUID
        try:
            prep_uuid = uuid.UUID(preparation_id)
            actor_uuid = uuid.UUID(committed_by)
        except ValueError as exc:
            raise DomainError("Invalid UUID format") from exc

        # Get preparation with lock
        preparation = await session.scalar(
            select(ExecutionPreparation)
            .where(ExecutionPreparation.id == prep_uuid)
            .with_for_update()
        )

        if preparation is None:
            raise ResourceNotFound("ExecutionPreparation 不存在")

        # 已提交的 Preparation 必须幂等返回既有 Run：一次性能力令牌在首次 Commit
        # 时已消费，重复提交不能把同一份授权再花一次，也不能退化成 409 让调用方
        # 以为执行没发生。这一段必须在 READY 校验之前，否则 COMMITTED 永远先被
        # READY 分支拦下而成为不可达代码。
        if preparation.status == ExecutionPreparationStatus.COMMITTED.value:
            existing_run = await session.scalar(
                select(ExecutionRun).where(
                    ExecutionRun.id.in_(
                        select(HarnessCapabilityGrant.execution_run_id).where(
                            HarnessCapabilityGrant.preparation_id == prep_uuid
                        )
                    )
                )
            )
            if existing_run is None:
                raise VersionConflict("Preparation 已提交但执行记录缺失")
            return str(existing_run.id)

        # Verify preparation is fully approved
        if preparation.status != ExecutionPreparationStatus.READY.value:
            raise VersionConflict("Preparation 尚未满足全部审批要求")

        # Verify all approvals are present
        approvals = list(
            (
                await session.scalars(
                    select(HarnessApprovalDecision).where(
                        HarnessApprovalDecision.preparation_id == prep_uuid
                    )
                )
            ).all()
        )

        if not _require_all_approvals(
            approvals,
            preparation.required_roles,
            preparation.required_approvals,
        ):
            raise PermissionDenied("审批证据不完整，禁止 Commit")

        # Issue capability token (one-time use)
        now = datetime.now(UTC)
        expires_at = preparation.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        token_expiry = min(expires_at, now + timedelta(minutes=5))

        if token_expiry <= now:
            preparation.status = ExecutionPreparationStatus.EXPIRED.value
            raise VersionConflict("Preparation 已过期，请重新发起")

        claims = CapabilityClaims(
            project_id=preparation.project_id,
            subject_id=preparation.requested_by,
            environment=preparation.environment,
            policy_decision_id=preparation.policy_decision_id,
            policy_bundle_version=preparation.policy_bundle_version,
            tool_name=preparation.tool_name,
            tool_version=preparation.tool_version,
            input_fingerprint=preparation.input_fingerprint,
            resource_fingerprint=preparation.resource_fingerprint,
            artifact_sha256=preparation.artifact_sha256,
            effects=[ToolEffect.SECRET_USE, ToolEffect.WRITE],
            issued_at=now,
            expires_at=token_expiry,
            single_use=True,
        )

        # Issue token
        token = self._token_issuer.issue(claims.model_dump(mode="json"), expires_in_seconds=300)
        # CapabilityClaims 是 pydantic 模型，不能直接交给 json.dumps；先转成 JSON 安全
        # 的 dict（UUID/枚举/datetime 都会变成字符串）再计算规范摘要。
        claims_signature = canonical_sha256(canonical_json_bytes(claims.model_dump(mode="json")))

        # Create capability grant record
        grant = HarnessCapabilityGrant(
            token_id=claims.token_id,
            preparation_id=prep_uuid,
            token_sha256=hashlib.sha256(token.encode("ascii")).hexdigest(),
            key_id="default",
            claims=claims.model_dump(mode="json"),
            claims_signature_algorithm="Ed25519",
            claims_signature_value=claims_signature,
            subject_id=claims.subject_id,
            issued_at=claims.issued_at,
            expires_at=claims.expires_at,
        )
        session.add(grant)
        await session.flush()

        # 一次性消费：Commit 是能力令牌唯一的授权落点，消费事实与 Run / Outbox /
        # 证据在同一事务提交，任何一步失败都整体回滚，令牌不会被白白花掉。
        if not await _PersistentTokenReplayGuard(session, consumed_by=actor_uuid).consume(
            claims.token_id
        ):
            raise PermissionDenied("能力令牌已经使用，禁止重放")

        # Create ExecutionRun and outbox command using handler
        handler = _ExecutionCommitHandler(
            session=session,
            preparation=preparation,
        )

        run, outbox = await handler.handle_commit(claims)

        # Link grant to execution run
        grant.execution_run_id = run.id

        # Update preparation status
        preparation.status = ExecutionPreparationStatus.COMMITTED.value
        preparation.committed_by = actor_uuid
        preparation.committed_at = now
        preparation.lock_version += 1

        # Record evidence to ledger
        await self._ledger.append(
            session,
            project_id=preparation.project_id,
            stream_type="execution-preparation",
            stream_key=str(preparation.id),
            event_type="COMMITTED",
            risk_level=preparation.risk_level,
            payload={
                "execution_run_id": str(run.id),
                "outbox_id": str(outbox.id),
                "capability_id": str(claims.token_id),
                "capability_sha256": grant.token_sha256,
                "committed_by": str(actor_uuid),
                "artifact_sha256": preparation.artifact_sha256,
                "policy_bundle_version": preparation.policy_bundle_version,
            },
            actor_id=actor_uuid,
        )

        await session.flush()

        return str(run.id)
