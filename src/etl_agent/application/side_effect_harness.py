"""Side Effect Harness — 副作用操作的安全治理（第 12 章完整实现）。

取消执行、回滚 Doris、密钥轮换与销毁等 9 种危险操作不能由普通 API 直达外部系统；
它们与 Pipeline 执行共用同一套门禁：

1. ``prepare``：冻结业务事实并计算 SHA-256，调用 PDP 裁决，创建不可绕过的
   Preparation（PDP DENY 直接落 DENIED 终态并阻断后续）；
2. ``decide``：按 ``REQUIRED_ROLES_BY_OPERATION`` 逐槽审批，申请人不能自批，
   同一自然人不能占据多个职责槽；
3. ``commit``：全部必需职责批准后签发一次性能力令牌（Ed25519 签名），并在
   同一事务里创建 Outbox 命令，供 Worker 至少一次消费；
4. Worker 侧 ``claim_committed_side_effect`` / ``complete_side_effect`` /
   ``fail_side_effect`` 用行锁 + 执行租约抵抗重放，执行结果只落摘要。

Preparation 状态与执行状态分层：Harness 审批门禁（status）与外部执行进度
（execution_status）互不覆盖；Commit 成功不等于外部副作用成功。
"""

import base64
import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.evidence_ledger import EvidenceLedgerService
from etl_agent.application.execution_diagnostics import redact_execution_error
from etl_agent.application.harness_access import HarnessAccessService
from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint, PolicyDecisionPoint
from etl_agent.application.profiling import require_project_role
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import (
    DomainError,
    PermissionDenied,
    ResourceNotFound,
    VersionConflict,
)
from etl_agent.domain.harness import (
    DataClassification,
    HarnessApprovalValue,
    HarnessOutboxStatus,
    HarnessRole,
    PolicyEvaluationContext,
    PolicyEvaluationRequest,
    PolicyOutcome,
    ResourceScope,
    ToolEffect,
    ToolIntent,
    canonical_sha256,
)
from etl_agent.domain.side_effects import (
    REQUIRED_ROLES_BY_OPERATION,
    SideEffectCommitResult,
    SideEffectExecutionStatus,
    SideEffectOperation,
    SideEffectPreparationStatus,
)
from etl_agent.infrastructure.harness_signing import CapabilityTokenIssuer, Ed25519Signer
from etl_agent.infrastructure.models import (
    HarnessCommandOutbox,
    HarnessSideEffectApproval,
    HarnessSideEffectCapabilityGrant,
    HarnessSideEffectPreparation,
)

_TOOL_VERSION = "2.1.0"
_PREPARE_PROJECT_ROLES = {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER}
_READ_PROJECT_ROLES = {
    ProjectRole.ADMIN,
    ProjectRole.DATA_ENGINEER,
    ProjectRole.APPROVER,
    ProjectRole.VIEWER,
}
_TERMINAL_STATUSES = {
    SideEffectPreparationStatus.DENIED,
    SideEffectPreparationStatus.REJECTED,
    SideEffectPreparationStatus.EXPIRED,
    SideEffectPreparationStatus.COMMITTED,
}


@dataclass(frozen=True, slots=True)
class SideEffectSpec:
    """冻结一种副作用的工具名与真实效果；效果由代码注册表决定，不由调用方自报。"""

    tool_name: str
    effects: tuple[ToolEffect, ...]


_SPECS: dict[SideEffectOperation, SideEffectSpec] = {
    SideEffectOperation.CANCEL_EXECUTION: SideEffectSpec(
        tool_name="seatunnel.cancel_execution",
        effects=(ToolEffect.WRITE,),
    ),
    SideEffectOperation.ROLLBACK_DORIS: SideEffectSpec(
        tool_name="doris.rollback_release",
        effects=(ToolEffect.SECRET_USE, ToolEffect.WRITE),
    ),
    SideEffectOperation.CLEANUP_DORIS: SideEffectSpec(
        tool_name="doris.destroy_retained_table",
        effects=(ToolEffect.DESTRUCTIVE, ToolEffect.SECRET_USE),
    ),
    SideEffectOperation.MIGRATE_SECRET: SideEffectSpec(
        tool_name="secret.migrate_provider",
        effects=(ToolEffect.SECRET_USE, ToolEffect.WRITE),
    ),
    SideEffectOperation.STAGE_SECRET_ROTATION: SideEffectSpec(
        tool_name="secret.stage_rotation",
        effects=(ToolEffect.SECRET_USE, ToolEffect.WRITE),
    ),
    SideEffectOperation.VALIDATE_SECRET_ROTATION: SideEffectSpec(
        tool_name="secret.validate_rotation",
        effects=(ToolEffect.READ_ONLY, ToolEffect.SECRET_USE),
    ),
    SideEffectOperation.ACTIVATE_SECRET_ROTATION: SideEffectSpec(
        tool_name="secret.activate_rotation",
        effects=(ToolEffect.SECRET_USE, ToolEffect.WRITE),
    ),
    SideEffectOperation.ROLLBACK_SECRET_ROTATION: SideEffectSpec(
        tool_name="secret.rollback_rotation",
        effects=(ToolEffect.SECRET_USE, ToolEffect.WRITE),
    ),
    SideEffectOperation.DESTROY_SECRET: SideEffectSpec(
        tool_name="secret.destroy_obsolete",
        effects=(ToolEffect.DESTRUCTIVE, ToolEffect.SECRET_USE),
    ),
}


def _freeze_facts_hash(
    operation: SideEffectOperation,
    aggregate_id: uuid.UUID,
    context: dict[str, Any],
) -> str:
    """对操作类型 + 业务对象 + 冻结事实计算规范 JSON SHA-256。"""
    return canonical_sha256(
        {
            "operation": operation.value,
            "aggregate_id": str(aggregate_id),
            "context": context,
        }
    )


def _required_roles(operation: SideEffectOperation) -> list[str]:
    """从领域注册表读取该操作要求的审批职责，客户端不能削减。"""
    return list(REQUIRED_ROLES_BY_OPERATION[operation])


def _issue_capability_token(
    issuer: CapabilityTokenIssuer,
    claims: dict[str, Any],
    ttl_seconds: int,
) -> str:
    """签发分钟级一次性紧凑令牌；原始令牌不落库，只持久化 SHA-256 摘要。"""
    return issuer.issue(claims, expires_in_seconds=ttl_seconds)


def _as_utc(value: datetime) -> datetime:
    """统一数据库返回的有/无时区时间，兼容 SQLite 与 PostgreSQL。"""
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.astimezone(UTC)


def _expire_if_needed(preparation: HarnessSideEffectPreparation) -> None:
    """只让非终态 Preparation 过期；已 Commit 的异步命令由 Worker 对账。"""
    if preparation.status not in _TERMINAL_STATUSES and _as_utc(
        preparation.expires_at
    ) <= datetime.now(UTC):
        preparation.status = SideEffectPreparationStatus.EXPIRED


class SideEffectHarnessService:
    """统一非 Pipeline 副作用的 Prepare、审批、Commit 与查询门禁。"""

    def __init__(
        self,
        *,
        pdp: PolicyDecisionPoint | None = None,
        token_issuer: CapabilityTokenIssuer | None = None,
        environment: str = "production",
        policy_bundle_version: str = "harness-side-effects-v1",
        preparation_ttl_seconds: int = 86_400,
        capability_ttl_seconds: int = 300,
        access: HarnessAccessService | None = None,
        ledger: EvidenceLedgerService | None = None,
    ) -> None:
        """注入策略裁决、令牌签发与证据账本；缺省时使用确定性内嵌实现。"""
        if not 30 <= capability_ttl_seconds <= 900:
            raise ValueError("能力令牌生命周期必须位于 30 到 900 秒")
        if not 300 <= preparation_ttl_seconds <= 604_800:
            raise ValueError("副作用 Preparation 生命周期必须位于 300 到 604800 秒")
        self._pdp = pdp or EmbeddedPolicyDecisionPoint()
        self._token_issuer = token_issuer or CapabilityTokenIssuer(
            Ed25519Signer.generate(), "side-effect-dev"
        )
        self._environment = environment
        self._policy_bundle_version = policy_bundle_version
        self._preparation_ttl = timedelta(seconds=preparation_ttl_seconds)
        self._capability_ttl_seconds = capability_ttl_seconds
        self._access = access or HarnessAccessService()
        self._ledger = ledger or EvidenceLedgerService()

    async def prepare(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        operation: SideEffectOperation | str,
        aggregate_id: uuid.UUID,
        context: dict[str, Any],
        requested_by: uuid.UUID,
        idempotency_key: str | None = None,
        command_payload: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> HarnessSideEffectPreparation:
        """冻结业务事实摘要，调用 PDP 后创建不可绕过的副作用 Preparation。"""
        op = SideEffectOperation(operation)
        key = idempotency_key or f"{op.value.casefold()}:{aggregate_id}"
        facts_sha256 = _freeze_facts_hash(op, aggregate_id, context)
        existing = await session.scalar(
            select(HarnessSideEffectPreparation).where(
                HarnessSideEffectPreparation.project_id == project_id,
                HarnessSideEffectPreparation.idempotency_key == key,
            )
        )
        if existing is not None:
            if existing.facts_sha256 != facts_sha256:
                raise VersionConflict("副作用幂等键已绑定另一组冻结事实")
            if existing.status is SideEffectPreparationStatus.DENIED:
                raise PermissionDenied("策略已拒绝该副作用操作，幂等键不能翻案")
            return existing

        await require_project_role(session, project_id, requested_by, _PREPARE_PROJECT_ROLES)
        spec = _SPECS[op]
        required_roles = _required_roles(op)
        intent = ToolIntent(
            tool_name=spec.tool_name,
            action=op.value.casefold(),
            project_id=project_id,
            actor_id=requested_by,
            resource_scopes=[
                ResourceScope(
                    project_id=project_id, resource_type=op.value, resource_id=str(aggregate_id)
                )
            ],
            effects=list(spec.effects),
            data_classifications=[
                DataClassification.SECRET
                if ToolEffect.SECRET_USE in spec.effects
                else DataClassification.INTERNAL
            ],
            bounded=True,
            artifact_sha256=facts_sha256,
            metadata={"operation": op.value, "aggregate_id": str(aggregate_id)},
        )
        context_model = PolicyEvaluationContext(
            environment=self._environment,
            actor_roles=[],
            policy_bundle_version=self._policy_bundle_version,
        )
        decision = await self._pdp.evaluate(
            PolicyEvaluationRequest(intent=intent, context=context_model)
        )
        now = datetime.now(UTC)
        expires_at = decision.expires_at or (now + self._preparation_ttl)
        payload = command_payload or {"aggregate_id": str(aggregate_id)}
        denied = decision.outcome in {PolicyOutcome.DENY, PolicyOutcome.DEFER}
        preparation = HarnessSideEffectPreparation(
            project_id=project_id,
            operation=op,
            aggregate_id=aggregate_id,
            status=SideEffectPreparationStatus.DENIED
            if denied
            else SideEffectPreparationStatus.WAITING_APPROVAL,
            execution_status=SideEffectExecutionStatus.PENDING,
            risk_level=decision.risk_level.value,
            facts_sha256=facts_sha256,
            frozen_facts=dict(context),
            command_payload=payload,
            required_roles=[] if denied else required_roles,
            required_approvals=0 if denied else len(required_roles),
            idempotency_key=key,
            requested_by=requested_by,
            expires_at=expires_at,
        )
        session.add(preparation)
        await session.flush()
        await self._append_event(
            session,
            preparation=preparation,
            actor_id=requested_by,
            event_type="PREPARED",
            payload={
                "policy_outcome": decision.outcome.value,
                "request_fingerprint": decision.request_fingerprint,
                "required_roles": preparation.required_roles,
                "required_approvals": preparation.required_approvals,
            },
            trace_id=trace_id,
            occurred_at=now,
        )
        if denied:
            raise PermissionDenied(
                f"策略拒绝该副作用操作（{decision.risk_level.value}）："
                + "；".join(match.message for match in decision.rule_matches)
            )
        return preparation

    async def decide(
        self,
        session: AsyncSession,
        *,
        preparation_id: uuid.UUID,
        actor_id: uuid.UUID,
        role: HarnessRole | str,
        decision: HarnessApprovalValue | str,
        reason: str,
        trace_id: str | None = None,
    ) -> HarnessSideEffectPreparation:
        """占用一个职责审批槽；同一自然人只能批一个槽，申请人不能自批。"""
        preparation = await self._get_locked(session, preparation_id)
        _expire_if_needed(preparation)
        if preparation.status is SideEffectPreparationStatus.EXPIRED:
            raise VersionConflict("副作用 Preparation 已过期，请重新发起")
        if preparation.status not in {
            SideEffectPreparationStatus.WAITING_APPROVAL,
            SideEffectPreparationStatus.READY,
        }:
            raise VersionConflict("副作用 Preparation 当前不处于可审批状态")
        if actor_id == preparation.requested_by:
            raise PermissionDenied("申请人不能审批自己的副作用 Preparation")
        approval_role = HarnessRole(role)
        approval_value = HarnessApprovalValue(decision)
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise DomainError("副作用审批理由不能为空")
        if approval_role.value not in preparation.required_roles:
            raise PermissionDenied("该职责不是当前副作用 Preparation 要求的审批槽位")
        held_roles = await self._access.roles_for(
            session,
            project_id=preparation.project_id,
            user_id=actor_id,
        )
        if approval_role not in held_roles:
            raise PermissionDenied("调用人没有被授予声明的审批职责")

        same_person = await session.scalar(
            select(HarnessSideEffectApproval).where(
                HarnessSideEffectApproval.preparation_id == preparation.id,
                HarnessSideEffectApproval.decided_by == actor_id,
            )
        )
        if same_person is not None:
            # 网络重试必须幂等：完全相同的职责与决定返回既有事实；但同一人不能
            # 借重试换职责或改决定，职责分离优先于便利。
            if same_person.role is approval_role and same_person.decision is approval_value:
                return preparation
            raise PermissionDenied("同一自然人不能覆盖多个副作用审批职责")
        occupied = await session.scalar(
            select(HarnessSideEffectApproval).where(
                HarnessSideEffectApproval.preparation_id == preparation.id,
                HarnessSideEffectApproval.role == approval_role,
            )
        )
        if occupied is not None:
            raise VersionConflict("该副作用审批职责已经由其他决定占用")

        decided_at = datetime.now(UTC)
        evidence_sha256 = canonical_sha256(
            {
                "preparation_id": str(preparation.id),
                "facts_sha256": preparation.facts_sha256,
                "role": approval_role.value,
                "decision": approval_value.value,
                "decided_by": str(actor_id),
                "reason": normalized_reason,
                "decided_at": decided_at.isoformat(),
            }
        )
        approval = HarnessSideEffectApproval(
            preparation_id=preparation.id,
            role=approval_role,
            decision=approval_value,
            decided_by=actor_id,
            reason=normalized_reason,
            evidence_sha256=evidence_sha256,
            decided_at=decided_at,
        )
        session.add(approval)
        await session.flush()
        if approval_value is HarnessApprovalValue.REJECT:
            preparation.status = SideEffectPreparationStatus.REJECTED
        elif await self._approvals_satisfied(session, preparation):
            preparation.status = SideEffectPreparationStatus.READY
        await self._append_event(
            session,
            preparation=preparation,
            actor_id=actor_id,
            event_type="APPROVAL_DECIDED",
            payload={
                "approval_id": str(approval.id),
                "role": approval_role.value,
                "decision": approval_value.value,
                "evidence_sha256": approval.evidence_sha256,
                "preparation_status": preparation.status.value,
            },
            trace_id=trace_id,
            occurred_at=decided_at,
        )
        await session.flush()
        return preparation

    async def commit(
        self,
        session: AsyncSession,
        *,
        preparation_id: uuid.UUID,
        committed_by: uuid.UUID,
        trace_id: str | None = None,
    ) -> SideEffectCommitResult:
        """签发一次性能力令牌并在同一事务创建 Outbox 命令，不直接触达外部系统。"""
        preparation = await self._get_locked(session, preparation_id)
        if preparation.status is SideEffectPreparationStatus.COMMITTED:
            return await self._committed_result(session, preparation)
        _expire_if_needed(preparation)
        if preparation.status is SideEffectPreparationStatus.EXPIRED:
            raise VersionConflict("副作用 Preparation 已过期，请重新发起")
        if preparation.status is not SideEffectPreparationStatus.READY:
            raise VersionConflict("副作用 Preparation 尚未满足全部审批要求")
        if not await self._approvals_satisfied(session, preparation):
            raise PermissionDenied("副作用审批证据不完整，禁止 Commit")
        await require_project_role(
            session, preparation.project_id, committed_by, _PREPARE_PROJECT_ROLES
        )

        now = datetime.now(UTC)
        token_expiry = min(
            _as_utc(preparation.expires_at), now + timedelta(seconds=self._capability_ttl_seconds)
        )
        if token_expiry <= now:
            preparation.status = SideEffectPreparationStatus.EXPIRED
            raise VersionConflict("副作用 Preparation 已过期，请重新发起")
        spec = _SPECS[preparation.operation]
        token_id = uuid.uuid4()
        claims: dict[str, Any] = {
            "token_id": str(token_id),
            "preparation_id": str(preparation.id),
            "project_id": str(preparation.project_id),
            "subject_id": str(committed_by),
            "operation": preparation.operation.value,
            "aggregate_id": str(preparation.aggregate_id),
            "tool_name": spec.tool_name,
            "tool_version": _TOOL_VERSION,
            "effects": [effect.value for effect in spec.effects],
            "facts_sha256": preparation.facts_sha256,
            "environment": self._environment,
            "issued_at": now.isoformat(),
            "expires_at": token_expiry.isoformat(),
            "single_use": True,
        }
        token = _issue_capability_token(
            self._token_issuer,
            claims,
            self._capability_ttl_seconds,
        )
        grant = HarnessSideEffectCapabilityGrant(
            id=token_id,
            preparation_id=preparation.id,
            token_sha256=hashlib.sha256(token.encode("ascii")).hexdigest(),
            claims_json=claims,
            subject_id=committed_by,
            issued_at=now,
            expires_at=token_expiry,
        )
        session.add(grant)
        await session.flush()

        outbox_payload = {
            "side_effect_preparation_id": str(preparation.id),
            "operation": preparation.operation.value,
            "aggregate_id": str(preparation.aggregate_id),
            "command_payload": preparation.command_payload,
            "token_id": str(token_id),
            "trace_id": trace_id,
        }
        outbox = HarnessCommandOutbox(
            command_payload=outbox_payload,
            target=f"worker.side_effect.{preparation.operation.value.casefold()}",
            status=HarnessOutboxStatus.PENDING,
            available_at=now,
            project_id=preparation.project_id,
            created_by=committed_by,
        )
        session.add(outbox)
        await session.flush()
        grant.execution_command_id = outbox.id

        preparation.status = SideEffectPreparationStatus.COMMITTED
        preparation.committed_at = now
        preparation.execution_status = SideEffectExecutionStatus.PENDING
        await self._append_event(
            session,
            preparation=preparation,
            actor_id=committed_by,
            event_type="COMMITTED",
            payload={
                "outbox_id": str(outbox.id),
                "capability_id": str(token_id),
                "capability_sha256": grant.token_sha256,
                "committed_by": str(committed_by),
            },
            trace_id=trace_id,
            occurred_at=now,
        )
        await session.flush()
        return SideEffectCommitResult(
            execution_id=str(outbox.id),
            preparation_id=str(preparation.id),
            token_id=str(token_id),
            command_payload_sha256=canonical_sha256(outbox_payload),
        )

    async def get(
        self,
        session: AsyncSession,
        *,
        preparation_id: uuid.UUID,
        actor_id: uuid.UUID,
    ) -> HarnessSideEffectPreparation:
        """读取单个副作用 Preparation；先验证调用人的项目可见性。"""
        preparation = await session.get(HarnessSideEffectPreparation, preparation_id)
        if preparation is None:
            raise ResourceNotFound("副作用 Preparation 不存在")
        await require_project_role(session, preparation.project_id, actor_id, _READ_PROJECT_ROLES)
        _expire_if_needed(preparation)
        return preparation

    async def list_for_project(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        actor_id: uuid.UUID,
        limit: int = 100,
    ) -> list[HarnessSideEffectPreparation]:
        """按新到旧返回项目副作用 Preparation，供控制台审查统一门禁。"""
        await require_project_role(session, project_id, actor_id, _READ_PROJECT_ROLES)
        rows = list(
            (
                await session.scalars(
                    select(HarnessSideEffectPreparation)
                    .where(HarnessSideEffectPreparation.project_id == project_id)
                    .order_by(HarnessSideEffectPreparation.created_at.desc())
                    .limit(limit)
                )
            ).all()
        )
        for row in rows:
            _expire_if_needed(row)
        return rows

    async def approvals(
        self,
        session: AsyncSession,
        *,
        preparation_id: uuid.UUID,
        actor_id: uuid.UUID,
    ) -> list[HarnessSideEffectApproval]:
        """返回具名职责决定；读取前先验证 Preparation 的项目可见性。"""
        await self.get(session, preparation_id=preparation_id, actor_id=actor_id)
        return list(
            (
                await session.scalars(
                    select(HarnessSideEffectApproval)
                    .where(HarnessSideEffectApproval.preparation_id == preparation_id)
                    .order_by(HarnessSideEffectApproval.decided_at)
                )
            ).all()
        )

    async def _approvals_satisfied(
        self,
        session: AsyncSession,
        preparation: HarnessSideEffectPreparation,
    ) -> bool:
        """同时验证人数、职责与自然人唯一性；任何 REJECT 都不满足。"""
        approvals = list(
            (
                await session.scalars(
                    select(HarnessSideEffectApproval).where(
                        HarnessSideEffectApproval.preparation_id == preparation.id
                    )
                )
            ).all()
        )
        if any(item.decision is HarnessApprovalValue.REJECT for item in approvals):
            return False
        approved = [item for item in approvals if item.decision is HarnessApprovalValue.APPROVE]
        return (
            len(approved) >= preparation.required_approvals
            and len({item.decided_by for item in approved}) >= preparation.required_approvals
            and set(preparation.required_roles).issubset({item.role.value for item in approved})
        )

    async def _committed_result(
        self,
        session: AsyncSession,
        preparation: HarnessSideEffectPreparation,
    ) -> SideEffectCommitResult:
        """恢复已 Commit 的令牌与 Outbox 事实，让重复 Commit 收敛到同一结果。"""
        grant = await session.scalar(
            select(HarnessSideEffectCapabilityGrant).where(
                HarnessSideEffectCapabilityGrant.preparation_id == preparation.id
            )
        )
        if grant is None or grant.execution_command_id is None:
            raise VersionConflict("副作用已 Commit 但能力授权缺失")
        outbox = await session.get(HarnessCommandOutbox, grant.execution_command_id)
        if outbox is None:
            raise VersionConflict("副作用已 Commit 但 Outbox 命令缺失")
        return SideEffectCommitResult(
            execution_id=str(outbox.id),
            preparation_id=str(preparation.id),
            token_id=str(grant.id),
            command_payload_sha256=canonical_sha256(outbox.command_payload),
        )

    async def _append_event(
        self,
        session: AsyncSession,
        *,
        preparation: HarnessSideEffectPreparation,
        actor_id: uuid.UUID | None,
        event_type: str,
        payload: dict[str, Any],
        trace_id: str | None,
        occurred_at: datetime | None = None,
    ) -> None:
        """追加证据事件；载荷只含摘要、职责和状态，不含资源秘密。"""
        await self._ledger.append(
            session,
            project_id=preparation.project_id,
            stream_type="side-effect-preparation",
            stream_key=str(preparation.id),
            payload={
                "preparation_id": str(preparation.id),
                "operation": preparation.operation.value,
                "aggregate_id": str(preparation.aggregate_id),
                "facts_sha256": preparation.facts_sha256,
                "status": preparation.status.value,
                **payload,
            },
            event_type=event_type,
            risk_level=preparation.risk_level,
            actor_id=actor_id,
            trace_id=trace_id,
            occurred_at=occurred_at,
        )

    @staticmethod
    async def _get_locked(
        session: AsyncSession,
        preparation_id: uuid.UUID,
    ) -> HarnessSideEffectPreparation:
        """以行锁读取副作用 Preparation，使审批与 Commit 状态串行化。"""
        row = await session.scalar(
            select(HarnessSideEffectPreparation)
            .where(HarnessSideEffectPreparation.id == preparation_id)
            .with_for_update()
        )
        if row is None:
            raise ResourceNotFound("副作用 Preparation 不存在")
        return row


async def claim_committed_side_effect(
    session: AsyncSession,
    worker_id: str,
    *,
    lease_seconds: int = 300,
) -> dict[str, Any] | None:
    """Worker 领取一条已 Commit 的副作用命令并占用执行租约。

    可领取条件：执行状态 PENDING/FAILED（首次或失败重试），或 RUNNING 但租约
    已过期（崩溃 Worker 的接管）。并发投递被租约挡住：同一 Preparation 在有效
    租约内不会被第二个 Worker 领取。
    """
    normalized_worker = worker_id.strip()
    if not normalized_worker or len(normalized_worker) > 128:
        raise ValueError("worker_id 必须是 1 到 128 个字符的标识")
    if not 60 <= lease_seconds <= 86_400:
        raise ValueError("副作用执行租约必须位于 60 到 86400 秒")
    now = datetime.now(UTC)
    preparation = await session.scalar(
        select(HarnessSideEffectPreparation)
        .where(
            HarnessSideEffectPreparation.status == SideEffectPreparationStatus.COMMITTED,
            or_(
                HarnessSideEffectPreparation.execution_status.in_(
                    [SideEffectExecutionStatus.PENDING, SideEffectExecutionStatus.FAILED]
                ),
                and_(
                    HarnessSideEffectPreparation.execution_status
                    == SideEffectExecutionStatus.RUNNING,
                    or_(
                        HarnessSideEffectPreparation.lease_expires_at.is_(None),
                        HarnessSideEffectPreparation.lease_expires_at <= now,
                    ),
                ),
            ),
        )
        .order_by(HarnessSideEffectPreparation.created_at)
        .limit(1)
        .with_for_update()
    )
    if preparation is None:
        return None
    grant = await session.scalar(
        select(HarnessSideEffectCapabilityGrant)
        .where(HarnessSideEffectCapabilityGrant.preparation_id == preparation.id)
        .with_for_update()
    )
    if grant is None:
        raise PermissionDenied("副作用已 Commit 但缺少能力授权")
    # 一次性语义：首次领取即消费授权；租约过期接管不重复消费。
    if grant.consumed_at is None:
        grant.consumed_at = now
    preparation.execution_status = SideEffectExecutionStatus.RUNNING
    preparation.lease_owner = normalized_worker
    preparation.lease_expires_at = now + timedelta(seconds=lease_seconds)
    preparation.execution_error = None
    await session.flush()
    return {
        "preparation_id": str(preparation.id),
        "command_id": str(grant.execution_command_id) if grant.execution_command_id else None,
        "project_id": str(preparation.project_id),
        "operation": preparation.operation.value,
        "aggregate_id": str(preparation.aggregate_id),
        "facts_sha256": preparation.facts_sha256,
        "token_id": str(grant.id),
        "command_payload": preparation.command_payload,
        "lease_expires_at": preparation.lease_expires_at.isoformat(),
    }


async def complete_side_effect(
    session: AsyncSession,
    command_id: str,
    result: dict[str, Any],
    *,
    ledger: EvidenceLedgerService | None = None,
) -> None:
    """标记副作用执行成功；结果只落 SHA-256 摘要，重复回执幂等。"""
    preparation, _ = await _lock_for_command(session, command_id)
    if preparation.execution_status is SideEffectExecutionStatus.SUCCEEDED:
        return
    now = datetime.now(UTC)
    preparation.execution_status = SideEffectExecutionStatus.SUCCEEDED
    preparation.executed_at = now
    preparation.result_sha256 = canonical_sha256(result)
    preparation.execution_error = None
    preparation.lease_owner = None
    preparation.lease_expires_at = None
    await (ledger or _default_evidence_ledger()).append(
        session,
        project_id=preparation.project_id,
        stream_type="side-effect-preparation",
        stream_key=str(preparation.id),
        payload={
            "preparation_id": str(preparation.id),
            "operation": preparation.operation.value,
            "aggregate_id": str(preparation.aggregate_id),
            "result_sha256": preparation.result_sha256,
        },
        event_type="EXECUTION_SUCCEEDED",
        risk_level=preparation.risk_level,
    )
    await session.flush()


async def fail_side_effect(
    session: AsyncSession,
    command_id: str,
    error: str,
    *,
    ledger: EvidenceLedgerService | None = None,
) -> None:
    """标记副作用最终失败；只保存脱敏错误摘要，保留授权供审计与人工重试。"""
    preparation, outbox = await _lock_for_command(session, command_id)
    if preparation.execution_status is SideEffectExecutionStatus.SUCCEEDED:
        return
    now = datetime.now(UTC)
    safe_error = redact_execution_error(error)[:2_000]
    preparation.execution_status = SideEffectExecutionStatus.FAILED
    preparation.executed_at = now
    preparation.execution_error = safe_error
    preparation.lease_owner = None
    preparation.lease_expires_at = None
    outbox.last_error = safe_error
    await (ledger or _default_evidence_ledger()).append(
        session,
        project_id=preparation.project_id,
        stream_type="side-effect-preparation",
        stream_key=str(preparation.id),
        payload={
            "preparation_id": str(preparation.id),
            "operation": preparation.operation.value,
            "aggregate_id": str(preparation.aggregate_id),
            "error_sha256": canonical_sha256({"error": safe_error}),
        },
        event_type="EXECUTION_FAILED",
        risk_level=preparation.risk_level,
    )
    await session.flush()


@lru_cache
def _default_evidence_ledger() -> EvidenceLedgerService:
    """进程内共享的签名证据账本，供模块级终态函数落到与能力令牌同源的签名。

    这两个终态函数由 Celery 任务与测试直接调用，拿不到 ``SideEffectHarnessService``
    实例上的 ``self._ledger``；此前它们各自内联 ``EvidenceLedgerService()``，结果
    副作用链路的证据长期是无签名的。改为统一从这里取签名账本。
    """
    from etl_agent.application.harness_factory import build_evidence_ledger

    return build_evidence_ledger()


async def _lock_for_command(
    session: AsyncSession,
    command_id: str,
) -> tuple[HarnessSideEffectPreparation, HarnessCommandOutbox]:
    """按 Outbox 命令 ID 锁定副作用 Preparation，供 Worker 原子落结果。"""
    try:
        command_uuid = uuid.UUID(command_id)
    except ValueError as exc:
        raise DomainError("command_id 必须是合法的 UUID") from exc
    outbox = await session.get(HarnessCommandOutbox, command_uuid)
    if outbox is None:
        raise ResourceNotFound("副作用 Outbox 命令不存在")
    raw_preparation_id = outbox.command_payload.get("side_effect_preparation_id")
    try:
        preparation_uuid = uuid.UUID(str(raw_preparation_id))
    except ValueError as exc:
        raise ResourceNotFound("副作用命令缺少可解析的 Preparation 引用") from exc
    preparation = await session.scalar(
        select(HarnessSideEffectPreparation)
        .where(HarnessSideEffectPreparation.id == preparation_uuid)
        .with_for_update()
    )
    if preparation is None:
        raise ResourceNotFound("副作用结果缺少 Preparation")
    return preparation, outbox


def load_signer(
    *,
    harness_private_key: str | None,
    key_id: str,
    dev_key_path: Path | None = None,
) -> Ed25519Signer:
    """按配置加载签名私钥：显式密钥 > 开发密钥文件 > 进程内临时密钥（仅非生产）。"""
    if harness_private_key is not None:
        try:
            material = bytes.fromhex(harness_private_key)
        except ValueError:
            material = base64.urlsafe_b64decode(harness_private_key.encode("ascii"))
        return Ed25519Signer(material, key_id)
    if dev_key_path is not None and dev_key_path.is_file():
        return Ed25519Signer(dev_key_path.read_bytes(), key_id)
    return Ed25519Signer.generate()
