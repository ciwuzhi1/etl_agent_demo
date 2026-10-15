"""Benchmark 治理与受控安全进化应用服务（第 14 章）。

这里把第 14 章的闭环落到可持久化的状态机上：

- ``BenchmarkGovernanceService`` 运行冻结套件并把内容寻址的报告作为签名证据落库；
- ``SafetyEvolutionService`` 只允许改进候选沿"提案→独立裁决→影子→小流量→推广授权"
  逐级前进，绝不热改策略或代码，任一阶段命中 P0/P1 回归、误报越线或执行错误都会
  自动撤销候选授权。
"""

import re
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.benchmark import HarnessBenchmarkRunner
from etl_agent.application.harness_policy import PolicyDecisionPoint
from etl_agent.domain.benchmark import BenchmarkManifest
from etl_agent.domain.errors import DomainError, PermissionDenied, ResourceNotFound, VersionConflict
from etl_agent.domain.harness import (
    DetachedSignature,
    canonical_json_bytes,
    canonical_sha256,
)
from etl_agent.domain.safety_evolution import (
    SafetyProposalKind,
    SafetyProposalStatus,
    SafetyReleaseManifest,
    SafetyReleaseStage,
    SafetyReleaseStatus,
    SafetyStageObservation,
    release_stage_for_status,
)
from etl_agent.infrastructure.harness_signing import Ed25519Signer
from etl_agent.infrastructure.models import BenchmarkRun, SafetyProposal, SafetyRelease

_SECRET_KEY = re.compile(
    r"(?i)(?:api.?key|password|passwd|secret|token|private.?key|credential|ciphertext)"
)
_SECRET_TEXT = re.compile(r"(?i)(?:\bsk-[A-Za-z0-9_-]{12,}|\bbearer\s+\S{8,}|password\s*[:=])")
_MAX_PAYLOAD_BYTES = 64 * 1024
_DEFAULT_FALSE_POSITIVE_THRESHOLD = 0.05


def require_safe_proposal_candidate(payload: dict[str, Any]) -> None:
    """拒绝候选中的凭据键、疑似密钥正文和超大载荷，但允许模板化 Prompt。"""
    encoded = canonical_json_bytes(payload)
    if len(encoded) > _MAX_PAYLOAD_BYTES:
        raise DomainError("安全改进候选正文不能超过 64 KiB")

    def inspect(child: Any, path: str, depth: int) -> None:
        """递归检查候选结构，限制深度并定位不安全字段。"""
        if depth > 20:
            raise DomainError("安全改进候选嵌套过深")
        if isinstance(child, dict):
            for key, nested in child.items():
                if _SECRET_KEY.search(str(key)):
                    raise DomainError(f"{path}.{key} 不允许包含凭据或密钥材料")
                inspect(nested, f"{path}.{key}", depth + 1)
        elif isinstance(child, list):
            for index, nested in enumerate(child):
                inspect(nested, f"{path}[{index}]", depth + 1)
        elif isinstance(child, str) and _SECRET_TEXT.search(child):
            raise DomainError(f"{path} 疑似包含凭据正文")

    inspect(payload, "payload", 0)


class BenchmarkGovernanceService:
    """运行冻结 Benchmark 并把内容寻址的报告作为签名证据原子持久化。"""

    async def run_and_persist(
        self,
        session: AsyncSession,
        manifest: BenchmarkManifest,
        pdp: PolicyDecisionPoint,
        actor_id: uuid.UUID,
    ) -> BenchmarkRun:
        """执行无外部副作用的冻结套件，写入完整报告、摘要与门禁结论。"""
        report = await HarnessBenchmarkRunner().run(manifest, pdp)
        report_json = report.model_dump(mode="json")
        run = BenchmarkRun(
            suite_id=report.suite_id,
            version=report.version,
            policy_bundle_version=report.policy_bundle_version,
            manifest_sha256=report.manifest_sha256,
            report=report_json,
            report_sha256=canonical_sha256(report_json),
            gates_passed=report.gates_passed,
            gate_failures=list(report.gate_failures),
            started_at=report.started_at,
            finished_at=report.finished_at,
            created_by=actor_id,
        )
        session.add(run)
        await session.flush()
        return run


class SafetyEvolutionService:
    """实施"只生成提案、独立签批、影子、小流量、异常回滚"的受控进化。"""

    def __init__(
        self,
        *,
        signer: Ed25519Signer | None = None,
        environment: str = "local",
        false_positive_threshold: float = _DEFAULT_FALSE_POSITIVE_THRESHOLD,
    ) -> None:
        """注入可选发布签名器、环境名与误报门槛；服务不解析或执行候选正文。"""
        self._signer = signer
        self._environment = environment
        self._false_positive_threshold = false_positive_threshold

    async def create_proposal(
        self,
        session: AsyncSession,
        kind: SafetyProposalKind,
        payload: dict[str, Any],
        actor_id: uuid.UUID,
        *,
        title: str | None = None,
        rationale: str | None = None,
    ) -> SafetyProposal:
        """创建不可执行草稿；候选中疑似凭据或超大正文会被拒绝。"""
        require_safe_proposal_candidate(payload)
        proposal = SafetyProposal(
            kind=kind,
            status=SafetyProposalStatus.DRAFT,
            payload=payload,
            payload_sha256=canonical_sha256(payload),
            title=title,
            rationale=rationale,
            created_by=actor_id,
            lock_version=1,
        )
        session.add(proposal)
        await session.flush()
        return proposal

    async def submit_proposal(
        self,
        session: AsyncSession,
        proposal_id: uuid.UUID,
        actor_id: uuid.UUID,
    ) -> SafetyProposal:
        """由创建者提交冻结提案，提交后不得通过接口修改候选正文。"""
        proposal = await self._proposal_for_update(session, proposal_id)
        if proposal.created_by != actor_id:
            raise PermissionDenied("只有提案创建者可以提交草稿")
        if proposal.status is not SafetyProposalStatus.DRAFT:
            raise VersionConflict("只有 DRAFT 提案可以提交审查")
        proposal.status = SafetyProposalStatus.IN_REVIEW
        proposal.submitted_at = datetime.now(UTC)
        proposal.lock_version += 1
        await session.flush()
        return proposal

    async def decide_proposal(
        self,
        session: AsyncSession,
        proposal_id: uuid.UUID,
        decided_by: uuid.UUID,
        approve: bool,
        *,
        reason: str | None = None,
    ) -> SafetyProposal:
        """由独立审批人批准或拒绝；提案创建者不能裁决自己的提案。"""
        proposal = await self._proposal_for_update(session, proposal_id)
        if proposal.created_by == decided_by:
            raise PermissionDenied("审批人不能裁决自己创建的提案")
        if proposal.status is not SafetyProposalStatus.IN_REVIEW:
            raise VersionConflict("只有 IN_REVIEW 提案可以裁决")
        proposal.status = (
            SafetyProposalStatus.APPROVED if approve else SafetyProposalStatus.REJECTED
        )
        proposal.reviewed_by = decided_by
        proposal.decided_at = datetime.now(UTC)
        proposal.decision_reason = reason
        proposal.lock_version += 1
        await session.flush()
        return proposal

    async def start_shadow(
        self,
        session: AsyncSession,
        proposal_id: uuid.UUID,
        *,
        actor_id: uuid.UUID | None = None,
    ) -> SafetyRelease:
        """把已批准提案签名后进入只读影子阶段。"""
        proposal = await self._proposal_for_update(session, proposal_id)
        if proposal.status is not SafetyProposalStatus.APPROVED:
            raise VersionConflict("只有 APPROVED 提案可以进入影子验证")
        if await self._existing_release(session, proposal_id) is not None:
            raise VersionConflict("该提案已存在发布记录")
        issuer = actor_id or proposal.created_by
        release_id = uuid.uuid4()
        manifest = SafetyReleaseManifest(
            release_id=release_id,
            proposal_id=proposal.id,
            proposal_sha256=_proposal_sha256(proposal),
            payload_sha256=proposal.payload_sha256,
            policy_bundle_version=_policy_bundle_version(proposal.payload),
            environment=self._environment,
            issued_by=issuer,
            issued_at=datetime.now(UTC),
        )
        manifest_dict = manifest.model_dump(mode="json")
        manifest_sha256 = canonical_sha256(manifest_dict)
        signature = self._sign(manifest_sha256)
        release = SafetyRelease(
            id=release_id,
            proposal_id=proposal.id,
            status=SafetyReleaseStatus.SHADOW_RUNNING,
            stage=SafetyReleaseStage.SHADOW,
            manifest=manifest_dict,
            manifest_sha256=manifest_sha256,
            signature_algorithm=signature.algorithm if signature else None,
            signature_key_id=signature.key_id if signature else None,
            signature_value=signature.value if signature else None,
            started_at=datetime.now(UTC),
            created_by=issuer,
        )
        session.add(release)
        await session.flush()
        return release

    async def verify_shadow(
        self,
        session: AsyncSession,
        proposal_id: uuid.UUID,
        observation: SafetyStageObservation,
    ) -> SafetyRelease:
        """记录只读影子结果；任何安全回归、误报越线或执行错误都会自动回滚。"""
        release = await self._release_for_update(session, proposal_id)
        if release.status is not SafetyReleaseStatus.SHADOW_RUNNING:
            raise VersionConflict("只有 SHADOW_RUNNING 发布可以验证影子结果")
        return await self._finalize_stage(
            session,
            release,
            observation,
            success_status=SafetyReleaseStatus.SHADOW_VERIFIED,
            failure_prefix="影子验证失败",
        )

    async def start_canary(
        self,
        session: AsyncSession,
        proposal_id: uuid.UUID,
        percentage: int,
        *,
        actor_id: uuid.UUID | None = None,
    ) -> SafetyRelease:
        """把影子通过的候选提升到 1%~10% 的受控小流量阶段。"""
        if not 1 <= percentage <= 10:
            raise DomainError("小流量比例必须位于 1% 到 10%")
        release = await self._release_for_update(session, proposal_id)
        if release.status is not SafetyReleaseStatus.SHADOW_VERIFIED:
            raise VersionConflict("只有 SHADOW_VERIFIED 发布可以进入小流量阶段")
        release.status = SafetyReleaseStatus.CANARY_RUNNING
        release.stage = SafetyReleaseStage.CANARY
        release.canary_traffic_percent = percentage
        release.started_at = datetime.now(UTC)
        if actor_id is not None:
            release.created_by = actor_id
        await session.flush()
        return release

    async def verify_canary(
        self,
        session: AsyncSession,
        proposal_id: uuid.UUID,
        observation: SafetyStageObservation,
    ) -> SafetyRelease:
        """记录小流量结果；门禁或聚合事实失败时立即自动回滚授权状态。"""
        release = await self._release_for_update(session, proposal_id)
        if release.status is not SafetyReleaseStatus.CANARY_RUNNING:
            raise VersionConflict("只有 CANARY_RUNNING 发布可以验证小流量结果")
        return await self._finalize_stage(
            session,
            release,
            observation,
            success_status=SafetyReleaseStatus.CANARY_VERIFIED,
            failure_prefix="小流量验证失败",
        )

    async def approve_promotion(
        self,
        session: AsyncSession,
        proposal_id: uuid.UUID,
        decided_by: uuid.UUID,
    ) -> SafetyRelease:
        """签署推广授权记录；该动作不修改代码、策略文件或运行中的任务。"""
        release = await self._release_for_update(session, proposal_id)
        proposal = await self._proposal_for_update(session, proposal_id)
        if proposal.created_by == decided_by:
            raise PermissionDenied("候选创建者不能批准推广自己的变更")
        if release.status is not SafetyReleaseStatus.CANARY_VERIFIED:
            raise VersionConflict("只有 CANARY_VERIFIED 发布可以批准推广")
        release.status = SafetyReleaseStatus.PROMOTION_APPROVED
        release.stage = SafetyReleaseStage.PROMOTED
        release.verified_at = datetime.now(UTC)
        proposal.status = SafetyProposalStatus.RELEASED
        proposal.lock_version += 1
        await session.flush()
        return release

    async def rollback(
        self,
        session: AsyncSession,
        proposal_id: uuid.UUID,
        reason: str,
        *,
        actor_id: uuid.UUID | None = None,
    ) -> SafetyRelease:
        """允许显式撤销候选授权，同时保留全部失败历史。"""
        release = await self._release_for_update(session, proposal_id)
        if release.status is SafetyReleaseStatus.ROLLED_BACK:
            return release
        await self._auto_rollback(session, release=release, reason=reason.strip())
        if actor_id is not None:
            release.created_by = actor_id
            await session.flush()
        return release

    async def _finalize_stage(
        self,
        session: AsyncSession,
        release: SafetyRelease,
        observation: SafetyStageObservation,
        *,
        success_status: SafetyReleaseStatus,
        failure_prefix: str,
    ) -> SafetyRelease:
        """按不可补偿安全门槛把阶段转换为验证通过或自动回滚。"""
        release.observation = observation.model_dump(mode="json")
        failure = self._stage_failure_reason(observation)
        if failure is not None:
            await self._auto_rollback(
                session, release=release, reason=f"{failure_prefix}：{failure}"
            )
            return release
        release.status = success_status
        release.stage = release_stage_for_status(success_status)
        release.verified_at = datetime.now(UTC)
        await session.flush()
        return release

    def _stage_failure_reason(self, observation: SafetyStageObservation) -> str | None:
        """返回首个必须触发自动回滚的原因；无则返回 None。"""
        if observation.p0_p1_regressions:
            return "发现 P0/P1 安全回归"
        if observation.false_positive_rate() > self._false_positive_threshold:
            return "策略误报率超过冻结门槛"
        if observation.execution_errors:
            return "影子或小流量执行出现错误"
        return None

    async def _auto_rollback(
        self,
        session: AsyncSession,
        *,
        release: SafetyRelease,
        reason: str,
    ) -> None:
        """把异常阶段确定性转换为回滚，并保留原因供审计。"""
        release.status = SafetyReleaseStatus.ROLLED_BACK
        release.stage = SafetyReleaseStage.ROLLED_BACK
        release.rolled_back_at = datetime.now(UTC)
        release.rollback_reason = reason
        await session.flush()

    def _sign(self, manifest_sha256: str) -> DetachedSignature | None:
        """用发布签名器对清单摘要签名；未配置签名器时返回未签名事实。"""
        if self._signer is None:
            return None
        return self._signer.sign_detached(manifest_sha256.encode("ascii"))

    @staticmethod
    async def _proposal_for_update(session: AsyncSession, proposal_id: uuid.UUID) -> SafetyProposal:
        """锁定提案行，防止两个审批或发布请求并发跨越状态机。"""
        proposal = await session.scalar(
            select(SafetyProposal).where(SafetyProposal.id == proposal_id).with_for_update()
        )
        if proposal is None:
            raise ResourceNotFound("安全改进提案不存在")
        return proposal

    @staticmethod
    async def _release_for_update(session: AsyncSession, proposal_id: uuid.UUID) -> SafetyRelease:
        """锁定发布行，保证影子、小流量、推广与回滚只发生一次。"""
        release = await session.scalar(
            select(SafetyRelease).where(SafetyRelease.proposal_id == proposal_id).with_for_update()
        )
        if release is None:
            raise ResourceNotFound("安全发布记录不存在")
        return release

    @staticmethod
    async def _existing_release(
        session: AsyncSession, proposal_id: uuid.UUID
    ) -> SafetyRelease | None:
        """读取提案已有发布记录，用于阻止重复进入影子阶段。"""
        return await session.scalar(
            select(SafetyRelease).where(SafetyRelease.proposal_id == proposal_id)
        )


def _proposal_sha256(proposal: SafetyProposal) -> str:
    """计算提案已冻结事实摘要，不包含会变化的数据库更新时间。"""
    return canonical_sha256(
        {
            "id": str(proposal.id),
            "kind": str(proposal.kind),
            "status": str(proposal.status),
            "payload_sha256": proposal.payload_sha256,
            "title": proposal.title,
            "rationale": proposal.rationale,
            "created_by": str(proposal.created_by),
            "reviewed_by": str(proposal.reviewed_by) if proposal.reviewed_by else None,
        }
    )


def _policy_bundle_version(payload: dict[str, Any]) -> str:
    """从候选正文提取策略包版本，缺省回退到通用占位版本。"""
    value = payload.get("policy_bundle_version")
    return str(value) if value else "unspecified"
