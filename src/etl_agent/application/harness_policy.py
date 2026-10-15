"""Policy Decision Point (PDP) engine for Chapter 11.

Implements deterministic policy rules, embedded and signed PDPs,
and a persistence service for policy decisions.
"""

from collections.abc import Sequence
from datetime import timedelta
from typing import ClassVar, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.domain.errors import DomainError
from etl_agent.domain.harness import (
    DataClassification,
    DetachedSignature,
    HarnessRole,
    PolicyBundleManifest,
    PolicyDecision,
    PolicyEvaluationRequest,
    PolicyOutcome,
    PolicyRuleMatch,
    RiskLevel,
    SignedPolicyBundle,
    ToolEffect,
    canonical_sha256,
)
from etl_agent.infrastructure.models import HarnessPolicyDecisionRecord


class PolicyRule(Protocol):
    """定义内嵌 PDP 规则的稳定端口，未来 OPA 适配器不改变调用方。"""

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """评估冻结事实；不命中返回 None，禁止读取数据库或产生副作用。"""
        ...


class PolicyDecisionPoint(Protocol):
    """定义 Harness 唯一策略裁决端口。"""

    async def evaluate(self, request: PolicyEvaluationRequest) -> PolicyDecision:
        """返回结构化、可解释且绑定请求指纹的策略决策。"""
        ...


class PolicyBundleSigner(Protocol):
    """定义策略发布端的分离签名能力，业务层不接触私钥格式。"""

    def sign_detached(self, payload: bytes) -> DetachedSignature:
        """对策略清单哈希生成 Ed25519 签名。"""
        ...


class PolicyBundleVerifier(Protocol):
    """定义 API、Worker 和审计工具共同使用的公钥验证能力。"""

    def verify_detached(self, payload: bytes, signature: DetachedSignature) -> bool:
        """验证策略清单哈希与签名是否匹配。"""
        ...


class CrossProjectRule:
    """阻断任何资源项目与调用项目不一致的工具意图。"""

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """发现跨项目 Scope 时返回 P0 拒绝，不能通过审批提升权限。"""
        foreign_projects = sorted(
            {
                str(scope.project_id)
                for scope in request.intent.resource_scopes
                if scope.project_id != request.intent.project_id
            }
        )
        if not foreign_projects:
            return None
        return PolicyRuleMatch(
            rule_id="HARNESS-P0-CROSS-PROJECT",
            rule_version="1",
            outcome=PolicyOutcome.DENY,
            risk_level=RiskLevel.P0,
            message="工具意图包含当前项目之外的资源范围",
            evidence={"foreign_project_ids": foreign_projects},
        )


class SecretExfiltrationRule:
    """阻断 Secret 级数据通过任意外部出口发送。"""

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """Secret 与外发副作用同时出现时直接拒绝，不提供人工放行通道。"""
        intent = request.intent
        if (
            ToolEffect.EXTERNAL_EGRESS not in intent.effects
            or DataClassification.SECRET not in intent.data_classifications
        ):
            return None
        return PolicyRuleMatch(
            rule_id="HARNESS-P0-SECRET-EXFILTRATION",
            rule_version="1",
            outcome=PolicyOutcome.DENY,
            risk_level=RiskLevel.P0,
            message="Secret 级数据禁止发送到外部目标",
            evidence={"external_destination": intent.external_destination},
        )


class DestructiveOperationRule:
    """把删除、覆盖和不可逆变更提升为需要双人审批的 P0 风险。"""

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """依据 Broker 注册的副作用判断，不能信任 Agent 自报的低风险标签。"""
        if ToolEffect.DESTRUCTIVE not in request.intent.effects:
            return None
        return PolicyRuleMatch(
            rule_id="HARNESS-P0-DESTRUCTIVE",
            rule_version="1",
            outcome=PolicyOutcome.REQUIRE_APPROVAL,
            risk_level=RiskLevel.P0,
            message="破坏性操作必须经过业务与安全双人审批",
            evidence={"action": request.intent.action},
            required_roles=[HarnessRole.DATA_APPROVER, HarnessRole.SECURITY_APPROVER],
            required_approvals=2,
        )


class SensitiveEgressRule:
    """治理非 Secret 数据跨安全域外发，按敏感等级提升审批。"""

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """外发敏感或未知数据要求双人审批，内部数据至少要求业务审批。"""
        intent = request.intent
        if ToolEffect.EXTERNAL_EGRESS not in intent.effects:
            return None
        classifications = set(intent.data_classifications)
        sensitive = {
            DataClassification.CONFIDENTIAL,
            DataClassification.RESTRICTED,
            DataClassification.SECRET,
            DataClassification.UNKNOWN,
        }
        if classifications & sensitive:
            return PolicyRuleMatch(
                rule_id="HARNESS-P0-SENSITIVE-EGRESS",
                rule_version="1",
                outcome=PolicyOutcome.REQUIRE_APPROVAL,
                risk_level=RiskLevel.P0,
                message="敏感或未分类数据外发必须经过业务与安全双人审批",
                evidence={
                    "classifications": sorted(value.value for value in classifications),
                    "external_destination": intent.external_destination,
                },
                required_roles=[HarnessRole.DATA_APPROVER, HarnessRole.SECURITY_APPROVER],
                required_approvals=2,
            )
        return PolicyRuleMatch(
            rule_id="HARNESS-P1-INTERNAL-EGRESS",
            rule_version="1",
            outcome=PolicyOutcome.REQUIRE_APPROVAL,
            risk_level=RiskLevel.P1,
            message="内部数据外发至少需要业务审批",
            evidence={"external_destination": intent.external_destination},
            required_roles=[HarnessRole.DATA_APPROVER],
            required_approvals=1,
        )


class UnboundedOperationRule:
    """阻止没有扫描、写入或调用上限的工具意图自动执行。"""

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """未冻结预算时返回 P1 审批要求，并保留具体工具证据。"""
        if request.intent.bounded:
            return None
        return PolicyRuleMatch(
            rule_id="HARNESS-P1-UNBOUNDED",
            rule_version="1",
            outcome=PolicyOutcome.REQUIRE_APPROVAL,
            risk_level=RiskLevel.P1,
            message="无界扫描、写入或外部调用不能自动执行",
            evidence={"tool_name": request.intent.tool_name},
            required_roles=[HarnessRole.DATA_APPROVER],
            required_approvals=1,
        )


class PrivilegedMutationRule:
    """要求可恢复但高影响的控制面变更至少经过独立数据审批。"""

    _TOOLS: ClassVar[set[str]] = {
        "doris.rollback_release",
        "secret.migrate_provider",
        "secret.activate_rotation",
        "secret.rollback_rotation",
    }

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """按 Broker 注册工具名识别特权变更，不能由客户端用普通 WRITE 冒充。"""
        if request.intent.tool_name not in self._TOOLS:
            return None
        return PolicyRuleMatch(
            rule_id="HARNESS-P1-PRIVILEGED-MUTATION",
            rule_version="1",
            outcome=PolicyOutcome.REQUIRE_APPROVAL,
            risk_level=RiskLevel.P1,
            message="特权控制面变更必须经过独立数据审批",
            evidence={"tool_name": request.intent.tool_name},
            required_roles=[HarnessRole.DATA_APPROVER],
            required_approvals=1,
        )


class SecretUseRule:
    """要求涉及运行时凭据的生产工具调用绑定人工审批证据。"""

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """本地/测试环境保留同一证据但允许自动处理，生产环境要求业务审批。"""
        if ToolEffect.SECRET_USE not in request.intent.effects:
            return None
        if request.intent.tool_name in {
            "secret.stage_rotation",
            "secret.validate_rotation",
        }:
            # 暂存只写隔离的新版本，验证只做固定连接的最小只读探针；两者都不切换
            # 活跃引用，因此可由 Broker 的一次性能力自动执行，激活仍必须独立审批。
            return PolicyRuleMatch(
                rule_id="HARNESS-P1-SECRET-USE",
                rule_version="1",
                outcome=PolicyOutcome.ALLOW,
                risk_level=RiskLevel.P1,
                message="隔离暂存或固定范围验证由一次性能力受控执行",
                evidence={"tool_name": request.intent.tool_name},
            )
        production = request.context.environment.strip().casefold() in {"prod", "production"}
        return PolicyRuleMatch(
            rule_id="HARNESS-P1-SECRET-USE",
            rule_version="1",
            outcome=(PolicyOutcome.REQUIRE_APPROVAL if production else PolicyOutcome.ALLOW),
            risk_level=RiskLevel.P1,
            message=(
                "生产凭据使用必须绑定业务审批"
                if production
                else "非生产凭据使用由 Tool Broker 在执行端解析"
            ),
            evidence={"environment": request.context.environment},
            required_roles=[HarnessRole.DATA_APPROVER] if production else [],
            required_approvals=1 if production else 0,
        )


class QualityGateBypassRule:
    """Deny if attempting to skip quality checks."""

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """Detect quality gate bypass attempts via metadata flags."""
        if not request.intent.metadata.get("skip_quality_checks", False):
            return None
        return PolicyRuleMatch(
            rule_id="HARNESS-P0-QUALITY-GATE-BYPASS",
            rule_version="1",
            outcome=PolicyOutcome.DENY,
            risk_level=RiskLevel.P0,
            message="禁止跳过质量检查",
            evidence={"tool_name": request.intent.tool_name},
        )


class AuditTamperingRule:
    """Deny if attempting to modify audit logs."""

    _AUDIT_ACTIONS: ClassVar[set[str]] = {
        "audit.delete",
        "audit.modify",
        "audit.clear",
        "audit.truncate",
    }

    def evaluate(self, request: PolicyEvaluationRequest) -> PolicyRuleMatch | None:
        """Detect attempts to tamper with audit records."""
        if request.intent.action not in self._AUDIT_ACTIONS:
            return None
        return PolicyRuleMatch(
            rule_id="HARNESS-P0-AUDIT-TAMPERING",
            rule_version="1",
            outcome=PolicyOutcome.DENY,
            risk_level=RiskLevel.P0,
            message="禁止修改或删除审计日志",
            evidence={"action": request.intent.action},
        )


class EmbeddedPolicyDecisionPoint:
    """按固定优先级组合确定性规则，作为 V2 默认内嵌 PDP。"""

    def __init__(
        self,
        rules: Sequence[PolicyRule] | None = None,
        *,
        decision_ttl: timedelta = timedelta(days=1),
    ) -> None:
        """允许测试注入规则与有效期；生产默认规则顺序不影响最终严重度聚合。"""
        self._rules = tuple(
            rules
            or (
                CrossProjectRule(),
                SecretExfiltrationRule(),
                DestructiveOperationRule(),
                SensitiveEgressRule(),
                UnboundedOperationRule(),
                PrivilegedMutationRule(),
                SecretUseRule(),
                QualityGateBypassRule(),
                AuditTamperingRule(),
            )
        )
        self._decision_ttl = decision_ttl

    async def evaluate(self, request: PolicyEvaluationRequest) -> PolicyDecision:
        """汇总全部命中规则；DENY、DEFER、审批、允许依次采用更严格结果。"""
        matches = [match for rule in self._rules if (match := rule.evaluate(request)) is not None]
        if not matches:
            matches = [
                PolicyRuleMatch(
                    rule_id="HARNESS-BASELINE-ALLOW",
                    rule_version="1",
                    outcome=PolicyOutcome.ALLOW,
                    risk_level=RiskLevel.P3,
                    message="工具意图未命中更高风险规则",
                    evidence={"tool_name": request.intent.tool_name},
                )
            ]

        outcome_order = {
            PolicyOutcome.ALLOW: 0,
            PolicyOutcome.REQUIRE_APPROVAL: 1,
            PolicyOutcome.DEFER: 2,
            PolicyOutcome.DENY: 3,
        }
        risk_order = {
            RiskLevel.P3: 0,
            RiskLevel.P2: 1,
            RiskLevel.P1: 2,
            RiskLevel.P0: 3,
        }
        outcome = max((match.outcome for match in matches), key=outcome_order.__getitem__)
        risk_level = max((match.risk_level for match in matches), key=risk_order.__getitem__)
        roles = sorted(
            {role for match in matches for role in match.required_roles},
            key=lambda value: value.value,
        )
        approvals = max(match.required_approvals for match in matches)
        if outcome is PolicyOutcome.DENY:
            # DENY 不存在可放行的审批路径。保留较低优先级规则的审批角色会让调用方
            # 误以为补充审批可以覆盖 P0 拒绝，因此最终裁决必须清空审批要求。
            roles = []
            approvals = 0
        return PolicyDecision(
            id=f"pol-{canonical_sha256(request)[:12]}",
            project_id=request.intent.project_id,
            actor_id=request.intent.actor_id,
            outcome=outcome,
            risk_level=risk_level,
            policy_bundle_version=request.context.policy_bundle_version,
            request_fingerprint=canonical_sha256(request),
            rule_matches=matches,
            required_roles=roles,
            required_approvals=approvals,
            artifact_sha256=request.intent.artifact_sha256,
            decided_at=request.context.evaluated_at,
            # 人工审批通常跨越数分钟甚至数小时，Preparation 有效期不能与一次性
            # Capability 的短时 TTL 混用；真正执行时仍只签发分钟级令牌。
            expires_at=request.context.evaluated_at + self._decision_ttl,
        )


class SignedPolicyDecisionPoint:
    """在允许 PDP 工作前验证签名策略包，并固定所有请求使用同一版本。"""

    def __init__(
        self,
        *,
        bundle: SignedPolicyBundle,
        verifier: PolicyBundleVerifier,
        delegate: PolicyDecisionPoint | None = None,
    ) -> None:
        """验证策略包哈希、签名和内置规则清单；任何不一致都拒绝启动。"""
        require_valid_policy_bundle(bundle, verifier=verifier)
        expected_rule_ids = _default_rule_ids()
        actual_rule_ids = {rule["rule_id"] for rule in bundle.manifest.rules}
        if actual_rule_ids != expected_rule_ids:
            raise DomainError("签名策略包规则清单与当前内嵌 PDP 实现不一致")
        self._bundle = bundle
        self._delegate = delegate or EmbeddedPolicyDecisionPoint()

    async def evaluate(self, request: PolicyEvaluationRequest) -> PolicyDecision:
        """拒绝请求临时切换策略版本，再交由已验证的确定性实现裁决。"""
        if request.context.policy_bundle_version != self._bundle.manifest.version:
            raise DomainError("策略评估上下文与已验证策略包版本不一致")
        return await self._delegate.evaluate(request)


def default_policy_bundle_manifest(*, version: str) -> PolicyBundleManifest:
    """构造与当前内嵌 PDP 对应的最小策略清单，供签名发布和测试复现。"""
    descriptors = [
        ("HARNESS-P0-CROSS-PROJECT", "1"),
        ("HARNESS-P0-SECRET-EXFILTRATION", "1"),
        ("HARNESS-P0-DESTRUCTIVE", "1"),
        ("HARNESS-P0-SENSITIVE-EGRESS", "1"),
        ("HARNESS-P1-INTERNAL-EGRESS", "1"),
        ("HARNESS-P1-UNBOUNDED", "1"),
        ("HARNESS-P1-PRIVILEGED-MUTATION", "1"),
        ("HARNESS-P1-SECRET-USE", "1"),
        ("HARNESS-BASELINE-ALLOW", "1"),
        ("HARNESS-P0-QUALITY-GATE-BYPASS", "1"),
        ("HARNESS-P0-AUDIT-TAMPERING", "1"),
    ]
    return PolicyBundleManifest(
        version=version,
        rules=[
            {"rule_id": rule_id, "rule_version": rule_version, "parameters": {}}
            for rule_id, rule_version in descriptors
        ],
        metadata={"engine": "embedded-python", "contract_version": 1},
    )


def sign_policy_bundle(
    manifest: PolicyBundleManifest,
    *,
    signer: PolicyBundleSigner,
) -> SignedPolicyBundle:
    """对规范策略清单哈希签名，发布物不包含任何私钥材料。"""
    manifest_sha256 = canonical_sha256(manifest)
    return SignedPolicyBundle(
        manifest=manifest,
        manifest_sha256=manifest_sha256,
        signature=signer.sign_detached(manifest_sha256.encode("ascii")),
    )


def require_valid_policy_bundle(
    bundle: SignedPolicyBundle,
    *,
    verifier: PolicyBundleVerifier,
) -> None:
    """验证策略清单哈希和发布签名，失败时阻止 PDP 启动或热切换。"""
    actual_sha256 = canonical_sha256(bundle.manifest)
    if actual_sha256 != bundle.manifest_sha256:
        raise DomainError("策略包清单哈希不匹配")
    if not verifier.verify_detached(
        bundle.manifest_sha256.encode("ascii"),
        bundle.signature,
    ):
        raise DomainError("策略包签名无效")


def _default_rule_ids() -> set[str]:
    """返回当前内嵌 PDP 的完整规则 ID 集，阻止签名清单漏掉实际规则。"""
    return {
        "HARNESS-P0-CROSS-PROJECT",
        "HARNESS-P0-SECRET-EXFILTRATION",
        "HARNESS-P0-DESTRUCTIVE",
        "HARNESS-P0-SENSITIVE-EGRESS",
        "HARNESS-P1-INTERNAL-EGRESS",
        "HARNESS-P1-UNBOUNDED",
        "HARNESS-P1-PRIVILEGED-MUTATION",
        "HARNESS-P1-SECRET-USE",
        "HARNESS-BASELINE-ALLOW",
        "HARNESS-P0-QUALITY-GATE-BYPASS",
        "HARNESS-P0-AUDIT-TAMPERING",
    }


class PolicyDecisionService:
    """在同一事务内评估并持久化结构化策略决策，供审批和证据链引用。"""

    def __init__(self, decision_point: PolicyDecisionPoint) -> None:
        """注入 PDP 端口，避免应用服务依赖具体 Python 或未来 OPA 实现。"""
        self._decision_point = decision_point

    async def evaluate_and_record(
        self,
        session: AsyncSession,
        *,
        request: PolicyEvaluationRequest,
        trace_id: str | None = None,
    ) -> PolicyDecision:
        """保存不含原始数据和凭据的规则证据，事务提交由调用方统一控制。"""
        decision = await self._decision_point.evaluate(request)
        session.add(
            HarnessPolicyDecisionRecord(
                id=decision.id,
                project_id=decision.project_id,
                actor_id=decision.actor_id,
                outcome=decision.outcome.value,
                risk_level=decision.risk_level.value,
                policy_bundle_version=decision.policy_bundle_version,
                request_fingerprint=decision.request_fingerprint,
                rule_matches=[match.__dict__ for match in decision.rule_matches],
                required_roles=[role.value for role in decision.required_roles],
                required_approvals=decision.required_approvals,
                artifact_sha256=decision.artifact_sha256,
                expires_at=decision.expires_at,
                trace_id=trace_id,
            )
        )
        await session.flush()
        return decision
