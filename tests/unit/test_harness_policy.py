"""第 11 章单元测试：内嵌 PDP 的确定性规则与最严聚合。

覆盖：
- 九条规则各自的触发条件与所需审批角色；
- 多规则同时命中时取“最严结果 + 最高风险 + 审批角色并集 + 审批人数上限”；
- P0 DENY 清空审批要求，杜绝“补审批就能放行拒绝项”的错觉；
- 同一请求得到确定性决策 ID 与请求指纹，不同风险等级不会互相抵消。
"""

import uuid

import pytest

from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint
from etl_agent.domain.harness import (
    DataClassification,
    HarnessRole,
    PolicyDecision,
    PolicyEvaluationContext,
    PolicyEvaluationRequest,
    PolicyOutcome,
    ResourceScope,
    RiskLevel,
    ToolEffect,
    ToolIntent,
)

_BUNDLE_VERSION = "harness-v2-20260808"
_PROJECT = uuid.UUID("11111111-1111-1111-1111-111111111111")
_OTHER_PROJECT = uuid.UUID("22222222-2222-2222-2222-222222222222")
_ACTOR = uuid.UUID("33333333-3333-3333-3333-333333333333")


def _context(environment: str = "production") -> PolicyEvaluationContext:
    """构造冻结的策略评估上下文；默认生产环境，使凭据使用需要人工审批。"""
    return PolicyEvaluationContext(
        environment=environment,
        policy_bundle_version=_BUNDLE_VERSION,
    )


def _intent(**overrides: object) -> ToolIntent:
    """构造一个默认有界的执行意图，测试只覆写关心的字段。"""
    intent = ToolIntent(
        tool_name="seatunnel.execute_pipeline",
        action="execute",
        project_id=_PROJECT,
        actor_id=_ACTOR,
        resource_scopes=[
            ResourceScope(
                project_id=_PROJECT,
                resource_type="pipeline_version",
                resource_id="v1",
            )
        ],
        effects=[ToolEffect.SECRET_USE, ToolEffect.WRITE],
        bounded=True,
    )
    for name, value in overrides.items():
        setattr(intent, name, value)
    return intent


async def _evaluate(
    intent: ToolIntent, context: PolicyEvaluationContext | None = None
) -> PolicyDecision:
    """用内嵌 PDP 裁决一次请求，返回可断言的决策对象。"""
    pdp = EmbeddedPolicyDecisionPoint()
    return await pdp.evaluate(PolicyEvaluationRequest(intent=intent, context=context or _context()))


def _rule_ids(decision: PolicyDecision) -> list[str]:
    """提取命中规则 ID，便于断言“哪些规则真的触发了”。"""
    return [match.rule_id for match in decision.rule_matches]


@pytest.mark.asyncio
async def test_baseline_allow_when_no_rule_matches() -> None:
    """只读且在本项目内的意图不命中任何风险规则，落到基线允许。"""
    decision = await _evaluate(
        _intent(effects=[ToolEffect.READ_ONLY]),
        _context(environment="local"),
    )
    assert decision.outcome is PolicyOutcome.ALLOW
    assert decision.risk_level is RiskLevel.P3
    assert _rule_ids(decision) == ["HARNESS-BASELINE-ALLOW"]
    assert decision.required_roles == []
    assert decision.required_approvals == 0


@pytest.mark.asyncio
async def test_cross_project_scope_is_denied_and_clears_approvals() -> None:
    """跨项目资源范围是 P0 拒绝；即使同时命中审批类规则也不给出放行路径。"""
    intent = _intent(
        resource_scopes=[
            ResourceScope(
                project_id=_OTHER_PROJECT,
                resource_type="pipeline_version",
                resource_id="v1",
            )
        ],
        effects=[ToolEffect.DESTRUCTIVE, ToolEffect.WRITE],
    )
    decision = await _evaluate(intent)
    assert decision.outcome is PolicyOutcome.DENY
    assert decision.risk_level is RiskLevel.P0
    assert "HARNESS-P0-CROSS-PROJECT" in _rule_ids(decision)
    # DENY 不存在可放行的审批路径，审批角色与人数必须被清空。
    assert decision.required_roles == []
    assert decision.required_approvals == 0


@pytest.mark.asyncio
async def test_secret_exfiltration_is_denied() -> None:
    """Secret 级数据叠加外发副作用直接拒绝。"""
    decision = await _evaluate(
        _intent(
            effects=[ToolEffect.EXTERNAL_EGRESS, ToolEffect.WRITE],
            data_classifications=[DataClassification.SECRET],
            external_destination="https://partner.example.com",
        )
    )
    assert decision.outcome is PolicyOutcome.DENY
    assert decision.risk_level is RiskLevel.P0
    assert "HARNESS-P0-SECRET-EXFILTRATION" in _rule_ids(decision)


@pytest.mark.asyncio
async def test_destructive_operation_requires_two_distinct_roles() -> None:
    """破坏性操作要求业务与安全两名审批人，共 2 个审批槽。"""
    decision = await _evaluate(_intent(effects=[ToolEffect.DESTRUCTIVE, ToolEffect.WRITE]))
    assert decision.outcome is PolicyOutcome.REQUIRE_APPROVAL
    assert decision.risk_level is RiskLevel.P0
    assert decision.required_approvals == 2
    assert set(decision.required_roles) == {
        HarnessRole.DATA_APPROVER,
        HarnessRole.SECURITY_APPROVER,
    }


@pytest.mark.asyncio
async def test_egress_rules_split_by_classification() -> None:
    """敏感/未分类数据外发是 P0 双人审批，内部数据外发降为 P1 单人审批。"""
    sensitive = await _evaluate(
        _intent(
            effects=[ToolEffect.EXTERNAL_EGRESS],
            data_classifications=[DataClassification.CONFIDENTIAL],
            external_destination="https://partner.example.com",
        )
    )
    assert "HARNESS-P0-SENSITIVE-EGRESS" in _rule_ids(sensitive)
    assert sensitive.risk_level is RiskLevel.P0
    assert sensitive.required_approvals == 2

    internal = await _evaluate(
        _intent(
            effects=[ToolEffect.EXTERNAL_EGRESS],
            data_classifications=[DataClassification.INTERNAL],
            external_destination="https://partner.example.com",
        )
    )
    assert "HARNESS-P1-INTERNAL-EGRESS" in _rule_ids(internal)
    assert internal.risk_level is RiskLevel.P1
    assert internal.required_roles == [HarnessRole.DATA_APPROVER]
    assert internal.required_approvals == 1


@pytest.mark.asyncio
async def test_unbounded_operation_requires_data_approver() -> None:
    """未冻结预算的意图不能自动执行，至少一名数据审批人。"""
    decision = await _evaluate(_intent(bounded=False))
    assert "HARNESS-P1-UNBOUNDED" in _rule_ids(decision)
    assert decision.outcome is PolicyOutcome.REQUIRE_APPROVAL
    assert decision.required_roles == [HarnessRole.DATA_APPROVER]


@pytest.mark.asyncio
async def test_privileged_mutation_is_matched_by_tool_name() -> None:
    """特权控制面变更是按 Broker 注册的工具名识别，不靠客户端自报副作用。"""
    decision = await _evaluate(
        _intent(tool_name="doris.rollback_release", action="rollback"),
    )
    assert "HARNESS-P1-PRIVILEGED-MUTATION" in _rule_ids(decision)
    assert decision.required_roles == [HarnessRole.DATA_APPROVER]


@pytest.mark.asyncio
async def test_secret_use_depends_on_environment() -> None:
    """生产环境的凭据使用必须绑定业务审批；非生产仅保留证据不要求审批。"""
    production = await _evaluate(_intent(), _context(environment="production"))
    assert "HARNESS-P1-SECRET-USE" in _rule_ids(production)
    assert production.outcome is PolicyOutcome.REQUIRE_APPROVAL
    assert production.required_roles == [HarnessRole.DATA_APPROVER]

    local = await _evaluate(_intent(), _context(environment="local"))
    assert "HARNESS-P1-SECRET-USE" in _rule_ids(local)
    assert local.outcome is PolicyOutcome.ALLOW
    assert local.required_approvals == 0


@pytest.mark.asyncio
async def test_isolated_rotation_steps_stay_auto_executable() -> None:
    """隔离暂存与固定范围验证不切换活跃引用，由一次性能力受控自动执行。"""
    for tool_name in ("secret.stage_rotation", "secret.validate_rotation"):
        decision = await _evaluate(_intent(tool_name=tool_name))
        assert decision.outcome is PolicyOutcome.ALLOW, tool_name
        assert decision.required_approvals == 0


@pytest.mark.asyncio
async def test_quality_gate_bypass_and_audit_tampering_are_denied() -> None:
    """质量门禁绕过与审计篡改都是 P0 拒绝，且不留审批后门。"""
    bypass = await _evaluate(_intent(metadata={"skip_quality_checks": True}))
    assert "HARNESS-P0-QUALITY-GATE-BYPASS" in _rule_ids(bypass)
    assert bypass.outcome is PolicyOutcome.DENY
    assert bypass.required_roles == []

    tampering = await _evaluate(_intent(action="audit.delete", effects=[ToolEffect.WRITE]))
    assert "HARNESS-P0-AUDIT-TAMPERING" in _rule_ids(tampering)
    assert tampering.outcome is PolicyOutcome.DENY
    assert tampering.required_roles == []


@pytest.mark.asyncio
async def test_aggregation_takes_most_severe_outcome_and_risk() -> None:
    """多规则命中时结果、风险、审批角色、审批人数分别按最严口径聚合。"""
    decision = await _evaluate(
        _intent(
            effects=[ToolEffect.DESTRUCTIVE, ToolEffect.SECRET_USE, ToolEffect.WRITE],
            bounded=False,
        )
    )
    hit = set(_rule_ids(decision))
    assert {"HARNESS-P0-DESTRUCTIVE", "HARNESS-P1-SECRET-USE", "HARNESS-P1-UNBOUNDED"} <= hit
    assert decision.outcome is PolicyOutcome.REQUIRE_APPROVAL
    # 风险取最高等级，不能用低风险规则平均稀释。
    assert decision.risk_level is RiskLevel.P0
    # 角色取并集且排序稳定，人数取最大值。
    assert decision.required_roles == sorted(
        {HarnessRole.DATA_APPROVER, HarnessRole.SECURITY_APPROVER},
        key=lambda value: value.value,
    )
    assert decision.required_approvals == 2


@pytest.mark.asyncio
async def test_decision_is_deterministic_for_identical_request() -> None:
    """同一份冻结请求得到同一决策 ID 与请求指纹，保证幂等落库。"""
    intent = _intent()
    context = _context()
    first = await _evaluate(intent, context)
    second = await _evaluate(intent, context)
    assert first.id == second.id
    assert first.request_fingerprint == second.request_fingerprint
    assert first.policy_bundle_version == _BUNDLE_VERSION
    assert first.expires_at > first.decided_at
