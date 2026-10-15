"""第 11 章单元测试：Tool Broker 的清单、冻结校验、一次性能力与失败关闭。

覆盖：
- 清单注册不可静默覆盖；未注册工具失败关闭；
- 意图声明的副作用必须与注册清单一致（Agent 少报/多报都拒绝）；
- 普通证据层禁止出现秘密字段与原始正文；
- Prepare 冻结输入/资源/策略指纹，执行前重算比对，替换输入即被拒绝；
- 只读 + 策略允许时不签发令牌；有副作用时必须绑定一次性能力令牌；
- 令牌绑定字段、single_use、重放二次消费、过期策略决策全部失败关闭；
- 任一失败路径下注册处理器都不被调用，杜绝“先执行后校验”。
"""

import uuid
from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint
from etl_agent.application.tool_broker import InMemoryTokenReplayGuard, ToolBroker
from etl_agent.domain.errors import DomainError, PermissionDenied, VersionConflict
from etl_agent.domain.harness import (
    PolicyEvaluationContext,
    PolicyEvaluationRequest,
    ResourceScope,
    ToolEffect,
    ToolIntent,
    canonical_sha256,
)

_BUNDLE_VERSION = "harness-v2-20260808"
_PROJECT = uuid.UUID("11111111-1111-1111-1111-111111111111")
_ACTOR = uuid.UUID("33333333-3333-3333-3333-333333333333")
_READ_TOOL = "etl.read_pipeline_version"
_WRITE_TOOL = "seatunnel.execute_pipeline"
_RAW_INPUT: dict[str, Any] = {"value": "x"}


class _ProbeInput(BaseModel):
    """测试输入模型：只允许一个字符串参数，避免测试耦合真实业务 Schema。"""

    value: str


class _ProbeOutput(BaseModel):
    """测试输出模型：回显处理结果，便于断言处理器确实被调用。"""

    result: str


class _CountingHandler:
    """记录调用次数的处理器，用于证明失败路径下副作用处理器从未被触达。"""

    def __init__(self) -> None:
        """初始化调用计数器。"""
        self.calls = 0

    async def __call__(self, payload: BaseModel, claims: Any) -> BaseModel:
        """累加调用次数并返回结构化输出。"""
        self.calls += 1
        return _ProbeOutput(result=f"handled:{payload.value}")


class _StubVerifier:
    """把预设令牌映射到声明字典，替代真实 Ed25519 验签，聚焦 Broker 语义。"""

    def __init__(self) -> None:
        """初始化空的令牌 → 声明映射。"""
        self._tokens: dict[str, dict[str, Any]] = {}

    def register(self, token: str, claims: dict[str, Any]) -> None:
        """登记一次性令牌声明；声明由测试按冻结结果构造。"""
        self._tokens[token] = claims

    def verify(self, token: str) -> dict[str, Any]:
        """返回预设声明；未知令牌按验签失败处理。"""
        if token not in self._tokens:
            raise DomainError("Capability token is invalid")
        return self._tokens[token]


def _context(environment: str = "production") -> PolicyEvaluationContext:
    """构造冻结的策略评估上下文。"""
    return PolicyEvaluationContext(
        environment=environment,
        policy_bundle_version=_BUNDLE_VERSION,
    )


def _intent(
    *,
    tool_name: str = _READ_TOOL,
    effects: list[ToolEffect] | None = None,
    **overrides: Any,
) -> ToolIntent:
    """构造工具意图；默认只读且资源范围限定在本项目内。"""
    intent = ToolIntent(
        tool_name=tool_name,
        action="invoke",
        project_id=_PROJECT,
        actor_id=_ACTOR,
        resource_scopes=[
            ResourceScope(
                project_id=_PROJECT,
                resource_type="pipeline_version",
                resource_id="v1",
            )
        ],
        effects=effects if effects is not None else [ToolEffect.READ_ONLY],
        bounded=True,
    )
    for name, value in overrides.items():
        setattr(intent, name, value)
    return intent


def _broker(
    *,
    handler: _CountingHandler | None = None,
    guard: InMemoryTokenReplayGuard | None = None,
    verifier: _StubVerifier | None = None,
    decision_ttl: timedelta = timedelta(days=1),
) -> tuple[ToolBroker, _CountingHandler]:
    """装配只读与有副作用两个工具的 Broker，返回实例与共用的处理器。"""
    probe = handler or _CountingHandler()
    broker = ToolBroker(
        decision_point=EmbeddedPolicyDecisionPoint(decision_ttl=decision_ttl),
        capability_verifier=verifier,
        replay_guard=guard,
    )
    broker.register(
        name=_READ_TOOL,
        version="1.0.0",
        description="读取只读版本元数据",
        effects=[ToolEffect.READ_ONLY],
        input_model=_ProbeInput,
        output_model=_ProbeOutput,
        handler=probe,
    )
    broker.register(
        name=_WRITE_TOOL,
        version="1.0.0",
        description="提交执行制品",
        effects=[ToolEffect.WRITE],
        input_model=_ProbeInput,
        output_model=_ProbeOutput,
        handler=probe,
        timeout_seconds=5,
    )
    return broker, probe


def _claims_for(
    prepared: Any,
    intent: ToolIntent,
    context: PolicyEvaluationContext,
    **overrides: Any,
) -> dict[str, Any]:
    """按冻结结果构造与 Prepared 完全对齐的声明，供负面用例覆写单个字段。"""
    claims: dict[str, Any] = {
        "token_id": uuid.uuid4(),
        "project_id": prepared.project_id,
        "subject_id": prepared.actor_id,
        "environment": context.environment,
        "policy_decision_id": prepared.policy_decision.id,
        "policy_bundle_version": prepared.policy_decision.policy_bundle_version,
        "tool_name": prepared.tool_name,
        "tool_version": prepared.tool_version,
        "input_fingerprint": prepared.input_fingerprint,
        "resource_fingerprint": prepared.resource_fingerprint,
        "artifact_sha256": intent.artifact_sha256,
        "effects": [ToolEffect.WRITE],
        "expires_at": "2030-01-01T00:00:00Z",
        "single_use": True,
    }
    claims.update(overrides)
    return claims


async def _prepare_write(
    *,
    verifier: _StubVerifier | None = None,
    guard: InMemoryTokenReplayGuard | None = None,
    probe: _CountingHandler | None = None,
    decision_ttl: timedelta = timedelta(days=1),
) -> tuple[ToolBroker, _CountingHandler, Any, ToolIntent, PolicyEvaluationContext]:
    """准备一次有副作用的调用，返回装配好的 Broker 与冻结结果。"""
    context = _context(environment="local")
    intent = _intent(tool_name=_WRITE_TOOL, effects=[ToolEffect.WRITE])
    broker, handler = _broker(
        handler=probe, guard=guard, verifier=verifier, decision_ttl=decision_ttl
    )
    prepared, _ = await broker.prepare(intent=intent, context=context, raw_input=_RAW_INPUT)
    return broker, handler, prepared, intent, context


@pytest.mark.asyncio
async def test_register_rejects_silent_override() -> None:
    """同名工具重复注册直接失败，避免实现或副作用被悄悄替换。"""
    broker, _ = _broker()
    with pytest.raises(VersionConflict):
        broker.register(
            name=_READ_TOOL,
            version="2.0.0",
            description="覆盖实现",
            effects=[ToolEffect.READ_ONLY],
            input_model=_ProbeInput,
            output_model=_ProbeOutput,
            handler=_CountingHandler(),
        )


@pytest.mark.asyncio
async def test_unknown_tool_fails_closed() -> None:
    """未注册工具在执行前失败关闭，不允许动态导入或用默认处理器兜底。"""
    broker, probe = _broker()
    with pytest.raises(PermissionDenied):
        await broker.prepare(
            intent=_intent(tool_name="unknown.tool"),
            context=_context(),
            raw_input=_RAW_INPUT,
        )
    assert probe.calls == 0


@pytest.mark.asyncio
async def test_effect_declaration_must_match_manifest() -> None:
    """意图声明的副作用与注册清单不一致时拒绝，Agent 少报或高报都不放行。"""
    broker, probe = _broker()
    with pytest.raises(DomainError):
        await broker.prepare(
            intent=_intent(tool_name=_WRITE_TOOL, effects=[ToolEffect.READ_ONLY]),
            context=_context(),
            raw_input=_RAW_INPUT,
        )
    assert probe.calls == 0


@pytest.mark.asyncio
async def test_raw_input_cannot_smuggle_secrets_or_raw_payload() -> None:
    """普通证据层禁止 secret / password / 原始正文等键进入 Broker 输入。"""
    broker, probe = _broker()
    for forbidden in ("password", "secret", "token", "raw_rows", "prompt"):
        with pytest.raises(DomainError):
            await broker.prepare(
                intent=_intent(),
                context=_context(),
                raw_input={"value": "x", forbidden: "leak"},
            )
    assert probe.calls == 0


@pytest.mark.asyncio
async def test_read_only_allow_path_skips_capability_token() -> None:
    """只读且策略允许的调用无需令牌即可执行，处理器被调用一次。"""
    broker, probe = _broker()
    context = _context(environment="local")
    intent = _intent()
    prepared, validated = await broker.prepare(intent=intent, context=context, raw_input=_RAW_INPUT)
    assert validated.value == "x"
    assert prepared.input_fingerprint == canonical_sha256(validated)
    result = await broker.execute(
        prepared=prepared,
        intent=intent,
        context=context,
        raw_input=_RAW_INPUT,
    )
    assert result.result == "handled:x"
    assert probe.calls == 1


@pytest.mark.asyncio
async def test_side_effect_without_capability_chain_fails_closed() -> None:
    """有副作用的工具在缺少完整令牌验证链时直接拒绝。"""
    broker, probe, prepared, intent, context = await _prepare_write()
    with pytest.raises(PermissionDenied):
        await broker.execute(
            prepared=prepared,
            intent=intent,
            context=context,
            raw_input=_RAW_INPUT,
            capability_token=None,
        )
    assert probe.calls == 0


@pytest.mark.asyncio
async def test_capability_token_is_consumed_once() -> None:
    """合法令牌首次执行成功，同令牌二次执行被拒绝且处理器不再被调用。"""
    verifier = _StubVerifier()
    probe = _CountingHandler()
    broker, _, prepared, intent, context = await _prepare_write(
        verifier=verifier, guard=InMemoryTokenReplayGuard(), probe=probe
    )
    verifier.register("token-once", _claims_for(prepared, intent, context))

    first = await broker.execute(
        prepared=prepared,
        intent=intent,
        context=context,
        raw_input=_RAW_INPUT,
        capability_token="token-once",
    )
    assert first.result == "handled:x"
    assert probe.calls == 1

    with pytest.raises(PermissionDenied):
        await broker.execute(
            prepared=prepared,
            intent=intent,
            context=context,
            raw_input=_RAW_INPUT,
            capability_token="token-once",
        )
    assert probe.calls == 1


@pytest.mark.asyncio
async def test_token_binding_mismatch_fails_closed() -> None:
    """令牌任一绑定字段被改写（输入、资源、项目、工具、策略、单次标记）都必须拒绝。"""
    for field, wrong_value in (
        ("input_fingerprint", "f" * 64),
        ("resource_fingerprint", "e" * 64),
        ("project_id", uuid.uuid4()),
        ("tool_name", "other.tool"),
        ("policy_decision_id", uuid.uuid4()),
        ("effects", [ToolEffect.DESTRUCTIVE]),
        ("single_use", False),
    ):
        verifier = _StubVerifier()
        probe = _CountingHandler()
        broker, _, prepared, intent, context = await _prepare_write(
            verifier=verifier, guard=InMemoryTokenReplayGuard(), probe=probe
        )
        token = f"token-{field}"
        verifier.register(token, _claims_for(prepared, intent, context, **{field: wrong_value}))
        with pytest.raises(PermissionDenied):
            await broker.execute(
                prepared=prepared,
                intent=intent,
                context=context,
                raw_input=_RAW_INPUT,
                capability_token=token,
            )
        assert probe.calls == 0, field


@pytest.mark.asyncio
async def test_prepared_fingerprint_is_rechecked_before_execute() -> None:
    """Prepare 之后替换输入，执行端重算指纹不一致即拒绝。"""
    verifier = _StubVerifier()
    probe = _CountingHandler()
    broker, _, prepared, intent, context = await _prepare_write(
        verifier=verifier, guard=InMemoryTokenReplayGuard(), probe=probe
    )
    verifier.register("token-swap", _claims_for(prepared, intent, context))

    with pytest.raises(PermissionDenied):
        await broker.execute(
            prepared=prepared,
            intent=intent,
            context=context,
            raw_input={"value": "swapped"},
            capability_token="token-swap",
        )
    assert probe.calls == 0


@pytest.mark.asyncio
async def test_expired_policy_decision_fails_closed() -> None:
    """冻结策略决策过期后不得执行，必须重新 Prepare。"""
    verifier = _StubVerifier()
    probe = _CountingHandler()
    broker, _, prepared, intent, context = await _prepare_write(
        verifier=verifier,
        guard=InMemoryTokenReplayGuard(),
        probe=probe,
        decision_ttl=timedelta(seconds=0),
    )
    verifier.register("token-expired", _claims_for(prepared, intent, context))

    with pytest.raises(PermissionDenied):
        await broker.execute(
            prepared=prepared,
            intent=intent,
            context=context,
            raw_input=_RAW_INPUT,
            capability_token="token-expired",
        )
    assert probe.calls == 0


@pytest.mark.asyncio
async def test_manifests_expose_schema_without_handlers() -> None:
    """公开清单只包含机器可读 Schema，绝不暴露处理器或运行时凭据。"""
    broker, _ = _broker()
    manifests = {manifest.name: manifest for manifest in broker.manifests()}
    assert set(manifests) == {_READ_TOOL, _WRITE_TOOL}
    read_manifest = manifests[_READ_TOOL]
    assert read_manifest.effects == [ToolEffect.READ_ONLY]
    assert "value" in read_manifest.input_schema.get("properties", {})
    assert "handler" not in read_manifest.model_dump()


@pytest.mark.asyncio
async def test_policy_request_is_recomputed_from_frozen_facts() -> None:
    """策略请求指纹取自冻结事实，相同意图与上下文得到相同裁决 ID。"""
    broker, _ = _broker()
    context = _context(environment="local")
    intent = _intent()
    first, _ = await broker.prepare(intent=intent, context=context, raw_input=_RAW_INPUT)
    second, _ = await broker.prepare(intent=intent, context=context, raw_input=_RAW_INPUT)
    assert first.policy_decision.id == second.policy_decision.id
    assert first.input_fingerprint == second.input_fingerprint
    expected = canonical_sha256(PolicyEvaluationRequest(intent=intent, context=context))
    assert first.policy_decision.request_fingerprint == expected
