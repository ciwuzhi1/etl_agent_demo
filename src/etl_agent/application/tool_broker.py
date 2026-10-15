"""Tool Broker service for Chapter 11.

Implements the ToolBroker as the single in-process entry point for all tool
invocations, enforcing manifests, policy decisions, capability tokens, and
replay protection.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from pydantic import BaseModel, ValidationError

from etl_agent.application.evidence_ledger import require_safe_evidence_payload
from etl_agent.application.harness_policy import PolicyDecisionPoint
from etl_agent.domain.errors import DomainError, PermissionDenied, VersionConflict
from etl_agent.domain.harness import (
    CapabilityClaims,
    PolicyEvaluationContext,
    PolicyEvaluationRequest,
    PolicyOutcome,
    PreparedToolInvocation,
    ToolEffect,
    ToolIntent,
    ToolManifest,
    canonical_sha256,
)


class ToolHandler(Protocol):
    """定义 Broker 内部工具处理器；处理器不能接受未经验证的原始字典。"""

    async def __call__(
        self,
        payload: BaseModel,
        claims: CapabilityClaims | None,
    ) -> BaseModel:
        """执行已校验输入，并返回可按注册输出模型验证的结构化结果。"""
        ...


class CapabilityVerifier(Protocol):
    """定义只使用公钥的能力令牌验证端口。"""

    def verify(self, token: str) -> dict[str, Any]:
        """验证签名和时间并返回受保护声明。"""
        ...


class TokenReplayGuard(Protocol):
    """定义一次性令牌消费端口；生产实现必须使用持久化原子写。"""

    async def consume(self, token_id: str) -> bool:
        """首次消费返回 True，重复或并发消费返回 False。"""
        ...


@dataclass(frozen=True, slots=True)
class RegisteredTool:
    """把公开能力清单与进程内输入/输出类型和处理器绑定。"""

    manifest: ToolManifest
    input_model: type[BaseModel]
    output_model: type[BaseModel]
    handler: ToolHandler


class InMemoryTokenReplayGuard:
    """提供本地开发和单元测试的一次性令牌保护，生产环境不得使用。"""

    def __init__(self) -> None:
        """初始化进程内消费集合和异步锁。"""
        self._consumed: set[str] = set()
        self._lock = asyncio.Lock()

    async def consume(self, token_id: str) -> bool:
        """在单进程锁内执行检查并写入，模拟数据库唯一约束语义。"""
        async with self._lock:
            if token_id in self._consumed:
                return False
            self._consumed.add(token_id)
            return True


class ToolBroker:
    """作为所有工具调用的唯一进程内入口，执行清单、策略、令牌和重放门禁。"""

    def __init__(
        self,
        *,
        decision_point: PolicyDecisionPoint,
        capability_verifier: CapabilityVerifier | None = None,
        replay_guard: TokenReplayGuard | None = None,
    ) -> None:
        """注入 PDP 与可选令牌能力；缺少令牌组件时所有副作用调用失败关闭。"""
        self._decision_point = decision_point
        self._capability_verifier = capability_verifier
        self._replay_guard = replay_guard
        self._tools: dict[str, RegisteredTool] = {}

    def register(
        self,
        *,
        name: str,
        version: str,
        description: str,
        effects: list[ToolEffect],
        input_model: type[BaseModel],
        output_model: type[BaseModel],
        handler: ToolHandler,
        timeout_seconds: float = 30,
        max_records: int | None = None,
        max_bytes: int | None = None,
        idempotent: bool = False,
        compensation_tool: str | None = None,
    ) -> ToolManifest:
        """注册版本化能力清单；同名工具禁止静默覆盖实现或副作用。"""
        if name in self._tools:
            raise VersionConflict(f"Tool Broker 已注册工具 {name}")
        manifest = ToolManifest(
            name=name,
            version=version,
            description=description,
            effects=effects,
            input_schema=input_model.model_json_schema(),
            output_schema=output_model.model_json_schema(),
            timeout_seconds=timeout_seconds,
            max_records=max_records,
            max_bytes=max_bytes,
            idempotent=idempotent,
            compensation_tool=compensation_tool,
        )
        self._tools[name] = RegisteredTool(
            manifest=manifest,
            input_model=input_model,
            output_model=output_model,
            handler=handler,
        )
        return manifest

    def manifests(self) -> list[ToolManifest]:
        """按名称返回公开能力清单，绝不暴露处理器或运行时凭据。"""
        return [self._tools[name].manifest for name in sorted(self._tools)]

    async def prepare(
        self,
        *,
        intent: ToolIntent,
        context: PolicyEvaluationContext,
        raw_input: dict[str, Any],
    ) -> tuple[PreparedToolInvocation, BaseModel]:
        """验证输入并调用 PDP，返回不包含原始载荷的冻结调用证据。"""
        registered = self._require_tool(intent.tool_name)
        # 副作用由代码注册清单决定；Agent 少报或多报都表示意图已被篡改或过期。
        if intent.effects != registered.manifest.effects:
            raise DomainError("工具意图声明的副作用与 Broker 能力清单不一致")
        require_safe_evidence_payload(raw_input, path="tool_input")
        try:
            validated_input = registered.input_model.model_validate(raw_input)
        except ValidationError as exc:
            raise DomainError("工具输入不符合注册 Schema") from exc
        request = PolicyEvaluationRequest(intent=intent, context=context)
        decision = await self._decision_point.evaluate(request)
        prepared = PreparedToolInvocation(
            invocation_id=intent.invocation_id,
            project_id=intent.project_id,
            actor_id=intent.actor_id,
            tool_name=registered.manifest.name,
            tool_version=registered.manifest.version,
            input_fingerprint=canonical_sha256(validated_input),
            resource_fingerprint=canonical_sha256(intent.resource_scopes),
            policy_decision=decision,
        )
        return prepared, validated_input

    async def execute(
        self,
        *,
        prepared: PreparedToolInvocation,
        intent: ToolIntent,
        context: PolicyEvaluationContext,
        raw_input: dict[str, Any],
        capability_token: str | None = None,
    ) -> BaseModel:
        """校验冻结 Preparation 后执行；副作用必须绑定对应的一次性能力令牌。"""
        registered = self._require_tool(intent.tool_name)
        if intent.effects != registered.manifest.effects:
            raise DomainError("工具意图声明的副作用与 Broker 能力清单不一致")
        require_safe_evidence_payload(raw_input, path="tool_input")
        try:
            validated_input = registered.input_model.model_validate(raw_input)
        except ValidationError as exc:
            raise DomainError("工具输入不符合注册 Schema") from exc
        self._require_prepared_match(
            prepared=prepared,
            intent=intent,
            context=context,
            validated_input=validated_input,
            manifest=registered.manifest,
        )
        decision = prepared.policy_decision
        if decision.outcome in {PolicyOutcome.DENY, PolicyOutcome.DEFER}:
            raise PermissionDenied("策略未授权该工具调用")
        requires_token = (
            decision.outcome is PolicyOutcome.REQUIRE_APPROVAL
            or registered.manifest.effects != [ToolEffect.READ_ONLY]
        )
        claims: CapabilityClaims | None = None
        if requires_token:
            claims = await self._verify_and_consume_token(
                token=capability_token,
                prepared=prepared,
                context=context,
                effects=registered.manifest.effects,
                artifact_sha256=intent.artifact_sha256,
            )
        try:
            async with asyncio.timeout(registered.manifest.timeout_seconds):
                result = await registered.handler(validated_input, claims)
        except TimeoutError as exc:
            raise DomainError(f"工具 {intent.tool_name} 执行超时") from exc
        try:
            validated_output = registered.output_model.model_validate(result)
        except ValidationError as exc:
            raise DomainError("工具输出不符合注册 Schema") from exc
        require_safe_evidence_payload(
            validated_output.model_dump(mode="json"),
            path="tool_output",
        )
        return validated_output

    @staticmethod
    def _require_prepared_match(
        *,
        prepared: PreparedToolInvocation,
        intent: ToolIntent,
        context: PolicyEvaluationContext,
        validated_input: BaseModel,
        manifest: ToolManifest,
    ) -> None:
        """重新计算所有冻结指纹，阻止 Prepare 后替换输入、资源、工具或策略包。"""
        decision = prepared.policy_decision
        current_facts = {
            "invocation_id": intent.invocation_id,
            "project_id": intent.project_id,
            "actor_id": intent.actor_id,
            "tool_name": manifest.name,
            "tool_version": manifest.version,
            "input_fingerprint": canonical_sha256(validated_input),
            "resource_fingerprint": canonical_sha256(intent.resource_scopes),
            "policy_request_fingerprint": canonical_sha256(
                PolicyEvaluationRequest(intent=intent, context=context)
            ),
            "policy_bundle_version": context.policy_bundle_version,
            "artifact_sha256": intent.artifact_sha256,
        }
        frozen_facts = {
            "invocation_id": prepared.invocation_id,
            "project_id": prepared.project_id,
            "actor_id": prepared.actor_id,
            "tool_name": prepared.tool_name,
            "tool_version": prepared.tool_version,
            "input_fingerprint": prepared.input_fingerprint,
            "resource_fingerprint": prepared.resource_fingerprint,
            "policy_request_fingerprint": decision.request_fingerprint,
            "policy_bundle_version": decision.policy_bundle_version,
            "artifact_sha256": decision.artifact_sha256,
        }
        if current_facts != frozen_facts:
            raise PermissionDenied("当前工具调用与冻结 Preparation 不一致")
        if decision.project_id != intent.project_id or decision.actor_id != intent.actor_id:
            raise PermissionDenied("冻结策略决策主体与当前调用不一致")
        if decision.expires_at is None or decision.expires_at <= datetime.now(UTC):
            raise PermissionDenied("冻结策略决策已经过期，请重新 Prepare")

    async def _verify_and_consume_token(
        self,
        *,
        token: str | None,
        prepared: PreparedToolInvocation,
        context: PolicyEvaluationContext,
        effects: list[ToolEffect],
        artifact_sha256: str | None,
    ) -> CapabilityClaims:
        """验证令牌全部绑定字段，并在调用处理器前原子消费一次性 Token ID。"""
        if token is None or self._capability_verifier is None or self._replay_guard is None:
            raise PermissionDenied("副作用工具调用缺少完整能力令牌验证链")
        claims_dict = self._capability_verifier.verify(token)
        
        # Convert dict to CapabilityClaims
        claims = CapabilityClaims(**claims_dict)
        
        expected = {
            "project_id": prepared.project_id,
            "subject_id": prepared.actor_id,
            "environment": context.environment,
            "policy_decision_id": prepared.policy_decision.id,
            "policy_bundle_version": prepared.policy_decision.policy_bundle_version,
            "tool_name": prepared.tool_name,
            "tool_version": prepared.tool_version,
            "input_fingerprint": prepared.input_fingerprint,
            "resource_fingerprint": prepared.resource_fingerprint,
            "artifact_sha256": artifact_sha256,
            "effects": effects,
        }
        actual = {
            "project_id": claims.project_id,
            "subject_id": claims.subject_id,
            "environment": claims.environment,
            "policy_decision_id": claims.policy_decision_id,
            "policy_bundle_version": claims.policy_bundle_version,
            "tool_name": claims.tool_name,
            "tool_version": claims.tool_version,
            "input_fingerprint": claims.input_fingerprint,
            "resource_fingerprint": claims.resource_fingerprint,
            "artifact_sha256": claims.artifact_sha256,
            "effects": claims.effects,
        }
        if actual != expected:
            raise PermissionDenied("能力令牌与当前工具、输入、资源或策略决策不一致")
        if not claims.single_use:
            raise PermissionDenied("副作用工具只接受一次性能力令牌")
        if not await self._replay_guard.consume(str(claims.token_id)):
            raise PermissionDenied("能力令牌已经使用，禁止重放")
        return claims

    def _require_tool(self, name: str) -> RegisteredTool:
        """读取已注册工具；未知工具失败关闭，不能动态导入或猜测处理器。"""
        registered = self._tools.get(name)
        if registered is None:
            raise PermissionDenied(f"Tool Broker 未注册工具 {name}")
        return registered
