"""LangGraph 六节点状态机骨架。

第 8 章核心：用 LangGraph 管理从"请求生成"到"等待审批"的完整流程。
六节点：generate → clarify → repair_generation → policy → wait_for_review → failed

本章先建接口定义，等 LangGraph 包安装后再补完整状态机逻辑。
"""

from collections.abc import Awaitable, Callable
from typing import Any, Literal, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from etl_agent.application.generation import (
    ConversationMessage,
    GenerationRequest,
    GenerationResult,
    PipelineGenerationPort,
)
from etl_agent.application.policy import (
    require_safe_generation_context,
)
from etl_agent.application.schema_contract import (
    canonicalize_managed_file_source_contract,
    require_database_endpoint_contract,
    require_hdfs_file_contract,
    require_managed_file_source_contract,
    require_profile_topology_contract,
    require_target_schema_contract,
)
from etl_agent.domain.enums import GenerationKind
from etl_agent.domain.errors import DomainError

MAX_ENTERPRISE_REPAIR_ATTEMPTS = 2


class AgentState(TypedDict, total=False):
    """LangGraph 状态，最小化恢复所需字段。"""

    requirement: str
    metadata_context: dict[str, Any]
    conversation_history: list[dict[str, Any]]
    generation: dict[str, Any]
    phase: str
    error: str
    repair_count: int
    engine_check: dict[str, Any]


def build_generation_graph(
    generator: PipelineGenerationPort,
    checkpointer: Any | None = None,
    candidate_validator: Callable[[str], Awaitable[dict[str, Any]]] | None = None,
) -> Any:
    """构建显式 Agent 状态机，并把确定性策略与真实引擎检查纳入共享修复预算。

    ``candidate_validator`` 由 Worker 注入，用于在不把凭据写入图状态的前提下完成
    运行期变量物化和真实 SeaTunnel ``--check``。引擎未通过时只把已脱敏摘要反馈
    给模型，并与格式错误、企业策略错误共同消耗最多三份候选的总预算。
    """

    async def generate_node(state: AgentState) -> AgentState:
        """在Agent 生状态机中生成结构化结果并保持失败语义明确；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        history = [
            ConversationMessage.model_validate(message)
            for message in state.get("conversation_history", [])
        ]
        result = await generator.generate(
            GenerationRequest(
                requirement=state["requirement"],
                metadata_context=state.get("metadata_context", {}),
                conversation_history=history,
            )
        )
        return {
            "generation": result.model_dump(mode="json"),
            "phase": result.kind.value,
            "error": "",
        }

    def route_generation(
        state: AgentState,
    ) -> Literal["clarify", "policy", "repair_generation"]:
        """根据生成结果选择澄清、策略校验或失败分支，避免不完整配置进入后续门禁。"""
        result = GenerationResult.model_validate(state["generation"])
        if result.kind is GenerationKind.QUESTION:
            return "clarify"
        if result.kind is GenerationKind.CONFIG:
            return "policy"
        return "repair_generation"

    def repair_generation_node(state: AgentState) -> AgentState:
        """把截断或不完整候选纳入三份总预算；预算耗尽后确定性终止。"""
        result = GenerationResult.model_validate(state["generation"])
        repair_count = state.get("repair_count", 0)
        if repair_count >= MAX_ENTERPRISE_REPAIR_ATTEMPTS:
            return {
                "phase": "failed",
                "error": "生成器在三份候选后仍未返回完整配置："
                + (result.explanation or "候选格式无效"),
            }
        next_count = repair_count + 1
        return {
            "phase": "generation_retry",
            "repair_count": next_count,
            "requirement": (
                f"{state['requirement']}\n\n"
                f"候选完整性反馈（修复 {next_count}/{MAX_ENTERPRISE_REPAIR_ATTEMPTS}）："
                f"{result.explanation or '配置不完整'}。"
                "请重新返回一份花括号与三引号均闭合、且只包含一个 ```hocon``` "
                "代码块的完整 SeaTunnel 配置。"
            ),
            "error": "",
        }

    def route_generation_repair(state: AgentState) -> Literal["generate", "failed"]:
        """仅在总候选预算尚未耗尽时再次调用模型，禁止无限自修复。"""
        return "generate" if state["phase"] == "generation_retry" else "failed"

    def clarify_node(state: AgentState) -> AgentState:
        """把必要澄清转为可持久化中断，等待用户补充后从同一检查点继续。"""
        result = GenerationResult.model_validate(state["generation"])
        answer = interrupt({"kind": "clarification", "question": result.question})
        return {
            "requirement": f"{state['requirement']}\n\n用户补充：{answer}",
            "phase": "clarification_received",
        }

    async def policy_node(state: AgentState) -> AgentState:
        """执行确定性策略及真实引擎校验，并在统一预算内驱动安全修复。"""
        result = GenerationResult.model_validate(state["generation"])

        # 规范化连接器契约
        from etl_agent.application.policy import (
            canonicalize_supported_connector_contract,
            canonicalize_supported_transform_contract,
        )

        canonical_hocon = canonicalize_supported_connector_contract(result.hocon or "")
        canonical_hocon = canonicalize_supported_transform_contract(canonical_hocon)

        # 规范化 Profile 占位符（如果 credential_materialization 模块存在）
        try:
            from etl_agent.application.credential_materialization import (
                canonicalize_profile_placeholders,
            )

            canonical_hocon = canonicalize_profile_placeholders(
                canonical_hocon,
                state.get("metadata_context", {}).get("profiles", []),
            )
        except ImportError:
            pass

        # 规范化受管文件 Source 契约
        canonical_hocon = canonicalize_managed_file_source_contract(
            state.get("metadata_context", {}).get("profiles", []),
            canonical_hocon,
        )

        if canonical_hocon != result.hocon:
            result = result.model_copy(update={"hocon": canonical_hocon})

        # 构建质量/增量/行LLM契约计划
        plan: dict[str, Any] = {}
        quality_contract = state.get("metadata_context", {}).get("quality_contract")
        if quality_contract is not None:
            plan["quality_contract"] = quality_contract
        incremental_contract = state.get("metadata_context", {}).get("incremental_contract")
        if incremental_contract is not None:
            plan["incremental_contract"] = incremental_contract
        row_llm_contract = state.get("metadata_context", {}).get("row_llm_contract")
        if row_llm_contract is not None:
            plan["row_llm_contract"] = row_llm_contract

        try:
            # 执行所有 schema_contract 验证函数
            require_safe_generation_context(result.hocon or "")
            require_profile_topology_contract(
                state.get("metadata_context", {}).get("profiles", []),
                result.hocon or "",
            )
            require_managed_file_source_contract(
                state.get("metadata_context", {}).get("profiles", []),
                result.hocon or "",
            )
            require_hdfs_file_contract(
                state.get("metadata_context", {}).get("profiles", []),
                result.hocon or "",
            )
            require_database_endpoint_contract(
                state.get("metadata_context", {}).get("profiles", []),
                result.hocon or "",
            )
            require_target_schema_contract(
                state.get("metadata_context", {}).get("profiles", []),
                result.hocon or "",
            )

            # 尝试执行质量契约验证（如果模块存在）
            try:
                from etl_agent.application.quality import (  # type: ignore[import-not-found]
                    require_quality_contract_routing,
                    require_quality_contract_sinks,
                    require_quality_rule_predicates,
                    require_quality_rule_traceability,
                )

                require_quality_contract_sinks(plan, result.hocon or "")
                require_quality_contract_routing(plan, result.hocon or "")
                require_quality_rule_traceability(plan, result.hocon or "")
                require_quality_rule_predicates(plan, result.hocon or "")
            except ImportError:
                pass

            # 尝试执行增量契约验证（如果模块存在）
            try:
                from etl_agent.application.incremental import (  # type: ignore[import-not-found]
                    require_incremental_hocon_contract,
                )

                require_incremental_hocon_contract(plan, result.hocon or "")
            except ImportError:
                pass

            # 尝试执行行LLM契约验证（如果模块存在）
            try:
                from etl_agent.application.row_llm import (  # type: ignore[import-not-found]
                    require_row_llm_hocon_contract,
                )

                require_row_llm_hocon_contract(plan, result.hocon or "")
            except ImportError:
                pass

            # 真实引擎检查必须发生在人工审批中断之前
            engine_check: dict[str, Any] = {"valid": True, "phase2_check": "NOT_CONFIGURED"}
            if candidate_validator is not None:
                engine_check = await candidate_validator(result.hocon or "")
                if engine_check.get("valid") is not True:
                    summary = str(engine_check.get("summary") or "SeaTunnel CLI --check 未通过")
                    raise DomainError("SeaTunnel 真实引擎检查未通过：" + summary)
        except DomainError as exc:
            repair_count = state.get("repair_count", 0)
            if repair_count >= MAX_ENTERPRISE_REPAIR_ATTEMPTS:
                return {
                    "phase": "enterprise_policy_failed",
                    "error": "企业确定性门禁在三份候选配置后仍未通过：" + str(exc),
                }
            next_count = repair_count + 1
            return {
                "phase": "enterprise_policy_retry",
                "repair_count": next_count,
                "requirement": (
                    f"{state['requirement']}\n\n"
                    f"企业确定性门禁反馈（修复 {next_count}/"
                    f"{MAX_ENTERPRISE_REPAIR_ATTEMPTS}）：{exc}。"
                    "请返回修复后的完整 SeaTunnel HOCON，不要加入任何明文凭据。"
                ),
            }
        return {
            "generation": result.model_dump(mode="json"),
            "engine_check": engine_check,
            "phase": "waiting_enterprise_approval",
            "error": "",
        }

    def route_policy(state: AgentState) -> Literal["generate", "wait_for_review", "failed"]:
        """依据策略结果和修复次数决定人工复核、再次生成或失败终态，防止无限循环。"""
        if state["phase"] == "enterprise_policy_retry":
            return "generate"
        if state["phase"] == "waiting_enterprise_approval":
            return "wait_for_review"
        return "failed"

    def wait_for_review_node(state: AgentState) -> AgentState:
        """在机器门禁通过后暂停流程，明确把最终执行授权留给独立企业审批。"""
        interrupt(
            {
                "kind": "enterprise_approval",
                "generation": state["generation"],
            }
        )
        return {"phase": "enterprise_approval_verified"}

    def failed_node(state: AgentState) -> AgentState:
        """在Agent 生状态机中执行"failed"对应的内部步骤；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        if state.get("error"):
            return {"phase": "failed", "error": state["error"]}
        result = GenerationResult.model_validate(state["generation"])
        return {"phase": "failed", "error": result.explanation or "生成失败"}

    # 构建图
    graph = StateGraph(AgentState)
    graph.add_node("generate", generate_node)
    graph.add_node("clarify", clarify_node)
    graph.add_node("repair_generation", repair_generation_node)
    graph.add_node("policy", policy_node)
    graph.add_node("wait_for_review", wait_for_review_node)
    graph.add_node("failed", failed_node)

    # 连接边
    graph.add_edge(START, "generate")
    graph.add_conditional_edges("generate", route_generation)
    graph.add_edge("clarify", "generate")
    graph.add_conditional_edges(
        "repair_generation",
        route_generation_repair,
        {"generate": "generate", "failed": "failed"},
    )
    graph.add_conditional_edges(
        "policy",
        route_policy,
        {"generate": "generate", "wait_for_review": "wait_for_review", "failed": "failed"},
    )
    graph.add_edge("wait_for_review", END)
    graph.add_edge("failed", END)

    return graph.compile(checkpointer=checkpointer)
