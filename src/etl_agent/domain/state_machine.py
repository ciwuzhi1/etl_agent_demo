from collections.abc import Mapping
from enum import StrEnum
from typing import TypeVar

from etl_agent.domain.enums import AgentRunStatus, ExecutionStatus, PipelineVersionStatus
from etl_agent.domain.errors import InvalidStateTransition

StateT = TypeVar("StateT", bound=StrEnum)


PIPELINE_TRANSITIONS: Mapping[PipelineVersionStatus, frozenset[PipelineVersionStatus]] = {
    # DRAFT 既可经显式 PROFILING 进入 GENERATED，也可直接进入 GENERATED：
    # 当前产品没有单独"发起探查"的入口，若只保留 DRAFT→PROFILING，生成链路会从
    # 第一步就断死。PROFILING 作为可选中间态保留，仍可被后续探查流程使用。
    PipelineVersionStatus.DRAFT: frozenset(
        {PipelineVersionStatus.PROFILING, PipelineVersionStatus.GENERATED}
    ),
    PipelineVersionStatus.PROFILING: frozenset({PipelineVersionStatus.GENERATED}),
    PipelineVersionStatus.GENERATED: frozenset({PipelineVersionStatus.VALIDATED}),
    PipelineVersionStatus.VALIDATED: frozenset({PipelineVersionStatus.WAITING_APPROVAL}),
    PipelineVersionStatus.WAITING_APPROVAL: frozenset(
        {PipelineVersionStatus.APPROVED, PipelineVersionStatus.REJECTED}
    ),
    PipelineVersionStatus.APPROVED: frozenset(),
    PipelineVersionStatus.REJECTED: frozenset(),
}

EXECUTION_TRANSITIONS: Mapping[ExecutionStatus, frozenset[ExecutionStatus]] = {
    ExecutionStatus.QUEUED: frozenset(
        {ExecutionStatus.SUBMITTED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED}
    ),
    ExecutionStatus.SUBMITTED: frozenset(
        {ExecutionStatus.RUNNING, ExecutionStatus.FAILED, ExecutionStatus.CANCELLING}
    ),
    ExecutionStatus.RUNNING: frozenset(
        {
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLING,
        }
    ),
    ExecutionStatus.CANCELLING: frozenset(
        {ExecutionStatus.CANCELLED, ExecutionStatus.FAILED, ExecutionStatus.SUCCEEDED}
    ),
    ExecutionStatus.SUCCEEDED: frozenset(),
    ExecutionStatus.FAILED: frozenset(),
    ExecutionStatus.CANCELLED: frozenset(),
}

AGENT_RUN_TRANSITIONS: Mapping[AgentRunStatus, frozenset[AgentRunStatus]] = {
    AgentRunStatus.QUEUED: frozenset({AgentRunStatus.RUNNING, AgentRunStatus.FAILED}),
    AgentRunStatus.RUNNING: frozenset(
        {
            AgentRunStatus.WAITING_INPUT,
            AgentRunStatus.WAITING_APPROVAL,
            AgentRunStatus.FAILED,
        }
    ),
    AgentRunStatus.WAITING_INPUT: frozenset({AgentRunStatus.QUEUED, AgentRunStatus.FAILED}),
    AgentRunStatus.WAITING_APPROVAL: frozenset({AgentRunStatus.FAILED}),
    AgentRunStatus.FAILED: frozenset(),
}


def require_transition(
    current: StateT,
    target: StateT,
    transitions: Mapping[StateT, frozenset[StateT]],
) -> None:
    """依据显式迁移表校验状态变化，拒绝跳步、非法回退和终态重启。"""
    if target not in transitions[current]:
        raise InvalidStateTransition(f"不允许从 {current.value} 转换到 {target.value}")


def require_pipeline_transition(
    current: PipelineVersionStatus, target: PipelineVersionStatus
) -> None:
    """在领域状态机中执行失败即关闭的前置条件校验；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    require_transition(current, target, PIPELINE_TRANSITIONS)


def require_execution_transition(current: ExecutionStatus, target: ExecutionStatus) -> None:
    """在领域状态机中执行失败即关闭的前置条件校验；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    require_transition(current, target, EXECUTION_TRANSITIONS)


def require_agent_run_transition(current: AgentRunStatus, target: AgentRunStatus) -> None:
    """在领域状态机中执行失败即关闭的前置条件校验；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    require_transition(current, target, AGENT_RUN_TRANSITIONS)
