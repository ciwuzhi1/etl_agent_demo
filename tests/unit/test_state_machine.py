from itertools import pairwise

import pytest

from etl_agent.domain.enums import ExecutionStatus, PipelineVersionStatus
from etl_agent.domain.errors import InvalidStateTransition
from etl_agent.domain.state_machine import (
    require_execution_transition,
    require_pipeline_transition,
)


def test_pipeline_happy_path_is_explicit() -> None:
    """验证 pipeline_happy_path_is_explicit 场景的预期结果与失败边界，防止对应行为回归。"""
    path = [
        PipelineVersionStatus.DRAFT,
        PipelineVersionStatus.PROFILING,
        PipelineVersionStatus.GENERATED,
        PipelineVersionStatus.VALIDATED,
        PipelineVersionStatus.WAITING_APPROVAL,
        PipelineVersionStatus.APPROVED,
    ]
    for current, target in pairwise(path):
        require_pipeline_transition(current, target)


def test_approval_cannot_skip_validation() -> None:
    """验证 approval_cannot_skip_validation 场景的预期结果与失败边界，防止对应行为回归。"""
    with pytest.raises(InvalidStateTransition, match="不允许"):
        require_pipeline_transition(
            PipelineVersionStatus.GENERATED, PipelineVersionStatus.WAITING_APPROVAL
        )


def test_terminal_execution_cannot_restart_in_place() -> None:
    """验证 terminal_execution_cannot_restart_in_place 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    with pytest.raises(InvalidStateTransition):
        require_execution_transition(ExecutionStatus.FAILED, ExecutionStatus.QUEUED)
