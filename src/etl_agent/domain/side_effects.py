"""副作用领域模型：9 种受治理的危险操作。

第 12 章：取消执行、回滚 Doris、密钥轮换等操作也必须走
Harness 的 prepare → approve → commit 流程，防止"批 A 做 B"。
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from etl_agent.domain.errors import DomainError


class SideEffectOperation(StrEnum):
    """受 Harness 治理的 9 种副作用操作。"""

    CANCEL_EXECUTION = "CANCEL_EXECUTION"
    ROLLBACK_DORIS = "ROLLBACK_DORIS"
    CLEANUP_DORIS = "CLEANUP_DORIS"
    MIGRATE_SECRET = "MIGRATE_SECRET"
    STAGE_SECRET_ROTATION = "STAGE_SECRET_ROTATION"
    VALIDATE_SECRET_ROTATION = "VALIDATE_SECRET_ROTATION"
    ACTIVATE_SECRET_ROTATION = "ACTIVATE_SECRET_ROTATION"
    ROLLBACK_SECRET_ROTATION = "ROLLBACK_SECRET_ROTATION"
    DESTROY_SECRET = "DESTROY_SECRET"


class SideEffectExecutionStatus(StrEnum):
    """副作用执行的生命周期状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SideEffectPreparationStatus(StrEnum):
    """副作用准备记录的审批状态。"""

    WAITING_APPROVAL = "WAITING_APPROVAL"
    READY = "READY"
    COMMITTED = "COMMITTED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    DENIED = "DENIED"


# 每种操作所需的最小审批角色集合
REQUIRED_ROLES_BY_OPERATION: dict[SideEffectOperation, tuple[str, ...]] = {
    SideEffectOperation.CANCEL_EXECUTION: ("OPERATOR",),
    SideEffectOperation.ROLLBACK_DORIS: ("DATA_ENGINEER", "DATA_APPROVER"),
    SideEffectOperation.CLEANUP_DORIS: ("DATA_ENGINEER", "DATA_APPROVER"),
    SideEffectOperation.MIGRATE_SECRET: ("SECURITY_APPROVER",),
    SideEffectOperation.STAGE_SECRET_ROTATION: ("SECURITY_APPROVER",),
    SideEffectOperation.VALIDATE_SECRET_ROTATION: ("SECURITY_APPROVER",),
    SideEffectOperation.ACTIVATE_SECRET_ROTATION: ("SECURITY_APPROVER", "OPERATOR"),
    SideEffectOperation.ROLLBACK_SECRET_ROTATION: ("SECURITY_APPROVER", "OPERATOR"),
    SideEffectOperation.DESTROY_SECRET: ("SECURITY_APPROVER", "DATA_APPROVER"),
}


@dataclass
class SideEffectCommandInput:
    """提交给 Worker 执行的命令负载（不含敏感数据）。"""

    preparation_id: str
    operation: SideEffectOperation
    project_id: str
    aggregate_id: str
    frozen_facts: dict[str, Any] = field(default_factory=dict)
    token_id: str | None = None


@dataclass
class SideEffectCommitResult:
    """commit 成功后返回的执行凭证。"""

    execution_id: str
    preparation_id: str
    token_id: str
    command_payload_sha256: str


class SideEffectLeaseBusy(DomainError):
    """同一副作用已被其他 Worker 领取（并发保护）。"""
