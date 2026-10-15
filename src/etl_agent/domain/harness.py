import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class RiskLevel(StrEnum):
    """表示 Harness 统一使用的风险等级。"""

    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class PolicyOutcome(StrEnum):
    """表示确定性策略对一次工具意图给出的最终处理结果。"""

    ALLOW = "ALLOW"
    DENY = "DENY"
    DEFER = "DEFER"
    REQUIRE_APPROVAL = "REQUIRE_APPROVAL"


class ToolEffect(StrEnum):
    """描述工具可能产生的副作用，Broker 以注册清单而非 Agent 声明为准。"""

    READ_ONLY = "READ_ONLY"
    WRITE = "WRITE"
    DESTRUCTIVE = "DESTRUCTIVE"
    EXTERNAL_EGRESS = "EXTERNAL_EGRESS"
    SECRET_USE = "SECRET_USE"


class DataClassification(StrEnum):
    """描述数据敏感等级；UNKNOWN 按保守等级参与策略判断。"""

    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"
    SECRET = "SECRET"
    UNKNOWN = "UNKNOWN"


class HarnessRole(StrEnum):
    """描述 V2 职责分离角色；M2 会把项目成员关系映射为这些能力。"""

    DATA_ENGINEER = "DATA_ENGINEER"
    DATA_APPROVER = "DATA_APPROVER"
    SECURITY_APPROVER = "SECURITY_APPROVER"
    OPERATOR = "OPERATOR"
    AUDITOR = "AUDITOR"


class ExecutionPreparationStatus(StrEnum):
    """描述一次安全执行准备单从策略裁决到最终提交的稳定状态。"""

    DENIED = "DENIED"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    READY = "READY"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    COMMITTED = "COMMITTED"


class HarnessApprovalValue(StrEnum):
    """描述职责分离审批人对冻结 Preparation 的明确决定。"""

    APPROVE = "APPROVE"
    REJECT = "REJECT"


class HarnessOutboxStatus(StrEnum):
    """表示 Harness 命令在 Outbox 表中的投递状态。"""

    PENDING = "PENDING"
    SENDING = "SENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    DEAD = "DEAD"


@dataclass
class ResourceScope:
    """描述工具意图涉及的资源范围。"""

    project_id: UUID
    resource_type: str | None = None
    resource_id: str | None = None


@dataclass
class ToolIntent:
    """表示 Agent 或人工入口提出的工具调用意图，不等同于执行授权。"""

    tool_name: str
    action: str
    project_id: UUID
    actor_id: UUID
    invocation_id: UUID = field(default_factory=uuid4)
    resource_scopes: list[ResourceScope] = field(default_factory=list)
    effects: list[ToolEffect] = field(default_factory=list)
    data_classifications: list[DataClassification] = field(default_factory=list)
    external_destination: str | None = None
    bounded: bool = True
    artifact_sha256: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyContext:
    """表示策略评估时的上下文信息。"""

    policy_bundle_version: str
    evaluated_at: datetime
    environment: str = "production"


# ============================================================================
# Pydantic Models for Tool Broker and Harness
# ============================================================================


class PolicyEvaluationContext(BaseModel):
    """冻结策略评估时的环境、角色和策略包版本。"""

    environment: str = Field(min_length=1, max_length=64)
    actor_roles: list[HarnessRole] = Field(default_factory=list, max_length=20)
    policy_bundle_version: str = Field(min_length=1, max_length=128)
    evaluated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("actor_roles")
    @classmethod
    def normalize_roles(cls, values: list[HarnessRole]) -> list[HarnessRole]:
        """对角色排序去重，使相同权限上下文生成相同请求指纹。"""
        unique = {value.value: value for value in values}
        return [unique[key] for key in sorted(unique)]


@dataclass
class PolicyRuleMatch:
    """表示一条规则命中后的结构化证据。"""

    rule_id: str
    rule_version: str
    outcome: PolicyOutcome
    risk_level: RiskLevel
    message: str
    evidence: dict[str, Any] = field(default_factory=dict)
    required_roles: list[HarnessRole] = field(default_factory=list)
    required_approvals: int = 0


@dataclass
class PolicyDecision:
    """表示可持久化、可签名且能解释的 PDP 最终决策。"""

    id: str
    project_id: UUID
    actor_id: UUID
    outcome: PolicyOutcome
    risk_level: RiskLevel
    policy_bundle_version: str
    request_fingerprint: str
    rule_matches: list[PolicyRuleMatch] = field(default_factory=list)
    required_roles: list[HarnessRole] = field(default_factory=list)
    required_approvals: int = 0
    artifact_sha256: str | None = None
    decided_at: datetime | None = None
    expires_at: datetime | None = None


@dataclass
class PolicyEvaluationRequest:
    """表示一次完整的策略评估请求。"""

    intent: ToolIntent
    context: PolicyEvaluationContext


@dataclass
class DetachedSignature:
    """表示 Ed25519 分离签名。"""

    algorithm: str = "Ed25519"
    key_id: str = ""
    value: str = ""


@dataclass
class EvidenceVerificationReport:
    """返回证据流哈希、签名和头指针的验证结果。"""

    valid: bool
    event_count: int = 0
    head_hash: str | None = None
    errors: list[str] = field(default_factory=list)


@dataclass
class PolicyBundleManifest:
    """表示策略包清单。"""

    version: str
    rules: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignedPolicyBundle:
    """表示已签名的策略包。"""

    manifest: PolicyBundleManifest
    manifest_sha256: str
    signature: DetachedSignature


class ToolManifest(BaseModel):
    """描述 Tool Broker 中一个版本化工具的机器可读能力清单。"""

    name: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_.-]*$")
    version: str = Field(min_length=1, max_length=64)
    description: str = Field(min_length=1, max_length=1_000)
    effects: list[ToolEffect] = Field(min_length=1, max_length=10)
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    timeout_seconds: float = Field(default=30, gt=0, le=3_600)
    max_records: int | None = Field(default=None, ge=1)
    max_bytes: int | None = Field(default=None, ge=1)
    idempotent: bool = False
    compensation_tool: str | None = Field(
        default=None, max_length=128, pattern=r"^[a-z][a-z0-9_.-]*$"
    )


class PreparedToolInvocation(BaseModel):
    """保存 Broker 对调用输入和资源范围的冻结结果，不包含原始载荷。"""

    invocation_id: UUID
    project_id: UUID
    actor_id: UUID
    tool_name: str
    tool_version: str
    input_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    resource_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_decision: PolicyDecision


class CapabilityClaims(BaseModel):
    """定义一次性能力令牌中受签名保护的最小权限声明。"""

    token_id: UUID = Field(default_factory=uuid4)
    project_id: UUID
    subject_id: UUID
    environment: str = Field(min_length=1, max_length=64)
    policy_decision_id: UUID
    policy_bundle_version: str = Field(min_length=1, max_length=128)
    tool_name: str = Field(min_length=1, max_length=128)
    tool_version: str = Field(min_length=1, max_length=64)
    input_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    resource_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifact_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    supervision_contract_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    data_classification_report_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    allowed_mitigations: list[str] = Field(default_factory=list, max_length=20)
    effects: list[ToolEffect] = Field(min_length=1, max_length=10)
    issued_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime
    single_use: bool = True


def canonical_json(obj: Any) -> str:
    """把结构化值编码为稳定 UTF-8 JSON，作为哈希和签名的唯一表示。"""
    return json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_json_bytes(value: Any) -> bytes:
    """把结构化值编码为稳定 UTF-8 JSON 字节，用于哈希和签名。"""
    return canonical_json(value).encode("utf-8")


def canonical_sha256(obj: Any) -> str:
    """计算对象的规范 JSON SHA-256 哈希值。"""
    if hasattr(obj, "__dict__"):
        data = obj.__dict__
    elif isinstance(obj, dict):
        data = obj
    else:
        data = {"value": obj}
    
    json_str = json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(json_str.encode("utf-8")).hexdigest()
