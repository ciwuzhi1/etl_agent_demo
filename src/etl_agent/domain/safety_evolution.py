"""受控安全进化领域模型：把改进候选约束成"只能提案、不能热改生产"的状态机。

第 14 章的关键约束：策略、Prompt 和 Benchmark 的改进只能沿 DRAFT→IN_REVIEW→
APPROVED/REJECTED 的评审轨道前进，批准后再经历影子→小流量→推广授权，
任一阶段出现 P0/P1 回归、误报越线或执行错误都会自动撤销授权。

这里的模型只描述可离线审查的事实，绝不包含可执行代码、部署命令或凭据材料。
"""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from etl_agent.domain.harness import DetachedSignature, canonical_sha256


class SafetyProposalKind(StrEnum):
    """限定安全进化只能提出策略、Prompt 或评测案例候选。"""

    POLICY = "POLICY"
    PROMPT = "PROMPT"
    BENCHMARK = "BENCHMARK"


class SafetyProposalStatus(StrEnum):
    """描述安全改进提案从草稿到发布放行的不可跳跃状态。"""

    DRAFT = "DRAFT"
    IN_REVIEW = "IN_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    RELEASED = "RELEASED"


class SafetyReleaseStatus(StrEnum):
    """描述候选版本的影子、小流量、推广授权和回滚状态。"""

    SHADOW_RUNNING = "SHADOW_RUNNING"
    SHADOW_VERIFIED = "SHADOW_VERIFIED"
    CANARY_RUNNING = "CANARY_RUNNING"
    CANARY_VERIFIED = "CANARY_VERIFIED"
    PROMOTION_APPROVED = "PROMOTION_APPROVED"
    ROLLED_BACK = "ROLLED_BACK"


class SafetyReleaseStage(StrEnum):
    """发布阶段粗粒度归并，供报表与查询按 SHADOW/CANARY/PROMOTED/ROLLED_BACK 分组。"""

    SHADOW = "SHADOW"
    CANARY = "CANARY"
    PROMOTED = "PROMOTED"
    ROLLED_BACK = "ROLLED_BACK"


def release_stage_for_status(status: SafetyReleaseStatus) -> SafetyReleaseStage:
    """把细粒度发布状态映射为报表所需的粗阶段，避免每个查询重复分支。"""
    if status in {SafetyReleaseStatus.SHADOW_RUNNING, SafetyReleaseStatus.SHADOW_VERIFIED}:
        return SafetyReleaseStage.SHADOW
    if status in {SafetyReleaseStatus.CANARY_RUNNING, SafetyReleaseStatus.CANARY_VERIFIED}:
        return SafetyReleaseStage.CANARY
    if status is SafetyReleaseStatus.PROMOTION_APPROVED:
        return SafetyReleaseStage.PROMOTED
    return SafetyReleaseStage.ROLLED_BACK


class SafetyStageObservation(BaseModel):
    """保存影子或小流量验证的脱敏聚合事实，禁止携带原始数据。"""

    sample_count: int = Field(ge=1, le=10_000_000_000)
    p0_p1_regressions: int = Field(default=0, ge=0)
    policy_false_positives: int = Field(default=0, ge=0)
    execution_errors: int = Field(default=0, ge=0)
    notes: list[str] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def require_counts_within_sample(self) -> "SafetyStageObservation":
        """拒绝大于样本量的异常计数，防止伪造不自洽的影子证据。"""
        counts = (
            self.p0_p1_regressions,
            self.policy_false_positives,
            self.execution_errors,
        )
        if any(value > self.sample_count for value in counts):
            raise ValueError("阶段验证异常计数不能超过样本量")
        return self

    def false_positive_rate(self) -> float:
        """返回脱敏聚合后的误报比例，供自动回滚门槛判断使用。"""
        return self.policy_false_positives / self.sample_count


class SafetyReleaseManifest(BaseModel):
    """冻结发布候选、基准证据和环境，作为分离签名的唯一载荷。

    清单不含候选正文，只包含候选与冻结 Benchmark 报告的规范摘要；签名对象即此清单，
    因此数据库改写会立即在验证阶段暴露。
    """

    schema_version: int = 1
    release_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    proposal_id: uuid.UUID
    proposal_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_bundle_version: str = Field(min_length=1, max_length=128)
    environment: str = Field(min_length=1, max_length=64)
    issued_by: uuid.UUID
    issued_at: datetime


class SignedSafetyReleaseManifest(BaseModel):
    """把不可变发布清单、规范哈希和服务端签名组合成可审计的发布对象。"""

    manifest: SafetyReleaseManifest
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: DetachedSignature


def candidate_payload_sha256(payload: dict[str, Any]) -> str:
    """计算候选正文的规范摘要；调用方只保存摘要，不复制可能含敏感的正文。"""
    return canonical_sha256(payload)
