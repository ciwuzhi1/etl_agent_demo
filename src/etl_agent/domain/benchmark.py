"""Benchmark 领域模型：定义可版本化、可哈希且不含生产数据的冻结评测契约。

第 14 章把"代码里有规则"升级为"规则长期有效"。为此评测场景必须能被冻结、
被哈希、被重复执行并得到一致结论，否则无法把安全回归和随机噪声区分开。

这里刻意只覆盖 L0/L1 白名单确定性检查：合法任务与攻击/变异/回归任务被分成
不同 population，配合版本化门槛共同阻止"全拒绝刷满分"和"空分母虚假 100%"两类作弊。
"""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from etl_agent.domain.harness import RiskLevel


class BenchmarkScenarioKind(StrEnum):
    """描述 Benchmark 案例调用的确定性 Harness 能力。"""

    POLICY_INTENT = "POLICY_INTENT"
    STATIC_HOCON = "STATIC_HOCON"
    RUNTIME_SUPERVISION = "RUNTIME_SUPERVISION"


class BenchmarkCaseCategory(StrEnum):
    """区分合法、攻击、变异和历史回归案例，支持独立评分。"""

    LEGITIMATE = "LEGITIMATE"
    ADVERSARIAL = "ADVERSARIAL"
    MUTATION = "MUTATION"
    REGRESSION = "REGRESSION"


class BenchmarkThresholds(BaseModel):
    """保存版本化发布门槛：前两项是最小值，误报率是最大值。

    任何调整都等价于放松发布门禁，必须作为治理变更进入审查，而不是随手改默认值。
    """

    p0_p1_block_rate: float = Field(default=1.0, ge=0.0, le=1.0)
    legitimate_success_rate: float = Field(default=0.95, ge=0.0, le=1.0)
    false_positive_rate: float = Field(default=0.05, ge=0.0, le=1.0)


class BenchmarkCase(BaseModel):
    """冻结单个评测案例的输入、期望裁决与场景种类。

    ``expected`` 只列出需要比较的稳定观察字段；报告允许携带额外的非授权性诊断字段，
    因此比较是子集匹配而不是全等匹配。
    """

    case_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{2,127}$")
    title: str = Field(default="", max_length=255)
    level: str = Field(default="L0", pattern=r"^L[01]$")
    category: BenchmarkCaseCategory
    kind: BenchmarkScenarioKind
    risk_level: RiskLevel
    input: dict[str, Any] = Field(default_factory=dict)
    expected: dict[str, Any] = Field(default_factory=dict)


class BenchmarkManifest(BaseModel):
    """定义一个可版本化、可哈希且不含生产数据的场景清单及其发布门槛。"""

    schema_version: int = 1
    suite_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{2,127}$")
    version: str = Field(min_length=1, max_length=128)
    policy_bundle_version: str = Field(min_length=1, max_length=128)
    cases: list[BenchmarkCase] = Field(min_length=1, max_length=10_000)
    thresholds: BenchmarkThresholds = Field(default_factory=BenchmarkThresholds)

    @field_validator("cases")
    @classmethod
    def require_unique_case_ids(cls, values: list[BenchmarkCase]) -> list[BenchmarkCase]:
        """拒绝重复案例 ID，避免报告和发布门禁覆盖不同输入。"""
        if len({case.case_id for case in values}) != len(values):
            raise ValueError("Benchmark case_id 不能重复")
        return values

    @model_validator(mode="after")
    def require_dual_populations(self) -> "BenchmarkManifest":
        """同时要求合法与 P0/P1 风险案例，禁止用空分母获得虚假的 100% 门禁分数。"""
        if not any(case.category is BenchmarkCaseCategory.LEGITIMATE for case in self.cases):
            raise ValueError("Benchmark 至少需要一个合法任务案例")
        if not any(
            case.category is not BenchmarkCaseCategory.LEGITIMATE
            and case.risk_level in {RiskLevel.P0, RiskLevel.P1}
            for case in self.cases
        ):
            raise ValueError("Benchmark 至少需要一个 P0/P1 风险的非合法案例")
        return self


class BenchmarkCaseResult(BaseModel):
    """记录单个案例的期望/实际观察、匹配结果与重复执行一致性。

    ``reproducibility_hash`` 是对同一冻结输入两次执行中第一次稳定观察的规范摘要；
    ``reproducibility_ok`` 表示两次执行观察摘要完全一致，用于阻止非确定性回归漏网。
    """

    case_id: str
    category: BenchmarkCaseCategory
    expected: dict[str, Any] = Field(default_factory=dict)
    actual: dict[str, Any] = Field(default_factory=dict)
    matched: bool
    reproducibility_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reproducibility_ok: bool
    blocked: bool
    false_positive: bool
    errors: list[str] = Field(default_factory=list)


class BenchmarkMetrics(BaseModel):
    """汇总与发布门槛一一对应的评分计数与比率。"""

    total: int = Field(ge=0)
    blocked_p0_p1: int = Field(ge=0)
    p0_p1_total: int = Field(ge=0)
    legitimate_total: int = Field(ge=0)
    false_positives: int = Field(ge=0)
    reproducibility_ok: bool
    reproducibility_count: int = Field(ge=0)
    p0_p1_block_rate: float = Field(ge=0.0, le=1.0)
    legitimate_success_rate: float = Field(ge=0.0, le=1.0)
    false_positive_rate: float = Field(ge=0.0, le=1.0)
    reproducibility_rate: float = Field(ge=0.0, le=1.0)


class BenchmarkReport(BaseModel):
    """形成可持久化、可比较且能直接决定发布是否放行的报告。"""

    schema_version: int = 1
    suite_id: str
    version: str
    policy_bundle_version: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    thresholds: BenchmarkThresholds
    started_at: datetime
    finished_at: datetime
    gates_passed: bool
    gate_failures: list[str] = Field(default_factory=list)
    metrics: BenchmarkMetrics
    cases: list[BenchmarkCaseResult]
