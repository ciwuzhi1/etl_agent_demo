"""运行时监督领域模型：第 13 章（Runtime Supervision）。

本模块只承载**纯数据结构**——把"运行时能观察到什么、预算是多少、命中了哪条规则、
应该采取哪种只减权动作"用不可变的 dataclass 固定下来，供 Application 层的
`RuntimeSupervisor` / `RuntimeSupervisionService` 使用。

一条贯穿全章的纪律：监督层**只能收窄权限**（节流、暂停、取消、隔离），绝不修改
Pipeline 语义、HOCON 或授权范围；CONTINUE 不能偷偷请求取消或阻断发布。

设计取舍：
- 用标准库 `dataclass` + `StrEnum`，与 `execution_metrics.py` / `execution_diagnostics.py`
  保持同一"轻量、无 Pydantic、Worker 高频可调用"的风格，而不是引用重型 Harness 签名栈。
- 单独定义 `RuntimeRiskLevel`（P0/P1/P3/OK），与 `domain/harness.py` 的 `RiskLevel`
  解耦：监督层只关心"运行时预算风险"，不需要 P2 这一档，也不依赖策略决策词汇。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class RuntimeMitigation(StrEnum):
    """监督层可采取的减损动作；动作只能收窄权限，不能修改作业语义。"""

    CONTINUE = "CONTINUE"
    THROTTLE = "THROTTLE"
    PAUSE = "PAUSE"
    CANCEL = "CANCEL"
    QUARANTINE = "QUARANTINE"


class RuntimeMitigationStatus(StrEnum):
    """需要外部副作用（取消/隔离）的监督动作，从判定到应用的状态机。"""

    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    APPLYING = "APPLYING"
    APPLIED = "APPLIED"
    FAILED = "FAILED"


class RuntimeRiskLevel(StrEnum):
    """运行时预算风险等级；OK 表示未命中任何规则的正常态。"""

    P0 = "P0"
    P1 = "P1"
    P3 = "P3"
    OK = "OK"


@dataclass(frozen=True)
class RuntimeBudget:
    """运行时不可扩权的记录、字节、时长、放大率与质量硬上限。

    `warning_ratio` 只用于在接近硬上限时发出节流信号，不放宽任何硬约束。
    """

    max_records: int = 1_000_000
    max_bytes: int = 1_073_741_824
    timeout_seconds: int = 3_600
    max_output_ratio: float = 2.0
    max_reject_rate: float = 0.5
    minimum_quality_rows: int = 100
    warning_ratio: float = 0.8


@dataclass(frozen=True)
class RuntimeSupervisionContract:
    """把冻结预算、允许的减损动作与分类报告摘要绑定为一次执行授权。

    `allowed_mitigations` 是监督层被授权可以施加的最大动作集合；CONTINUE 是默认结果，
    不占用授权额度。分类报告 SHA-256 用于把"数据敏感等级"这一事实钉进快照哈希链。
    """

    budget: RuntimeBudget
    classification_report_sha256: str
    allowed_mitigations: tuple[RuntimeMitigation, ...] = (
        RuntimeMitigation.CANCEL,
        RuntimeMitigation.PAUSE,
        RuntimeMitigation.QUARANTINE,
        RuntimeMitigation.THROTTLE,
    )


@dataclass(frozen=True)
class RuntimeJobSnapshot:
    """一次引擎轮询观察到的脱敏运行事实；不含原始数据或日志正文。"""

    status: str = "RUNNING"
    source_rows: int = 0
    sink_rows: int = 0
    source_bytes: int = 0
    sink_bytes: int = 0


@dataclass(frozen=True)
class RuntimeRuleMatch:
    """一条确定性规则命中的结构化证据；`reason` 面向运维，不携带敏感值。"""

    rule_id: str
    risk_level: RuntimeRiskLevel
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class RuntimeSupervisionDecision:
    """一次运行快照的监督结论：动作、风险、发布隔离与外部取消要求。

    用可变 dataclass（非 frozen）而非 Pydantic，是因为 `evaluate` 需要在构造后
    回填 `snapshot_sha256`（内容自哈希）；其余字段一经产出即视为不可变。
    """

    action: RuntimeMitigation
    risk_level: RuntimeRiskLevel
    cancel_required: bool = False
    block_publish: bool = False
    next_poll_seconds: int = 2
    matches: list[RuntimeRuleMatch] = field(default_factory=list)
    observed: dict[str, Any] = field(default_factory=dict)
    budget: RuntimeBudget = field(default_factory=RuntimeBudget)
    contract_sha256: str = ""
    snapshot_sha256: str = ""
    id: uuid.UUID = field(default_factory=uuid.uuid4)
