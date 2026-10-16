"""运行时监督应用层：Runtime Supervision。

两个协作角色：
- `RuntimeSupervisor`：对**脱敏指标**执行确定性预算规则，不调用模型、不修改 Pipeline
  或 HOCON，返回一个只减权的 `RuntimeSupervisionDecision`。规则集合固定为
  record_budget / byte_budget / timeout / output_amplification / reject_rate /
  warning_threshold，可离线复算、可审计。
- `RuntimeSupervisionService`：持久化快照（带 SHA-256 哈希链）、认领并完成外部减损
  副作用、在原子发布前隔离被判定为 P0 的运行结果。

设计原则（适配本项目轻量 Harness）：
1. **硬上限优先**：任一硬上限命中即取最严格动作；只有全部硬上限未命中时才看告警阈值。
2. **只减权一致性**：CONTINUE 不得携带 cancel_required / block_publish；强动作必须
   附带确定性规则证据（matches 非空）。
3. **副作用幂等**：claim_mitigation 用行锁把 PENDING 收窄为 APPLYING，重复投递不会
   触发第二次外部取消调用。
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.execution_diagnostics import redact_execution_error
from etl_agent.domain.enums import ExecutionStatus
from etl_agent.domain.errors import ResourceNotFound, VersionConflict
from etl_agent.domain.harness import canonical_sha256
from etl_agent.domain.supervision import (
    RuntimeBudget,
    RuntimeJobSnapshot,
    RuntimeMitigation,
    RuntimeMitigationStatus,
    RuntimeRiskLevel,
    RuntimeRuleMatch,
    RuntimeSupervisionDecision,
)
from etl_agent.infrastructure.models import ExecutionRun, RuntimeSupervisionSnapshot

_ZERO_HASH = "0" * 64

# 动作与风险的严重度排序，用于把多条命中收敛为唯一的最终动作/风险。
_ACTION_PRIORITY = {
    RuntimeMitigation.CONTINUE: 0,
    RuntimeMitigation.THROTTLE: 1,
    RuntimeMitigation.PAUSE: 2,
    RuntimeMitigation.CANCEL: 3,
    RuntimeMitigation.QUARANTINE: 4,
}
_RISK_PRIORITY = {
    RuntimeRiskLevel.OK: 0,
    RuntimeRiskLevel.P3: 1,
    RuntimeRiskLevel.P1: 2,
    RuntimeRiskLevel.P0: 3,
}
# 需要外部副作用的强动作：取消运行并隔离发布。
_STRONG_ACTIONS = frozenset(
    {RuntimeMitigation.PAUSE, RuntimeMitigation.CANCEL, RuntimeMitigation.QUARANTINE}
)


class RuntimeSupervisor:
    """对脱敏指标执行确定性预算规则，返回只减权的监督决定。"""

    def evaluate(
        self,
        snapshot: RuntimeJobSnapshot,
        budget: RuntimeBudget,
        *,
        elapsed_seconds: float,
        records_written: int | None = None,
        quality_stats: dict[str, Any] | None = None,
        contract_sha256: str = "",
    ) -> RuntimeSupervisionDecision:
        """评估记录、字节、时长、放大率和拒绝率，命中最严格的减权动作。

        Args:
            snapshot: 引擎观察到的行数/字节快照。
            budget: 冻结的运行时预算（只减权，不可扩权）。
            elapsed_seconds: 从运行开始到本次观察经过的秒数。
            records_written: 累计写入记录数；缺省用快照 sink_rows 兜底。
            quality_stats: 质量侧脱敏统计，识别 `reject_rows`（可含 `sink_rows` 覆盖）。
            contract_sha256: 关联的监督契约摘要，回填到决定以便持久化。
        """
        stats = quality_stats or {}
        reject_rows = int(stats.get("reject_rows", 0) or 0)
        reject_sink_rows = int(stats.get("sink_rows", snapshot.sink_rows) or 0)
        observed_records = max(snapshot.source_rows, snapshot.sink_rows, records_written or 0)
        observed_bytes = max(snapshot.source_bytes, snapshot.sink_bytes)
        output_ratio = (
            snapshot.sink_rows / snapshot.source_rows if snapshot.source_rows > 0 else None
        )
        reject_rate = reject_rows / reject_sink_rows if reject_sink_rows > 0 else None

        observed = {
            "engine_status": snapshot.status,
            "source_rows": snapshot.source_rows,
            "sink_rows": snapshot.sink_rows,
            "source_bytes": snapshot.source_bytes,
            "sink_bytes": snapshot.sink_bytes,
            "records_written": records_written,
            "reject_rows": reject_rows,
            "reject_rate": reject_rate,
            "output_ratio": output_ratio,
            "elapsed_seconds": round(float(elapsed_seconds), 3),
        }

        matches, actions, risks = self._hard_limit_matches(
            observed=observed,
            budget=budget,
            output_ratio=output_ratio,
            reject_rate=reject_rate,
            reject_sink_rows=reject_sink_rows,
            observed_records=observed_records,
            observed_bytes=observed_bytes,
            elapsed_seconds=float(elapsed_seconds),
        )
        if not matches:
            matches, actions, risks = self._warning_matches(
                observed=observed,
                budget=budget,
                observed_records=observed_records,
                observed_bytes=observed_bytes,
                elapsed_seconds=float(elapsed_seconds),
            )

        if not matches:
            decision = RuntimeSupervisionDecision(
                action=RuntimeMitigation.CONTINUE,
                risk_level=RuntimeRiskLevel.OK,
                cancel_required=False,
                block_publish=False,
                next_poll_seconds=2,
                matches=[],
                observed=observed,
                budget=budget,
                contract_sha256=contract_sha256,
            )
            decision.snapshot_sha256 = _decision_sha256(decision)
            return decision

        action = max(actions, key=_ACTION_PRIORITY.__getitem__)
        risk_level = max(risks, key=_RISK_PRIORITY.__getitem__)
        strong = action in _STRONG_ACTIONS
        decision = RuntimeSupervisionDecision(
            action=action,
            risk_level=risk_level,
            cancel_required=strong,
            block_publish=strong,
            next_poll_seconds=5 if action is RuntimeMitigation.THROTTLE else 2,
            matches=matches,
            observed=observed,
            budget=budget,
            contract_sha256=contract_sha256,
        )
        decision.snapshot_sha256 = _decision_sha256(decision)
        return decision

    @staticmethod
    def _hard_limit_matches(
        *,
        observed: dict[str, Any],
        budget: RuntimeBudget,
        output_ratio: float | None,
        reject_rate: float | None,
        reject_sink_rows: int,
        observed_records: int,
        observed_bytes: int,
        elapsed_seconds: float,
    ) -> tuple[list[RuntimeRuleMatch], list[RuntimeMitigation], list[RuntimeRiskLevel]]:
        """返回全部硬上限命中；调用方按动作/风险优先级收敛为最严格结果。"""
        matches: list[RuntimeRuleMatch] = []
        actions: list[RuntimeMitigation] = []
        risks: list[RuntimeRiskLevel] = []

        def hit(
            rule_id: str,
            action: RuntimeMitigation,
            risk: RuntimeRiskLevel,
            reason: str,
            evidence: dict[str, Any],
        ) -> None:
            matches.append(
                RuntimeRuleMatch(
                    rule_id=rule_id,
                    risk_level=risk,
                    reason=reason,
                    evidence=evidence,
                )
            )
            actions.append(action)
            risks.append(risk)

        if observed_records > budget.max_records:
            hit(
                "SUPERVISOR-P0-RECORD-BUDGET",
                RuntimeMitigation.CANCEL,
                RuntimeRiskLevel.P0,
                "读取或写入记录数超过冻结上限",
                {"observed": observed_records, "limit": budget.max_records},
            )
        if observed_bytes > budget.max_bytes:
            hit(
                "SUPERVISOR-P0-BYTE-BUDGET",
                RuntimeMitigation.CANCEL,
                RuntimeRiskLevel.P0,
                "读取或写入字节数超过冻结上限",
                {"observed": observed_bytes, "limit": budget.max_bytes},
            )
        if elapsed_seconds > budget.timeout_seconds:
            hit(
                "SUPERVISOR-P0-TIMEOUT",
                RuntimeMitigation.CANCEL,
                RuntimeRiskLevel.P0,
                "执行时长超过冻结上限",
                {"observed_seconds": elapsed_seconds, "limit_seconds": budget.timeout_seconds},
            )
        if isinstance(output_ratio, (int, float)) and output_ratio > budget.max_output_ratio:
            hit(
                "SUPERVISOR-P0-OUTPUT-AMPLIFICATION",
                RuntimeMitigation.QUARANTINE,
                RuntimeRiskLevel.P0,
                "输出记录放大率超过冻结上限",
                {"observed": output_ratio, "limit": budget.max_output_ratio},
            )
        if (
            isinstance(reject_rate, (int, float))
            and reject_sink_rows >= budget.minimum_quality_rows
            and reject_rate > budget.max_reject_rate
        ):
            hit(
                "SUPERVISOR-P1-REJECT-RATE",
                RuntimeMitigation.QUARANTINE,
                RuntimeRiskLevel.P1,
                "实时拒绝率超过冻结质量上限",
                {
                    "observed": reject_rate,
                    "limit": budget.max_reject_rate,
                    "sink_rows": reject_sink_rows,
                },
            )
        return matches, actions, risks

    @staticmethod
    def _warning_matches(
        *,
        observed: dict[str, Any],
        budget: RuntimeBudget,
        observed_records: int,
        observed_bytes: int,
        elapsed_seconds: float,
    ) -> tuple[list[RuntimeRuleMatch], list[RuntimeMitigation], list[RuntimeRiskLevel]]:
        """在接近但未突破预算时发出节流信号；只降频观察，不改作业语义。"""
        ratios = [
            ("records", observed_records / budget.max_records),
            ("bytes", observed_bytes / budget.max_bytes),
            ("elapsed_seconds", elapsed_seconds / budget.timeout_seconds),
        ]
        if not ratios:
            return [], [], []
        dimension, ratio = max(ratios, key=lambda item: item[1])
        if ratio < budget.warning_ratio:
            return [], [], []
        match = RuntimeRuleMatch(
            rule_id="SUPERVISOR-P3-BUDGET-WARNING",
            risk_level=RuntimeRiskLevel.P3,
            reason="资源使用接近冻结上限，降低轮询频率并持续观察",
            evidence={"dimension": dimension, "ratio": ratio, "warning": budget.warning_ratio},
        )
        return (
            [match],
            [RuntimeMitigation.THROTTLE],
            [RuntimeRiskLevel.P3],
        )


def _decision_sha256(decision: RuntimeSupervisionDecision) -> str:
    """对决定的可观察内容做规范 JSON SHA-256，作为快照哈希链的当前载荷。"""
    document = {
        "action": decision.action.value,
        "risk_level": decision.risk_level.value,
        "cancel_required": decision.cancel_required,
        "block_publish": decision.block_publish,
        "next_poll_seconds": decision.next_poll_seconds,
        "observed": decision.observed,
        "budget": decision.budget.__dict__,
        "rule_matches": [match.__dict__ for match in decision.matches],
    }
    return canonical_sha256(document)


def build_snapshot_row(
    decision: RuntimeSupervisionDecision,
    *,
    run_id: uuid.UUID,
    sequence: int,
    previous_hash: str,
    observed_at: datetime,
) -> RuntimeSupervisionSnapshot:
    """把监督决定映射为一行 ORM 快照，并按哈希链计算 snapshot_sha256。

    与 `RuntimeSupervisionService.observe` 和 `ExecutionService.record_supervision`
    共用，保证哈希链构造口径唯一。
    """
    chain_document = {
        "run_id": str(run_id),
        "sequence": sequence,
        "observed_at": observed_at.astimezone(UTC).isoformat(timespec="microseconds"),
        "previous_hash": previous_hash,
        "decision_sha256": decision.snapshot_sha256,
    }
    mitigation_status = (
        RuntimeMitigationStatus.PENDING
        if decision.cancel_required
        else RuntimeMitigationStatus.NOT_REQUIRED
    )
    return RuntimeSupervisionSnapshot(
        id=decision.id,
        run_id=run_id,
        sequence=sequence,
        observed_at=observed_at,
        engine_status=decision.observed.get("engine_status", "")[:32],
        action=decision.action.value,
        risk_level=decision.risk_level.value,
        cancel_required=decision.cancel_required,
        block_publish=decision.block_publish,
        next_poll_seconds=decision.next_poll_seconds,
        observed=decision.observed,
        budget=decision.budget.__dict__,
        rule_matches=[match.__dict__ for match in decision.matches],
        contract_sha256=decision.contract_sha256 or _ZERO_HASH,
        snapshot_sha256=canonical_sha256(chain_document),
        mitigation_status=mitigation_status.value,
    )


async def _latest_snapshot(
    session: AsyncSession,
    run_id: uuid.UUID,
) -> RuntimeSupervisionSnapshot | None:
    """取该 run 序号最大的快照，作为哈希链前序与下一序号的基准。"""
    return await session.scalar(
        select(RuntimeSupervisionSnapshot)
        .where(RuntimeSupervisionSnapshot.run_id == run_id)
        .order_by(RuntimeSupervisionSnapshot.sequence.desc())
        .limit(1)
    )


async def next_sequence_and_previous_hash(
    session: AsyncSession,
    run_id: uuid.UUID,
) -> tuple[int, str]:
    """在锁定的最新快照上算出下一序号与哈希链前序。

    公开给 `ExecutionService.record_supervision` 复用，保证哈希链构造口径唯一。
    """
    latest = await _latest_snapshot(session, run_id)
    if latest is None:
        return 1, _ZERO_HASH
    return int(latest.sequence) + 1, latest.snapshot_sha256 or _ZERO_HASH


class RuntimeSupervisionService:
    """评估、持久化并协调运行时减损状态；发布前做隔离门禁。"""

    def __init__(self, supervisor: RuntimeSupervisor | None = None) -> None:
        """允许测试注入仿真监督器；生产使用确定性规则实现。"""
        self._supervisor = supervisor or RuntimeSupervisor()

    async def observe(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
        snapshot: RuntimeJobSnapshot,
        *,
        budget: RuntimeBudget,
        elapsed_seconds: float,
        records_written: int | None = None,
        quality_stats: dict[str, Any] | None = None,
        contract_sha256: str = "",
        observed_at: datetime | None = None,
    ) -> RuntimeSupervisionSnapshot:
        """评估一个引擎快照，并以哈希链原子持久化监督快照、更新运行隔离标记。"""
        run = await session.scalar(
            select(ExecutionRun).where(ExecutionRun.id == run_id).with_for_update()
        )
        if run is None:
            raise ResourceNotFound("执行记录不存在")

        decision = self._supervisor.evaluate(
            snapshot,
            budget,
            elapsed_seconds=elapsed_seconds,
            records_written=records_written,
            quality_stats=quality_stats,
            contract_sha256=contract_sha256,
        )
        timestamp = observed_at or datetime.now(UTC)
        sequence, previous_hash = await next_sequence_and_previous_hash(session, run_id)
        persisted = build_snapshot_row(
            decision,
            run_id=run_id,
            sequence=sequence,
            previous_hash=previous_hash,
            observed_at=timestamp,
        )
        session.add(persisted)

        run.supervision_action = decision.action.value
        if contract_sha256:
            run.supervision_contract_sha256 = contract_sha256
        if decision.block_publish:
            run.quarantined_at = run.quarantined_at or timestamp
            run.quarantine_reason = "; ".join(match.reason for match in decision.matches)[:2000]

        await session.flush()
        return persisted

    async def claim_mitigation(
        self,
        session: AsyncSession,
        decision_id: uuid.UUID,
    ) -> RuntimeSupervisionSnapshot | None:
        """在外部取消前用行锁把 PENDING 收窄为 APPLYING；重复投递返回 None。"""
        snapshot = await session.scalar(
            select(RuntimeSupervisionSnapshot)
            .where(RuntimeSupervisionSnapshot.id == decision_id)
            .with_for_update()
        )
        if snapshot is None:
            raise ResourceNotFound("运行监督决定不存在")
        if not snapshot.cancel_required:
            return None
        if snapshot.mitigation_status != RuntimeMitigationStatus.PENDING.value:
            return None

        snapshot.mitigation_status = RuntimeMitigationStatus.APPLYING.value
        snapshot.mitigation_error = None

        run = await session.scalar(
            select(ExecutionRun).where(ExecutionRun.id == snapshot.run_id).with_for_update()
        )
        if run is not None and run.status not in {
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }:
            run.status = ExecutionStatus.CANCELLING
            run.cancel_requested_at = datetime.now(UTC)
        await session.flush()
        return snapshot

    async def complete_mitigation(
        self,
        session: AsyncSession,
        decision_id: uuid.UUID,
        error: str | None = None,
    ) -> RuntimeSupervisionSnapshot:
        """记录外部减损调用结果；失败保留为可重试状态且错误必须脱敏。"""
        snapshot = await session.scalar(
            select(RuntimeSupervisionSnapshot)
            .where(RuntimeSupervisionSnapshot.id == decision_id)
            .with_for_update()
        )
        if snapshot is None:
            raise ResourceNotFound("运行监督决定不存在")
        if error is None:
            snapshot.mitigation_status = RuntimeMitigationStatus.APPLIED.value
            snapshot.applied_at = datetime.now(UTC)
            snapshot.mitigation_error = None
        else:
            snapshot.mitigation_status = RuntimeMitigationStatus.FAILED.value
            snapshot.mitigation_error = redact_execution_error(error)[:2000]
        await session.flush()
        return snapshot

    async def require_publish_allowed(
        self,
        session: AsyncSession,
        run_id: uuid.UUID,
    ) -> None:
        """在原子发布前阻断被监督层隔离的运行结果。

        任一 P0 快照 `block_publish=True`，或运行已被隔离，都禁止发布。
        """
        run = await session.get(ExecutionRun, run_id)
        if run is None:
            raise ResourceNotFound("执行记录不存在")
        blocked = await session.scalar(
            select(func.count())
            .select_from(RuntimeSupervisionSnapshot)
            .where(
                RuntimeSupervisionSnapshot.run_id == run_id,
                RuntimeSupervisionSnapshot.block_publish.is_(True),
                RuntimeSupervisionSnapshot.risk_level == RuntimeRiskLevel.P0.value,
            )
        )
        if blocked or run.quarantined_at is not None:
            raise VersionConflict(
                f"运行结果已被监督层隔离，禁止发布：{run.quarantine_reason or '未说明'}"
            )
