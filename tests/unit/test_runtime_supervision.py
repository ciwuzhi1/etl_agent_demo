"""第 13 章（Runtime Supervision）单元测试。

覆盖两层：
- 纯规则：`RuntimeSupervisor.evaluate` 对每条预算规则给出确定性的只减权动作。
- 服务：`RuntimeSupervisionService` 的哈希链持久化、发布门禁与减损副作用状态机。
"""

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.execution import ExecutionService
from etl_agent.application.runtime_supervision import (
    RuntimeSupervisionService,
    RuntimeSupervisor,
)
from etl_agent.domain.errors import VersionConflict
from etl_agent.domain.supervision import (
    RuntimeBudget,
    RuntimeJobSnapshot,
    RuntimeMitigation,
    RuntimeMitigationStatus,
    RuntimeRiskLevel,
)
from etl_agent.infrastructure.models import ExecutionRun


def test_record_budget_triggers_cancel_p0() -> None:
    """记录数突破硬上限 -> CANCEL（P0），并要求取消与阻断发布。"""
    supervisor = RuntimeSupervisor()
    decision = supervisor.evaluate(
        RuntimeJobSnapshot(source_rows=150),
        RuntimeBudget(max_records=100, max_bytes=10**9, timeout_seconds=1000),
        elapsed_seconds=0.0,
    )
    assert decision.action is RuntimeMitigation.CANCEL
    assert decision.risk_level is RuntimeRiskLevel.P0
    assert decision.cancel_required is True
    assert decision.block_publish is True
    assert decision.matches[0].rule_id == "SUPERVISOR-P0-RECORD-BUDGET"


def test_byte_budget_triggers_cancel_p0() -> None:
    """字节数突破硬上限 -> CANCEL（P0）。"""
    decision = RuntimeSupervisor().evaluate(
        RuntimeJobSnapshot(source_bytes=2000),
        RuntimeBudget(max_records=10**9, max_bytes=1000, timeout_seconds=1000),
        elapsed_seconds=0.0,
    )
    assert decision.action is RuntimeMitigation.CANCEL
    assert decision.risk_level is RuntimeRiskLevel.P0
    assert decision.matches[0].rule_id == "SUPERVISOR-P0-BYTE-BUDGET"


def test_timeout_triggers_cancel_p0() -> None:
    """执行时长突破硬上限 -> CANCEL（P0）。"""
    decision = RuntimeSupervisor().evaluate(
        RuntimeJobSnapshot(),
        RuntimeBudget(max_records=10**9, max_bytes=10**9, timeout_seconds=10),
        elapsed_seconds=20.0,
    )
    assert decision.action is RuntimeMitigation.CANCEL
    assert decision.risk_level is RuntimeRiskLevel.P0
    assert decision.matches[0].rule_id == "SUPERVISOR-P0-TIMEOUT"


def test_output_amplification_triggers_quarantine_p0() -> None:
    """输出放大率突破上限 -> QUARANTINE（P0）。"""
    decision = RuntimeSupervisor().evaluate(
        RuntimeJobSnapshot(source_rows=100, sink_rows=500),
        RuntimeBudget(
            max_records=10_000,
            max_bytes=10**9,
            timeout_seconds=1000,
            max_output_ratio=2.0,
        ),
        elapsed_seconds=0.0,
    )
    assert decision.action is RuntimeMitigation.QUARANTINE
    assert decision.risk_level is RuntimeRiskLevel.P0
    assert decision.block_publish is True
    assert decision.matches[0].rule_id == "SUPERVISOR-P0-OUTPUT-AMPLIFICATION"


def test_reject_rate_triggers_quarantine_p1() -> None:
    """质量达标样本上拒绝率超上限 -> QUARANTINE（P1）。"""
    decision = RuntimeSupervisor().evaluate(
        RuntimeJobSnapshot(source_rows=200, sink_rows=200),
        RuntimeBudget(
            max_records=10_000,
            max_bytes=10**9,
            timeout_seconds=1000,
            max_output_ratio=10.0,
            max_reject_rate=0.5,
            minimum_quality_rows=10,
        ),
        elapsed_seconds=0.0,
        quality_stats={"reject_rows": 150, "sink_rows": 200},
    )
    assert decision.action is RuntimeMitigation.QUARANTINE
    assert decision.risk_level is RuntimeRiskLevel.P1
    assert decision.matches[0].rule_id == "SUPERVISOR-P1-REJECT-RATE"


def test_reject_rate_below_quality_floor_is_ignored() -> None:
    """样本未达到最小质量行数时，拒绝率规则不参与判定。"""
    decision = RuntimeSupervisor().evaluate(
        RuntimeJobSnapshot(source_rows=5, sink_rows=5),
        RuntimeBudget(
            max_records=10_000,
            max_bytes=10**9,
            timeout_seconds=1000,
            max_reject_rate=0.5,
            minimum_quality_rows=100,
        ),
        elapsed_seconds=0.0,
        quality_stats={"reject_rows": 5, "sink_rows": 5},
    )
    assert decision.action is RuntimeMitigation.CONTINUE
    assert decision.risk_level is RuntimeRiskLevel.OK


def test_warning_threshold_triggers_throttle_p3() -> None:
    """接近但未突破预算 -> THROTTLE（P3），不请求取消或阻断发布。"""
    decision = RuntimeSupervisor().evaluate(
        RuntimeJobSnapshot(source_rows=850),
        RuntimeBudget(
            max_records=1000,
            max_bytes=10**9,
            timeout_seconds=1000,
            warning_ratio=0.8,
        ),
        elapsed_seconds=0.0,
    )
    assert decision.action is RuntimeMitigation.THROTTLE
    assert decision.risk_level is RuntimeRiskLevel.P3
    assert decision.cancel_required is False
    assert decision.block_publish is False
    assert decision.next_poll_seconds == 5
    assert decision.matches[0].rule_id == "SUPERVISOR-P3-BUDGET-WARNING"


def test_within_bounds_continues_ok() -> None:
    """全部指标在界内 -> CONTINUE（OK），无命中、无副作用。"""
    decision = RuntimeSupervisor().evaluate(
        RuntimeJobSnapshot(source_rows=10, sink_rows=10, source_bytes=100, sink_bytes=100),
        RuntimeBudget(max_records=1000, max_bytes=10_000, timeout_seconds=1000),
        elapsed_seconds=1.0,
    )
    assert decision.action is RuntimeMitigation.CONTINUE
    assert decision.risk_level is RuntimeRiskLevel.OK
    assert decision.matches == []
    assert decision.cancel_required is False
    assert decision.block_publish is False


def test_evaluate_is_deterministic() -> None:
    """相同输入产生相同内容自哈希，保证快照哈希链可离线复算。"""
    snapshot = RuntimeJobSnapshot(source_rows=500)
    budget = RuntimeBudget(max_records=100, max_bytes=10**9, timeout_seconds=1000)
    supervisor = RuntimeSupervisor()
    first = supervisor.evaluate(snapshot, budget, elapsed_seconds=0.0)
    second = supervisor.evaluate(snapshot, budget, elapsed_seconds=0.0)
    assert first.snapshot_sha256 == second.snapshot_sha256
    assert len(first.snapshot_sha256) == 64


async def _make_run(session: AsyncSession) -> ExecutionRun:
    """插入一行最小 ExecutionRun 供服务测试引用（SQLite 不强制外键）。"""
    run = ExecutionRun(
        id=uuid.uuid4(),
        pipeline_version_id=uuid.uuid4(),
        idempotency_key=f"sup-{uuid.uuid4().hex[:12]}",
        requested_by=uuid.uuid4(),
    )
    session.add(run)
    await session.flush()
    return run


async def test_observe_persists_snapshot_with_hash_chain(session: AsyncSession) -> None:
    """两次观察形成序号递增、哈希互异的快照链，并回写监督动作。"""
    run = await _make_run(session)
    service = RuntimeSupervisionService()
    fixed = datetime(2026, 1, 1, tzinfo=UTC)
    budget = RuntimeBudget(max_records=1000, max_bytes=10**9, timeout_seconds=1000)

    snap1 = await service.observe(
        session,
        run.id,
        RuntimeJobSnapshot(source_rows=10),
        budget=budget,
        elapsed_seconds=1.0,
        observed_at=fixed,
    )
    snap2 = await service.observe(
        session,
        run.id,
        RuntimeJobSnapshot(source_rows=20),
        budget=budget,
        elapsed_seconds=2.0,
        observed_at=fixed,
    )

    assert snap1.sequence == 1
    assert snap2.sequence == 2
    assert snap1.snapshot_sha256 != snap2.snapshot_sha256
    assert snap1.mitigation_status == RuntimeMitigationStatus.NOT_REQUIRED.value
    assert run.supervision_action == RuntimeMitigation.CONTINUE.value


async def test_observe_records_pending_mitigation_for_cancel(session: AsyncSession) -> None:
    """触发 CANCEL 的观察会持久化 PENDING 减损状态并置隔离标记。"""
    run = await _make_run(session)
    snap = await RuntimeSupervisionService().observe(
        session,
        run.id,
        RuntimeJobSnapshot(source_rows=5000),
        budget=RuntimeBudget(max_records=100, max_bytes=10**9, timeout_seconds=1000),
        elapsed_seconds=0.0,
    )
    assert snap.action == RuntimeMitigation.CANCEL.value
    assert snap.mitigation_status == RuntimeMitigationStatus.PENDING.value
    assert snap.block_publish is True
    assert run.quarantined_at is not None


async def test_claim_and_complete_mitigation(session: AsyncSession) -> None:
    """claim 把 PENDING 收窄为 APPLYING 且重复投递返回 None；complete 标记 APPLIED。"""
    run = await _make_run(session)
    service = RuntimeSupervisionService()
    snap = await service.observe(
        session,
        run.id,
        RuntimeJobSnapshot(source_bytes=9999),
        budget=RuntimeBudget(max_records=10**9, max_bytes=1000, timeout_seconds=1000),
        elapsed_seconds=0.0,
    )

    claimed = await service.claim_mitigation(session, snap.id)
    assert claimed is not None
    assert claimed.mitigation_status == RuntimeMitigationStatus.APPLYING.value

    again = await service.claim_mitigation(session, snap.id)
    assert again is None

    done = await service.complete_mitigation(session, snap.id)
    assert done.mitigation_status == RuntimeMitigationStatus.APPLIED.value
    assert done.applied_at is not None


async def test_complete_mitigation_failure_redacts_error(session: AsyncSession) -> None:
    """外部减损失败保留为 FAILED，且错误信息脱敏。"""
    run = await _make_run(session)
    service = RuntimeSupervisionService()
    snap = await service.observe(
        session,
        run.id,
        RuntimeJobSnapshot(source_bytes=9999),
        budget=RuntimeBudget(max_records=10**9, max_bytes=1000, timeout_seconds=1000),
        elapsed_seconds=0.0,
    )
    await service.claim_mitigation(session, snap.id)

    failed = await service.complete_mitigation(
        session,
        snap.id,
        error="engine cancel failed password=hunter2",
    )
    assert failed.mitigation_status == RuntimeMitigationStatus.FAILED.value
    assert failed.mitigation_error is not None
    assert "hunter2" not in failed.mitigation_error


async def test_require_publish_allowed_blocks_p0_decision(session: AsyncSession) -> None:
    """存在 P0 且 block_publish 的快照时，发布门禁抛出 VersionConflict。"""
    run = await _make_run(session)
    decision = RuntimeSupervisor().evaluate(
        RuntimeJobSnapshot(source_rows=100, sink_rows=900),
        RuntimeBudget(
            max_records=10_000,
            max_bytes=10**9,
            timeout_seconds=1000,
            max_output_ratio=2.0,
        ),
        elapsed_seconds=0.0,
    )
    await ExecutionService().record_supervision(session, run.id, decision)

    with pytest.raises(VersionConflict):
        await RuntimeSupervisionService().require_publish_allowed(session, run.id)


async def test_require_publish_allowed_permits_clean_run(session: AsyncSession) -> None:
    """无隔离标记且无 P0 阻断快照时，发布门禁放行。"""
    run = await _make_run(session)
    await RuntimeSupervisionService().observe(
        session,
        run.id,
        RuntimeJobSnapshot(source_rows=5),
        budget=RuntimeBudget(max_records=1000, max_bytes=10**9, timeout_seconds=1000),
        elapsed_seconds=0.0,
    )
    # 未抛异常即代表放行。
    await RuntimeSupervisionService().require_publish_allowed(session, run.id)
