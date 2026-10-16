"""执行指标归一化与引擎指标回写（第 10 章）。

回归的缺陷：`/job-info` 明明返回了真实计数，适配器却把整个 `metrics` 丢掉了，
监控侧再硬写 0 落库——Doris 里确实落了 4 行，平台上却永远显示 "0 行"。
本文件锁住三件事：

- 引擎返回的真实指标能被归一化（含分表明细）；
- 指标缺失时退化为 0，不虚构；
- 归一化结果真的被写进 ExecutionRun，而不是继续写 0。
"""

import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.execution_metrics import normalize_seatunnel_metrics
from etl_agent.domain.enums import ExecutionStatus, PipelineVersionStatus, ProjectRole
from etl_agent.infrastructure.models import (
    ExecutionRun,
    Pipeline,
    PipelineVersion,
    Project,
    ProjectMembership,
    User,
)
from etl_agent.infrastructure.seatunnel.zeta_adapter import (
    SeatunnelJobInfo,
    ZetaRestAdapter,
)

# 2026-10-16 实测：MySQL 8 行经 4 条拒绝规则过滤后落 Doris 4 行。
REAL_ENGINE_PAYLOAD: dict[str, Any] = {
    "jobId": "1163299551575015425",
    "jobName": "exec-bbe69a0578fb4d97",
    "jobStatus": "FINISHED",
    "createTime": "2026-10-16 02:17:06",
    "metrics": {
        "SourceReceivedCount": "8",
        "SourceReceivedBytes": "932",
        "SinkCommittedCount": "4",
        "SinkCommittedBytes": "482",
        "SinkWriteCount": "4",
        "SinkWriteBytes": "482",
        "TableSourceReceivedCount": {"Source[0].source_db.orders": "8"},
        "TableSourceReceivedBytes": {"Source[0].source_db.orders": "932"},
        "TableSinkCommittedCount": {"Sink[0].analytics.clean_orders": "4"},
        "TableSinkCommittedBytes": {"Sink[0].analytics.clean_orders": "482"},
        "TableSinkWriteCount": {"Sink[0].analytics.clean_orders": "4"},
        "TableSinkWriteBytes": {"Sink[0].analytics.clean_orders": "482"},
    },
}


def test_normalize_reads_real_engine_metrics() -> None:
    """引擎指标应以字符串形式返回，归一化后必须变成整数而不是 0。"""
    metrics = normalize_seatunnel_metrics(REAL_ENGINE_PAYLOAD["metrics"])
    assert metrics.source_rows == 8
    assert metrics.sink_rows == 4
    assert metrics.source_bytes == 932
    assert metrics.sink_bytes == 482


def test_normalize_keeps_per_table_detail() -> None:
    """分表指标保留全名，源端 8 行 / 汇端 4 行的差异在明细里看得见。"""
    metrics = normalize_seatunnel_metrics(REAL_ENGINE_PAYLOAD["metrics"])
    by_name = {table.table_name: table for table in metrics.tables}
    assert by_name["Source[0].source_db.orders"].source_rows == 8
    assert by_name["Sink[0].analytics.clean_orders"].sink_rows == 4


@pytest.mark.parametrize(
    "raw_metrics",
    [{}, {"SourceReceivedCount": None}, {"SourceReceivedCount": "not-a-number"}],
)
def test_normalize_never_invents_metrics(raw_metrics: dict[str, Any]) -> None:
    """指标缺失或不可解析时一律归零，绝不猜数字。"""
    metrics = normalize_seatunnel_metrics(raw_metrics)
    assert (metrics.source_rows, metrics.sink_rows) == (0, 0)
    assert (metrics.source_bytes, metrics.sink_bytes) == (0, 0)


@pytest.mark.asyncio
async def test_get_job_surfaces_engine_metrics() -> None:
    """适配器必须把 `metrics` 透传给上层——丢掉它上层就只能写 0。"""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=REAL_ENGINE_PAYLOAD)

    adapter = ZetaRestAdapter(base_url="http://seatunnel.test")
    await adapter.aclose()
    adapter._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://seatunnel.test"
    )
    try:
        info = await adapter.get_job("1163299551575015425")
    finally:
        await adapter.aclose()

    assert isinstance(info, SeatunnelJobInfo)
    assert info.status == "FINISHED"
    assert info.metrics["SourceReceivedCount"] == "8"


@pytest.mark.asyncio
async def test_get_job_tolerates_missing_metrics() -> None:
    """引擎还不提供指标时退化为空字典，不能让轮询整个失败。"""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"jobId": "1", "jobStatus": "RUNNING"})

    adapter = ZetaRestAdapter(base_url="http://seatunnel.test")
    await adapter.aclose()
    adapter._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://seatunnel.test"
    )
    try:
        info = await adapter.get_job("1")
    finally:
        await adapter.aclose()

    assert info.metrics == {}


async def _seed_running_run(session: AsyncSession) -> ExecutionRun:
    """建项目/流水线/版本与一条 RUNNING 的执行记录。"""
    suffix = uuid.uuid4()
    project = Project(name=f"metrics-{suffix}")
    engineer = User(subject=f"metrics-engineer-{suffix}", display_name="指标工程师")
    session.add_all([project, engineer])
    await session.flush()
    session.add(
        ProjectMembership(
            project_id=project.id, user_id=engineer.id, role=ProjectRole.DATA_ENGINEER
        )
    )
    pipeline = Pipeline(project_id=project.id, name=f"pipeline-{suffix}")
    session.add(pipeline)
    await session.flush()
    version = PipelineVersion(
        pipeline_id=pipeline.id,
        version_number=1,
        status=PipelineVersionStatus.APPROVED,
        requirement="指标测试",
        plan={},
        created_by=engineer.id,
    )
    session.add(version)
    await session.flush()
    run = ExecutionRun(
        pipeline_version_id=version.id,
        status=ExecutionStatus.RUNNING,
        idempotency_key=f"exec-{suffix.hex[:16]}",
        requested_by=engineer.id,
    )
    session.add(run)
    await session.commit()
    return run


@pytest.mark.asyncio
async def test_engine_finish_persists_real_rows(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """引擎 FINISHED 时，Run 上落库的必须是引擎真实计数，而不是硬编码 0。"""
    from etl_agent import execution_tasks

    run = await _seed_running_run(session)

    def factory() -> AsyncSession:
        return session

    monkeypatch.setattr(execution_tasks, "SessionFactory", factory)

    await execution_tasks._advance_from_engine_status(
        run.id, "FINISHED", REAL_ENGINE_PAYLOAD["metrics"]
    )

    refreshed = await session.get(ExecutionRun, run.id)
    assert refreshed is not None
    assert refreshed.status is ExecutionStatus.SUCCEEDED
    assert refreshed.source_rows == 8
    assert refreshed.sink_rows == 4
    assert refreshed.source_bytes == 932
    assert refreshed.sink_bytes == 482


@pytest.mark.asyncio
async def test_engine_finish_without_metrics_stays_zero(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """引擎没给指标时仍要能正常收敛到 SUCCEEDED，指标写 0。"""
    from etl_agent import execution_tasks

    run = await _seed_running_run(session)

    def factory() -> AsyncSession:
        return session

    monkeypatch.setattr(execution_tasks, "SessionFactory", factory)

    await execution_tasks._advance_from_engine_status(run.id, "FINISHED", None)

    refreshed = await session.get(ExecutionRun, run.id)
    assert refreshed is not None
    assert refreshed.status is ExecutionStatus.SUCCEEDED
    assert (refreshed.source_rows, refreshed.sink_rows) == (0, 0)
