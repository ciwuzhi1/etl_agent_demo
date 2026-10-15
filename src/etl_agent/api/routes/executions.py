"""执行 API：运行历史、事件、质量报告、取消与重试准备（第 10 章）。

端点：
- GET  /projects/{project_id}/execution-runs                 列出项目执行历史
- GET  /execution-runs/{run_id}                              单个执行详情
- GET  /execution-runs/{run_id}/events                       执行事件流
- GET  /execution-runs/{run_id}/quality-report               质量报告
- POST /execution-runs/{run_id}/cancellation                 请求取消（RUNNING -> CANCELLING）
- POST /execution-runs/{run_id}/retry-preparations           为失败且可重试的执行创建新 QUEUED 运行

ExecutionEvent / QualityReport 模型尚未落库时按任务口径返回 mock：
事件由运行时间戳推导，质量报告由行数指标推导，绝不虚构未发生的事件。
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    ExecutionEventResponse,
    ExecutionRunResponse,
    QualityReportResponse,
)
from etl_agent.application.profiling import require_project_role
from etl_agent.domain.enums import ExecutionStatus, ProjectRole, QualityStatus
from etl_agent.domain.errors import ResourceNotFound, VersionConflict
from etl_agent.domain.state_machine import require_execution_transition
from etl_agent.infrastructure import models
from etl_agent.infrastructure.database import get_session

router = APIRouter(tags=["executions"])

_VIEWER_ROLES = {
    ProjectRole.ADMIN,
    ProjectRole.DATA_ENGINEER,
    ProjectRole.APPROVER,
    ProjectRole.VIEWER,
}
_ENGINEER_ROLES = {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER}
_TERMINAL_STATUSES = {
    ExecutionStatus.SUCCEEDED,
    ExecutionStatus.FAILED,
    ExecutionStatus.CANCELLED,
}

# 第 10 章可能尚未在 models.py 建出事件/质量报告表；缺失时降级为推导的 mock。
ExecutionEvent: Any = getattr(models, "ExecutionEvent", None)
QualityReport: Any = getattr(models, "QualityReport", None)


@router.get(
    "/projects/{project_id}/execution-runs",
    response_model=list[ExecutionRunResponse],
)
async def list_execution_runs(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[ExecutionRunResponse]:
    """按创建时间倒序列出项目全部执行记录（含重试产生的新行）。"""
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    runs = (
        await session.scalars(
            select(models.ExecutionRun)
            .join(
                models.PipelineVersion,
                models.PipelineVersion.id == models.ExecutionRun.pipeline_version_id,
            )
            .join(models.Pipeline, models.Pipeline.id == models.PipelineVersion.pipeline_id)
            .where(models.Pipeline.project_id == project_id)
            .order_by(models.ExecutionRun.created_at.desc())
            .limit(100)
        )
    ).all()
    return [_run_response(run) for run in runs]


@router.get("/execution-runs/{run_id}", response_model=ExecutionRunResponse)
async def get_execution_run(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> ExecutionRunResponse:
    """读取单个执行；权限经其 PipelineVersion 所属项目回溯校验。"""
    run = await _load_run(session, run_id)
    await require_project_role(session, await _project_id_of(session, run), actor, _VIEWER_ROLES)
    return _run_response(run)


@router.get("/execution-runs/{run_id}/events", response_model=list[ExecutionEventResponse])
async def list_execution_events(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[ExecutionEventResponse]:
    """执行事件流：ExecutionEvent 表落地后读表，否则由生命周期时间戳推导。"""
    run = await _load_run(session, run_id)
    await require_project_role(session, await _project_id_of(session, run), actor, _VIEWER_ROLES)
    if ExecutionEvent is not None:
        rows: list[Any] = list(
            (
                await session.scalars(
                    select(ExecutionEvent)
                    .where(ExecutionEvent.run_id == run_id)
                    .order_by(ExecutionEvent.occurred_at)
                )
            ).all()
        )
        return [ExecutionEventResponse.model_validate(row) for row in rows]
    return _derived_events(run)


@router.get(
    "/execution-runs/{run_id}/quality-report",
    response_model=QualityReportResponse,
)
async def get_quality_report(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> QualityReportResponse:
    """质量报告：QualityReport 表落地后读表，否则由行数指标推导（INCOMPLETE≠通过）。"""
    run = await _load_run(session, run_id)
    await require_project_role(session, await _project_id_of(session, run), actor, _VIEWER_ROLES)
    if QualityReport is not None:
        report = await session.scalar(
            select(QualityReport)
            .where(QualityReport.run_id == run_id)
            .order_by(QualityReport.id.desc())
            .limit(1)
        )
        if report is not None:
            return QualityReportResponse.model_validate(report)
    return _derived_report(run)


@router.post(
    "/execution-runs/{run_id}/cancellation",
    response_model=ExecutionRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def request_cancellation(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> ExecutionRunResponse:
    """把运行推进到 CANCELLING，真正的引擎停止由 Worker 轮询该状态后代执行。"""
    async with session.begin():
        run = await _load_run(session, run_id)
        await require_project_role(
            session, await _project_id_of(session, run), actor, _ENGINEER_ROLES
        )
        if run.status in _TERMINAL_STATUSES:
            raise VersionConflict("执行已进入终态，无法取消")
        if run.status is ExecutionStatus.CANCELLING:
            return _run_response(run)
        require_execution_transition(run.status, ExecutionStatus.CANCELLING)
        run.status = ExecutionStatus.CANCELLING
        await session.flush()
        return _run_response(run)


@router.post(
    "/execution-runs/{run_id}/retry-preparations",
    response_model=ExecutionRunResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_retry_preparation(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> ExecutionRunResponse:
    """为失败且可重试的执行新建一行 QUEUED 运行；原失败行保持不可变。"""
    async with session.begin():
        run = await _load_run(session, run_id)
        await require_project_role(
            session, await _project_id_of(session, run), actor, _ENGINEER_ROLES
        )
        if run.status is not ExecutionStatus.FAILED:
            raise VersionConflict("只有 FAILED 的执行可以准备重试")
        if run.retryable is False:
            raise VersionConflict("该执行被判定为不可重试，请修正配置后开新版本")
        preparation = models.ExecutionRun(
            pipeline_version_id=run.pipeline_version_id,
            status=ExecutionStatus.QUEUED,
            idempotency_key=f"retry-{run.id}-{uuid.uuid4().hex[:12]}",
            requested_by=actor,
        )
        session.add(preparation)
        await session.flush()
        return _run_response(preparation)


async def _load_run(session: AsyncSession, run_id: uuid.UUID) -> models.ExecutionRun:
    """按 ID 读取执行记录，缺失时返回 404 语义。"""
    run = await session.get(models.ExecutionRun, run_id)
    if run is None:
        raise ResourceNotFound("执行记录不存在")
    return run


async def _project_id_of(session: AsyncSession, run: models.ExecutionRun) -> uuid.UUID:
    """经 PipelineVersion -> Pipeline 回溯执行所属项目。"""
    version = await session.get(models.PipelineVersion, run.pipeline_version_id)
    if version is None:
        raise ResourceNotFound("Pipeline 版本不存在")
    project_id = await session.scalar(
        select(models.Pipeline.project_id).where(models.Pipeline.id == version.pipeline_id)
    )
    if project_id is None:
        raise ResourceNotFound("Pipeline 不存在")
    return project_id


def _run_response(run: models.ExecutionRun) -> ExecutionRunResponse:
    """执行响应构造：engine_status 尚未落库，先由 SeaTunnel JobId 是否存在推导占位。"""
    response = ExecutionRunResponse.model_validate(run)
    if response.engine_status is None and run.seatunnel_job_id is not None:
        response.engine_status = run.status.value
    return response


def _as_utc(moment: datetime) -> datetime:
    """把 SQLite 回读的 naive 时间统一补成 UTC，避免排序时 naive/aware 混比。"""
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _derived_events(run: models.ExecutionRun) -> list[ExecutionEventResponse]:
    """由生命周期时间戳推导事件流；没有发生过的阶段不虚构事件。"""
    now = datetime.now(UTC)
    raw: list[tuple[datetime | None, str, str, str, str, dict[str, Any]]] = [
        (run.created_at or now, "INFO", "SUBMISSION", "execution.queued", "执行已排队", {}),
    ]
    if run.submitted_at:
        raw.append(
            (
                run.submitted_at,
                "INFO",
                "SUBMISSION",
                "execution.submitted",
                "已提交 SeaTunnel 引擎",
                {"seatunnel_job_id": run.seatunnel_job_id},
            )
        )
    if run.started_at:
        raw.append((run.started_at, "INFO", "SOURCE", "execution.running", "执行开始运行", {}))
    if run.cancelled_at:
        raw.append(
            (run.cancelled_at, "WARN", "CANCELLATION", "execution.cancelled", "执行已取消", {})
        )
    if run.failed_at or run.status is ExecutionStatus.FAILED:
        raw.append(
            (
                run.failed_at or run.finished_at,
                "ERROR",
                run.failure_stage or "UNKNOWN",
                "execution.failed",
                run.error or "执行失败",
                {"failure_category": run.failure_category, "retryable": run.retryable},
            )
        )
    if run.status is ExecutionStatus.SUCCEEDED:
        raw.append(
            (
                run.finished_at or now,
                "INFO",
                "SINK",
                "execution.succeeded",
                "执行成功",
                {"source_rows": run.source_rows, "sink_rows": run.sink_rows},
            )
        )
    events: list[ExecutionEventResponse] = []
    for index, (occurred, level, stage, event_type, message, details) in enumerate(
        sorted(raw, key=lambda item: _as_utc(item[0] or now))
    ):
        events.append(
            ExecutionEventResponse(
                id=uuid.uuid5(uuid.NAMESPACE_URL, f"{run.id}:event:{index}"),
                occurred_at=_as_utc(occurred or now),
                level=level,
                stage=stage,
                event_type=event_type,
                message=message,
                details=details,
            )
        )
    return events


def _derived_report(run: models.ExecutionRun) -> QualityReportResponse:
    """由行数指标推导质量结论；缺指标即 INCOMPLETE，不等于通过。"""
    if run.status is ExecutionStatus.SUCCEEDED and run.source_rows is not None:
        conclusion = QualityStatus.PASSED
    elif run.status is ExecutionStatus.FAILED:
        conclusion = QualityStatus.FAILED
    else:
        conclusion = QualityStatus.INCOMPLETE
    total = run.source_rows
    passed = run.sink_rows if conclusion is QualityStatus.PASSED else None
    rejected = total - passed if total is not None and passed is not None else None
    rate: float | None = None
    if conclusion is QualityStatus.PASSED and total is not None:
        rate = (total - (passed or 0)) / total if total > 0 else 0.0
    return QualityReportResponse(
        id=uuid.uuid5(uuid.NAMESPACE_URL, f"{run.id}:quality"),
        run_id=run.id,
        status=conclusion,
        total_rows=total,
        passed_rows=passed,
        rejected_rows=rejected,
        reject_rate=rate,
        valid_sink=passed if conclusion is QualityStatus.PASSED else None,
        error_sink=rejected if conclusion is QualityStatus.PASSED else None,
    )
