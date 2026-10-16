"""执行任务：向 SeaTunnel Zeta 提交作业，并轮询引擎状态推进 ExecutionRun。

第 10 章执行闭环的 Worker 侧实现。两条纪律贯穿本模块：

- **失败文案脱敏**：提交/轮询失败一律走 `application/execution_diagnostics` 的
  `diagnose_execution_failure` + `redact_execution_error`，只把确定性诊断给出的固定
  中文文案写进 `execution_runs.error`，绝不落异常原文、连接串、主机路径或 JobId 细节。
- **引擎不可达必须优雅**：本机未启用 e2e（无 Doris/SeaTunnel），提交与轮询都要在
  引擎不可用时安全返回，而不是把 Worker 打崩。
"""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote, urlparse

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.execution import ExecutionService
from etl_agent.application.execution_credentials import materialize_execution_placeholders
from etl_agent.application.execution_diagnostics import diagnose_execution_failure
from etl_agent.asyncio_compat import run_async
from etl_agent.config import get_settings
from etl_agent.domain.enums import ExecutionStatus
from etl_agent.domain.errors import DomainError
from etl_agent.domain.harness import HarnessOutboxStatus
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.models import (
    ExecutionRun,
    HarnessCommandOutbox,
    Pipeline,
    PipelineArtifact,
    PipelineVersion,
)
from etl_agent.infrastructure.seatunnel.zeta_adapter import (
    SeatunnelSubmitRequest,
    ZetaRestAdapter,
)
from etl_agent.worker import celery_app

# 引擎提交/轮询的单次超时与制品类型常量。
_ENGINE_TIMEOUT_SECONDS = 10.0
_HOCON_ARTIFACT_KIND = "SEATUNNEL_HOCON"
_TERMINAL_STATUSES = {
    ExecutionStatus.SUCCEEDED,
    ExecutionStatus.FAILED,
    ExecutionStatus.CANCELLED,
}
_MISSING_ARTIFACT_MESSAGE = "未找到可提交的 SeaTunnel 制品配置，请先生成并审批该版本。"
_ENGINE_FAILURE_MESSAGE = "执行节点不可达或作业提交失败，请检查引擎与网络后重试。"


def _local_candidates(uri: str) -> list[Path]:
    """枚举制品 ``uri`` 可能对应的本地路径；非本地协议返回空列表。

    与 console 制品读取口径一致：相对路径先按工作目录解析，再退化到 artifact_root，
    兼容"相对制品根"与"相对项目根"两种写入习惯。
    """
    candidate = uri.strip()
    if not candidate:
        return []
    parsed = urlparse(candidate)
    if parsed.scheme == "file":
        raw = unquote(parsed.path)
        if parsed.netloc and parsed.netloc not in {"", "localhost"}:
            raw = f"//{parsed.netloc}{raw}"
        if len(raw) >= 3 and raw[0] == "/" and raw[2] == ":":
            raw = raw[1:]
        return [Path(raw)]
    if "://" in candidate:
        return []
    path = Path(candidate)
    if path.is_absolute():
        return [path]
    return [path, get_settings().artifact_root / candidate]


def _read_hocon_artifact(uri: str) -> str:
    """按 ``uri`` 读取 SeaTunnel 制品正文；不是本地文件或读取失败时返回空串。"""
    for path in _local_candidates(uri):
        try:
            return path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
    return ""


def _build_adapter() -> ZetaRestAdapter:
    """按配置构造 SeaTunnel Zeta REST 适配器。"""
    return ZetaRestAdapter(
        base_url=str(get_settings().seatunnel_api_url),
        timeout_seconds=_ENGINE_TIMEOUT_SECONDS,
    )


async def _load_run_for_update(session: AsyncSession, run_id: uuid.UUID) -> ExecutionRun | None:
    """带行锁读取执行记录，缺失时返回 None。"""
    return await session.scalar(
        select(ExecutionRun).where(ExecutionRun.id == run_id).with_for_update()
    )


async def _mark_failed_safely(
    run_id: uuid.UUID,
    *,
    error_message: str,
    failure_category: str | None,
    failure_stage: str | None,
    retryable: bool,
) -> None:
    """把执行推进到 FAILED；非法转换或终态已定时静默跳过，绝不让 Worker 崩。"""
    async with SessionFactory() as session:
        async with session.begin():
            try:
                await ExecutionService().mark_failed(
                    session,
                    run_id,
                    error_message,
                    failure_category=failure_category,
                    failure_stage=failure_stage,
                    retryable=retryable,
                )
            except DomainError:
                pass


async def _record_engine_failure(run_id: uuid.UUID, exc: Exception) -> None:
    """按确定性诊断把引擎失败落库：分类用异常文本，落库只写固定脱敏文案。"""
    diagnosis = diagnose_execution_failure(str(exc))
    await _mark_failed_safely(
        run_id,
        error_message=diagnosis.message,
        failure_category=diagnosis.category.value,
        failure_stage=diagnosis.stage.value,
        retryable=diagnosis.retryable,
    )


async def _submit_execution(run_id: uuid.UUID) -> dict[str, object]:
    """读取 Run → 解析制品 → 提交 SeaTunnel → 推进 SUBMITTED/RUNNING。"""
    async with SessionFactory() as session:
        async with session.begin():
            run = await _load_run_for_update(session, run_id)
            if run is None:
                return {"run_id": str(run_id), "status": "NOT_FOUND"}
            if run.status is ExecutionStatus.SUBMITTED and run.seatunnel_job_id:
                return {
                    "run_id": str(run_id),
                    "status": run.status.value,
                    "seatunnel_job_id": run.seatunnel_job_id,
                }
            if run.status is not ExecutionStatus.QUEUED:
                return {"run_id": str(run_id), "status": run.status.value, "submitted": False}
            version_id = run.pipeline_version_id
            job_name = run.idempotency_key

    hocon = await _load_hocon(version_id)
    if not hocon:
        await _mark_failed_safely(
            run_id,
            error_message=_MISSING_ARTIFACT_MESSAGE,
            failure_category="CONFIGURATION",
            failure_stage="SUBMISSION",
            retryable=False,
        )
        return {"run_id": str(run_id), "status": ExecutionStatus.FAILED.value}

    adapter = _build_adapter()
    try:
        try:
            job_id = await adapter.submit(SeatunnelSubmitRequest(hocon=hocon, job_name=job_name))
        finally:
            await adapter.aclose()
    except Exception as exc:
        await _record_engine_failure(run_id, exc)
        return {"run_id": str(run_id), "status": ExecutionStatus.FAILED.value}

    async with SessionFactory() as session:
        async with session.begin():
            service = ExecutionService()
            await service.mark_submitted(session, run_id, job_id)
            await service.mark_running(session, run_id)
    # 作业已在引擎里跑，立刻投出第一条监控命令，否则没人把终态写回 Run。
    await _enqueue_monitor(run_id, delay_seconds=_MONITOR_POLL_SECONDS)
    return {
        "run_id": str(run_id),
        "status": ExecutionStatus.RUNNING.value,
        "seatunnel_job_id": job_id,
    }


async def _load_hocon(version_id: uuid.UUID) -> str:
    """读取某版本的 SeaTunnel 制品正文并物化平台占位符；没有可用制品时返回空串。

    物化要用到连接的 Secret 与主机信息，所以在同一会话内完成：制品本身只带
    ``${<CONNECTOR>_...}`` 占位符，真实凭据直到这里才进入内存。
    """
    async with SessionFactory() as session:
        artifact = await session.scalar(
            select(PipelineArtifact)
            .where(
                PipelineArtifact.pipeline_version_id == version_id,
                PipelineArtifact.kind == _HOCON_ARTIFACT_KIND,
            )
            .order_by(PipelineArtifact.created_at.desc())
            .limit(1)
        )
        if artifact is None or not artifact.uri:
            return ""
        hocon = _read_hocon_artifact(artifact.uri)
        if not hocon:
            return ""
        version = await session.get(PipelineVersion, version_id)
        plan = dict(version.plan) if version is not None and version.plan else None
        return await materialize_execution_placeholders(session, plan=plan, hocon=hocon)


_MONITOR_POLL_SECONDS = 5
# 引擎侧终态：出现即停止续投，交给 _advance_from_engine_status 落库。
_ENGINE_TERMINAL_STATUSES = frozenset(
    {"FINISHED", "SUCCEEDED", "SUCCESS", "FAILED", "CANCELED", "CANCELLED"}
)


async def _enqueue_monitor(run_id: uuid.UUID, *, delay_seconds: int = 0) -> None:
    """把一次作业状态轮询写进事务 Outbox，交给 ``scan-outbox`` 扫描后派发。

    提交与监控都由这条 Outbox 命令驱动：作业提交成功后投出第一条，此后每条监控
    命令若发现引擎仍在运行便自行续投，直到引擎给出终态为止——否则 Run 会永远停在
    RUNNING（作业早已 FINISHED，只是没人回写）。
    """
    async with SessionFactory() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(ExecutionRun, Pipeline.project_id)
                    .join(PipelineVersion, PipelineVersion.id == ExecutionRun.pipeline_version_id)
                    .join(Pipeline, Pipeline.id == PipelineVersion.pipeline_id)
                    .where(ExecutionRun.id == run_id)
                )
            ).first()
            if row is None:
                return
            run, project_id = row
            session.add(
                HarnessCommandOutbox(
                    project_id=project_id,
                    command_payload={"execution_run_id": str(run_id)},
                    target="worker.execution.monitor",
                    status=HarnessOutboxStatus.PENDING,
                    available_at=datetime.now(UTC) + timedelta(seconds=delay_seconds),
                    created_by=run.requested_by,
                )
            )


async def _monitor_execution(run_id: uuid.UUID) -> dict[str, object]:
    """查询引擎作业状态并推进 Run；引擎不可用时安全返回，不崩。"""
    async with SessionFactory() as session:
        run = await session.get(ExecutionRun, run_id)
        if run is None:
            return {"run_id": str(run_id), "status": "NOT_FOUND"}
        if run.status in _TERMINAL_STATUSES:
            return {"run_id": str(run_id), "status": run.status.value, "advanced": False}
        seatunnel_job_id = run.seatunnel_job_id
    if not seatunnel_job_id:
        return {"run_id": str(run_id), "status": "NO_JOB"}

    adapter = _build_adapter()
    try:
        try:
            info = await adapter.get_job(seatunnel_job_id)
        finally:
            await adapter.aclose()
    except Exception:
        return {"run_id": str(run_id), "status": "ENGINE_UNAVAILABLE"}

    engine_status = info.status.strip().upper()
    await _advance_from_engine_status(run_id, engine_status)
    # 尚未到终态就续投下一条，形成自续轮询直到引擎收敛。
    if engine_status not in _ENGINE_TERMINAL_STATUSES:
        await _enqueue_monitor(run_id, delay_seconds=_MONITOR_POLL_SECONDS)
    return {"run_id": str(run_id), "engine_status": engine_status}


async def _advance_from_engine_status(run_id: uuid.UUID, engine_status: str) -> None:
    """按引擎状态推进 Run；非法转换静默跳过（幂等的轮询不应互相打架）。"""
    service = ExecutionService()
    async with SessionFactory() as session:
        async with session.begin():
            try:
                if engine_status in {"RUNNING", "CREATED", "SCHEDULED", "RESTARTING"}:
                    await service.mark_running(session, run_id)
                elif engine_status in {"FINISHED", "SUCCEEDED", "SUCCESS"}:
                    # 引擎 JobInfo 未提供行/字节指标，这里写 0 占位，避免虚构指标。
                    await service.mark_succeeded(session, run_id, 0, 0, 0, 0)
                elif engine_status in {"FAILED", "CANCELED", "CANCELLED"}:
                    await service.mark_failed(
                        session,
                        run_id,
                        _ENGINE_FAILURE_MESSAGE,
                        failure_category="ENGINE",
                        failure_stage="EXECUTION",
                        retryable=False,
                    )
            except DomainError:
                pass


@celery_app.task(name="etl_agent.submit_execution")  # type: ignore[untyped-decorator]
def submit_execution(execution_run_id: str) -> dict[str, object]:
    """由 Celery 执行一次作业提交，任务参数只携带 ExecutionRun UUID。"""
    return run_async(_submit_execution(uuid.UUID(execution_run_id)))


@celery_app.task(name="etl_agent.monitor_execution")  # type: ignore[untyped-decorator]
def monitor_execution(execution_run_id: str) -> dict[str, object]:
    """由 Celery 轮询一次作业状态，任务参数只携带 ExecutionRun UUID。"""
    return run_async(_monitor_execution(uuid.UUID(execution_run_id)))
