"""控制台只读端点（第 15 章 Web Console 数据接入）。

这些端点是给五视图（总览 / 数据连接 / Studio / 运行中心 / 安全治理）读取真实数据的
只读查询，全部落在既有表上，不引入新的写入路径：

- GET /context                             本地演示会话上下文（仅 dev-header）
- GET /projects/{id}/overview              总览指标与就绪连接
- GET /projects/{id}/secret-lifecycles     凭据到期窗口
- GET /projects/{id}/data-profiles         项目级 Profile 列表
- GET /workers                             Worker 心跳（心跳表未落库时为空）
- GET /execution-runs/{id}/supervision     监督快照历史
- GET /execution-runs/{id}/supervision/latest
- GET /benchmark-runs/latest               最近一次 Benchmark
- GET /benchmark-runs/{id}/cases           Benchmark 用例明细
- GET /projects/{id}/safety-proposals      项目视图下的安全提案（全量）
- GET /safety-proposals/{id}               单个提案
- GET /safety-proposals/{id}/releases      提案的发布记录
- GET /projects/{id}/execution-preparations 执行准备单

权限沿用 `require_project_role`；context 仅本地 dev-header 模式开放。
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    BenchmarkCaseResponse,
    BenchmarkRunResponse,
    DataConnectionResponse,
    DataProfileResponse,
    ExecutionPreparationResponse,
    FlowStageResponse,
    GuardrailResponse,
    OverviewMetricsResponse,
    OverviewResponse,
    SecretLifecycleResponse,
    SupervisionSnapshotResponse,
    WorkerStatusResponse,
    WorkspaceContextResponse,
    WorkspaceMemberResponse,
)
from etl_agent.application.harness_access import HarnessAccessService
from etl_agent.application.profiling import require_project_role
from etl_agent.config import Settings, get_settings
from etl_agent.domain.enums import (
    ConnectionStatus,
    ExecutionStatus,
    ProjectRole,
)
from etl_agent.domain.errors import ResourceNotFound
from etl_agent.infrastructure import models
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import Project, ProjectMembership, User

router = APIRouter(tags=["console"])

harness_access = HarnessAccessService()

_VIEWER_ROLES = {
    ProjectRole.ADMIN,
    ProjectRole.DATA_ENGINEER,
    ProjectRole.APPROVER,
    ProjectRole.VIEWER,
}

# 静态展示内容：受管执行主线与确定性护栏。属于说明性文案，不随库内状态变化。
_FLOW_STAGES = [
    FlowStageResponse(key="requirement", label="需求", hint="自然语言只作为意图"),
    FlowStageResponse(key="profile", label="探查", hint="脱敏 Profile 快照"),
    FlowStageResponse(key="generate", label="生成", hint="模型只产出候选"),
    FlowStageResponse(key="gate", label="门禁", hint="确定性契约校验"),
    FlowStageResponse(key="approve", label="审批", hint="四眼职责分离"),
    FlowStageResponse(key="commit", label="授权", hint="一次性 Capability"),
    FlowStageResponse(key="execute", label="执行", hint="SeaTunnel Zeta"),
    FlowStageResponse(key="evidence", label="留证", hint="证据哈希链"),
]
_GUARDRAILS = [
    GuardrailResponse(code="PDP", label="策略裁决在生成之后、执行之前"),
    GuardrailResponse(code="CAP", label="副作用只认一次性签名 Capability"),
    GuardrailResponse(code="OUTBOX", label="命令经事务 Outbox 可靠投递"),
    GuardrailResponse(code="EVIDENCE", label="关键事件写入哈希链"),
    GuardrailResponse(code="MASK", label="样本返回前完成字段级脱敏"),
    GuardrailResponse(code="NO-SECRET", label="秘密不入日志、响应与证据"),
]


@router.get("/context", response_model=WorkspaceContextResponse)
async def get_workspace_context(
    settings: Settings = Depends(get_settings),
    session: AsyncSession = Depends(get_session),
) -> WorkspaceContextResponse:
    """本地演示控制台用它拿到工作区与成员，再以真实成员身份调用其余端点。

    仅在 dev-header 模式下开放：生产 trusted-gateway 模式的身份由网关注入，
    不应存在这种"无身份即可读成员名单"的端点。
    """
    if settings.auth_mode != "dev-header":
        raise ResourceNotFound("当前身份模式下不提供控制台上下文")
    project = await session.scalar(select(Project).order_by(Project.created_at).limit(1))
    if project is None:
        raise ResourceNotFound("工作区尚未初始化")
    membership_rows = (
        await session.execute(
            select(User, ProjectMembership)
            .join(ProjectMembership, ProjectMembership.user_id == User.id)
            .where(ProjectMembership.project_id == project.id)
            .order_by(ProjectMembership.created_at)
        )
    ).all()
    members: list[WorkspaceMemberResponse] = []
    for user, membership in membership_rows:
        roles = await harness_access.roles_for(session, project_id=project.id, user_id=user.id)
        members.append(
            WorkspaceMemberResponse(
                user_id=user.id,
                subject=user.subject,
                display_name=user.display_name,
                role=membership.role,
                harness_roles=sorted(roles, key=lambda value: value.value),
            )
        )
    return WorkspaceContextResponse(
        project_id=project.id, project_name=project.name, members=members
    )


@router.get("/projects/{project_id}/overview", response_model=OverviewResponse)
async def get_overview(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> OverviewResponse:
    """聚合总览指标：今日成败、待办准备单、就绪连接与受管文件计数。"""
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    start_of_day = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    today_succeeded = await _count_runs(
        session, project_id, ExecutionStatus.SUCCEEDED, start_of_day
    )
    today_failed = await _count_runs(session, project_id, ExecutionStatus.FAILED, start_of_day)
    pending_preparations = await session.scalar(
        select(func.count())
        .select_from(models.ExecutionPreparation)
        .where(
            models.ExecutionPreparation.project_id == project_id,
            models.ExecutionPreparation.status == "WAITING_APPROVAL",
        )
    )
    ready_connections = await session.scalar(
        select(func.count())
        .select_from(models.DataConnection)
        .where(
            models.DataConnection.project_id == project_id,
            models.DataConnection.status == ConnectionStatus.READY,
        )
    )
    file_assets = await session.scalar(
        select(func.count())
        .select_from(models.FileAsset)
        .where(models.FileAsset.project_id == project_id)
    )
    ready_rows = (
        await session.scalars(
            select(models.DataConnection)
            .where(
                models.DataConnection.project_id == project_id,
                models.DataConnection.status == ConnectionStatus.READY,
            )
            .order_by(models.DataConnection.created_at.desc())
            .limit(20)
        )
    ).all()
    return OverviewResponse(
        metrics=OverviewMetricsResponse(
            today_succeeded=int(today_succeeded or 0),
            today_failed=int(today_failed or 0),
            pending_preparations=int(pending_preparations or 0),
            ready_connections=int(ready_connections or 0),
            file_assets=int(file_assets or 0),
        ),
        ready_connections=[DataConnectionResponse.model_validate(item) for item in ready_rows],
        flow_stages=_FLOW_STAGES,
        guardrails=_GUARDRAILS,
    )


@router.get(
    "/projects/{project_id}/secret-lifecycles",
    response_model=list[SecretLifecycleResponse],
)
async def list_secret_lifecycles(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[SecretLifecycleResponse]:
    """按连接的凭据年龄列推导到期窗口；不读取密文，也不回显任何秘密引用。"""
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    connections = (
        await session.scalars(
            select(models.DataConnection)
            .where(models.DataConnection.project_id == project_id)
            .order_by(models.DataConnection.created_at.desc())
        )
    ).all()
    now = datetime.now(UTC)
    lifecycles: list[SecretLifecycleResponse] = []
    for connection in connections:
        issued_at = connection.credential_issued_at
        if issued_at.tzinfo is None:
            issued_at = issued_at.replace(tzinfo=UTC)
        expires_at = issued_at + _days(connection.credential_max_age_days)
        days_to_expiry = (expires_at - now).days
        lifecycles.append(
            SecretLifecycleResponse(
                id=connection.secret_ref,
                connection_id=connection.id,
                connection_name=connection.name,
                status="ACTIVE" if days_to_expiry > 0 else "PENDING",
                issued_at=issued_at,
                expires_at=expires_at,
                days_to_expiry=days_to_expiry,
            )
        )
    return lifecycles


@router.get(
    "/projects/{project_id}/data-profiles",
    response_model=list[DataProfileResponse],
)
async def list_project_profiles(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[DataProfileResponse]:
    """列出项目下所有连接产生的 Profile，供连接页聚合展示。"""
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    profiles = (
        await session.scalars(
            select(models.DataProfile)
            .join(
                models.DataConnection,
                models.DataConnection.id == models.DataProfile.connection_id,
            )
            .where(models.DataConnection.project_id == project_id)
            .order_by(models.DataProfile.created_at.desc())
            .limit(200)
        )
    ).all()
    return [DataProfileResponse.model_validate(item) for item in profiles]


@router.get("/workers", response_model=list[WorkerStatusResponse])
async def list_workers(
    _actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> list[WorkerStatusResponse]:
    """读 `worker_heartbeats` 心跳行，按 `worker_heartbeat_stale_seconds` 现算在线状态。

    在线与否是读侧现算而非写入侧结论：进程崩溃时它没有机会把行改成 OFFLINE，
    所以"最后一次心跳距今是否超过陈旧阈值"才是唯一可信信号。心跳表为空时返回空列表。
    """
    heartbeats = (
        await session.scalars(
            select(models.WorkerHeartbeat).order_by(models.WorkerHeartbeat.worker_id)
        )
    ).all()
    now = datetime.now(UTC)
    stale_after = settings.worker_heartbeat_stale_seconds
    statuses: list[WorkerStatusResponse] = []
    for heartbeat in heartbeats:
        last_seen = heartbeat.last_seen_at
        if last_seen is not None and last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=UTC)
        online = last_seen is not None and (now - last_seen).total_seconds() <= stale_after
        statuses.append(
            WorkerStatusResponse(
                id=str(heartbeat.id),
                name=heartbeat.worker_id,
                online=online,
                last_heartbeat_at=last_seen,
                active_runs=heartbeat.active_tasks,
            )
        )
    return statuses


@router.get(
    "/execution-runs/{run_id}/supervision",
    response_model=list[SupervisionSnapshotResponse],
)
async def list_supervision_snapshots(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[SupervisionSnapshotResponse]:
    """按 sequence 升序返回一次执行的监督快照历史。"""
    run = await _load_run(session, run_id)
    await require_project_role(session, await _project_id_of(session, run), actor, _VIEWER_ROLES)
    snapshots = (
        await session.scalars(
            select(models.RuntimeSupervisionSnapshot)
            .where(models.RuntimeSupervisionSnapshot.run_id == run_id)
            .order_by(models.RuntimeSupervisionSnapshot.sequence)
        )
    ).all()
    return [_supervision_response(item) for item in snapshots]


@router.get(
    "/execution-runs/{run_id}/supervision/latest",
    response_model=SupervisionSnapshotResponse | None,
)
async def get_latest_supervision(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SupervisionSnapshotResponse | None:
    """返回最新一条监督快照；尚无快照时返回 null（发送者自行降级为空态）。"""
    run = await _load_run(session, run_id)
    await require_project_role(session, await _project_id_of(session, run), actor, _VIEWER_ROLES)
    snapshot = await session.scalar(
        select(models.RuntimeSupervisionSnapshot)
        .where(models.RuntimeSupervisionSnapshot.run_id == run_id)
        .order_by(models.RuntimeSupervisionSnapshot.sequence.desc())
        .limit(1)
    )
    return _supervision_response(snapshot) if snapshot is not None else None


@router.get("/benchmark-runs/latest", response_model=BenchmarkRunResponse | None)
async def get_latest_benchmark(
    _actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> BenchmarkRunResponse | None:
    """返回最近一次 Benchmark 运行；从未运行过时返回 null。"""
    run = await session.scalar(
        select(models.BenchmarkRun).order_by(models.BenchmarkRun.created_at.desc()).limit(1)
    )
    return _benchmark_response(run) if run is not None else None


@router.get(
    "/benchmark-runs/{benchmark_run_id}/cases",
    response_model=list[BenchmarkCaseResponse],
)
async def list_benchmark_cases(
    benchmark_run_id: uuid.UUID,
    _actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[BenchmarkCaseResponse]:
    """从持久化报告中抽出用例明细，不从别处重新执行评测。"""
    run = await session.get(models.BenchmarkRun, benchmark_run_id)
    if run is None:
        raise ResourceNotFound("Benchmark 运行不存在")
    cases = (run.report or {}).get("cases", [])
    responses: list[BenchmarkCaseResponse] = []
    for case in cases:
        if not isinstance(case, dict):
            continue
        responses.append(
            BenchmarkCaseResponse(
                id=str(case.get("case_id", "")),
                name=str(case.get("case_id", "")),
                expected=case.get("expected") or {},
                observed=case.get("actual") or {},
                passed=bool(case.get("matched")),
                evidence_hash=str(case.get("reproducibility_hash", "")),
            )
        )
    return responses


@router.get(
    "/projects/{project_id}/safety-proposals",
    response_model=list[dict[str, Any]],
)
async def list_project_safety_proposals(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """项目视图下的安全提案列表。

    `safety_proposals` 表没有项目外键（安全进化是全局治理对象），因此这里在项目
    成员校验通过后返回全量提案，而不是按项目过滤。
    """
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    proposals = (
        await session.scalars(
            select(models.SafetyProposal).order_by(models.SafetyProposal.created_at.desc())
        )
    ).all()
    return [_proposal_dict(item) for item in proposals]


@router.get("/safety-proposals/{proposal_id}", response_model=dict[str, Any])
async def get_safety_proposal(
    proposal_id: uuid.UUID,
    _actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """读取单个提案的摘要事实。"""
    proposal = await session.get(models.SafetyProposal, proposal_id)
    if proposal is None:
        raise ResourceNotFound("安全提案不存在")
    return _proposal_dict(proposal)


@router.get("/safety-proposals/{proposal_id}/releases", response_model=list[dict[str, Any]])
async def list_safety_releases(
    proposal_id: uuid.UUID,
    _actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """列出提案的发布记录；模型约束每个提案最多一条，返回 0 或 1 条。"""
    releases = (
        await session.scalars(
            select(models.SafetyRelease)
            .where(models.SafetyRelease.proposal_id == proposal_id)
            .order_by(models.SafetyRelease.created_at.desc())
        )
    ).all()
    return [_release_dict(item) for item in releases]


@router.get(
    "/projects/{project_id}/execution-preparations",
    response_model=list[ExecutionPreparationResponse],
)
async def list_execution_preparations(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[ExecutionPreparationResponse]:
    """列出项目下的执行准备单（Harness Prepare 的产物）。"""
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    preparations = (
        await session.scalars(
            select(models.ExecutionPreparation)
            .where(models.ExecutionPreparation.project_id == project_id)
            .order_by(models.ExecutionPreparation.created_at.desc())
            .limit(100)
        )
    ).all()
    return [ExecutionPreparationResponse.model_validate(item) for item in preparations]


def _days(count: int) -> Any:
    """把"天数"转成 timedelta；单独成函数便于阅读调用点。"""
    from datetime import timedelta

    return timedelta(days=count)


async def _count_runs(
    session: AsyncSession,
    project_id: uuid.UUID,
    status: ExecutionStatus,
    since: datetime,
) -> int:
    """统计项目内某状态、且自 since 起创建的执行数。"""
    value = await session.scalar(
        select(func.count())
        .select_from(models.ExecutionRun)
        .join(
            models.PipelineVersion,
            models.PipelineVersion.id == models.ExecutionRun.pipeline_version_id,
        )
        .join(models.Pipeline, models.Pipeline.id == models.PipelineVersion.pipeline_id)
        .where(
            models.Pipeline.project_id == project_id,
            models.ExecutionRun.status == status,
            models.ExecutionRun.created_at >= since,
        )
    )
    return int(value or 0)


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


def _supervision_response(
    snapshot: models.RuntimeSupervisionSnapshot,
) -> SupervisionSnapshotResponse:
    """把规则命中转成面向运维的原因列表，丢弃其中的结构化证据细节。"""
    reasons = [
        str(match.get("reason"))
        for match in (snapshot.rule_matches or [])
        if isinstance(match, dict) and match.get("reason")
    ]
    return SupervisionSnapshotResponse(
        id=snapshot.id,
        run_id=snapshot.run_id,
        sequence=snapshot.sequence,
        observed_at=snapshot.observed_at,
        engine_status=snapshot.engine_status,
        action=snapshot.action,
        risk_level=snapshot.risk_level,
        cancel_required=snapshot.cancel_required,
        block_publish=snapshot.block_publish,
        reasons=reasons,
    )


def _benchmark_response(run: models.BenchmarkRun) -> BenchmarkRunResponse:
    """把持久化的 Benchmark 运行映射为视图对象。"""
    return BenchmarkRunResponse(
        id=run.id,
        suite_id=run.suite_id,
        version=run.version,
        policy_bundle_version=run.policy_bundle_version,
        gates_passed=run.gates_passed,
        gate_failures=list(run.gate_failures or []),
        started_at=run.started_at,
        finished_at=run.finished_at,
        report=run.report or {},
    )


def _proposal_dict(proposal: models.SafetyProposal) -> dict[str, Any]:
    """提案的 JSON 友好摘要；用 dict 而非固定模型，避免与既有路由的本地模型重复。"""
    return {
        "id": str(proposal.id),
        "kind": proposal.kind.value if hasattr(proposal.kind, "value") else str(proposal.kind),
        "status": proposal.status.value
        if hasattr(proposal.status, "value")
        else str(proposal.status),
        "title": proposal.title,
        "rationale": proposal.rationale,
        "payload_sha256": proposal.payload_sha256,
        "created_by": str(proposal.created_by),
        "reviewed_by": str(proposal.reviewed_by) if proposal.reviewed_by else None,
        "submitted_at": proposal.submitted_at.isoformat() if proposal.submitted_at else None,
        "decided_at": proposal.decided_at.isoformat() if proposal.decided_at else None,
        "created_at": proposal.created_at.isoformat() if proposal.created_at else None,
    }


def _release_dict(release: models.SafetyRelease) -> dict[str, Any]:
    """发布记录的 JSON 友好摘要；不回显签名私钥或候选正文。"""
    return {
        "id": str(release.id),
        "proposal_id": str(release.proposal_id),
        "status": release.status.value if hasattr(release.status, "value") else str(release.status),
        "stage": release.stage.value if hasattr(release.stage, "value") else str(release.stage),
        "canary_traffic_percent": release.canary_traffic_percent,
        "manifest_sha256": release.manifest_sha256,
        "started_at": release.started_at.isoformat() if release.started_at else None,
        "verified_at": release.verified_at.isoformat() if release.verified_at else None,
        "rollback_reason": release.rollback_reason,
        "created_at": release.created_at.isoformat() if release.created_at else None,
    }
