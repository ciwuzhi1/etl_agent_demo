"""Pipeline 版本 API：创建、版本查询、生成记录与确定性门禁推进（第 9 章）。

端点：
- POST /projects/{project_id}/pipelines                    创建 Pipeline + 首个 DRAFT 版本
- GET  /projects/{project_id}/pipeline-versions            列出项目的版本历史
- GET  /pipeline-versions/{version_id}                     版本详情
- POST /pipeline-versions/{version_id}/record-generation   回写生成产物（契约门禁在服务端）
- POST /pipeline-versions/{version_id}/validate            GENERATED -> VALIDATED

写权限由 PipelineService 内部再判一次（ADMIN/DATA_ENGINEER），
读权限在这里用 require_project_role 把关。
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    PipelineCreateRequest,
    PipelineVersionResponse,
    RecordGenerationRequest,
)
from etl_agent.application.pipelines import PipelineService
from etl_agent.application.profiling import require_project_role
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import ResourceNotFound
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import Pipeline, PipelineVersion

router = APIRouter(tags=["pipelines"])

_VIEWER_ROLES = {
    ProjectRole.ADMIN,
    ProjectRole.DATA_ENGINEER,
    ProjectRole.APPROVER,
    ProjectRole.VIEWER,
}
_ENGINEER_ROLES = {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER}

pipeline_service = PipelineService()


@router.post(
    "/projects/{project_id}/pipelines",
    response_model=PipelineVersionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_pipeline(
    project_id: uuid.UUID,
    request: PipelineCreateRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> PipelineVersionResponse:
    """创建 Pipeline 并同时冻结 v1 DRAFT 版本，需求文本与后续产物分离存储。"""
    async with session.begin():
        await require_project_role(session, project_id, actor, _ENGINEER_ROLES)
        version = await pipeline_service.create(
            session,
            project_id=project_id,
            actor_id=actor,
            name=request.name,
            requirement=request.requirement,
            trace_id=trace_id,
        )
        response = PipelineVersionResponse.model_validate(version)
    return response


@router.get(
    "/projects/{project_id}/pipeline-versions",
    response_model=list[PipelineVersionResponse],
)
async def list_pipeline_versions(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[PipelineVersionResponse]:
    """按创建时间倒序列出项目全部 Pipeline 版本。"""
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    versions = (
        await session.scalars(
            select(PipelineVersion)
            .join(Pipeline, Pipeline.id == PipelineVersion.pipeline_id)
            .where(Pipeline.project_id == project_id)
            .order_by(PipelineVersion.created_at.desc())
        )
    ).all()
    return [PipelineVersionResponse.model_validate(version) for version in versions]


@router.get("/pipeline-versions/{version_id}", response_model=PipelineVersionResponse)
async def get_pipeline_version(
    version_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> PipelineVersionResponse:
    """读取单个版本；跨项目访问在角色校验时被挡下。"""
    version, project_id = await _load_version(session, version_id)
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    return PipelineVersionResponse.model_validate(version)


@router.post(
    "/pipeline-versions/{version_id}/record-generation",
    response_model=PipelineVersionResponse,
)
async def record_generation(
    version_id: uuid.UUID,
    request: RecordGenerationRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> PipelineVersionResponse:
    """保存生成结果：先过 require_generation_contract 门禁，再冻结产物并推进 GENERATED。"""
    async with session.begin():
        _version, project_id = await _load_version(session, version_id)
        await require_project_role(session, project_id, actor, _ENGINEER_ROLES)
        updated = await pipeline_service.record_generation(
            session,
            version_id=version_id,
            actor_id=actor,
            plan=request.plan,
            hocon=request.hocon,
            artifact_uri=request.artifact_uri,
            upstream_version=request.upstream_version,
            trace_id=trace_id,
        )
        response = PipelineVersionResponse.model_validate(updated)
    return response


@router.post("/pipeline-versions/{version_id}/validate", response_model=PipelineVersionResponse)
async def validate_pipeline_version(
    version_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> PipelineVersionResponse:
    """执行确定性校验后把版本推进到 VALIDATED，非法状态由状态机拒绝。"""
    async with session.begin():
        _version, project_id = await _load_version(session, version_id)
        await require_project_role(session, project_id, actor, _ENGINEER_ROLES)
        updated = await pipeline_service.validate(
            session, version_id=version_id, actor_id=actor, trace_id=trace_id
        )
        response = PipelineVersionResponse.model_validate(updated)
    return response


async def _load_version(
    session: AsyncSession, version_id: uuid.UUID
) -> tuple[PipelineVersion, uuid.UUID]:
    """解析版本及其所属项目，缺失时统一抛 404 语义。"""
    row = (
        await session.execute(
            select(PipelineVersion, Pipeline.project_id)
            .join(Pipeline, Pipeline.id == PipelineVersion.pipeline_id)
            .where(PipelineVersion.id == version_id)
        )
    ).one_or_none()
    if row is None:
        raise ResourceNotFound("Pipeline 版本不存在")
    version: PipelineVersion = row[0]
    return version, row[1]
