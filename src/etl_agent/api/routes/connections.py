import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    CreateConnectionRequest,
    CreateProfileRequest,
    DataConnectionResponse,
    DataProfileResponse,
)
from etl_agent.application.connections import ConnectionService
from etl_agent.application.profiling import ProfileService, require_project_role
from etl_agent.application.secrets import ConnectionCredentials
from etl_agent.connection_tasks import profile_table, test_data_connection
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import ResourceNotFound
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import DataConnection, DataProfile
from etl_agent.infrastructure.secret_factory import create_secret_provider

router = APIRouter(tags=["connections"])
# 凭据轮换策略端点（PUT /connections/{id}/credential-rotation-policy）依赖
# application/credential_expiry.py，该服务在课件里没有对应章节正文，本章不实现；
# data_connections 上的凭据年龄列已建好并保持默认值，端点接入后无需再改表结构。


@router.get("/projects/{project_id}/connections", response_model=list[DataConnectionResponse])
async def list_connections(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[DataConnectionResponse]:
    """在数据连接与探查接口中分页或按权限列出资源；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    await require_project_role(
        session,
        project_id,
        actor,
        {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER, ProjectRole.APPROVER, ProjectRole.VIEWER},
    )
    connections = (
        await session.scalars(
            select(DataConnection)
            .where(DataConnection.project_id == project_id)
            .order_by(DataConnection.created_at.desc())
        )
    ).all()
    return [DataConnectionResponse.model_validate(item) for item in connections]


@router.post(
    "/connections",
    response_model=DataConnectionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_connection(
    request: CreateConnectionRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> DataConnectionResponse:
    """登记连接元数据并把凭据交给 SecretProvider 加密保存，响应和审计均不包含明文。"""
    service = ConnectionService(create_secret_provider())
    async with session.begin():
        connection = await service.create(
            session,
            project_id=request.project_id,
            actor_id=actor,
            name=request.name,
            connector_type=request.connector_type,
            host=request.host,
            port=request.port,
            database_name=request.database_name,
            options=request.options,
            credentials=ConnectionCredentials.model_validate(request.credentials.model_dump()),
            trace_id=trace_id,
        )
    return DataConnectionResponse.model_validate(connection)


@router.get("/connections/{connection_id}", response_model=DataConnectionResponse)
async def get_connection(
    connection_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> DataConnectionResponse:
    """在数据连接与探查接口中读取单个资源并校验可见性；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    connection = await session.get(DataConnection, connection_id)
    if connection is None:
        raise ResourceNotFound("数据连接不存在")
    await require_project_role(
        session,
        connection.project_id,
        actor,
        {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER, ProjectRole.APPROVER, ProjectRole.VIEWER},
    )
    return DataConnectionResponse.model_validate(connection)


@router.post(
    "/connections/{connection_id}/tests",
    response_model=DataConnectionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def test_connection(
    connection_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> DataConnectionResponse:
    """将连接标记为测试中并投递连通性任务，API 不阻塞等待外部数据库。

    语义是"已受理，结果轮询 ``GET /connections/{id}``"，因此与同章节的
    ``/profiles``、``/file-assets`` 一样返回 202，而不是把已受理错报成 200 完成。
    """
    service = ConnectionService(create_secret_provider())
    async with session.begin():
        connection = await service.mark_testing(
            session, connection_id=connection_id, actor_id=actor
        )
    test_data_connection.delay(str(connection.id))
    return DataConnectionResponse.model_validate(connection)


@router.post(
    "/connections/{connection_id}/profiles",
    response_model=DataProfileResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_profile(
    connection_id: uuid.UUID,
    request: CreateProfileRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> DataProfileResponse:
    """创建表级探查请求，异步采集 Schema、主键和经过脱敏的受限样本。"""
    async with session.begin():
        profile = await ProfileService().request(
            session,
            connection_id=connection_id,
            actor_id=actor,
            schema_name=request.schema_name,
            table_name=request.table_name,
            endpoint_role=request.endpoint_role,
            trace_id=trace_id,
        )
    profile_table.delay(str(profile.id), request.sample_limit)
    return DataProfileResponse.model_validate(profile)


@router.get("/data-profiles/{profile_id}", response_model=DataProfileResponse)
async def get_profile(
    profile_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> DataProfileResponse:
    """在数据连接与探查接口中读取单个资源并校验可见性；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    profile = await session.get(DataProfile, profile_id)
    if profile is None:
        raise ResourceNotFound("DataProfile 不存在")
    connection = await session.get(DataConnection, profile.connection_id)
    if connection is None:
        raise ResourceNotFound("数据连接不存在")
    await require_project_role(
        session,
        connection.project_id,
        actor,
        {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER, ProjectRole.APPROVER, ProjectRole.VIEWER},
    )
    return DataProfileResponse.model_validate(profile)


@router.get("/connections/{connection_id}/profiles", response_model=list[DataProfileResponse])
async def list_profiles(
    connection_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[DataProfileResponse]:
    """在数据连接与探查接口中分页或按权限列出资源；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    connection = await session.get(DataConnection, connection_id)
    if connection is None:
        raise ResourceNotFound("数据连接不存在")
    await require_project_role(
        session,
        connection.project_id,
        actor,
        {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER, ProjectRole.APPROVER, ProjectRole.VIEWER},
    )
    profiles = (
        await session.scalars(
            select(DataProfile)
            .where(DataProfile.connection_id == connection_id)
            .order_by(DataProfile.created_at.desc())
        )
    ).all()
    return [DataProfileResponse.model_validate(item) for item in profiles]
