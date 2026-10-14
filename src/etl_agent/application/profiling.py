import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.secrets import ConnectionCredentials
from etl_agent.domain.enums import (
    ConnectionStatus,
    ConnectorType,
    DataEndpointRole,
    DataProfileStatus,
    ProjectRole,
)
from etl_agent.domain.errors import DomainError, PermissionDenied, ResourceNotFound
from etl_agent.infrastructure.models import (
    AuditEvent,
    DataConnection,
    DataProfile,
    ProjectMembership,
)

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,127}$")
HDFS_PATH_PATTERN = re.compile(r"^[A-Za-z0-9_./-]{1,512}$")


class ConnectionTarget(BaseModel):
    connector_type: ConnectorType
    host: str
    port: int = Field(ge=1, le=65535)
    database_name: str
    options: dict[str, Any] = Field(default_factory=dict)


class ConnectionTestResult(BaseModel):
    server_version: str
    latency_ms: int = Field(ge=0)


class TableProfileResult(BaseModel):
    schema_definition: dict[str, Any]
    primary_keys: list[str]
    statistics: dict[str, Any]
    masked_sample: list[dict[str, Any]]


class MetadataProbePort(Protocol):
    async def test_connection(
        self, target: ConnectionTarget, credentials: ConnectionCredentials
    ) -> ConnectionTestResult:
        """执行最小权限连通性测试，只返回不含凭据的结构化结果。"""
        ...

    async def profile_table(
        self,
        target: ConnectionTarget,
        credentials: ConnectionCredentials,
        *,
        schema_name: str,
        table_name: str,
        sample_limit: int,
    ) -> TableProfileResult:
        """探查指定表的 Schema、主键和受限样本，并在返回前完成脱敏。"""
        ...


def require_identifier(value: str, label: str) -> None:
    """校验数据库对象标识符，防止探查 SQL 被恶意拼接。"""
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise DomainError(f"{label} 不是安全的数据库标识符")


def require_hdfs_path(value: str, label: str) -> None:
    """校验 HDFS 相对路径片段，拒绝父目录跳转、反斜杠和查询参数注入。"""
    if not HDFS_PATH_PATTERN.fullmatch(value) or ".." in value.split("/") or "\\" in value:
        raise DomainError(f"{label} 不是安全的 HDFS 路径")


class ProfileService:
    async def request(
        self,
        session: AsyncSession,
        *,
        connection_id: uuid.UUID,
        actor_id: uuid.UUID,
        schema_name: str,
        table_name: str,
        endpoint_role: DataEndpointRole = DataEndpointRole.SOURCE,
        trace_id: str | None = None,
    ) -> DataProfile:
        """在数据探查应用服务中执行“request”对应的内部步骤；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        connection = await session.get(DataConnection, connection_id)
        if connection is None:
            raise ResourceNotFound("数据连接不存在")
        await require_project_role(
            session,
            connection.project_id,
            actor_id,
            {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER},
        )
        if connection.status is not ConnectionStatus.READY:
            raise DomainError("数据连接尚未测试成功")
        if connection.connector_type is ConnectorType.HDFS:
            require_hdfs_path(schema_name, "HDFS 目录")
            require_hdfs_path(table_name, "HDFS 文件")
        else:
            require_identifier(schema_name, "Schema")
            require_identifier(table_name, "表名")

        profile = DataProfile(
            connection_id=connection.id,
            schema_name=schema_name,
            table_name=table_name,
            endpoint_role=endpoint_role,
            requested_by=actor_id,
        )
        session.add(profile)
        await session.flush()
        session.add(
            AuditEvent(
                actor_id=actor_id,
                project_id=connection.project_id,
                action="data_profile.requested",
                resource_type="DataProfile",
                resource_id=profile.id,
                before=None,
                after={
                    "schema": schema_name,
                    "table": table_name,
                    "endpoint_role": endpoint_role.value,
                },
                trace_id=trace_id,
            )
        )
        await session.flush()
        return profile

    async def mark_running(self, session: AsyncSession, profile_id: uuid.UUID) -> DataProfile:
        """在数据探查应用服务中持久化受控状态变化；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        profile = await self._get_for_update(session, profile_id)
        if profile.status is not DataProfileStatus.PENDING:
            raise DomainError("只有待处理的 Profile 可以开始探查")
        profile.status = DataProfileStatus.RUNNING
        await session.flush()
        return profile

    async def complete(
        self,
        session: AsyncSession,
        *,
        profile_id: uuid.UUID,
        result: TableProfileResult,
        ttl_hours: int = 24,
    ) -> DataProfile:
        """在数据探查应用服务中保存结果并完成合法状态迁移；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        profile = await self._get_for_update(session, profile_id)
        if profile.status is not DataProfileStatus.RUNNING:
            raise DomainError("Profile 当前状态不能写入探查结果")
        profile.status = DataProfileStatus.READY
        profile.schema_json = result.schema_definition
        profile.primary_keys = result.primary_keys
        profile.statistics = result.statistics
        profile.masked_sample = result.masked_sample
        profile.sample_size = len(result.masked_sample)
        profile.expires_at = datetime.now(UTC) + timedelta(hours=ttl_hours)
        profile.error = None
        await session.flush()
        return profile

    async def fail(
        self, session: AsyncSession, *, profile_id: uuid.UUID, error: str
    ) -> DataProfile:
        """在数据探查应用服务中记录脱敏失败原因并进入失败状态；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        profile = await self._get_for_update(session, profile_id)
        profile.status = DataProfileStatus.FAILED
        profile.error = error[:10_000]
        await session.flush()
        return profile

    @staticmethod
    async def _get_for_update(session: AsyncSession, profile_id: uuid.UUID) -> DataProfile:
        """使用数据库行锁读取目标记录，串行化并发状态变更和重复任务投递。"""
        profile = (
            await session.execute(
                select(DataProfile).where(DataProfile.id == profile_id).with_for_update()
            )
        ).scalar_one_or_none()
        if profile is None:
            raise ResourceNotFound("DataProfile 不存在")
        return profile


async def require_project_role(
    session: AsyncSession,
    project_id: uuid.UUID,
    actor_id: uuid.UUID,
    allowed: set[ProjectRole],
) -> None:
    """在数据探查应用服务中执行失败即关闭的前置条件校验；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    role = await session.scalar(
        select(ProjectMembership.role).where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.user_id == actor_id,
        )
    )
    if role not in allowed:
        raise PermissionDenied("当前用户没有访问该数据连接的项目角色")
