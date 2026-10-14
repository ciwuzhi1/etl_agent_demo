import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.policy import require_safe_generation_context
from etl_agent.application.profiling import require_project_role
from etl_agent.application.secrets import ConnectionCredentials, SecretProvider
from etl_agent.domain.enums import ConnectionStatus, ConnectorType, ProjectRole
from etl_agent.domain.errors import DomainError, ResourceNotFound
from etl_agent.infrastructure.models import AuditEvent, DataConnection

DEFAULT_PORTS = {
    ConnectorType.MYSQL: 3306,
    ConnectorType.DORIS: 9030,
    ConnectorType.POSTGRESQL: 5432,
    ConnectorType.ORACLE: 1521,
    ConnectorType.SQL_SERVER: 1433,
    ConnectorType.HIVE: 10000,
    ConnectorType.HDFS: 9870,
}


class ConnectionService:
    def __init__(self, secrets: SecretProvider) -> None:
        """初始化实例依赖和运行参数；依赖显式保存以便替换、测试和生命周期管理。"""
        self._secrets = secrets

    async def create(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        actor_id: uuid.UUID,
        name: str,
        connector_type: ConnectorType,
        host: str | None,
        port: int | None,
        database_name: str | None,
        options: dict[str, Any],
        credentials: ConnectionCredentials,
        trace_id: str | None = None,
    ) -> DataConnection:
        """在连接管理应用服务中创建领域资源并校验输入和权限；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        await require_project_role(
            session,
            project_id,
            actor_id,
            {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER},
        )
        require_safe_generation_context(options, "connection.options")
        if connector_type is ConnectorType.HDFS:
            if not host:
                raise DomainError("HDFS 连接必须提供 NameNode WebHDFS 主机")
            port = port or DEFAULT_PORTS[ConnectorType.HDFS]
            database_name = database_name or "/"
            auth_mode = str(options.get("auth_mode") or "simple").casefold()
            if auth_mode != "simple":
                raise DomainError("当前 V1 仅支持 WebHDFS simple 认证；Kerberos 必须显式拒绝")
        elif connector_type is not ConnectorType.LOCAL_FILE:
            if not host or not database_name:
                raise DomainError("数据库连接必须提供 host 和 database_name")
            port = port or DEFAULT_PORTS.get(connector_type)
        secret_ref = await self._secrets.store(session, credentials)
        connection = DataConnection(
            project_id=project_id,
            name=name,
            connector_type=connector_type,
            host=host,
            port=port,
            database_name=database_name,
            options=options,
            secret_ref=secret_ref,
            created_by=actor_id,
        )
        session.add(connection)
        await session.flush()
        session.add(
            AuditEvent(
                actor_id=actor_id,
                project_id=project_id,
                action="data_connection.created",
                resource_type="DataConnection",
                resource_id=connection.id,
                before=None,
                after={
                    "name": name,
                    "connector_type": connector_type.value,
                    "host": host,
                    "port": port,
                    "database_name": database_name,
                },
                trace_id=trace_id,
            )
        )
        await session.flush()
        return connection

    async def mark_testing(
        self, session: AsyncSession, *, connection_id: uuid.UUID, actor_id: uuid.UUID
    ) -> DataConnection:
        """在连接管理应用服务中持久化受控状态变化；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        connection = await self._get(session, connection_id)
        await require_project_role(
            session,
            connection.project_id,
            actor_id,
            {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER},
        )
        connection.status = ConnectionStatus.TESTING
        connection.test_error = None
        await session.flush()
        return connection

    async def record_test_result(
        self,
        session: AsyncSession,
        *,
        connection_id: uuid.UUID,
        success: bool,
        error: str | None = None,
    ) -> DataConnection:
        """持久化连接测试结果，对驱动错误先脱敏再截断，避免秘密进入数据库。"""
        connection = await self._get(session, connection_id)
        connection.status = ConnectionStatus.READY if success else ConnectionStatus.FAILED
        connection.test_error = None if success else (error or "连接测试失败")[:10_000]
        connection.last_tested_at = datetime.now(UTC)
        await session.flush()
        return connection

    async def resolve(
        self, session: AsyncSession, connection_id: uuid.UUID
    ) -> tuple[DataConnection, ConnectionCredentials]:
        """在连接管理应用服务中解析受控引用并验证所属边界；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        connection = await self._get(session, connection_id)
        credentials = await self._secrets.resolve(session, connection.secret_ref)
        return connection, credentials

    @staticmethod
    async def _get(session: AsyncSession, connection_id: uuid.UUID) -> DataConnection:
        """在连接管理应用服务中读取单个资源并校验可见性；作为内部辅助函数，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        connection = await session.get(DataConnection, connection_id)
        if connection is None:
            raise ResourceNotFound("数据连接不存在")
        return connection
