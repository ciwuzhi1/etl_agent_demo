import uuid

from etl_agent.application.connections import ConnectionService
from etl_agent.application.profiling import ConnectionTarget, ProfileService
from etl_agent.asyncio_compat import run_async
from etl_agent.domain.errors import DomainError
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.models import DataConnection
from etl_agent.infrastructure.probe_registry import ProbeRegistry
from etl_agent.infrastructure.secret_factory import create_secret_provider
from etl_agent.worker import celery_app


def _target(connection: DataConnection) -> ConnectionTarget:
    """在连接测试与探查任务中执行“target”对应的内部步骤；作为内部辅助函数，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    if not connection.host or not connection.port or not connection.database_name:
        raise DomainError("当前连接类型尚未提供可探查的数据库地址")
    return ConnectionTarget(
        connector_type=connection.connector_type,
        host=connection.host,
        port=connection.port,
        database_name=connection.database_name,
        options=connection.options,
    )


async def _test_connection(connection_id: uuid.UUID) -> dict[str, object]:
    """解析连接密钥并执行最小连通性测试，最终只记录脱敏结果。"""
    service = ConnectionService(create_secret_provider())
    async with SessionFactory() as session:
        connection, credentials = await service.resolve(session, connection_id)
        target = _target(connection)
    try:
        result = (
            await ProbeRegistry().get(target.connector_type).test_connection(target, credentials)
        )
    except Exception as exc:
        async with SessionFactory() as session:
            async with session.begin():
                await service.record_test_result(
                    session,
                    connection_id=connection_id,
                    success=False,
                    error=credentials.redact_error(exc),
                )
        raise
    async with SessionFactory() as session:
        async with session.begin():
            await service.record_test_result(session, connection_id=connection_id, success=True)
    return result.model_dump(mode="json")


async def _profile_table(profile_id: uuid.UUID, sample_limit: int) -> dict[str, object]:
    """执行表级探查，并在成功和失败路径上显式更新 Profile 状态。"""
    profile_service = ProfileService()
    connection_service = ConnectionService(create_secret_provider())
    async with SessionFactory() as session:
        async with session.begin():
            profile = await profile_service.mark_running(session, profile_id)
        connection, credentials = await connection_service.resolve(session, profile.connection_id)
        target = _target(connection)
        schema_name = profile.schema_name
        table_name = profile.table_name
    try:
        result = (
            await ProbeRegistry()
            .get(target.connector_type)
            .profile_table(
                target,
                credentials,
                schema_name=schema_name,
                table_name=table_name,
                sample_limit=sample_limit,
            )
        )
    except Exception as exc:
        async with SessionFactory() as session:
            async with session.begin():
                await profile_service.fail(
                    session, profile_id=profile_id, error=credentials.redact_error(exc)
                )
        raise
    async with SessionFactory() as session:
        async with session.begin():
            await profile_service.complete(session, profile_id=profile_id, result=result)
    return {"profile_id": str(profile_id), "sample_size": len(result.masked_sample)}


@celery_app.task  # type: ignore[untyped-decorator]
def test_data_connection(connection_id: str) -> dict[str, object]:
    """在连接测试与探查任务中执行最小范围验证并返回脱敏结果；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    return run_async(_test_connection(uuid.UUID(connection_id)))


@celery_app.task  # type: ignore[untyped-decorator]
def profile_table(profile_id: str, sample_limit: int) -> dict[str, object]:
    """在连接测试与探查任务中采集结构化元数据和脱敏样本；作为业务入口，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    return run_async(_profile_table(uuid.UUID(profile_id), sample_limit))
