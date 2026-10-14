import uuid

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.connections import ConnectionService
from etl_agent.application.profiling import (
    ConnectionTarget,
    ProfileService,
    TableProfileResult,
)
from etl_agent.application.secrets import (
    ConnectionCredentials,
    FernetDatabaseSecretProvider,
)
from etl_agent.domain.enums import ConnectionStatus, ConnectorType, DataProfileStatus, ProjectRole
from etl_agent.domain.errors import DomainError
from etl_agent.infrastructure.models import (
    AuditEvent,
    EncryptedSecret,
    Project,
    ProjectMembership,
    User,
)


async def seed_engineer(session: AsyncSession) -> tuple[Project, User]:
    """构造或模拟测试所需的 seed_engineer 行为，使断言聚焦于被测业务边界。"""
    project = Project(name=f"project-{uuid.uuid4()}")
    engineer = User(subject=f"engineer-{uuid.uuid4()}", display_name="工程师")
    session.add_all([project, engineer])
    await session.flush()
    session.add(
        ProjectMembership(
            project_id=project.id,
            user_id=engineer.id,
            role=ProjectRole.DATA_ENGINEER,
        )
    )
    await session.commit()
    return project, engineer


@pytest.mark.asyncio
async def test_connection_password_is_encrypted_and_excluded_from_audit(
    session: AsyncSession,
) -> None:
    """验证 connection_password_is_encrypted_and_excluded_from_audit 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    project, engineer = await seed_engineer(session)
    provider = FernetDatabaseSecretProvider(Fernet.generate_key().decode())
    service = ConnectionService(provider)
    credentials = ConnectionCredentials(username="etl_reader", password="very-secret")

    connection = await service.create(
        session,
        project_id=project.id,
        actor_id=engineer.id,
        name="mysql-orders",
        connector_type=ConnectorType.MYSQL,
        host="mysql.internal",
        port=None,
        database_name="sales",
        options={"connect_timeout": 5},
        credentials=credentials,
    )
    await session.commit()

    encrypted = await session.get(EncryptedSecret, connection.secret_ref)
    assert encrypted is not None
    assert b"very-secret" not in encrypted.ciphertext
    resolved = await provider.resolve(session, connection.secret_ref)
    assert resolved.password.get_secret_value() == "very-secret"
    audit = await session.scalar(select(AuditEvent).where(AuditEvent.resource_id == connection.id))
    assert audit is not None
    assert "very-secret" not in str(audit.after)
    assert connection.port == 3306


@pytest.mark.asyncio
async def test_connection_options_cannot_smuggle_secrets(session: AsyncSession) -> None:
    """验证 connection_options_cannot_smuggle_secrets 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    project, engineer = await seed_engineer(session)
    service = ConnectionService(FernetDatabaseSecretProvider(Fernet.generate_key().decode()))

    with pytest.raises(DomainError, match="不允许进入模型上下文"):
        await service.create(
            session,
            project_id=project.id,
            actor_id=engineer.id,
            name="unsafe",
            connector_type=ConnectorType.MYSQL,
            host="mysql.internal",
            port=3306,
            database_name="sales",
            options={"password": "smuggled"},
            credentials=ConnectionCredentials(username="user", password="secret"),
        )


@pytest.mark.asyncio
async def test_profile_persists_only_masked_result(session: AsyncSession) -> None:
    """验证 profile_persists_only_masked_result 场景的预期结果与失败边界，防止对应行为回归。"""
    project, engineer = await seed_engineer(session)
    provider = FernetDatabaseSecretProvider(Fernet.generate_key().decode())
    connection = await ConnectionService(provider).create(
        session,
        project_id=project.id,
        actor_id=engineer.id,
        name="ready-mysql",
        connector_type=ConnectorType.MYSQL,
        host="mysql.internal",
        port=3306,
        database_name="sales",
        options={},
        credentials=ConnectionCredentials(username="user", password="secret"),
    )
    connection.status = ConnectionStatus.READY
    await session.commit()

    service = ProfileService()
    profile = await service.request(
        session,
        connection_id=connection.id,
        actor_id=engineer.id,
        schema_name="sales",
        table_name="orders",
    )
    await service.mark_running(session, profile.id)
    await service.complete(
        session,
        profile_id=profile.id,
        result=TableProfileResult(
            schema_definition={"columns": [{"name": "phone", "type": "varchar(20)"}]},
            primary_keys=["id"],
            statistics={"table_rows": 10},
            masked_sample=[{"id": 1, "phone": "138****8000"}],
        ),
    )

    assert profile.status is DataProfileStatus.READY
    assert profile.masked_sample == [{"id": 1, "phone": "138****8000"}]
    assert profile.expires_at is not None


def test_error_redaction_removes_password() -> None:
    """验证 error_redaction_removes_password 场景的预期结果与失败边界，防止对应行为回归。"""
    credentials = ConnectionCredentials(username="user", password="secret-value")
    assert "secret-value" not in credentials.redact_error(
        RuntimeError("login failed for secret-value")
    )


def target() -> ConnectionTarget:
    """构造或模拟测试所需的 target 行为，使断言聚焦于被测业务边界。"""
    return ConnectionTarget(
        connector_type=ConnectorType.MYSQL,
        host="mysql.internal",
        port=3306,
        database_name="sales",
    )
