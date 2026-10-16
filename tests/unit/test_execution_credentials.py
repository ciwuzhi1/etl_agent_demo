"""执行前占位符物化的契约（第 10 章）。

制品里只允许出现平台占位符，真实主机与凭据必须在提交前这一瞬物化。覆盖：

- 已声明连接的占位符必须被替换成真实连接信息与凭据；
- **未声明**的连接不得被猜测使用，占位符原样保留（否则等于绕开审批冻结的端点契约）；
- 连接缺主机/端口/库名时失败关闭，而不是拼出半截 URL 交给引擎；
- Doris fenodes 与 JDBC 默认参数按连接器拼装。
"""

import uuid
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.connections import ConnectionService
from etl_agent.application.execution_credentials import materialize_execution_placeholders
from etl_agent.application.secrets import ConnectionCredentials
from etl_agent.domain.enums import ConnectorType
from etl_agent.domain.errors import DomainError
from etl_agent.infrastructure.models import DataConnection

_MYSQL_HOCON = (
    '    url = "${MYSQL_JDBC_URL}"\n'
    '    user = "${MYSQL_USERNAME}"\n'
    '    password = "${MYSQL_PASSWORD}"\n'
)


def _connection(**overrides: Any) -> DataConnection:
    """构造一条未落库的连接记录，仅供拼装断言使用。

    ``id`` 必须显式给：列默认值只在 flush 时生效，未落库时 ``connection.id`` 是 ``None``。
    """
    fields: dict[str, Any] = {
        "id": uuid.uuid4(),
        "project_id": uuid.uuid4(),
        "name": "src",
        "connector_type": ConnectorType.MYSQL,
        "host": "mysql-e2e",
        "port": 3306,
        "database_name": "source_db",
        "options": {},
        "secret_ref": uuid.uuid4(),
    }
    fields.update(overrides)
    return DataConnection(**fields)


def _credentials(username: str = "etl_reader", password: str = "s3cret") -> ConnectionCredentials:
    return ConnectionCredentials(username=username, password=password)


async def _materialize(
    session: AsyncSession,
    *,
    plan: dict[str, Any] | None,
    hocon: str,
    connection: DataConnection,
    credentials: ConnectionCredentials,
) -> str:
    """固定连接解析结果后跑一次物化，避免测试依赖真实 Secret 后端。"""
    resolver = AsyncMock(return_value=(connection, credentials))
    with patch.object(ConnectionService, "resolve", new=resolver):
        return await materialize_execution_placeholders(session, plan=plan, hocon=hocon)


async def test_replaces_declared_connection_placeholders(session: AsyncSession) -> None:
    connection = _connection()
    plan = {"profile_contracts": [{"connection_id": str(connection.id), "connector_type": "MYSQL"}]}

    result = await _materialize(
        session, plan=plan, hocon=_MYSQL_HOCON, connection=connection, credentials=_credentials()
    )

    assert "etl_reader" in result
    assert "s3cret" in result
    assert "jdbc:mysql://mysql-e2e:3306/source_db?" in result
    assert "${MYSQL_" not in result


async def test_leaves_undeclared_placeholder_untouched(session: AsyncSession) -> None:
    """没有声明连接时不得凭空补一个连接进去。"""
    connection = _connection()

    result = await _materialize(
        session,
        plan={"profile_contracts": []},
        hocon=_MYSQL_HOCON,
        connection=connection,
        credentials=_credentials(),
    )

    assert result == _MYSQL_HOCON


async def test_returns_input_when_plan_or_hocon_missing(session: AsyncSession) -> None:
    connection = _connection()

    assert (
        await _materialize(
            session,
            plan=None,
            hocon=_MYSQL_HOCON,
            connection=connection,
            credentials=_credentials(),
        )
        == _MYSQL_HOCON
    )
    assert (
        await _materialize(
            session,
            plan={"profile_contracts": []},
            hocon="",
            connection=connection,
            credentials=_credentials(),
        )
        == ""
    )


async def test_raises_when_connection_lacks_host(session: AsyncSession) -> None:
    connection = _connection(host=None, port=None)
    plan = {"profile_contracts": [{"connection_id": str(connection.id), "connector_type": "MYSQL"}]}

    with pytest.raises(DomainError):
        await _materialize(
            session,
            plan=plan,
            hocon=_MYSQL_HOCON,
            connection=connection,
            credentials=_credentials(),
        )


async def test_raises_on_malformed_connection_id(session: AsyncSession) -> None:
    connection = _connection()
    plan = {"profile_contracts": [{"connection_id": "not-a-uuid", "connector_type": "MYSQL"}]}

    with pytest.raises(DomainError):
        await _materialize(
            session,
            plan=plan,
            hocon=_MYSQL_HOCON,
            connection=connection,
            credentials=_credentials(),
        )


async def test_doris_fenodes_defaults_to_fe_http_port(session: AsyncSession) -> None:
    connection = _connection(
        connector_type=ConnectorType.DORIS, host="doris-fe", port=None, database_name="analytics"
    )
    plan = {"profile_contracts": [{"connection_id": str(connection.id), "connector_type": "DORIS"}]}
    hocon = '    fenodes = "${DORIS_FENODES}"\n    username = "${DORIS_USERNAME}"\n'

    result = await _materialize(
        session,
        plan=plan,
        hocon=hocon,
        connection=connection,
        credentials=_credentials("root", "pw"),
    )

    assert 'fenodes = "doris-fe:8030"' in result
    assert 'username = "root"' in result
