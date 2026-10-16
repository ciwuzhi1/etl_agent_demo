"""执行前把制品里的平台占位符物化为真实连接信息与凭据。

制品（SeaTunnel HOCON）里只允许出现平台占位符，例如 ``${MYSQL_JDBC_URL}``、
``${MYSQL_USERNAME}``、``${MYSQL_PASSWORD}``、``${DORIS_FENODES}``——它们由
``application/generation_context.py`` 统一拼装，真实主机、端口与凭据一律不落制品。

SeaTunnel 的 REST 提交**不解析 HOCON 变量**（实测 ``${VAR}`` 会被原样当作字面量送给
引擎，表现为 ``Access denied ... using password: YES``），所以必须由平台在提交前这一
瞬物化。明文只存在于进程内存与引擎侧，既不落库也不落盘。

只物化本次版本 ``plan`` 里显式声明的连接：宁可让引擎报"参数未定义"，也不去猜测
"这个占位符应该用哪个连接"，否则等于绕开了审批时冻结的那份端点契约。
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.connections import ConnectionService
from etl_agent.application.secrets import ConnectionCredentials
from etl_agent.domain.enums import ConnectorType
from etl_agent.domain.errors import DomainError
from etl_agent.infrastructure.models import DataConnection
from etl_agent.infrastructure.secret_factory import create_secret_provider

# 连接器 → JDBC URL 的 scheme。Doris / Hive 走各自原生连接器，不适用 JDBC。
_JDBC_SCHEMES: dict[ConnectorType, str] = {
    ConnectorType.MYSQL: "jdbc:mysql",
    ConnectorType.POSTGRESQL: "jdbc:postgresql",
    ConnectorType.ORACLE: "jdbc:oracle:thin",
    ConnectorType.SQL_SERVER: "jdbc:sqlserver",
}

# 各连接器的默认 JDBC 参数：只补连通性必需项，不替业务猜参数；连接上可覆盖。
_DEFAULT_JDBC_PARAMS: dict[ConnectorType, str] = {
    ConnectorType.MYSQL: "useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai",
    ConnectorType.POSTGRESQL: "sslmode=prefer",
    ConnectorType.ORACLE: "",
    ConnectorType.SQL_SERVER: "encrypt=false",
}

# Doris FE 默认 HTTP（fenodes）端口；连接未登记端口时用它兜底。
_DORIS_FE_DEFAULT_PORT = 8030


def _jdbc_url(connection: DataConnection) -> str:
    """按连接器拼出 JDBC URL；缺主机/端口/库名一律失败关闭。"""
    scheme = _JDBC_SCHEMES.get(connection.connector_type)
    if scheme is None:
        raise DomainError(f"连接器 {connection.connector_type.value} 不适用 JDBC URL 物化")
    if not connection.host or not connection.port or not connection.database_name:
        raise DomainError("数据连接缺少主机/端口/库名，无法物化 JDBC URL")
    base = f"{scheme}://{connection.host}:{connection.port}/{connection.database_name}"
    raw_params = connection.options.get("jdbc_params")
    params = (
        str(raw_params)
        if raw_params is not None
        else _DEFAULT_JDBC_PARAMS[connection.connector_type]
    )
    return f"{base}?{params}" if params else base


def _fenodes(connection: DataConnection) -> str:
    """Doris FE 的 fenodes（``host:http_port``）。

    **不能复用 ``connection.port``**：那是 FE 的 MySQL 协议端口（供探查/连接测试用，
    默认 9030），而 Doris Sink 要的是 HTTP 端口（默认 8030）。混用会让作业拿到
    ``fe:9030`` 而 Doris Connector 直连不上。需要时用 ``options.fe_http_port`` 覆盖。
    """
    if not connection.host:
        raise DomainError("数据连接缺少主机，无法物化 Doris fenodes")
    raw_port = connection.options.get("fe_http_port")
    port = int(raw_port) if raw_port is not None else _DORIS_FE_DEFAULT_PORT
    return f"{connection.host}:{port}"


def _profile_placeholders(
    connection: DataConnection, credentials: ConnectionCredentials
) -> dict[str, str]:
    """把一条连接翻译成它对应的平台占位符取值。

    只生成该连接器真正用得上的端点占位符：JDBC 系列给 ``JDBC_URL``，Doris 给
    ``FENODES``。给不出的类型宁可少给一个 key——占位符留在制品里会让引擎显式报错，
    而拼一个无意义的地址只会把故障推迟到更难定位的地方。
    """
    connector = connection.connector_type.value
    placeholders = {
        f"${{{connector}_USERNAME}}": credentials.username,
        f"${{{connector}_PASSWORD}}": credentials.password.get_secret_value(),
    }
    if connection.connector_type in _JDBC_SCHEMES:
        placeholders[f"${{{connector}_JDBC_URL}}"] = _jdbc_url(connection)
    else:
        placeholders[f"${{{connector}_FENODES}}"] = _fenodes(connection)
    return placeholders


async def materialize_execution_placeholders(
    session: AsyncSession,
    *,
    plan: dict[str, Any] | None,
    hocon: str,
) -> str:
    """用版本声明的连接替换制品里的平台占位符，返回可直接提交的 HOCON。"""
    if not hocon or not plan:
        return hocon
    service = ConnectionService(create_secret_provider())
    materialized = hocon
    for profile in plan.get("profile_contracts") or []:
        if not isinstance(profile, dict):
            continue
        raw_connection_id = profile.get("connection_id")
        if not raw_connection_id:
            continue
        try:
            connection_id = uuid.UUID(str(raw_connection_id))
        except ValueError as exc:
            raise DomainError(
                f"profile_contracts 里的 connection_id 非法：{raw_connection_id}"
            ) from exc
        connection, credentials = await service.resolve(session, connection_id)
        for alias, value in _profile_placeholders(connection, credentials).items():
            materialized = materialized.replace(alias, value)
    return materialized
