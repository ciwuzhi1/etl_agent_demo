"""生成上下文构建：把 DataProfile 与 DataConnection 编译成平台占位符描述符。

第 8 章的 policy 节点同时用 ``state["metadata_context"]["profiles"]`` 做两件事：

1. **确定性门禁**——``application/schema_contract.py`` 的连接器拓扑、数据库端点与目标
   Schema 校验都读这份描述符；
2. **给模型提示**——让模型知道本次任务有哪些表、哪些列、哪些占位符可用。

此前路由只传了 ``profile_ids``，于是门禁按"零个端点"校验、模型也拿不到表结构，
永远产不出被接受的候选。本模块是**唯一**把库内画像翻译成该描述符的地方。

输出纪律：描述符里**只有**平台占位符（``${<CONNECTOR>_...}``）与逻辑对象名，
绝不包含真实主机、端口、用户名或密码。库里本来就没有明文，这里连脱敏样本都不读，
避免把任何样本带进模型上下文或制品快照。

缺列策略：当某个 Profile 的 ``schema_json.columns`` 为空或全部无效时，本模块**抛
``DomainError``**（fail-closed），而不是产出空 ``columns``。空列会让下游门禁把
"没有列约束"误判为"通过"，所以宁可显式失败。
"""

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.domain.errors import DomainError, ResourceNotFound
from etl_agent.infrastructure.models import DataConnection, DataProfile

# JDBC 连接器 → 驱动类名。Doris/Hive 走各自原生连接器，不在此表。
_JDBC_DRIVER_CLASSES: dict[str, str] = {
    "MYSQL": "com.mysql.cj.jdbc.Driver",
    "POSTGRESQL": "org.postgresql.Driver",
    "ORACLE": "oracle.jdbc.OracleDriver",
    "SQL_SERVER": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# 连接器 → SeaTunnel 插件类型。仅登记可构建数据库端点描述符的连接器；
# HDFS / LOCAL_FILE 的执行契约需要 FileAsset 冻结事实，不属于本模块职责。
_CONNECTOR_PLUGINS: dict[str, str] = {
    "MYSQL": "jdbc",
    "POSTGRESQL": "jdbc",
    "ORACLE": "jdbc",
    "SQL_SERVER": "jdbc",
    "DORIS": "doris",
    "HIVE": "hive",
}

# 平台认可占位符中会出现这些后缀；统一在这里拼装，避免各处手写字符串。
_SOURCE_QUERY_TEMPLATE = "SELECT * FROM {table}"


def canonical_placeholder_contract(
    connector_type: str,
    *,
    schema_name: str,
    table_name: str,
    role: str,
) -> dict[str, dict[str, Any]]:
    """由连接器类型、逻辑对象名与端点角色推导出确定性的占位符契约。

    纯函数，不触碰数据库，便于单测直接断言占位符。返回三块内容：

    - ``connector_contract``：``{"plugin": ..., "required_options": {...}}``；
    - ``credential_placeholders``：``username`` / ``password`` 占位符；
    - ``connection_placeholders``：``jdbc_url`` / ``fenodes`` 占位符（HIVE 额外带
      ``metastore_uri``）。

    占位符中的 ``<CONNECTOR>`` 用连接器枚举值原样大写（如 ``SQL_SERVER``）。连接器不在
    映射表内时抛 ``DomainError``，让调用方显式感知"暂不支持"，而不是产出半成品。
    """
    connector = connector_type.upper()
    plugin = _CONNECTOR_PLUGINS.get(connector)
    if plugin is None:
        raise DomainError(f"暂不支持为连接器类型 {connector} 构建生成上下文占位符")

    normalized_role = role.casefold()
    username = "${" + f"{connector}_USERNAME" + "}"
    password = "${" + f"{connector}_PASSWORD" + "}"
    jdbc_url = "${" + f"{connector}_JDBC_URL" + "}"
    fenodes = "${" + f"{connector}_FENODES" + "}"
    metastore_uri = "${" + f"{connector}_METASTORE_URI" + "}"

    connection_placeholders: dict[str, Any] = {
        "jdbc_url": jdbc_url,
        "fenodes": fenodes,
    }

    required_options: dict[str, Any]
    if plugin == "jdbc":
        required_options = {
            "url": jdbc_url,
            "driver": _JDBC_DRIVER_CLASSES[connector],
            "user": username,
            "password": password,
        }
        if normalized_role == "sink":
            required_options["table"] = f"{schema_name}.{table_name}"
        else:
            required_options["query"] = _SOURCE_QUERY_TEMPLATE.format(table=table_name)
    elif plugin == "doris":
        required_options = {
            "fenodes": fenodes,
            "database": schema_name,
            "table": table_name,
            "username": username,
            "password": password,
        }
    else:  # hive
        required_options = {
            "metastore_uri": metastore_uri,
            "table_name": table_name,
            "remote_user": username,
        }
        connection_placeholders["metastore_uri"] = metastore_uri

    return {
        "connector_contract": {"plugin": plugin, "required_options": required_options},
        "credential_placeholders": {"username": username, "password": password},
        "connection_placeholders": connection_placeholders,
    }


def _profile_columns(profile: DataProfile) -> list[dict[str, str]]:
    """从 Profile 的 ``schema_json.columns`` 提取规范化的 ``{name, type}`` 列快照。

    只保留下游门禁真正需要的 ``name`` / ``type``；其余探查字段（nullable、comment、
    样本等）一律丢弃，避免把非必要信息带进模型上下文。列缺失时抛 ``DomainError``。
    """
    raw_columns = (profile.schema_json or {}).get("columns")
    normalized: list[dict[str, str]] = []
    if isinstance(raw_columns, list):
        for column in raw_columns:
            if not isinstance(column, dict):
                continue
            name = str(column.get("name") or "").strip()
            column_type = str(column.get("type") or "").strip()
            if name and column_type:
                normalized.append({"name": name, "type": column_type})
    if not normalized:
        raise DomainError(
            f"DataProfile {profile.id}（{profile.schema_name}.{profile.table_name}）"
            "缺少可用的列快照，无法构建生成上下文"
        )
    return normalized


def _describe_profile(profile: DataProfile, connection: DataConnection) -> dict[str, Any]:
    """把一对 Profile/Connection 物化为平台占位符描述符。"""
    connector_type = connection.connector_type.value
    role = profile.endpoint_role.value.casefold()
    placeholders = canonical_placeholder_contract(
        connector_type,
        schema_name=profile.schema_name,
        table_name=profile.table_name,
        role=role,
    )
    return {
        "profile_id": str(profile.id),
        "connection_id": str(connection.id),
        "connector_type": connector_type,
        "schema": profile.schema_name,
        "table": profile.table_name,
        "role": role,
        "columns": _profile_columns(profile),
        "connector_contract": placeholders["connector_contract"],
        "credential_placeholders": placeholders["credential_placeholders"],
        "connection_placeholders": placeholders["connection_placeholders"],
    }


async def build_generation_context(
    session: AsyncSession,
    profile_ids: list[uuid.UUID],
) -> list[dict[str, Any]]:
    """按给定 Profile 逐个联表读取连接，产出平台占位符描述符列表。

    行为约定：

    - 空 ``profile_ids`` 直接返回 ``[]``，不打开任何查询；
    - 结果顺序与入参一致，重复 ID 去重（保留首次出现位置），避免重复统计端点拓扑；
    - 某个 ``profile_id`` 在库中不存在时抛 ``ResourceNotFound``；
    - 某个 Profile 缺列时抛 ``DomainError``（见模块文档的 fail-closed 策略）。
    """
    ordered_ids: list[uuid.UUID] = []
    seen: set[uuid.UUID] = set()
    for profile_id in profile_ids:
        if profile_id not in seen:
            seen.add(profile_id)
            ordered_ids.append(profile_id)
    if not ordered_ids:
        return []

    rows = (
        await session.execute(
            select(DataProfile, DataConnection)
            .join(DataConnection, DataConnection.id == DataProfile.connection_id)
            .where(DataProfile.id.in_(ordered_ids))
        )
    ).all()
    by_profile_id: dict[uuid.UUID, tuple[DataProfile, DataConnection]] = {}
    for row in rows:
        profile: DataProfile = row[0]
        connection: DataConnection = row[1]
        by_profile_id[profile.id] = (profile, connection)

    descriptors: list[dict[str, Any]] = []
    for profile_id in ordered_ids:
        found = by_profile_id.get(profile_id)
        if found is None:
            raise ResourceNotFound(f"DataProfile {profile_id} 不存在")
        descriptors.append(_describe_profile(*found))
    return descriptors
