"""凭据物化与 Profile 占位符规范化。

第 8 章新增：在策略节点执行前，把模型常见的类型别名映射到已登记的连接占位符，
确保 HOCON 中不出现任何未经平台批准的变量名。
"""

from typing import Any


def canonicalize_profile_placeholders(hocon: str, profiles: list[dict[str, Any]]) -> str:
    """仅在连接唯一时，把模型常见的类型别名映射到已登记的连接占位符。"""
    by_connector: dict[str, dict[str, dict[str, Any]]] = {}
    for profile in profiles:
        connector = str(profile.get("connector_type") or "").upper()
        connection_id = str(profile.get("connection_id") or "")
        if connector and connection_id:
            by_connector.setdefault(connector, {})[connection_id] = profile

    canonical = hocon
    for connector, connections in by_connector.items():
        if len(connections) != 1:
            continue
        profile = next(iter(connections.values()))
        credentials = profile.get("credential_placeholders", {})
        metadata = profile.get("connection_placeholders", {})
        replacements = {
            f"${{{connector}_USERNAME}}": credentials.get("username"),
            f"${{{connector}_PASSWORD}}": credentials.get("password"),
            f"${{{connector}_JDBC_URL}}": metadata.get("jdbc_url"),
            f"${{{connector}_FENODES}}": metadata.get("fenodes"),
            f"${{{connector}_HDFS_URI}}": metadata.get("hdfs_uri"),
            f"${{{connector}_METASTORE_URI}}": metadata.get("metastore_uri"),
        }
        for alias, approved in replacements.items():
            if isinstance(approved, str):
                canonical = canonical.replace(alias, approved)
    return canonical
