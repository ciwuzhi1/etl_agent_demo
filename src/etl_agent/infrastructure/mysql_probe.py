import asyncio
import time
from typing import Any, cast

import pymysql
from pymysql.cursors import DictCursor

from etl_agent.application.masking import mask_sample
from etl_agent.application.profiling import (
    ConnectionTarget,
    ConnectionTestResult,
    MetadataProbePort,
    TableProfileResult,
    require_identifier,
)
from etl_agent.application.secrets import ConnectionCredentials
from etl_agent.domain.enums import ConnectorType
from etl_agent.domain.errors import DomainError


class MySqlMetadataProbe(MetadataProbePort):
    async def test_connection(
        self, target: ConnectionTarget, credentials: ConnectionCredentials
    ) -> ConnectionTestResult:
        """将连接标记为测试中并投递连通性任务，API 不阻塞等待外部数据库。"""
        return await asyncio.to_thread(self._test_sync, target, credentials)

    async def profile_table(
        self,
        target: ConnectionTarget,
        credentials: ConnectionCredentials,
        *,
        schema_name: str,
        table_name: str,
        sample_limit: int,
    ) -> TableProfileResult:
        """在MySQL 元数据探查器中采集结构化元数据和脱敏样本；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        require_identifier(schema_name, "Schema")
        require_identifier(table_name, "表名")
        if not 1 <= sample_limit <= 100:
            raise DomainError("sample_limit 必须在 1 到 100 之间")
        return await asyncio.to_thread(
            self._profile_sync,
            target,
            credentials,
            schema_name,
            table_name,
            sample_limit,
        )

    def _connect(
        self, target: ConnectionTarget, credentials: ConnectionCredentials
    ) -> pymysql.Connection:
        """在MySQL 元数据探查器中执行“connect”对应的内部步骤；作为内部辅助函数，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        if target.connector_type not in {ConnectorType.MYSQL, ConnectorType.DORIS}:
            raise DomainError("MySqlMetadataProbe 只支持 MySQL 或 Doris FE 协议")
        timeout = min(max(int(target.options.get("connect_timeout", 10)), 1), 30)
        return pymysql.connect(
            host=target.host,
            port=target.port,
            user=credentials.username,
            password=credentials.password.get_secret_value(),
            database=target.database_name,
            charset="utf8mb4",
            connect_timeout=timeout,
            read_timeout=30,
            write_timeout=30,
            autocommit=True,
            cursorclass=DictCursor,
        )

    def _test_sync(
        self, target: ConnectionTarget, credentials: ConnectionCredentials
    ) -> ConnectionTestResult:
        """在MySQL 元数据探查器中执行最小范围验证并返回脱敏结果；作为内部辅助函数，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        started = time.perf_counter()
        with self._connect(target, credentials) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT VERSION() AS server_version")
                row = cast(dict[str, Any], cursor.fetchone())
        latency = int((time.perf_counter() - started) * 1000)
        return ConnectionTestResult(
            server_version=str(row.get("server_version") or "unknown"), latency_ms=latency
        )

    def _profile_sync(
        self,
        target: ConnectionTarget,
        credentials: ConnectionCredentials,
        schema_name: str,
        table_name: str,
        sample_limit: int,
    ) -> TableProfileResult:
        """读取表结构、主键、行数估计和受限样本，并在返回前完成脱敏。"""
        with self._connect(target, credentials) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
                           COLUMN_KEY, EXTRA, COLUMN_COMMENT, ORDINAL_POSITION
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """,
                    (schema_name, table_name),
                )
                columns = cast(list[dict[str, Any]], cursor.fetchall())
                if not columns:
                    raise DomainError("目标表不存在或当前账号无权读取元数据")

                primary_keys = [
                    str(column["COLUMN_NAME"])
                    for column in columns
                    if column.get("COLUMN_KEY") == "PRI"
                ]
                cursor.execute(
                    """
                    SELECT TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, UPDATE_TIME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    """,
                    (schema_name, table_name),
                )
                stats = cast(dict[str, Any] | None, cursor.fetchone()) or {}

                order = ""
                if primary_keys:
                    order = " ORDER BY " + ", ".join(f"`{key}`" for key in primary_keys)
                query = f"SELECT * FROM `{schema_name}`.`{table_name}`{order} LIMIT %s"
                cursor.execute(query, (sample_limit,))
                raw_rows = cast(list[dict[str, Any]], cursor.fetchall())

        schema_columns = [
            {
                "name": column["COLUMN_NAME"],
                "type": column["COLUMN_TYPE"],
                "nullable": column["IS_NULLABLE"] == "YES",
                "default": _json_safe(column["COLUMN_DEFAULT"]),
                "key": column["COLUMN_KEY"],
                "extra": column["EXTRA"],
                "comment": column["COLUMN_COMMENT"],
                "position": column["ORDINAL_POSITION"],
            }
            for column in columns
        ]
        return TableProfileResult(
            schema_definition={"columns": schema_columns},
            primary_keys=primary_keys,
            statistics={key.lower(): _json_safe(value) for key, value in stats.items()},
            masked_sample=mask_sample(raw_rows),
        )


def _json_safe(value: Any) -> Any:
    """在MySQL 元数据探查器中执行“json_safe”对应的内部步骤；作为内部辅助函数，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, bytes):
        return f"[BINARY:{len(value)} bytes]"
    return value
