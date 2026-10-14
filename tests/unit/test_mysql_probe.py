from typing import Any

import pytest

from etl_agent.application.profiling import ConnectionTarget
from etl_agent.application.secrets import ConnectionCredentials
from etl_agent.domain.enums import ConnectorType
from etl_agent.domain.errors import DomainError
from etl_agent.infrastructure.mysql_probe import MySqlMetadataProbe


class FakeCursor:
    def __init__(self) -> None:
        """初始化实例依赖和运行参数；依赖显式保存以便替换、测试和生命周期管理。"""
        self.query = ""

    def __enter__(self) -> "FakeCursor":
        """进入上下文管理边界并返回当前资源对象。"""
        return self

    def __exit__(self, *_args: object) -> None:
        """退出上下文并按约定决定异常是否继续传播。"""
        return None

    def execute(self, query: str, _params: object = None) -> None:
        """构造或模拟测试所需的 execute 行为，使断言聚焦于被测业务边界。"""
        self.query = query

    def fetchall(self) -> list[dict[str, Any]]:
        """构造或模拟测试所需的 fetchall 行为，使断言聚焦于被测业务边界。"""
        if "INFORMATION_SCHEMA.COLUMNS" in self.query:
            return [
                {
                    "COLUMN_NAME": "id",
                    "COLUMN_TYPE": "bigint",
                    "IS_NULLABLE": "NO",
                    "COLUMN_DEFAULT": None,
                    "COLUMN_KEY": "PRI",
                    "EXTRA": "",
                    "COLUMN_COMMENT": "",
                    "ORDINAL_POSITION": 1,
                },
                {
                    "COLUMN_NAME": "phone",
                    "COLUMN_TYPE": "varchar(20)",
                    "IS_NULLABLE": "YES",
                    "COLUMN_DEFAULT": None,
                    "COLUMN_KEY": "",
                    "EXTRA": "",
                    "COLUMN_COMMENT": "",
                    "ORDINAL_POSITION": 2,
                },
            ]
        return [{"id": 1, "phone": "13800138000"}]

    def fetchone(self) -> dict[str, Any]:
        """构造或模拟测试所需的 fetchone 行为，使断言聚焦于被测业务边界。"""
        return {"TABLE_ROWS": 10, "DATA_LENGTH": 1024, "INDEX_LENGTH": 128, "UPDATE_TIME": None}


class FakeConnection:
    def __enter__(self) -> "FakeConnection":
        """进入上下文管理边界并返回当前资源对象。"""
        return self

    def __exit__(self, *_args: object) -> None:
        """退出上下文并按约定决定异常是否继续传播。"""
        return None

    def cursor(self) -> FakeCursor:
        """构造或模拟测试所需的 cursor 行为，使断言聚焦于被测业务边界。"""
        return FakeCursor()


@pytest.mark.asyncio
async def test_mysql_profile_uses_metadata_and_masks_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 mysql_profile_uses_metadata_and_masks_sample 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    probe = MySqlMetadataProbe()
    monkeypatch.setattr(probe, "_connect", lambda *_args: FakeConnection())
    target = ConnectionTarget(
        connector_type=ConnectorType.MYSQL,
        host="mysql.internal",
        port=3306,
        database_name="sales",
    )

    result = await probe.profile_table(
        target,
        ConnectionCredentials(username="user", password="secret"),
        schema_name="sales",
        table_name="orders",
        sample_limit=20,
    )

    assert result.primary_keys == ["id"]
    assert result.masked_sample == [{"id": 1, "phone": "138****8000"}]
    assert len(result.schema_definition["columns"]) == 2


@pytest.mark.asyncio
async def test_mysql_profile_rejects_identifier_injection() -> None:
    """验证 mysql_profile_rejects_identifier_injection 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    with pytest.raises(DomainError, match="安全的数据库标识符"):
        await MySqlMetadataProbe().profile_table(
            ConnectionTarget(
                connector_type=ConnectorType.MYSQL,
                host="mysql.internal",
                port=3306,
                database_name="sales",
            ),
            ConnectionCredentials(username="user", password="secret"),
            schema_name="sales",
            table_name="orders; DROP TABLE users",
            sample_limit=20,
        )
