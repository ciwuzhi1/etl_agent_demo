import pytest

from etl_agent.application.schema_contract import (
    canonicalize_managed_file_source_contract,
    profile_contracts,
    require_database_endpoint_contract,
    require_hdfs_file_contract,
    require_managed_file_source_contract,
    require_profile_topology_contract,
    require_target_schema_contract,
)
from etl_agent.domain.errors import DomainError

TARGET_PROFILES = [
    {
        "profile_id": "clean-profile",
        "connector_type": "DORIS",
        "schema": "analytics",
        "table": "clean_orders",
        "columns": [
            {"name": "order_id", "type": "bigint"},
            {"name": "email", "type": "varchar(255)"},
            {"name": "source_system", "type": "varchar(32)"},
        ],
        "masked_sample": [{"email": "***@example.com"}],
        "credential_placeholders": {"password": "${SECRET}"},
    }
]


def _hocon(projection: str) -> str:
    """构造或模拟测试所需的 _hocon 行为，使断言聚焦于被测业务边界。"""
    return f'''transform {{
  Sql {{
    plugin_input = "normalized"
    plugin_output = "valid_rows"
    query = """SELECT {projection} FROM normalized WHERE order_id IS NOT NULL"""
  }}
}}
sink {{
  Doris {{
    plugin_input = "valid_rows"
    database = "analytics"
    table = "clean_orders"
  }}
}}'''


def test_target_schema_contract_accepts_exact_explicit_projection() -> None:
    """验证 target_schema_contract_accepts_exact_explicit_projection 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    require_target_schema_contract(
        TARGET_PROFILES,
        _hocon("order_id, REGEXP_REPLACE(email, 'x', 'y') AS email, 'mysql' AS source_system"),
    )


def test_target_schema_contract_rejects_missing_nullable_column() -> None:
    """验证 target_schema_contract_rejects_missing_nullable_column 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    with pytest.raises(DomainError, match="source_system"):
        require_target_schema_contract(
            TARGET_PROFILES,
            _hocon("order_id, email"),
        )


def test_target_schema_contract_rejects_unprofiled_sink() -> None:
    """验证 target_schema_contract_rejects_unprofiled_sink 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    with pytest.raises(DomainError, match=r"没有经过.*Schema 画像"):
        require_target_schema_contract(
            TARGET_PROFILES,
            _hocon("order_id, email, 'mysql' AS source_system").replace(
                'table = "clean_orders"', 'table = "shadow_orders"'
            ),
        )


def test_target_schema_contract_rejects_wildcard_projection() -> None:
    """验证 target_schema_contract_rejects_wildcard_projection 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    with pytest.raises(DomainError, match=r"不得使用 \*"):
        require_target_schema_contract(TARGET_PROFILES, _hocon("*"))


def test_profile_contract_snapshot_excludes_samples_and_credentials() -> None:
    """验证 profile_contract_snapshot_excludes_samples_and_credentials 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    snapshot = profile_contracts(TARGET_PROFILES)

    assert snapshot[0]["columns"][0]["name"] == "order_id"
    assert "masked_sample" not in snapshot[0]
    assert "credential_placeholders" not in snapshot[0]


def test_profile_topology_rejects_duplicate_source_scan() -> None:
    """验证 profile_topology_rejects_duplicate_source_scan 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    profiles = [
        {"connector_type": "MYSQL"},
        {"connector_type": "DORIS"},
        {"connector_type": "DORIS"},
    ]
    hocon = "source { Jdbc {} Jdbc {} } sink { Doris {} Doris {} }"

    with pytest.raises(DomainError, match="精确 1 个 Jdbc"):
        require_profile_topology_contract(profiles, hocon)


def test_profile_topology_accepts_selected_connector_counts() -> None:
    """验证 profile_topology_accepts_selected_connector_counts 场景的预期结果与失败边界，
    防止对应行为回归。
    """
    profiles = [
        {"connector_type": "MYSQL"},
        {"connector_type": "DORIS"},
        {"connector_type": "DORIS"},
    ]

    require_profile_topology_contract(
        profiles,
        "source { Jdbc {} } sink { Doris {} Doris {} }",
    )


def test_profile_topology_rejects_connector_when_expected_count_is_zero() -> None:
    """验证未选择 JDBC Profile 时额外生成 Jdbc 块也会 fail-closed。"""
    with pytest.raises(DomainError, match="精确 0 个 Jdbc"):
        require_profile_topology_contract(
            [{"connector_type": "LOCAL_FILE", "role": "source"}],
            "source { LocalFile {} Jdbc {} }",
        )


MANAGED_FILE_PROFILE = {
    "profile_id": "managed-file-profile",
    "connector_type": "LOCAL_FILE",
    "schema": "managed_file",
    "table": "orders.csv",
    "columns": [
        {"name": "order_id", "type": "string"},
        {"name": "amount", "type": "double"},
    ],
    "content_sha256": "a" * 64,
    "connector_contract": {
        "required_options": {
            "path": "/data/file-assets/asset-1/normalized.jsonl",
            "file_format_type": "json",
            "encoding": "UTF-8",
        }
    },
}


def _managed_file_hocon(*, path: str | None = None, amount_type: str = "double") -> str:
    """构造受管 LocalFile Source，集中表达测试使用的固定执行契约。"""
    source_path = path or "/data/file-assets/asset-1/normalized.jsonl"
    return f'''source {{
  LocalFile {{
    path = "{source_path}"
    file_format_type = "json"
    encoding = "UTF-8"
    schema {{
      fields {{
        order_id = string
        amount = {amount_type}
      }}
    }}
  }}
}}'''


def test_managed_file_contract_accepts_exact_frozen_source() -> None:
    """验证受管文件的精确路径、格式、编码和字段类型可以通过确定性门禁。"""
    snapshots = profile_contracts([MANAGED_FILE_PROFILE])

    require_profile_topology_contract(snapshots, _managed_file_hocon())
    require_managed_file_source_contract(snapshots, _managed_file_hocon())
    assert snapshots[0]["managed_source"]["content_sha256"] == "a" * 64


def test_managed_file_contract_rejects_path_substitution() -> None:
    """验证模型不能把已画像文件静默替换为容器中的其他路径。"""
    with pytest.raises(DomainError, match="精确使用受管路径"):
        require_managed_file_source_contract(
            [MANAGED_FILE_PROFILE],
            _managed_file_hocon(path="/data/file-assets/other/normalized.jsonl"),
        )


def test_managed_file_path_is_compiled_from_single_profile() -> None:
    """验证单文件任务会把模型给出的目录或错误文件名编译为平台冻结路径。"""
    candidate = _managed_file_hocon(path="/data/file-assets/asset-1")

    canonical = canonicalize_managed_file_source_contract([MANAGED_FILE_PROFILE], candidate)

    assert 'path = "/data/file-assets/asset-1/normalized.jsonl"' in canonical
    require_managed_file_source_contract([MANAGED_FILE_PROFILE], canonical)


def test_managed_file_path_canonicalizer_does_not_guess_duplicate_sources() -> None:
    """验证多个 LocalFile 块时不猜测 Profile 映射，交由拓扑门禁明确拒绝。"""
    candidate = _managed_file_hocon(path="/tmp/a.jsonl") + _managed_file_hocon(path="/tmp/b.jsonl")

    canonical = canonicalize_managed_file_source_contract([MANAGED_FILE_PROFILE], candidate)

    assert canonical == candidate


def test_managed_file_schema_is_compiled_when_model_omits_it() -> None:
    """验证模型省略 schema.fields 时由文件画像确定性补齐，不再消耗修复调用。"""
    candidate = """source {
  LocalFile {
    path = "/tmp/model-path"
    plugin_output = "rows"
  }
}"""

    canonical = canonicalize_managed_file_source_contract([MANAGED_FILE_PROFILE], candidate)

    assert 'file_format_type = "json"' in canonical
    assert 'encoding = "UTF-8"' in canonical
    assert 'order_id = "string"' in canonical
    assert 'amount = "double"' in canonical
    require_managed_file_source_contract([MANAGED_FILE_PROFILE], canonical)


def test_managed_file_contract_rejects_schema_drift() -> None:
    """验证 LocalFile 字段类型与 Profile 漂移时必须在审批前失败。"""
    with pytest.raises(DomainError, match="字段和类型不一致"):
        require_managed_file_source_contract(
            [MANAGED_FILE_PROFILE],
            _managed_file_hocon(amount_type="string"),
        )


def test_profile_topology_rejects_duplicate_managed_file_scan() -> None:
    """验证一个文件 Profile 不能通过两个 LocalFile Source 被重复读取。"""
    with pytest.raises(DomainError, match="精确 1 个 LocalFile"):
        require_profile_topology_contract(
            [MANAGED_FILE_PROFILE],
            "source { LocalFile {} LocalFile {} }",
        )


HDFS_SOURCE_PROFILE = {
    "profile_id": "hdfs-source",
    "connector_type": "HDFS",
    "role": "source",
    "schema": "sales",
    "table": "orders",
    "columns": [
        {"name": "id", "type": "bigint"},
        {"name": "email", "type": "string"},
    ],
    "hdfs_endpoint": {
        "path": "/warehouse/sales/orders",
        "file_format_type": "csv",
        "fs.defaultFS": "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_HDFS_URI}",
        "remote_user": "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_USERNAME}",
    },
}


def _hdfs_source_hocon(*, path: str = "/warehouse/sales/orders") -> str:
    """构造带冻结占位符和 Schema 的 HdfsFile Source 测试配置。"""
    return f'''source {{
  HdfsFile {{
    fs.defaultFS = "${{CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_HDFS_URI}}"
    remote_user = "${{CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_USERNAME}}"
    path = "{path}"
    file_format_type = "csv"
    plugin_output = "source_rows"
    schema {{
      fields {{
        id = bigint
        email = string
      }}
    }}
  }}
}}'''


def test_hdfs_source_contract_accepts_frozen_endpoint_and_schema() -> None:
    """验证 HDFS Source 的集群占位符、路径、格式和字段完全一致时通过。"""
    require_profile_topology_contract([HDFS_SOURCE_PROFILE], _hdfs_source_hocon())
    require_hdfs_file_contract([HDFS_SOURCE_PROFILE], _hdfs_source_hocon())


def test_hdfs_source_contract_rejects_path_substitution() -> None:
    """验证 Agent 不能把已画像 HDFS 数据集替换到同集群的其他目录。"""
    with pytest.raises(DomainError, match="精确使用已画像路径"):
        require_hdfs_file_contract(
            [HDFS_SOURCE_PROFILE],
            _hdfs_source_hocon(path="/warehouse/sales/shadow_orders"),
        )


def test_jdbc_sink_schema_contract_runs_without_doris_profile() -> None:
    """验证纯 JDBC 目标也执行 Schema 门禁，不再被 Doris 空集合提前返回绕过。"""
    profile = {
        "connector_type": "POSTGRESQL",
        "role": "sink",
        "schema": "analytics",
        "table": "clean_orders",
        "columns": [{"name": "id", "type": "bigint"}],
    }
    hocon = """transform { Sql {
      plugin_input = "source_rows"
      plugin_output = "sink_rows"
      query = "SELECT id, email FROM source_rows"
    } }
    sink { Jdbc {
      generate_sink_sql = true
      table = "analytics.clean_orders"
      plugin_input = "sink_rows"
    } }"""

    with pytest.raises(DomainError, match="SQL 投影与目标 Schema 不一致"):
        require_target_schema_contract([profile], hocon)


def test_database_endpoint_contract_binds_each_jdbc_source_to_its_profile() -> None:
    """验证多表任务不能用正确数量的 Jdbc 块重复读取同一张表来绕过拓扑门禁。"""
    profiles = [
        {
            "connector_type": "MYSQL",
            "role": "source",
            "database_endpoint": {
                "plugin": "Jdbc",
                "required_options": {
                    "url": "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_JDBC_URL}",
                    "driver": "com.mysql.cj.jdbc.Driver",
                    "user": "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_USERNAME}",
                    "password": "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_PASSWORD}",
                    "query": "SELECT * FROM sales.orders",
                },
            },
        },
        {
            "connector_type": "MYSQL",
            "role": "source",
            "database_endpoint": {
                "plugin": "Jdbc",
                "required_options": {
                    "url": "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_JDBC_URL}",
                    "driver": "com.mysql.cj.jdbc.Driver",
                    "user": "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_USERNAME}",
                    "password": "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_PASSWORD}",
                    "query": "SELECT * FROM sales.customers",
                },
            },
        },
    ]
    hocon = """source {
      Jdbc {
        url = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_JDBC_URL}"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_USERNAME}"
        password = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_PASSWORD}"
        query = "SELECT * FROM sales.orders"
        plugin_output = "orders"
      }
      Jdbc {
        url = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_JDBC_URL}"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_USERNAME}"
        password = "${CONNECTION_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_PASSWORD}"
        query = "SELECT * FROM sales.customers"
        plugin_output = "customers"
      }
    }"""

    require_database_endpoint_contract(profiles, hocon)
    duplicated = hocon.replace("SELECT * FROM sales.customers", "SELECT * FROM sales.orders")
    with pytest.raises(DomainError, match="唯一匹配"):
        require_database_endpoint_contract(profiles, duplicated)
