import re
from typing import Any

from etl_agent.application.policy import _iter_named_block_bodies, _iter_named_block_spans
from etl_agent.domain.errors import DomainError

_DORIS_BLOCK_START = re.compile(r"(?i)\bDoris\s*\{")
_JDBC_BLOCK_START = re.compile(r"(?i)\bJdbc\s*\{")
_HIVE_BLOCK_START = re.compile(r"(?i)\bHive\s*\{")
_HDFS_FILE_BLOCK_START = re.compile(r"(?i)\bHdfsFile\s*\{")
_LOCAL_FILE_BLOCK_START = re.compile(r"(?i)\bLocalFile\s*\{")
_SQL_BLOCK_START = re.compile(r"(?i)\bSql\s*\{")
_SCHEMA_BLOCK_START = re.compile(r"(?i)(?<![\w.-])schema\s*\{")
_FIELDS_BLOCK_START = re.compile(r"(?i)(?<![\w.-])fields\s*\{")
_FIELD_ASSIGNMENT = re.compile(
    r"(?im)^\s*([A-Za-z_][\w$]*)\s*=\s*['\"]?"
    r"([A-Za-z][A-Za-z0-9_]*(?:\([^)]*\))?)['\"]?\s*$"
)


def profile_contracts(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """生成可随制品持久化的 Schema 快照，不携带样本、拓扑或凭据。"""
    snapshots: list[dict[str, Any]] = []
    for profile in profiles:
        snapshot: dict[str, Any] = {
            "profile_id": profile.get("profile_id"),
            "connector_type": profile.get("connector_type"),
            "schema": profile.get("schema"),
            "table": profile.get("table"),
            "role": profile.get("role"),
            "columns": profile.get("columns", []),
        }
        if str(profile.get("connector_type") or "").upper() == "LOCAL_FILE":
            required_options = profile.get("connector_contract", {}).get("required_options", {})
            # 受管文件没有外部连接记录，执行所需路径必须随不可变制品冻结；这里只保留
            # 引擎只读路径和内容哈希，不携带原始上传路径或脱敏样本。
            snapshot["managed_source"] = {
                "path": required_options.get("path"),
                "file_format_type": required_options.get("file_format_type"),
                "encoding": required_options.get("encoding"),
                "content_sha256": profile.get("content_sha256"),
            }
        if str(profile.get("connector_type") or "").upper() == "HDFS":
            required_options = profile.get("connector_contract", {}).get("required_options", {})
            # HDFS 快照只冻结逻辑路径、格式和变量名，不保存 NameNode 真实地址或凭据。
            snapshot["hdfs_endpoint"] = {
                "path": required_options.get("path"),
                "file_format_type": required_options.get("file_format_type"),
                "fs.defaultFS": required_options.get("fs.defaultFS"),
                "remote_user": required_options.get("remote_user"),
            }
        if str(profile.get("connector_type") or "").upper() not in {"LOCAL_FILE", "HDFS"}:
            required_options = profile.get("connector_contract", {}).get("required_options", {})
            # 数据库快照只保留变量占位符和逻辑对象名；真实主机在执行时才由连接记录物化。
            snapshot["database_endpoint"] = {
                "plugin": profile.get("connector_contract", {}).get("plugin"),
                "required_options": {
                    key: required_options.get(key)
                    for key in (
                        "url",
                        "driver",
                        "user",
                        "username",
                        "password",
                        "query",
                        "fenodes",
                        "database",
                        "table",
                        "table_name",
                        "metastore_uri",
                        "remote_user",
                        "generate_sink_sql",
                        "schema_save_mode",
                        "data_save_mode",
                    )
                    if required_options.get(key) is not None
                },
            }
        snapshots.append(snapshot)
    return snapshots


def canonicalize_managed_file_source_contract(
    profiles: list[dict[str, Any]],
    hocon: str,
) -> str:
    """把唯一受管文件 Source 编译为平台冻结的物理读取契约。

    path、格式、编码和字段类型都由上传画像流程产生，不属于大模型可以设计的 ETL
    语义。仅在一个 Profile 对应一个 LocalFile 块时编译，避免多 Source 场景猜测映射；
    重复字段块或重复选项仍保持原样并由严格门禁拒绝。
    """
    managed_profiles = [
        profile
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "LOCAL_FILE"
    ]
    spans = _iter_named_block_spans(hocon, _LOCAL_FILE_BLOCK_START)
    if len(managed_profiles) != 1 or len(spans) != 1:
        return hocon

    profile = managed_profiles[0]
    source_contract = _managed_source_contract(profile)
    expected_path = str(source_contract.get("path") or "")
    if not expected_path:
        return hocon
    start, end = spans[0]
    body = hocon[start:end]

    canonical_body = body
    for key, value in (
        ("path", expected_path),
        ("file_format_type", str(source_contract.get("file_format_type") or "json")),
        ("encoding", str(source_contract.get("encoding") or "UTF-8")),
    ):
        updated = _canonicalize_unique_assignment(canonical_body, key, value)
        if updated is None:
            return hocon
        canonical_body = updated
    canonical_schema = _canonicalize_managed_schema(profile, canonical_body)
    if canonical_schema is None:
        return hocon
    return hocon[:start] + canonical_schema + hocon[end:]


def _canonicalize_unique_assignment(body: str, key: str, value: str) -> str | None:
    """替换或补充唯一标量选项；出现重复声明时拒绝猜测优先级。"""
    assignment = re.compile(
        rf"(?im)^(?P<prefix>\s*{re.escape(key)}\s*=\s*)"
        rf"(?:(?P<quote>['\"])(?P<quoted>.*?)(?P=quote)|(?P<bare>[^\s#]+))\s*$"
    )
    matches = list(assignment.finditer(body))
    if len(matches) > 1:
        return None
    if len(matches) == 1:
        return assignment.sub(
            lambda match: f'{match.group("prefix")}"{value}"',
            body,
            count=1,
        )
    # LocalFile 块的 body 不含外层花括号；在首行补充选项不会改变其他插件配置。
    return f'\n    {key} = "{value}"' + body


def _canonicalize_managed_schema(profile: dict[str, Any], body: str) -> str | None:
    """替换或补充唯一 schema.fields，字段顺序沿用已冻结 Profile 快照。"""
    field_lines = [
        f'        {column["name"]} = "{column["type"]}"'
        for column in profile.get("columns", [])
        if isinstance(column, dict) and column.get("name") and column.get("type")
    ]
    if not field_lines:
        return None
    rendered_fields = "\n" + "\n".join(field_lines) + "\n      "
    field_spans = _iter_named_block_spans(body, _FIELDS_BLOCK_START)
    if len(field_spans) > 1:
        return None
    if len(field_spans) == 1:
        field_start, field_end = field_spans[0]
        return body[:field_start] + rendered_fields + body[field_end:]

    rendered_block = "\n      fields {" + rendered_fields + "}\n    "
    schema_spans = _iter_named_block_spans(body, _SCHEMA_BLOCK_START)
    if len(schema_spans) > 1:
        return None
    if len(schema_spans) == 1:
        _schema_start, schema_end = schema_spans[0]
        return body[:schema_end] + rendered_block + body[schema_end:]
    return body.rstrip() + "\n    schema {" + rendered_block + "}\n  "


def require_managed_file_source_contract(
    profiles: list[dict[str, Any]],
    hocon: str,
) -> None:
    """校验受管文件 Source 必须精确使用平台冻结的只读 JSONL 契约。

    官方 CLI 能验证 LocalFile 配置可解析，却无法证明模型没有换成另一个容器内文件。
    因此审批前还要绑定精确路径、格式、编码和字段类型，阻止路径替换与 Schema 漂移。
    """
    managed_profiles = [
        profile
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "LOCAL_FILE"
    ]
    blocks = _iter_named_block_bodies(hocon, _LOCAL_FILE_BLOCK_START)
    if len(blocks) != len(managed_profiles):
        raise DomainError(
            f"Profile 拓扑要求精确 {len(managed_profiles)} 个 LocalFile Source，"
            f"制品实际包含 {len(blocks)} 个"
        )

    unmatched = list(blocks)
    for profile in managed_profiles:
        source_contract = _managed_source_contract(profile)
        expected_path = str(source_contract.get("path") or "")
        if not expected_path:
            raise DomainError("受管文件 Profile 缺少已冻结的 SeaTunnel 只读路径")
        candidates = [block for block in unmatched if _assignment(block, "path") == expected_path]
        if len(candidates) != 1:
            raise DomainError(f"LocalFile SOURCE 必须精确使用受管路径：{expected_path}")
        block = candidates[0]
        unmatched.remove(block)

        if (_assignment(block, "file_format_type") or "").casefold() != "json":
            raise DomainError("受管文件 LocalFile Source 必须使用 file_format_type = json")
        if (_assignment(block, "encoding") or "").replace("-", "").casefold() != "utf8":
            raise DomainError("受管文件 LocalFile Source 必须显式使用 UTF-8 编码")
        _require_managed_file_fields(profile, block)


def _managed_source_contract(profile: dict[str, Any]) -> dict[str, Any]:
    """兼容生成上下文与持久化快照两种结构，提取受管文件执行契约。"""
    snapshot = profile.get("managed_source")
    if isinstance(snapshot, dict):
        return snapshot
    connector_contract = profile.get("connector_contract")
    if not isinstance(connector_contract, dict):
        return {}
    required = connector_contract.get("required_options")
    return required if isinstance(required, dict) else {}


def _require_managed_file_fields(profile: dict[str, Any], block: str) -> None:
    """比较 LocalFile schema.fields 与 Profile 列快照，拒绝缺列、多列和类型漂移。"""
    field_blocks = _iter_named_block_bodies(block, _FIELDS_BLOCK_START)
    if len(field_blocks) != 1:
        raise DomainError("受管文件 LocalFile Source 必须包含唯一 schema.fields 块")
    actual = {
        name.casefold(): field_type.casefold()
        for name, field_type in _FIELD_ASSIGNMENT.findall(field_blocks[0])
    }
    expected = {
        str(column.get("name") or "").casefold(): str(column.get("type") or "").casefold()
        for column in profile.get("columns", [])
        if isinstance(column, dict) and column.get("name") and column.get("type")
    }
    if actual != expected:
        raise DomainError("LocalFile schema.fields 与受管文件 Profile 的字段和类型不一致")


def require_hdfs_file_contract(
    profiles: list[dict[str, Any]],
    hocon: str,
) -> None:
    """把每个 HdfsFile 块绑定到冻结的路径、格式、角色、Schema 与安全写入模式。

    官方 CLI 只能判断配置能否被连接器解析，不能证明模型没有换目录、换集群或采用
    删除式保存模式；因此这些企业数据边界必须在审批前由控制面确定性比较。
    """
    hdfs_profiles = [
        profile
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "HDFS"
    ]
    blocks = _iter_named_block_bodies(hocon, _HDFS_FILE_BLOCK_START)
    if len(blocks) != len(hdfs_profiles):
        raise DomainError(
            f"Profile 拓扑要求精确 {len(hdfs_profiles)} 个 HdfsFile 端点，"
            f"制品实际包含 {len(blocks)} 个"
        )

    unmatched = list(blocks)
    for profile in hdfs_profiles:
        contract = _hdfs_endpoint_contract(profile)
        path = str(contract.get("path") or "")
        role = _endpoint_role(profile)
        candidates = [
            block
            for block in unmatched
            if _assignment(block, "path") == path
            and (
                (_assignment(block, "plugin_output") is not None)
                if role == "source"
                else (_assignment(block, "plugin_input") is not None)
            )
        ]
        if len(candidates) != 1:
            raise DomainError(f"HdfsFile {role} 必须精确使用已画像路径：{path}")
        block = candidates[0]
        unmatched.remove(block)

        expected_format = str(contract.get("file_format_type") or "").casefold()
        if (_assignment(block, "file_format_type") or "").casefold() != expected_format:
            raise DomainError(f"HdfsFile {path} 的文件格式与 Profile 快照不一致")
        for option in ("fs.defaultFS", "remote_user"):
            expected = str(contract.get(option) or "")
            if not expected or _assignment(block, option) != expected:
                raise DomainError(f"HdfsFile {path} 必须使用已登记的 {option} 占位符")

        if role == "source":
            _require_profile_fields(profile, block, connector_label="HdfsFile")
        else:
            if (_assignment(block, "schema_save_mode") or "").upper() != (
                "ERROR_WHEN_SCHEMA_NOT_EXIST"
            ):
                raise DomainError("HdfsFile Sink 必须使用 ERROR_WHEN_SCHEMA_NOT_EXIST")
            if (_assignment(block, "data_save_mode") or "").upper() != "APPEND_DATA":
                raise DomainError("HdfsFile Sink 只允许 APPEND_DATA，禁止删除既有数据")


def require_database_endpoint_contract(
    profiles: list[dict[str, Any]],
    hocon: str,
) -> None:
    """逐一绑定数据库 Profile 与物理连接块，防止多表任务只满足数量却读错表或写错库。"""
    database_profiles = [
        profile
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() not in {"LOCAL_FILE", "HDFS"}
        and bool(_database_endpoint_contract(profile))
    ]
    if not database_profiles:
        # 兼容引入端点快照前已冻结的历史 Artifact；新生成版本始终带该契约。
        return
    jdbc_profiles = [
        profile
        for profile in database_profiles
        if str(_database_endpoint_contract(profile).get("plugin") or "").casefold() == "jdbc"
    ]
    _require_jdbc_endpoints(jdbc_profiles, _iter_named_block_bodies(hocon, _JDBC_BLOCK_START))

    doris_profiles = [
        profile
        for profile in database_profiles
        if str(_database_endpoint_contract(profile).get("plugin") or "").casefold() == "doris"
    ]
    _require_native_database_endpoints(
        doris_profiles,
        _iter_named_block_bodies(hocon, _DORIS_BLOCK_START),
        match_options=("fenodes", "database", "table"),
        credential_options=("username", "password"),
        connector_label="Doris",
        require_schema_mode=False,
    )

    hive_profiles = [
        profile
        for profile in database_profiles
        if str(_database_endpoint_contract(profile).get("plugin") or "").casefold() == "hive"
    ]
    _require_native_database_endpoints(
        hive_profiles,
        _iter_named_block_bodies(hocon, _HIVE_BLOCK_START),
        match_options=("metastore_uri", "table_name"),
        credential_options=("remote_user",),
        connector_label="Hive",
        require_schema_mode=True,
    )


def _require_jdbc_endpoints(
    profiles: list[dict[str, Any]],
    blocks: list[str],
) -> None:
    """校验每个 Jdbc 块精确匹配一个 Profile 的 URL、驱动、凭据和源/目标对象。"""
    if len(blocks) != len(profiles):
        raise DomainError(
            f"数据库端点契约要求精确 {len(profiles)} 个 Jdbc 块，制品实际包含 {len(blocks)} 个"
        )
    unmatched = list(profiles)
    for block in blocks:
        url = _assignment(block, "url")
        generates_sink_sql = (_assignment(block, "generate_sink_sql") or "").casefold() == "true"
        role = "sink" if generates_sink_sql else "source"
        candidates = [
            profile
            for profile in unmatched
            if _endpoint_role(profile) == role
            and url
            == str(
                _database_endpoint_contract(profile).get("required_options", {}).get("url") or ""
            )
        ]
        if role == "source":
            candidates = [
                profile
                for profile in candidates
                if _source_query_matches(
                    block,
                    str(
                        _database_endpoint_contract(profile)
                        .get("required_options", {})
                        .get("query")
                        or ""
                    ),
                )
            ]
        else:
            candidates = [
                profile
                for profile in candidates
                if _assignment(block, "table")
                == str(
                    _database_endpoint_contract(profile).get("required_options", {}).get("table")
                    or ""
                )
            ]
        if len(candidates) != 1:
            raise DomainError("Jdbc 块未能唯一匹配本次 AgentRun 的数据库 Profile")
        profile = candidates[0]
        required = _database_endpoint_contract(profile).get("required_options", {})
        if _assignment(block, "driver") != str(required.get("driver") or ""):
            raise DomainError("Jdbc driver 与冻结连接器契约不一致")
        username = _assignment(block, "user") or _assignment(block, "username")
        if username != str(required.get("user") or required.get("username") or ""):
            raise DomainError("Jdbc 用户名必须使用该 Profile 的已登记占位符")
        if _assignment(block, "password") != str(required.get("password") or ""):
            raise DomainError("Jdbc 密码必须使用该 Profile 的已登记占位符")
        if role == "source" and not _assignment(block, "plugin_output"):
            raise DomainError("每个 Jdbc Source 必须声明 plugin_output")
        if role == "sink":
            _require_append_only_modes(block, "Jdbc")
            if not _assignment(block, "plugin_input"):
                raise DomainError("每个 Jdbc Sink 必须声明 plugin_input")
        unmatched.remove(profile)


def _source_query_matches(block: str, expected_query: str) -> bool:
    """只允许冻结全表查询，或由增量门禁另行验证的标准水位 WHERE 子句。"""
    actual = _assignment(block, "query") or ""
    normalized_actual = " ".join(actual.split())
    normalized_expected = " ".join(expected_query.split())
    if normalized_actual.casefold() == normalized_expected.casefold():
        return True
    prefix = normalized_expected + " WHERE "
    starts_with_query = normalized_actual.casefold().startswith(prefix.casefold())
    remainder = normalized_actual[len(prefix) :] if starts_with_query else ""
    return bool(
        remainder
        and "__ETL_AGENT_WATERMARK_LOWER__" in remainder
        and "__ETL_AGENT_WATERMARK_UPPER__" in remainder
    )


def _require_native_database_endpoints(
    profiles: list[dict[str, Any]],
    blocks: list[str],
    *,
    match_options: tuple[str, ...],
    credential_options: tuple[str, ...],
    connector_label: str,
    require_schema_mode: bool,
) -> None:
    """校验 Doris/Hive 原生连接块的占位符、对象名、路由和只追加保存模式。"""
    if len(blocks) != len(profiles):
        raise DomainError(
            f"数据库端点契约要求精确 {len(profiles)} 个 {connector_label} 块，"
            f"制品实际包含 {len(blocks)} 个"
        )
    unmatched = list(profiles)
    for block in blocks:
        target = next(
            (
                profile
                for profile in unmatched
                if all(
                    _assignment(block, option)
                    == str(
                        _database_endpoint_contract(profile).get("required_options", {}).get(option)
                        or ""
                    )
                    for option in match_options
                )
            ),
            None,
        )
        if target is None:
            raise DomainError(f"{connector_label} 块未匹配本次 AgentRun 的冻结端点")
        required = _database_endpoint_contract(target).get("required_options", {})
        for option in credential_options:
            if _assignment(block, option) != str(required.get(option) or ""):
                raise DomainError(f"{connector_label} {option} 必须使用已登记占位符")
        _require_append_only_modes(
            block,
            connector_label,
            require_schema_mode=require_schema_mode,
        )
        if not _assignment(block, "plugin_input"):
            raise DomainError(f"每个 {connector_label} Sink 必须声明 plugin_input")
        unmatched.remove(target)


def _require_append_only_modes(
    block: str,
    connector_label: str,
    *,
    require_schema_mode: bool = True,
) -> None:
    """统一拒绝数据库目标的建表覆盖和清空数据模式。"""
    if (
        require_schema_mode
        and (_assignment(block, "schema_save_mode") or "").upper() != "ERROR_WHEN_SCHEMA_NOT_EXIST"
    ):
        raise DomainError(f"{connector_label} Sink 必须使用 ERROR_WHEN_SCHEMA_NOT_EXIST")
    if (_assignment(block, "data_save_mode") or "").upper() != "APPEND_DATA":
        raise DomainError(f"{connector_label} Sink 只允许 APPEND_DATA")


def _database_endpoint_contract(profile: dict[str, Any]) -> dict[str, Any]:
    """兼容生成上下文与 Artifact 快照两种结构，读取数据库物理端点契约。"""
    snapshot = profile.get("database_endpoint")
    if isinstance(snapshot, dict):
        return snapshot
    contract = profile.get("connector_contract")
    return contract if isinstance(contract, dict) else {}


def _hdfs_endpoint_contract(profile: dict[str, Any]) -> dict[str, Any]:
    """兼容生成上下文与 Artifact 快照两种结构，读取 HDFS 物理端点契约。"""
    snapshot = profile.get("hdfs_endpoint")
    if isinstance(snapshot, dict):
        return snapshot
    connector_contract = profile.get("connector_contract")
    if not isinstance(connector_contract, dict):
        return {}
    required = connector_contract.get("required_options")
    return required if isinstance(required, dict) else {}


def _require_profile_fields(
    profile: dict[str, Any],
    block: str,
    *,
    connector_label: str,
) -> None:
    """比较文件 Source 的 schema.fields 与冻结画像，拒绝缺列、多列和类型漂移。"""
    field_blocks = _iter_named_block_bodies(block, _FIELDS_BLOCK_START)
    if len(field_blocks) != 1:
        raise DomainError(f"{connector_label} Source 必须包含唯一 schema.fields 块")
    actual = {
        name.casefold(): field_type.casefold()
        for name, field_type in _FIELD_ASSIGNMENT.findall(field_blocks[0])
    }
    expected = {
        str(column.get("name") or "").casefold(): str(column.get("type") or "").casefold()
        for column in profile.get("columns", [])
        if isinstance(column, dict) and column.get("name") and column.get("type")
    }
    if actual != expected:
        raise DomainError(f"{connector_label} schema.fields 与 Profile 的字段和类型不一致")


def require_target_schema_contract(
    profiles: list[dict[str, Any]],
    hocon: str,
) -> None:
    """确保 Doris Sink 的 SQL 输出与已画像目标表列集合完全一致。

    Doris JSON 写入可能默默将缺失列写成 NULL，因此官方 CLI 校验通过不代表
    业务 Schema 契约正确；这里在审批前显式拒绝缺列和多列。
    """
    target_profiles = [
        profile
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "DORIS"
        and _endpoint_role(profile) == "sink"
    ]
    sql_outputs = _sql_outputs(hocon)
    doris_blocks = _iter_named_block_bodies(hocon, _DORIS_BLOCK_START)
    matched_targets: set[tuple[str, str]] = set()

    for block in doris_blocks:
        database = _assignment(block, "database")
        table = _assignment(block, "table")
        plugin_input = _assignment(block, "plugin_input")
        if not database or not table or not plugin_input:
            # Connector 硬门禁会报告必填项；此处仅处理 Schema 对齐。
            continue

        target = next(
            (
                profile
                for profile in target_profiles
                if str(profile.get("schema") or "").casefold() == database.casefold()
                and str(profile.get("table") or "").casefold() == table.casefold()
            ),
            None,
        )
        if target is None:
            raise DomainError(
                f"Doris Sink {database}.{table} 没有经过本次 AgentRun 的目标 Schema 画像"
            )

        expected = _profile_column_names(target)
        actual = sql_outputs.get(plugin_input.casefold())
        if actual is None:
            raise DomainError(
                f"Doris Sink {database}.{table} 的 plugin_input={plugin_input} "
                "没有对应的 Sql Transform 输出，无法验证列契约"
            )
        missing = [column for column in expected if column.casefold() not in actual]
        extra = [
            column for column in actual.values() if column.casefold() not in _casefold(expected)
        ]
        if missing or extra:
            details: list[str] = []
            if missing:
                details.append("缺少列：" + "、".join(missing))
            if extra:
                details.append("多余列：" + "、".join(extra))
            raise DomainError(
                f"Doris Sink {database}.{table} 的 SQL 投影与目标 Schema 不一致（"
                + "；".join(details)
                + "）"
            )
        matched_targets.add((database.casefold(), table.casefold()))

    expected_targets = {
        (str(profile.get("schema") or "").casefold(), str(profile.get("table") or "").casefold())
        for profile in target_profiles
    }
    missing_targets = expected_targets - matched_targets
    if missing_targets:
        names = "、".join(f"{schema}.{table}" for schema, table in sorted(missing_targets))
        raise DomainError(f"已选择的 Doris 目标画像未在制品中使用：{names}")

    jdbc_target_profiles = [
        profile
        for profile in profiles
        if str(profile.get("connector_type") or "").upper()
        in {"MYSQL", "POSTGRESQL", "ORACLE", "SQL_SERVER"}
        and _endpoint_role(profile) == "sink"
    ]
    jdbc_sink_blocks = [
        block
        for block in _iter_named_block_bodies(hocon, _JDBC_BLOCK_START)
        if (_assignment(block, "generate_sink_sql") or "").casefold() == "true"
    ]
    _require_generic_sink_schemas(
        jdbc_target_profiles,
        jdbc_sink_blocks,
        table_option="table",
        sql_outputs=sql_outputs,
        connector_label="Jdbc",
    )

    hive_target_profiles = [
        profile
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "HIVE"
        and _endpoint_role(profile) == "sink"
    ]
    _require_generic_sink_schemas(
        hive_target_profiles,
        _iter_named_block_bodies(hocon, _HIVE_BLOCK_START),
        table_option="table_name",
        sql_outputs=sql_outputs,
        connector_label="Hive",
    )

    hdfs_target_profiles = [
        profile
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "HDFS"
        and _endpoint_role(profile) == "sink"
    ]
    hdfs_sink_blocks = [
        block
        for block in _iter_named_block_bodies(hocon, _HDFS_FILE_BLOCK_START)
        if _assignment(block, "plugin_input") is not None
    ]
    _require_hdfs_sink_schemas(hdfs_target_profiles, hdfs_sink_blocks, sql_outputs)


def require_profile_topology_contract(
    profiles: list[dict[str, Any]],
    hocon: str,
) -> None:
    """连接块数量必须与已选 Profile 拓扑一致，防止用重复 Source 伪造行分流。"""
    jdbc_source_connectors = {
        "MYSQL",
        "POSTGRESQL",
        "ORACLE",
        "SQL_SERVER",
        "HIVE",
        "DORIS",
    }
    jdbc_sink_connectors = {"MYSQL", "POSTGRESQL", "ORACLE", "SQL_SERVER"}
    expected_jdbc = sum(
        1
        for profile in profiles
        if (
            _endpoint_role(profile) == "source"
            and str(profile.get("connector_type") or "").upper() in jdbc_source_connectors
        )
        or (
            _endpoint_role(profile) == "sink"
            and str(profile.get("connector_type") or "").upper() in jdbc_sink_connectors
        )
    )
    expected_doris = sum(
        1
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "DORIS"
        and _endpoint_role(profile) == "sink"
    )
    expected_hive = sum(
        1
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "HIVE"
        and _endpoint_role(profile) == "sink"
    )
    expected_local_file = sum(
        1
        for profile in profiles
        if str(profile.get("connector_type") or "").upper() == "LOCAL_FILE"
    )
    expected_hdfs = sum(
        1 for profile in profiles if str(profile.get("connector_type") or "").upper() == "HDFS"
    )
    # "期望数量为零"同样是硬约束。此前用 ``if expected_*`` 跳过零值比较，
    # 会让模型凭空增加未授权端点，属于典型 fail-open。
    actual_counts = {
        "Jdbc 端点": len(_iter_named_block_bodies(hocon, _JDBC_BLOCK_START)),
        "Doris Sink": len(_iter_named_block_bodies(hocon, _DORIS_BLOCK_START)),
        "LocalFile Source": len(_iter_named_block_bodies(hocon, _LOCAL_FILE_BLOCK_START)),
        "Hive Sink": len(_iter_named_block_bodies(hocon, _HIVE_BLOCK_START)),
        "HdfsFile 端点": len(_iter_named_block_bodies(hocon, _HDFS_FILE_BLOCK_START)),
    }
    expected_counts = {
        "Jdbc 端点": expected_jdbc,
        "Doris Sink": expected_doris,
        "LocalFile Source": expected_local_file,
        "Hive Sink": expected_hive,
        "HdfsFile 端点": expected_hdfs,
    }
    for label, expected in expected_counts.items():
        actual = actual_counts[label]
        if actual != expected:
            raise DomainError(
                f"Profile 拓扑要求精确 {expected} 个 {label}，制品实际包含 {actual} 个"
            )


def _require_generic_sink_schemas(
    profiles: list[dict[str, Any]],
    blocks: list[str],
    *,
    table_option: str,
    sql_outputs: dict[str, dict[str, str]],
    connector_label: str,
) -> None:
    """校验 JDBC/Hive 目标块与显式 SQL 投影，复用 Doris 的冻结 Schema 边界。"""
    if len(blocks) != len(profiles):
        raise DomainError(
            f"Profile 拓扑要求精确 {len(profiles)} 个 {connector_label} Sink，"
            f"制品实际包含 {len(blocks)} 个"
        )
    unmatched = list(profiles)
    for block in blocks:
        table_name = _assignment(block, table_option)
        plugin_input = _assignment(block, "plugin_input")
        if not table_name or not plugin_input:
            raise DomainError(f"每个 {connector_label} Sink 必须声明目标表和 plugin_input")
        target = next(
            (
                profile
                for profile in unmatched
                if table_name.casefold()
                == f"{profile.get('schema')}.{profile.get('table')}".casefold()
            ),
            None,
        )
        if target is None:
            raise DomainError(f"{connector_label} Sink {table_name} 没有匹配的目标画像")
        expected = _profile_column_names(target)
        actual = sql_outputs.get(plugin_input.casefold())
        if actual is None or _casefold(expected) != set(actual):
            raise DomainError(
                f"{connector_label} Sink {table_name} 的 SQL 投影与目标 Schema 不一致"
            )
        unmatched.remove(target)


def _require_hdfs_sink_schemas(
    profiles: list[dict[str, Any]],
    blocks: list[str],
    sql_outputs: dict[str, dict[str, str]],
) -> None:
    """校验 HDFS Sink 路径与上游显式 SQL 投影，避免文件列顺序静默漂移。"""
    if len(blocks) != len(profiles):
        raise DomainError(
            f"Profile 拓扑要求精确 {len(profiles)} 个 HdfsFile Sink，制品实际包含 {len(blocks)} 个"
        )
    unmatched = list(profiles)
    for block in blocks:
        path = _assignment(block, "path")
        plugin_input = _assignment(block, "plugin_input")
        if not path or not plugin_input:
            raise DomainError("每个 HdfsFile Sink 必须声明目标路径和 plugin_input")
        target = next(
            (
                profile
                for profile in unmatched
                if path == str(_hdfs_endpoint_contract(profile).get("path") or "")
            ),
            None,
        )
        if target is None:
            raise DomainError(f"HdfsFile Sink {path} 没有匹配的目标画像")
        expected = _profile_column_names(target)
        actual = sql_outputs.get(plugin_input.casefold())
        if actual is None or _casefold(expected) != set(actual):
            raise DomainError(f"HdfsFile Sink {path} 的 SQL 投影与目标 Schema 不一致")
        unmatched.remove(target)


def _endpoint_role(profile: dict[str, Any]) -> str:
    """兼容历史快照：未冻结角色时沿用早期约定的 Doris Sink、其他 Source。"""
    explicit = str(profile.get("role") or "").casefold()
    if explicit in {"source", "sink"}:
        return explicit
    return "sink" if str(profile.get("connector_type") or "").upper() == "DORIS" else "source"


def _sql_outputs(hocon: str) -> dict[str, dict[str, str]]:
    """在Schema 契约校验中执行"sql_outputs"对应的内部步骤；作为内部辅助函数，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    outputs: dict[str, dict[str, str]] = {}
    for block in _iter_named_block_bodies(hocon, _SQL_BLOCK_START):
        plugin_output = _assignment(block, "plugin_output")
        query = _assignment(block, "query")
        if not plugin_output or not query:
            continue
        outputs[plugin_output.casefold()] = _select_output_columns(query)
    return outputs


def _assignment(block: str, key: str) -> str | None:
    """在Schema 契约校验中执行"assignment"对应的内部步骤；作为内部辅助函数，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    escaped_key = re.escape(key)
    triple = re.search(rf'(?is)(?<![\w.-]){escaped_key}\s*=\s*"""(.*?)"""', block)
    if triple:
        return triple.group(1).strip()
    quoted = re.search(
        rf"(?im)(?<![\w.-]){escaped_key}\s*=\s*(['\"])(.*?)\1\s*$",
        block,
    )
    if quoted:
        return quoted.group(2).strip()
    bare = re.search(rf"(?im)(?<![\w.-]){escaped_key}\s*=\s*([^\s#]+)", block)
    return bare.group(1).strip() if bare else None


def _select_output_columns(query: str) -> dict[str, str]:
    """从 SELECT 投影提取显式输出列，拒绝无法证明 Schema 的通配符。"""
    projection = _select_projection(query)
    expressions = _split_top_level(projection)
    columns: dict[str, str] = {}
    for expression in expressions:
        value = expression.strip()
        if value == "*" or value.endswith(".*"):
            raise DomainError("Doris Sink 的最终 Sql Transform 必须显式列出目标列，不得使用 *")
        alias = re.search(r'(?is)\s+AS\s+[`"\[]?([A-Za-z_][\w$]*)[`"\]]?\s*$', value)
        if alias:
            name = alias.group(1)
        else:
            simple = re.fullmatch(
                r'(?is)(?:[`"\[]?[A-Za-z_][\w$]*[`"\]]?\.)?'
                r'[`"\[]?([A-Za-z_][\w$]*)[`"\]]?',
                value,
            )
            if not simple:
                raise DomainError(f"Doris Sink 最终 SQL 投影表达式必须声明 AS 别名：{value[:120]}")
            name = simple.group(1)
        folded = name.casefold()
        if folded in columns:
            raise DomainError(f"Doris Sink 最终 SQL 投影包含重复列：{name}")
        columns[folded] = name
    return columns


def _select_projection(query: str) -> str:
    """定位最外层 SELECT/FROM 边界，忽略括号和字符串内部关键字。"""
    select_match = re.search(r"(?is)\bSELECT\b", query)
    if not select_match:
        raise DomainError("Sql Transform query 缺少 SELECT")
    index = select_match.end()
    start = index
    depth = 0
    quote: str | None = None
    while index < len(query):
        char = query[index]
        if quote is not None:
            if char == quote and (index == 0 or query[index - 1] != "\\"):
                quote = None
            index += 1
            continue
        if char in "'\"`":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and re.match(r"(?is)FROM\b", query[index:]):
            return query[start:index].strip()
        index += 1
    raise DomainError("Sql Transform query 缺少 FROM")


def _split_top_level(value: str) -> list[str]:
    """仅在表达式最外层切分分隔符，避免破坏函数参数和嵌套 SQL。"""
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    for index, char in enumerate(value):
        if quote is not None:
            if char == quote and (index == 0 or value[index - 1] != "\\"):
                quote = None
            continue
        if char in "'\"`":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(value[start:index])
            start = index + 1
    parts.append(value[start:])
    return [part for part in parts if part.strip()]


def _profile_column_names(profile: dict[str, Any]) -> list[str]:
    """在Schema 契约校验中采集结构化元数据和脱敏样本；作为内部辅助函数，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    columns = profile.get("columns")
    if not isinstance(columns, list) or not columns:
        raise DomainError(
            f"Doris 目标 {profile.get('schema')}.{profile.get('table')} 缺少可用的 Schema 列快照"
        )
    names = [
        str(column.get("name") or "").strip() for column in columns if isinstance(column, dict)
    ]
    if not names or any(not name for name in names):
        raise DomainError(
            f"Doris 目标 {profile.get('schema')}.{profile.get('table')} 的 Schema 列快照不完整"
        )
    return names


def _casefold(values: list[str]) -> set[str]:
    """在Schema 契约校验中执行"casefold"对应的内部步骤；作为内部辅助函数，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    return {value.casefold() for value in values}
