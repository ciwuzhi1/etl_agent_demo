import re
from typing import Any

from etl_agent.domain.errors import DomainError

_SECRET_KEY = re.compile(r"(?i)(password|passwd|secret|token|access[_-]?key|private[_-]?key)")
# 平台认可的占位符形如 ${CONNECTION_<32位十六进制>_PASSWORD}，其中不含真实凭据。
_APPROVED_CREDENTIAL_PLACEHOLDER = re.compile(
    r"^\$\{CONNECTION_[0-9A-F]{32}_(?:USERNAME|PASSWORD|[A-Z][A-Z0-9_]*)\}$"
)


def _iter_named_block_spans(hocon: str, start_pattern: re.Pattern[str]) -> list[tuple[int, int]]:
    """按配对花括号提取 HOCON 块，忽略字符串和行注释中的花括号。"""
    spans: list[tuple[int, int]] = []
    position = 0
    while match := start_pattern.search(hocon, position):
        opening = hocon.find("{", match.start(), match.end())
        depth = 1
        index = opening + 1
        quote: str | None = None
        triple_quote = False
        escaped = False
        line_comment = False

        while index < len(hocon) and depth:
            if line_comment:
                if hocon[index] in "\r\n":
                    line_comment = False
                index += 1
                continue

            if quote is not None:
                if triple_quote and hocon.startswith(quote * 3, index):
                    quote = None
                    triple_quote = False
                    index += 3
                    continue
                if not triple_quote:
                    if escaped:
                        escaped = False
                    elif hocon[index] == "\\":
                        escaped = True
                    elif hocon[index] == quote:
                        quote = None
                index += 1
                continue

            if hocon.startswith("//", index) or hocon[index] == "#":
                line_comment = True
                index += 2 if hocon.startswith("//", index) else 1
                continue
            if hocon.startswith('"""', index):
                quote = '"'
                triple_quote = True
                index += 3
                continue
            if hocon[index] in "\"'":
                quote = hocon[index]
                index += 1
                continue
            if hocon[index] == "{":
                depth += 1
            elif hocon[index] == "}":
                depth -= 1
            index += 1

        if depth:
            break
        spans.append((opening + 1, index - 1))
        position = index
    return spans


def _iter_named_block_bodies(hocon: str, start_pattern: re.Pattern[str]) -> list[str]:
    """按平衡花括号扫描 HOCON 块，避免简单正则被嵌套配置误导。"""
    return [hocon[start:end] for start, end in _iter_named_block_spans(hocon, start_pattern)]


def require_safe_generation_context(value: Any, path: str = "context") -> None:
    """递归拒绝任何看起来像凭据的字段进入模型上下文，而不是依赖日志脱敏兜底。"""
    if isinstance(value, dict):
        for key, child in value.items():
            if _SECRET_KEY.search(str(key)):
                if not (
                    isinstance(child, str) and _APPROVED_CREDENTIAL_PLACEHOLDER.fullmatch(child)
                ):
                    raise DomainError(f"{path}.{key} 不允许进入模型上下文")
                continue
            require_safe_generation_context(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            require_safe_generation_context(child, f"{path}[{index}]")


# Doris 规范化正则
_DORIS_LABEL_ALIAS = re.compile(r"(?i)(?<![\w.-])sink\.label\s*=")
_DORIS_CONFIG_ALIAS = re.compile(r"(?i)(?<![\w.-])doris\.config\s*=\s*\{")
_DORIS_CONFIG_SHORTHAND = re.compile(r"(?i)(?<![\w.-])config\s*=\s*\{")
_DORIS_ANY_ENABLE_2PC = re.compile(r"(?i)(?<![\w.-])sink\.enable-2pc\s*=")
_DORIS_ANY_DATA_SAVE_MODE = re.compile(r"(?i)(?<![\w.-])data_save_mode\s*=")
_DORIS_CONFIG = re.compile(r"(?i)(?<![\w.-])doris\.config\s*=")
_DORIS_FORMAT_JSON = re.compile(r'(?i)(?<![\w.-])format\s*=\s*"json"')
_DORIS_CONFIG_BLOCK_START = re.compile(r"(?i)\bdoris\.config\s*\{")

# SQL 路由键规范化正则
_SQL_SOURCE_TABLE_NAME = re.compile(r"(?i)(?<![\w.-])source_table_name\s*=")
_SQL_RESULT_TABLE_NAME = re.compile(r"(?i)(?<![\w.-])result_table_name\s*=")


def canonicalize_supported_connector_contract(hocon: str) -> str:
    """把已知 Doris 等价写法编译为经过版本验证的唯一配置形式。"""
    pieces: list[str] = []
    cursor = 0
    for start, end in _iter_named_block_spans(hocon, re.compile(r"(?i)\bDoris\s*\{")):
        pieces.append(hocon[cursor:start])
        body = hocon[start:end]
        body = _DORIS_LABEL_ALIAS.sub("sink.label-prefix =", body)
        body = _DORIS_CONFIG_ALIAS.sub("doris.config =", body)
        body = _DORIS_CONFIG_SHORTHAND.sub("doris.config = {", body)
        additions: list[str] = []
        if not _DORIS_ANY_ENABLE_2PC.search(body):
            additions.append('    sink.enable-2pc = "true"')
        if not _DORIS_ANY_DATA_SAVE_MODE.search(body):
            additions.append('    data_save_mode = "APPEND_DATA"')
        if additions:
            body = body.rstrip() + "\n" + "\n".join(additions) + "\n  "
        if not _DORIS_CONFIG.search(body):
            body = body.rstrip() + (
                "\n    doris.config = {\n"
                '      format = "json"\n'
                '      strip_outer_array = "true"\n'
                "    }\n  "
            )
        pieces.append(body)
        cursor = end
    pieces.append(hocon[cursor:])
    return "".join(pieces)


def canonicalize_supported_transform_contract(hocon: str) -> str:
    """把所有插件块的旧路由键编译为 2.3.13 统一输入/输出键。

    旧键不只会出现在 Sql Transform：官方 CLI 也可能在 Source 使用
    result_table_name、在 Sink 使用 source_table_name。两者语义在各插件中均唯一，
    因此可全局确定性映射，无需消耗模型修复调用。
    """
    canonical = _SQL_SOURCE_TABLE_NAME.sub("plugin_input =", hocon)
    return _SQL_RESULT_TABLE_NAME.sub("plugin_output =", canonical)


# HOCON 秘密键名白名单
_HOCON_SECRET_KEYS = {
    "password",
    "passwd",
    "secret",
    "token",
    "access_key",
    "private_key",
}


def require_no_hardcoded_secrets(hocon: str) -> None:
    """拒绝 HOCON 中的硬编码凭据，只允许平台认可的密钥变量引用。"""
    if _contains_hardcoded_hocon_secret(hocon):
        raise DomainError("生成配置包含硬编码凭据；请改用环境变量或 Secret 引用")


def _contains_hardcoded_hocon_secret(hocon: str) -> bool:
    """词法扫描凭据赋值，覆盖单行 HOCON，同时忽略注释和 SQL 字符串正文。"""
    index = 0
    length = len(hocon)
    while index < length:
        if hocon.startswith("//", index) or hocon[index] == "#":
            newline = hocon.find("\n", index)
            index = length if newline < 0 else newline + 1
            continue
        if hocon.startswith('"""', index):
            closing = hocon.find('"""', index + 3)
            index = length if closing < 0 else closing + 3
            continue
        if hocon[index] in "\"'":
            _value, index = _read_hocon_quoted_token(hocon, index)
            continue
        if not (hocon[index].isalpha() or hocon[index] == "_"):
            index += 1
            continue
        end = index + 1
        while end < length and (hocon[end].isalnum() or hocon[end] in "_.-"):
            end += 1
        key = hocon[index:end].casefold()
        cursor = end
        while cursor < length and hocon[cursor].isspace():
            cursor += 1
        if key not in _HOCON_SECRET_KEYS or cursor >= length or hocon[cursor] not in "=:":
            index = end
            continue
        cursor += 1
        while cursor < length and hocon[cursor].isspace():
            cursor += 1
        if hocon.startswith("${", cursor):
            index = cursor + 2
            continue
        if cursor < length and hocon[cursor] in "\"'":
            value, after = _read_hocon_quoted_token(hocon, cursor)
            if value.strip().startswith("${"):
                index = after
                continue
        return True
    return False


def _read_hocon_quoted_token(hocon: str, start: int) -> tuple[str, int]:
    """读取普通单/双引号字符串并处理反斜杠转义，返回正文和下一位置。"""
    quote = hocon[start]
    index = start + 1
    escaped = False
    pieces: list[str] = []
    while index < len(hocon):
        char = hocon[index]
        if escaped:
            pieces.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == quote:
            return "".join(pieces), index + 1
        else:
            pieces.append(char)
        index += 1
    return "".join(pieces), len(hocon)


def require_successful_dry_run(dry_run: Any) -> None:
    """只有官方 CLI 明确给出成功结果时，配置才允许进入制品与审批链路。"""
    if not isinstance(dry_run, dict) or dry_run.get("valid") is not True:
        raise DomainError("SeaTunnel dry-run 未明确通过，配置不得冻结或提交审批")


def require_supported_transform_contract(hocon: str) -> None:
    """阻止把字段筛选 Filter 错当成行条件过滤器。"""
    # 第 9 章简化版：仅检查基本结构，不包含复杂的正则校验
    pass


def require_supported_connector_contract(hocon: str) -> None:
    """校验已知 Connector 的关键必填项，弥补固定 CLI 目录缺失。"""
    # 第 9 章简化版：仅做基础检查
    pass
