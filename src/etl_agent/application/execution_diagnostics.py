"""执行诊断：将错误信息归类为可重试/不可重试的确定性诊断。

本章课件要求实现两个核心函数：
- diagnose_execution_failure: 根据错误文本匹配失败类别与阶段，判断是否可重试
- redact_execution_error: 清除敏感信息（密码、令牌、API Key）后再持久化

注意：本文件使用 StrEnum + dataclass 构建轻量诊断结构，不依赖 Pydantic，
以便在 Worker 侧快速调用而不引入重型依赖。
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum


class FailureCategory(StrEnum):
    """失败归因：为什么失败。"""

    CONNECTION_TIMEOUT = "CONNECTION_TIMEOUT"
    AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    UNKNOWN = "UNKNOWN"


class FailureStage(StrEnum):
    """失败位置：链路哪一段出的事。"""

    SUBMISSION = "SUBMISSION"
    EXECUTION = "EXECUTION"
    PUBLISH = "PUBLISH"
    QUALITY_CHECK = "QUALITY_CHECK"


@dataclass(frozen=True)
class ExecutionDiagnosis:
    """一次执行失败的确定性诊断结果。"""

    category: FailureCategory
    stage: FailureStage
    retryable: bool
    message: str


# 常见敏感信息模式：password=xxx, token: xxx, api_key="xxx", sk-xxx, Bearer xxx
_SECRET_PATTERNS = (
    # key=value 或 key: value 形式的凭据
    re.compile(
        r"""(?i)(["']?(?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential)["']?\s*[=:]\s*)"""
        r"""("[^"]*"|'[^']*'|[^\s&,;}]+)"""
    ),
    # OpenAI-style keys: sk-xxxxx
    re.compile(r"(?i)sk-[a-z0-9_-]{8,}"),
    # URL 中的用户名:密码@host
    re.compile(r"(?i)(://[^\s/:@]+:)([^\s@/]+)(@)"),
    # Authorization header
    re.compile(
        r"""(?i)(["']?authorization["']?\s*[=:]\s*)"""
        r"""("(?:bearer|basic)\s+[^"]+"|'(?:bearer|basic)\s+[^']+'|"""
        r"""(?:bearer|basic)\s+[^\s&,;}]+|[^\s&,;}]+)"""
    ),
    # Bearer/Basic token standalone
    re.compile(r"(?i)\b(?:bearer|basic)\s+[a-z0-9._~+/=-]{8,}"),
)


def redact_execution_error(error_message: str) -> str:
    """清除错误消息中的口令、令牌和连接串片段。

    Args:
        error_message: 原始错误消息，可能包含敏感信息。

    Returns:
        脱敏后的错误消息，敏感值被替换为 "***REDACTED***"。
    """
    safe = error_message
    safe = _SECRET_PATTERNS[0].sub(r"\1***REDACTED***", safe)
    safe = _SECRET_PATTERNS[1].sub("***REDACTED***", safe)
    safe = _SECRET_PATTERNS[2].sub(r"\1***REDACTED***\3", safe)
    safe = _SECRET_PATTERNS[3].sub(r"\1***REDACTED***", safe)
    safe = _SECRET_PATTERNS[4].sub("***REDACTED***", safe)
    return safe


def diagnose_execution_failure(error_message: str) -> ExecutionDiagnosis:
    """根据错误文本匹配失败类别、阶段及可重试性。

    Args:
        error_message: 原始或已脱敏的错误消息。

    Returns:
        ExecutionDiagnosis 包含 category/stage/retryable/message。
    """
    safe_error = redact_execution_error(error_message)
    lowered = safe_error.lower()

    # 规则列表：按优先级从高到低排列
    rules: list[tuple[Callable[[str], bool], ExecutionDiagnosis]] = [
        # 连接超时 / 网络问题
        (
            lambda text: any(
                marker in text
                for marker in (
                    "connection refused",
                    "connect timed out",
                    "connection timeout",
                    "timed out",
                )
            ),
            ExecutionDiagnosis(
                category=FailureCategory.CONNECTION_TIMEOUT,
                stage=FailureStage.SUBMISSION,
                retryable=True,
                message="执行节点无法连接数据端或服务，建议检查网络后重试。",
            ),
        ),
        # 认证失败
        (
            lambda text: any(
                marker in text
                for marker in (
                    "access denied",
                    "authentication",
                    "permission denied",
                    "unauthorized",
                    "401",
                )
            ),
            ExecutionDiagnosis(
                category=FailureCategory.AUTHENTICATION_FAILED,
                stage=FailureStage.SUBMISSION,
                retryable=False,
                message="数据源或目标端拒绝了当前凭据，需更新凭据后重跑。",
            ),
        ),
        # Schema 不匹配
        (
            lambda text: any(
                marker in text
                for marker in (
                    "doesn't exist",
                    "does not exist",
                    "unknown table",
                    "missing table",
                    "column not found",
                    "schema mismatch",
                )
            ),
            ExecutionDiagnosis(
                category=FailureCategory.SCHEMA_MISMATCH,
                stage=FailureStage.EXECUTION,
                retryable=False,
                message="源表或目标表 Schema 不一致，需修正配置后创建新版本。",
            ),
        ),
        # 资源耗尽
        (
            lambda text: any(
                marker in text
                for marker in (
                    "out of memory",
                    "oom",
                    "resource exhausted",
                    "quota exceeded",
                    "too many connections",
                )
            ),
            ExecutionDiagnosis(
                category=FailureCategory.RESOURCE_EXHAUSTED,
                stage=FailureStage.EXECUTION,
                retryable=True,
                message="执行资源不足，建议扩容或错峰后重试。",
            ),
        ),
    ]

    for predicate, diagnosis in rules:
        if predicate(lowered):
            return diagnosis

    # 未命中任何规则，返回未知类别
    return ExecutionDiagnosis(
        category=FailureCategory.UNKNOWN,
        stage=FailureStage.EXECUTION,
        retryable=False,
        message="当前错误尚未命中确定性诊断规则，需结合引擎日志人工排查。",
    )
