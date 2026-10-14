import re
from typing import Any

from etl_agent.domain.errors import DomainError

_SECRET_KEY = re.compile(r"(?i)(password|passwd|secret|token|access[_-]?key|private[_-]?key)")
# 平台认可的占位符形如 ${CONNECTION_<32位十六进制>_PASSWORD}，其中不含真实凭据。
_APPROVED_CREDENTIAL_PLACEHOLDER = re.compile(
    r"^\$\{CONNECTION_[0-9A-F]{32}_(?:USERNAME|PASSWORD|[A-Z][A-Z0-9_]*)\}$"
)


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
