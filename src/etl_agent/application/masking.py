import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

SENSITIVE_COLUMN = re.compile(
    r"(?i)(password|passwd|secret|token|phone|mobile|tel|email|mail|id_?card|identity|"
    r"address|name|real_?name|bank|account|card_?no|密码|口令|密钥|令牌|手机|电话|"
    r"邮箱|邮件|身份证|地址|姓名|银行卡|账号)"
)


def mask_sample(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """逐字段生成脱敏样本，保留结构信息但不让自由文本或凭据进入模型。"""
    return [{column: mask_value(column, value) for column, value in row.items()} for row in rows]


def mask_value(column: str, value: Any) -> Any:
    """按字段语义、值类型和保守默认规则处理单个样本值。"""
    if value is None or isinstance(value, bool):
        return value
    # 字段语义必须先于运行时类型判断；CSV 推断出的纯数字手机号仍然是敏感信息。
    text = str(value)
    lowered = column.lower()
    if any(
        key in lowered
        for key in ("password", "passwd", "secret", "token", "密码", "口令", "密钥", "令牌")
    ):
        return "[REDACTED]"
    if any(key in lowered for key in ("mail", "邮箱", "邮件")) and "@" in text:
        local, domain = text.split("@", 1)
        return f"{local[:1]}***@{domain}"
    if any(key in lowered for key in ("phone", "mobile", "tel", "手机", "电话")) and len(text) >= 7:
        return f"{text[:3]}****{text[-4:]}"
    if SENSITIVE_COLUMN.search(column):
        return _mask_text(text)

    if isinstance(value, (int, float)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return f"[BINARY:{len(value)} bytes]"

    # 未建立字段分级前采用保守默认：数值保留，任意自由文本均做部分遮蔽。
    return _mask_text(text)


def _mask_text(value: str) -> str:
    """在样本脱敏中按保守策略遮蔽敏感信息；作为内部辅助函数，
    统一维护该步骤的输入校验、状态约束和异常语义。
    """
    if not value:
        return value
    if len(value) <= 2:
        return "*" * len(value)
    if len(value) <= 6:
        return f"{value[0]}***{value[-1]}"
    return f"{value[:2]}***{value[-2:]}"
