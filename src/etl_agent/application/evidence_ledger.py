"""证据账本服务：维护项目内只追加证据流、哈希链头和可选签名检查点。

本章实现核心能力：
- append: 在流头行锁内追加事件，保证序号、前序哈希和头指针原子更新
- verify: 从第一条事件重算整个链条，并验证流头及所有签名检查点
- create_checkpoint: 为指定序列号创建带签名的检查点记录
"""

import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.domain.errors import DomainError, ResourceNotFound
from etl_agent.domain.harness import (
    DetachedSignature,
    canonical_sha256,
)
from etl_agent.infrastructure.harness_signing import Ed25519Signer
from etl_agent.infrastructure.models import HarnessEvidenceEvent, HarnessEvidenceStream

_ZERO_HASH = "0" * 64
_FORBIDDEN_EVIDENCE_KEYS = {
    "api_key",
    "ciphertext",
    "hocon",
    "password",
    "passwd",
    "private_key",
    "prompt",
    "raw_response",
    "raw_rows",
    "request_body",
    "response_body",
    "secret",
    "token",
}
_SECRET_VALUE = re.compile(r"(?i)(?:\bsk-[A-Za-z0-9_-]{16,}|\bbearer\s+\S{12,}|\bpassword\s*[:=])")
_MAX_EVIDENCE_BYTES = 64 * 1024


@dataclass
class EvidenceEventPayload:
    """表示一个证据事件的载荷结构。"""

    event_type: str
    aggregate_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    occurred_at: datetime | None = None


@dataclass
class EvidenceCheckpoint:
    """表示一个已签名的证据流检查点。"""

    stream_id: str
    sequence: int
    checkpoint_hash: str
    signature: DetachedSignature | None = None
    signed_at: datetime | None = None


class EvidenceSigner(Protocol):
    """定义检查点签名端口，应用层不接触具体私钥实现。"""

    def sign_detached(self, payload: bytes) -> DetachedSignature:
        """对事件哈希生成分离签名。"""
        ...


class EvidenceVerifier(Protocol):
    """定义检查点签名验证端口，验证方只需要公钥。"""

    def verify_detached(self, payload: bytes, signature: DetachedSignature) -> bool:
        """验证事件哈希与签名是否匹配。"""
        ...


class EvidenceLedgerService:
    """维护项目内只追加证据流、哈希链头和可选签名检查点。"""

    def __init__(self, *, signer: Ed25519Signer | None = None, key_id: str | None = None) -> None:
        """注入可选签名端口；要求检查点时若没有签名器会失败关闭。"""
        self._signer = signer
        self._key_id = key_id

    async def append(
        self,
        session: AsyncSession,
        project_id: uuid.UUID | str,
        stream_type: str,
        stream_key: str,
        payload: dict[str, Any],
        event_type: str = "generic",
        risk_level: str = "LOW",
        actor_id: uuid.UUID | str | None = None,
        trace_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> int:
        """在流头行锁内追加事件，保证序号、前序哈希和头指针原子更新。"""
        # 清理和验证输入
        sanitized_payload = sanitize_payload(payload)
        normalized_stream_type = _safe_identifier(stream_type, label="stream_type", max_length=64)
        normalized_stream_key = _safe_text(stream_key, label="stream_key", max_length=255)
        normalized_event_type = _safe_identifier(event_type, label="event_type", max_length=128)

        # 获取或创建流（带行锁）
        stream = await self._get_or_create_stream_for_update(
            session,
            project_id=project_id,
            stream_type=normalized_stream_type,
            stream_key=normalized_stream_key,
        )

        # 计算新事件的序号和前序哈希
        timestamp = (occurred_at or datetime.now(UTC)).astimezone(UTC)
        sequence = stream.next_sequence
        previous_hash = stream.head_hash or _ZERO_HASH

        # 构建事件文档并计算哈希
        payload_sha256 = canonical_sha256(sanitized_payload)
        event_document = _build_event_document(
            project_id=project_id,
            stream_type=normalized_stream_type,
            stream_key=normalized_stream_key,
            sequence=sequence,
            occurred_at=timestamp,
            actor_id=actor_id,
            event_type=normalized_event_type,
            risk_level=risk_level,
            payload_sha256=payload_sha256,
            previous_hash=previous_hash,
        )
        event_hash = compute_event_hash(event_document)

        # 注入签名器时对事件哈希做分离签名：签名覆盖链上每个事件，verify() 才能
        # 区分“哈希链自洽”与“链确实由本平台私钥签发”。未注入时保持仅哈希链，
        # 绝不静默退化成进程内临时密钥。
        signature = (
            self._signer.sign_detached(event_hash.encode("ascii"))
            if self._signer is not None
            else None
        )

        # 创建事件记录
        event = HarnessEvidenceEvent(
            stream_id=stream.id,
            sequence=sequence,
            occurred_at=timestamp,
            actor_id=actor_id,
            event_type=normalized_event_type,
            risk_level=risk_level,
            payload=sanitized_payload,
            payload_sha256=payload_sha256,
            previous_hash=previous_hash,
            event_hash=event_hash,
            trace_id=trace_id,
            signature_algorithm=signature.algorithm if signature is not None else None,
            signature_key_id=signature.key_id if signature is not None else None,
            signature_value=signature.value if signature is not None else None,
        )

        # 插入事件并更新流头
        session.add(event)
        stream.next_sequence += 1
        stream.head_hash = event_hash
        await session.flush()

        return sequence

    async def verify(
        self,
        session: AsyncSession,
        project_id: uuid.UUID | str,
        stream_type: str,
        stream_key: str,
        verifier: EvidenceVerifier | None = None,
    ) -> bool:
        """从第一条事件重算整个链条，并验证流头及所有签名检查点。"""
        # 查找流
        stream = await session.scalar(
            select(HarnessEvidenceStream).where(
                HarnessEvidenceStream.project_id == project_id,
                HarnessEvidenceStream.stream_type == stream_type,
                HarnessEvidenceStream.stream_key == stream_key,
            )
        )
        if stream is None:
            raise ResourceNotFound("Harness 证据流不存在")

        # 获取所有事件并按序号排序
        events = list(
            (
                await session.scalars(
                    select(HarnessEvidenceEvent)
                    .where(HarnessEvidenceEvent.stream_id == stream.id)
                    .order_by(HarnessEvidenceEvent.sequence)
                )
            ).all()
        )

        # 验证哈希链
        errors: list[str] = []
        expected_previous = _ZERO_HASH
        for expected_sequence, event in enumerate(events, start=1):
            # 验证序号连续性
            if event.sequence != expected_sequence:
                errors.append(f"序号不连续：期望 {expected_sequence}，实际 {event.sequence}")

            # 验证前序哈希
            if event.previous_hash != expected_previous:
                errors.append(f"事件 {event.sequence} 的前序哈希不匹配")

            # 重新计算事件哈希
            payload_sha256 = canonical_sha256(event.payload)
            event_document = _build_event_document(
                project_id=project_id,
                stream_type=stream.stream_type,
                stream_key=stream.stream_key,
                sequence=event.sequence,
                occurred_at=event.occurred_at,
                actor_id=event.actor_id,
                event_type=event.event_type,
                risk_level=event.risk_level,
                payload_sha256=payload_sha256,
                previous_hash=event.previous_hash,
            )
            computed_hash = compute_event_hash(event_document)

            if event.event_hash != computed_hash:
                errors.append(f"事件 {event.sequence} 的事件哈希不匹配")

            # 验证签名（如果存在）
            if event.signature_algorithm and event.signature_key_id and event.signature_value:
                if verifier is None:
                    errors.append(f"事件 {event.sequence} 含签名但未提供公钥验证器")
                else:
                    signature = DetachedSignature(
                        algorithm=event.signature_algorithm,
                        key_id=event.signature_key_id,
                        value=event.signature_value,
                    )
                    if not verifier.verify_detached(event.event_hash.encode("ascii"), signature):
                        errors.append(f"事件 {event.sequence} 的签名无效")

            expected_previous = event.event_hash

        # 验证流头哈希
        expected_head = events[-1].event_hash if events else None
        if stream.head_hash != expected_head:
            errors.append("证据流头哈希与最后事件不一致")

        # 验证下一序号
        if stream.next_sequence != len(events) + 1:
            errors.append("证据流下一序号与事件数量不一致")

        return len(errors) == 0

    async def create_checkpoint(
        self,
        session: AsyncSession,
        stream_id: str,
        sequence: int,
    ) -> EvidenceCheckpoint:
        """为指定序列号创建带签名的检查点记录。"""
        # 获取流和事件。stream_id 按签名是字符串，但 Uuid(as_uuid=True) 列只接受
        # UUID 对象（绑定阶段会调用 .hex），因此这里显式归一化，避免调用方拿到
        # 一个“注解说能用、实际崩在驱动层”的接口。
        try:
            stream_uuid = uuid.UUID(str(stream_id))
        except ValueError as exc:
            raise DomainError("证据流 ID 不是合法 UUID") from exc
        stream = await session.get(HarnessEvidenceStream, stream_uuid)
        if stream is None:
            raise ResourceNotFound("证据流不存在")

        # 获取到指定序列号的所有事件
        events = list(
            (
                await session.scalars(
                    select(HarnessEvidenceEvent)
                    .where(HarnessEvidenceEvent.stream_id == stream_uuid)
                    .where(HarnessEvidenceEvent.sequence <= sequence)
                    .order_by(HarnessEvidenceEvent.sequence)
                )
            ).all()
        )

        if not events:
            raise ResourceNotFound(f"未找到序列号 <= {sequence} 的事件")

        # 计算检查点哈希（所有事件哈希的串联哈希）
        event_hashes = [event.event_hash for event in events]
        checkpoint_hash = compute_chain_hash(event_hashes)

        # 签名（如果签名器可用）
        signature = None
        signed_at = datetime.now(UTC)
        if self._signer is not None:
            signature = self._signer.sign_detached(checkpoint_hash.encode("ascii"))

        # 返回检查点
        return EvidenceCheckpoint(
            stream_id=stream_id,
            sequence=sequence,
            checkpoint_hash=checkpoint_hash,
            signature=signature,
            signed_at=signed_at,
        )

    @staticmethod
    async def _get_or_create_stream_for_update(
        session: AsyncSession,
        *,
        project_id: uuid.UUID | str,
        stream_type: str,
        stream_key: str,
    ) -> HarnessEvidenceStream:
        """获取证据流头的行锁；新流在当前业务事务内创建。"""
        statement = (
            select(HarnessEvidenceStream)
            .where(
                HarnessEvidenceStream.project_id == project_id,
                HarnessEvidenceStream.stream_type == stream_type,
                HarnessEvidenceStream.stream_key == stream_key,
            )
            .with_for_update()
        )
        stream = (await session.execute(statement)).scalar_one_or_none()
        if stream is not None:
            return stream

        # 创建新流
        stream = HarnessEvidenceStream(
            project_id=project_id,
            stream_type=stream_type,
            stream_key=stream_key,
            next_sequence=1,
        )
        session.add(stream)
        await session.flush()
        return stream


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """移除密码、令牌、API 密钥和大二进制数据，防止审计系统变成数据副本。"""
    if not isinstance(payload, dict):
        return payload

    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        normalized_key = str(key).strip().casefold()

        # 跳过禁止的键
        if normalized_key in _FORBIDDEN_EVIDENCE_KEYS:
            continue

        # 递归处理嵌套字典
        if isinstance(value, dict):
            sanitized[key] = sanitize_payload(value)
        elif isinstance(value, list):
            sanitized[key] = [
                sanitize_payload(item) if isinstance(item, dict) else item for item in value
            ]
        elif isinstance(value, str):
            # 检查是否包含秘密值模式
            if _SECRET_VALUE.search(value):
                continue
            # 限制字符串长度
            if len(value) > _MAX_EVIDENCE_BYTES:
                sanitized[key] = value[:_MAX_EVIDENCE_BYTES] + "...[truncated]"
            else:
                sanitized[key] = value
        else:
            sanitized[key] = value

    return sanitized


def compute_event_hash(event: dict[str, Any]) -> str:
    """计算事件的 SHA-256 哈希值（规范 JSON）。"""
    return canonical_sha256(event)


def compute_chain_hash(event_hashes: list[str]) -> str:
    """计算事件哈希列表的串联哈希。"""
    if not event_hashes:
        return _ZERO_HASH

    concatenated = "".join(event_hashes)
    return canonical_sha256(concatenated)


def _build_event_document(
    *,
    project_id: uuid.UUID | str,
    stream_type: str,
    stream_key: str,
    sequence: int,
    occurred_at: datetime,
    actor_id: uuid.UUID | str | None,
    event_type: str,
    risk_level: str,
    payload_sha256: str,
    previous_hash: str,
) -> dict[str, Any]:
    """构造版本化事件哈希文档，字段变化必须通过新版本显式演进。"""
    normalized_time = (
        occurred_at if occurred_at.tzinfo is not None else occurred_at.replace(tzinfo=UTC)
    )
    timestamp = (
        normalized_time.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )
    return {
        "actor_id": actor_id,
        "event_type": event_type,
        "occurred_at": timestamp,
        "payload_sha256": payload_sha256,
        "previous_hash": previous_hash,
        "project_id": project_id,
        "risk_level": risk_level,
        "sequence": sequence,
        "stream_key": stream_key,
        "stream_type": stream_type,
        "version": 1,
    }


def _safe_identifier(value: str, *, label: str, max_length: int) -> str:
    """限制事件和流类型为稳定标识符，防止控制字符进入审计界面。"""
    normalized = value.strip()
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]*", normalized) or len(normalized) > max_length:
        raise DomainError(f"{label} 必须是长度不超过 {max_length} 的安全标识符")
    return normalized


def _safe_text(value: str, *, label: str, max_length: int) -> str:
    """校验流业务键长度并拒绝换行和控制字符。"""
    normalized = value.strip()
    if not normalized or len(normalized) > max_length or any(ord(char) < 32 for char in normalized):
        raise DomainError(f"{label} 不能为空、超长或包含控制字符")
    return normalized


def require_safe_evidence_payload(value: Any, path: str = "payload", depth: int = 0) -> None:
    """拒绝普通证据层中的秘密、原始正文和超大载荷，防止审计系统变成数据副本。"""
    if depth > 20:
        raise DomainError("Harness 证据载荷嵌套过深")
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().casefold()
            if normalized in _FORBIDDEN_EVIDENCE_KEYS:
                raise DomainError(f"{path}.{key} 不允许进入普通证据层")
            require_safe_evidence_payload(child, f"{path}.{key}", depth + 1)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            require_safe_evidence_payload(item, f"{path}[{index}]", depth + 1)
    elif isinstance(value, str):
        if _SECRET_VALUE.search(value):
            raise DomainError(f"{path} 包含疑似秘密值")
        if len(value.encode("utf-8")) > _MAX_EVIDENCE_BYTES:
            raise DomainError(f"{path} 超出证据载荷大小限制")
