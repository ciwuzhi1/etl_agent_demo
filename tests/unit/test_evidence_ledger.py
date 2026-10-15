"""第 11 章单元测试：证据账本的哈希链、篡改检测与 Ed25519 签名检查点。

覆盖：
- append 在流内递增序号、串联 previous_hash，首事件以 64 个 0 作为创世前序；
- verify 从创世重算整条链，未篡改时通过；
- 篡改载荷、篡改前序哈希、篡改流头都会让 verify 失败；
- 注入签名器时每个事件落 Ed25519 分离签名，缺少公钥验证器或签名被改都失败；
- 秘密键与疑似秘密值不进入证据层（账本不能变成数据副本）；
- create_checkpoint 对链哈希做签名，供审计离线复核。
"""

import base64
import secrets
import uuid

import pytest
from nacl.signing import VerifyKey
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.evidence_ledger import EvidenceLedgerService
from etl_agent.domain.harness import DetachedSignature, canonical_sha256
from etl_agent.infrastructure.harness_signing import Ed25519Signer
from etl_agent.infrastructure.models import HarnessEvidenceEvent, HarnessEvidenceStream

_ZERO_HASH = "0" * 64
_STREAM_TYPE = "execution-preparation"
_PROJECT_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
_KEY_ID = "test-key"


def _signer() -> Ed25519Signer:
    """生成带稳定 key_id 的测试签名密钥，使签名里的 key_id 可被断言。"""
    return Ed25519Signer(secrets.token_bytes(32), _KEY_ID)


class _RingVerifier:
    """只用公钥验证分离签名的测试适配器，对应 EvidenceVerifier 端口。"""

    def __init__(self, public_key: bytes) -> None:
        """绑定 Ed25519 公钥。"""
        self._verify_key = VerifyKey(public_key)

    def verify_detached(self, payload: bytes, signature: DetachedSignature) -> bool:
        """验签失败返回 False，不向上抛异常以便 verify() 统一汇总错误。"""
        try:
            self._verify_key.verify(payload, _base64url_decode(signature.value))
        except Exception:
            return False
        return True


def _base64url_decode(value: str) -> bytes:
    """解码无填充 Base64URL 签名串。"""
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


async def _append_three(
    service: EvidenceLedgerService,
    session: AsyncSession,
    *,
    stream_key: str,
) -> list[HarnessEvidenceEvent]:
    """向同一个流追加三件事件并返回按序排列的 ORM 行。"""
    for index in range(1, 4):
        await service.append(
            session,
            project_id=_PROJECT_ID,
            stream_type=_STREAM_TYPE,
            stream_key=stream_key,
            payload={"index": index},
            event_type=f"EVENT_{index}",
            risk_level="LOW",
        )
    await session.flush()
    stream = await _load_stream(session, stream_key)
    events = list(
        (
            await session.scalars(
                select(HarnessEvidenceEvent)
                .where(HarnessEvidenceEvent.stream_id == stream.id)
                .order_by(HarnessEvidenceEvent.sequence)
            )
        ).all()
    )
    return events


async def _load_stream(session: AsyncSession, stream_key: str) -> HarnessEvidenceStream:
    """按业务键读取证据流头。"""
    stream = await session.scalar(
        select(HarnessEvidenceStream).where(
            HarnessEvidenceStream.project_id == _PROJECT_ID,
            HarnessEvidenceStream.stream_type == _STREAM_TYPE,
            HarnessEvidenceStream.stream_key == stream_key,
        )
    )
    assert stream is not None
    return stream


@pytest.mark.asyncio
async def test_append_chains_sequence_and_previous_hash(session: AsyncSession) -> None:
    """序号从 1 递增，前序哈希逐条串联，首事件以创世哈希开头。"""
    service = EvidenceLedgerService()
    events = await _append_three(service, session, stream_key="prep-chain")

    assert [event.sequence for event in events] == [1, 2, 3]
    assert events[0].previous_hash == _ZERO_HASH
    assert events[1].previous_hash == events[0].event_hash
    assert events[2].previous_hash == events[1].event_hash
    assert len({event.event_hash for event in events}) == 3
    assert all(event.payload_sha256 == canonical_sha256(event.payload) for event in events)

    stream = await _load_stream(session, "prep-chain")
    assert stream.head_hash == events[-1].event_hash
    assert stream.next_sequence == 4


@pytest.mark.asyncio
async def test_verify_passes_for_untampered_stream(session: AsyncSession) -> None:
    """未篡改的流自洽：序号、前序哈希、事件哈希、流头全部一致。"""
    service = EvidenceLedgerService()
    await _append_three(service, session, stream_key="prep-ok")
    assert await service.verify(session, _PROJECT_ID, _STREAM_TYPE, "prep-ok") is True


@pytest.mark.asyncio
async def test_verify_detects_payload_tampering(session: AsyncSession) -> None:
    """改写事件载荷后重算哈希对不上，verify 必须返回 False。"""
    service = EvidenceLedgerService()
    events = await _append_three(service, session, stream_key="prep-payload")
    events[1].payload = {"index": 999}
    await session.flush()
    assert await service.verify(session, _PROJECT_ID, _STREAM_TYPE, "prep-payload") is False


@pytest.mark.asyncio
async def test_verify_detects_broken_chain_and_stream_head(session: AsyncSession) -> None:
    """篡改前序哈希或流头指针都会破坏链条自洽。"""
    service = EvidenceLedgerService()
    events = await _append_three(service, session, stream_key="prep-broken")
    events[2].previous_hash = "a" * 64
    await session.flush()
    assert await service.verify(session, _PROJECT_ID, _STREAM_TYPE, "prep-broken") is False

    events[2].previous_hash = events[1].event_hash
    stream = await _load_stream(session, "prep-broken")
    stream.head_hash = "b" * 64
    await session.flush()
    assert await service.verify(session, _PROJECT_ID, _STREAM_TYPE, "prep-broken") is False


@pytest.mark.asyncio
async def test_signed_append_persists_signature_and_verifies(session: AsyncSession) -> None:
    """注入签名器后每个事件落 Ed25519 签名，持有公钥即可验证整条链。"""
    signer = _signer()
    service = EvidenceLedgerService(signer=signer, key_id=_KEY_ID)
    events = await _append_three(service, session, stream_key="prep-signed")

    assert all(event.signature_algorithm == "Ed25519" for event in events)
    assert all(event.signature_key_id == _KEY_ID for event in events)
    assert all(event.signature_value for event in events)

    verifier = _RingVerifier(signer.get_public_key())
    assert await service.verify(session, _PROJECT_ID, _STREAM_TYPE, "prep-signed", verifier) is True
    # 签名覆盖事件哈希本身：签名内容与哈希一致但不等于哈希。
    assert all(event.signature_value != event.event_hash for event in events)


@pytest.mark.asyncio
async def test_signed_events_require_verifier_and_reject_tampered_signature(
    session: AsyncSession,
) -> None:
    """有签名却不提供验证器视为不可验证；签名被改后验签失败。"""
    signer = _signer()
    service = EvidenceLedgerService(signer=signer, key_id=_KEY_ID)
    events = await _append_three(service, session, stream_key="prep-sig-tamper")

    assert await service.verify(session, _PROJECT_ID, _STREAM_TYPE, "prep-sig-tamper") is False

    verifier = _RingVerifier(signer.get_public_key())
    assert (
        await service.verify(session, _PROJECT_ID, _STREAM_TYPE, "prep-sig-tamper", verifier)
        is True
    )

    # 把某个事件的签名替换成另一把密钥产生的合法签名，验签必须失败。
    other = Ed25519Signer.generate()
    forged = other.sign_detached(events[0].event_hash.encode("ascii"))
    events[0].signature_value = forged.value
    await session.flush()
    assert (
        await service.verify(session, _PROJECT_ID, _STREAM_TYPE, "prep-sig-tamper", verifier)
        is False
    )


@pytest.mark.asyncio
async def test_forbidden_and_secret_like_values_stay_out_of_evidence(
    session: AsyncSession,
) -> None:
    """秘密键与疑似秘密值被剥离，账本不保存凭据或原始正文。"""
    service = EvidenceLedgerService()
    await service.append(
        session,
        project_id=_PROJECT_ID,
        stream_type=_STREAM_TYPE,
        stream_key="prep-sanitize",
        payload={
            "password": "leak",
            "token": "leak",
            "raw_rows": [{"a": 1}],
            "note": "bearer abcdefghijklmnop",
            "keep": "visible",
        },
    )
    await session.flush()
    events = await _get_events(session, "prep-sanitize")
    assert events[-1].payload == {"keep": "visible"}


@pytest.mark.asyncio
async def test_checkpoint_signs_the_chain_hash(session: AsyncSession) -> None:
    """检查点对链哈希做分离签名，验证方可用公钥离线复核。"""
    signer = _signer()
    service = EvidenceLedgerService(signer=signer, key_id=_KEY_ID)
    events = await _append_three(service, session, stream_key="prep-checkpoint")
    stream = await _load_stream(session, "prep-checkpoint")

    checkpoint = await service.create_checkpoint(session, str(stream.id), 3)
    assert checkpoint.sequence == 3
    assert checkpoint.signature is not None
    assert checkpoint.signature.algorithm == "Ed25519"
    assert checkpoint.signature.key_id == _KEY_ID

    verifier = _RingVerifier(signer.get_public_key())
    payload = checkpoint.checkpoint_hash.encode("ascii")
    assert verifier.verify_detached(payload, checkpoint.signature) is True
    # 换一个事件集合后链哈希必然变化，旧签名不再成立。
    tampered = "".join(event.event_hash for event in events[:-1])
    assert canonical_sha256(tampered) != checkpoint.checkpoint_hash


async def _get_events(session: AsyncSession, stream_key: str) -> list[HarnessEvidenceEvent]:
    """按业务键取回流内全部事件（按序号）。"""
    stream = await _load_stream(session, stream_key)
    return list(
        (
            await session.scalars(
                select(HarnessEvidenceEvent)
                .where(HarnessEvidenceEvent.stream_id == stream.id)
                .order_by(HarnessEvidenceEvent.sequence)
            )
        ).all()
    )
