"""执行 Harness 装配工厂（第 11 章）。

把签名私钥、Ed25519 一次性能力签发器与内嵌策略裁决点装配成
``ExecutionHarnessService``，供 API 路由共享同一个签发身份。

私钥解析顺序与 ``side_effect_harness.load_signer`` 保持一致，只是多了一步
“开发密钥缺失时生成并持久化”，让本地多进程（API + Worker）共享同一把密钥：

1. 显式配置 ``harness_private_key``：按十六进制优先、Base64URL 兜底解码为 32 字节；
2. 开发密钥文件 ``harness_dev_key_path``：存在即读取，缺失则生成 32 字节随机种子并落盘；
3. 任何一步拿不到合法 32 字节材料都直接失败关闭（``Ed25519Signer`` 抛错），
   绝不在生产静默退化成进程内临时密钥。
"""

import base64
import secrets
from pathlib import Path

from etl_agent.application.evidence_ledger import EvidenceLedgerService
from etl_agent.application.execution_harness import ExecutionHarnessService
from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint
from etl_agent.config import Settings, get_settings
from etl_agent.infrastructure.harness_signing import CapabilityTokenIssuer, Ed25519Signer

# Ed25519 私钥就是 32 字节随机种子；长度不符的密钥材料必须在装配阶段被拒绝。
_PRIVATE_KEY_BYTES = 32


def build_evidence_ledger() -> EvidenceLedgerService:
    """按配置装配带签名的证据账本，供所有 Harness 链路复用同一签发身份。

    私钥解析规则与能力令牌签发完全一致（见 ``_resolve_signer``），因此校验方
    只要持有 ``harness_public_keys`` 里那一份公钥，就能同时验能力令牌与验证据事件。
    任何一步拿不到合法 32 字节材料都会失败关闭，不会退化成"证据不带签名"的弱模式。
    """
    settings = get_settings()
    return EvidenceLedgerService(
        signer=_resolve_signer(settings),
        key_id=settings.harness_signing_key_id,
    )


def build_execution_harness() -> ExecutionHarnessService:
    """按配置装配执行 Harness：内嵌 PDP + 一次性能力签发器 + 签名证据账本。

    Returns:
        可直接注入路由的 ``ExecutionHarnessService``；每次调用重新读取配置，
        密钥材料的复用由上层的 ``lru_cache`` 或调用方负责。
    """
    settings = get_settings()
    signer = _resolve_signer(settings)
    return ExecutionHarnessService(
        pdp=EmbeddedPolicyDecisionPoint(),
        token_issuer=CapabilityTokenIssuer(signer, settings.harness_signing_key_id),
        # 证据账本共用同一把 Ed25519 身份：能力令牌与证据事件由同一签发方背书，
        # 校验方只需一份公钥即可同时验令牌和验证据。
        ledger=EvidenceLedgerService(signer=signer, key_id=settings.harness_signing_key_id),
    )


def _resolve_signer(settings: Settings) -> Ed25519Signer:
    """解析签名私钥：显式配置优先，否则使用（必要时新建）开发密钥文件。"""
    if settings.harness_private_key is not None:
        material = _decode_configured_key(settings.harness_private_key.get_secret_value())
        return Ed25519Signer(material, settings.harness_signing_key_id)
    material = _load_or_create_dev_key(settings.harness_dev_key_path)
    return Ed25519Signer(material, settings.harness_signing_key_id)


def _decode_configured_key(value: str) -> bytes:
    """解码显式配置的私钥材料：十六进制优先，Base64URL 兜底。

    与 ``side_effect_harness.load_signer`` 保持同一顺序，避免同一份配置在不同
    入口下被解析成不同字节；Base64URL 允许省略填充。
    """
    try:
        return bytes.fromhex(value.strip())
    except ValueError:
        padded = value.strip() + "=" * (-len(value.strip()) % 4)
        return base64.urlsafe_b64decode(padded.encode("ascii"))


def _load_or_create_dev_key(path: Path) -> bytes:
    """读取开发密钥文件；缺失时生成 32 字节私钥种子并持久化。

    路径沿用配置的原样值（相对路径按进程工作目录解析，即 ``uv run`` 下的项目根），
    与 ``load_signer`` 对 ``dev_key_path`` 的处理保持一致。
    """
    if path.is_file():
        return path.read_bytes()
    material = secrets.token_bytes(_PRIVATE_KEY_BYTES)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(material)
    return material
