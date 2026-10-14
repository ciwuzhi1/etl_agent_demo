import time
import uuid

import pytest
from fastapi import HTTPException, Request
from pydantic import ValidationError
from redis.exceptions import RedisError

from etl_agent.api.dependencies import GatewayNonceStore, actor_id, sign_gateway_identity
from etl_agent.config import Settings


class _MemoryNonceStore(GatewayNonceStore):
    """在单元测试中模拟 Redis SET NX 的原子防重放语义。"""

    def __init__(self) -> None:
        """创建空的已消费 Nonce 集合。"""
        self._values: set[str] = set()

    async def consume(self, nonce: str, *, ttl_seconds: int) -> bool:
        """首次消费成功，后续相同 Nonce 拒绝；TTL 只验证调用契约。"""
        assert ttl_seconds >= 15
        if nonce in self._values:
            return False
        self._values.add(nonce)
        return True


class _UnavailableNonceStore(GatewayNonceStore):
    """模拟生产防重放存储故障，验证认证边界失败关闭。"""

    async def consume(self, nonce: str, *, ttl_seconds: int) -> bool:
        """始终抛出 Redis 连接异常，不允许退化为可重放的纯 HMAC。"""
        raise RedisError(f"nonce store unavailable (ttl={ttl_seconds}, nonce_length={len(nonce)})")


def _request(path: str = "/api/v1/projects?limit=20") -> Request:
    """构造保留原始路径和查询串的最小 ASGI 请求，覆盖网关规范签名输入。"""
    raw_path, _, query = path.partition("?")
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": raw_path,
            "raw_path": raw_path.encode("ascii"),
            "query_string": query.encode("ascii"),
            "headers": [],
        }
    )


def _trusted_settings(**overrides: object) -> Settings:
    """创建启用可信网关的测试配置，并允许单项覆盖时钟窗口等边界。"""
    return Settings(
        _env_file=None,
        auth_mode="trusted-gateway",
        gateway_hmac_secret="unit-test-shared-secret-with-32-bytes-minimum",
        **overrides,
    )


@pytest.mark.asyncio
async def test_trusted_gateway_accepts_valid_signed_actor() -> None:
    """可信网关模式只接受覆盖 Actor、时间、Nonce、方法和请求目标的有效签名。"""
    actor = str(uuid.uuid4())
    timestamp = str(int(time.time()))
    nonce = "request_nonce_123456"
    target = "/api/v1/projects?limit=20"
    signature = sign_gateway_identity(
        secret="unit-test-shared-secret-with-32-bytes-minimum",
        actor=actor,
        timestamp=timestamp,
        nonce=nonce,
        method="GET",
        path_and_query=target,
    )

    result = await actor_id(
        _request(target),
        actor,
        timestamp,
        nonce,
        signature,
        _trusted_settings(),
        _MemoryNonceStore(),
    )

    assert result == uuid.UUID(actor)


@pytest.mark.asyncio
async def test_trusted_gateway_rejects_tampered_path() -> None:
    """签名后的路径或查询参数被修改时必须失败，不能把身份复用于另一接口。"""
    actor = str(uuid.uuid4())
    timestamp = str(int(time.time()))
    nonce = "request_nonce_123456"
    signature = sign_gateway_identity(
        secret="unit-test-shared-secret-with-32-bytes-minimum",
        actor=actor,
        timestamp=timestamp,
        nonce=nonce,
        method="GET",
        path_and_query="/api/v1/projects?limit=20",
    )

    with pytest.raises(HTTPException) as captured:
        await actor_id(
            _request("/api/v1/projects?limit=200"),
            actor,
            timestamp,
            nonce,
            signature,
            _trusted_settings(),
            _MemoryNonceStore(),
        )

    assert captured.value.status_code == 401


@pytest.mark.asyncio
async def test_trusted_gateway_rejects_expired_signature() -> None:
    """超过允许时钟偏差的签名必须过期，缩短被截获请求的可重放窗口。"""
    actor = str(uuid.uuid4())
    timestamp = str(int(time.time()) - 301)
    nonce = "request_nonce_123456"
    signature = sign_gateway_identity(
        secret="unit-test-shared-secret-with-32-bytes-minimum",
        actor=actor,
        timestamp=timestamp,
        nonce=nonce,
        method="GET",
        path_and_query="/api/v1/projects",
    )

    with pytest.raises(HTTPException) as captured:
        await actor_id(
            _request("/api/v1/projects"),
            actor,
            timestamp,
            nonce,
            signature,
            _trusted_settings(gateway_max_clock_skew_seconds=60),
            _MemoryNonceStore(),
        )

    assert captured.value.status_code == 401


@pytest.mark.asyncio
async def test_trusted_gateway_rejects_replayed_valid_request() -> None:
    """同一份合法签名在时钟窗口内重放时，第二次必须在业务路由前被拒绝。"""
    actor = str(uuid.uuid4())
    timestamp = str(int(time.time()))
    nonce = "single_use_nonce_123456"
    target = "/api/v1/projects?limit=20"
    signature = sign_gateway_identity(
        secret="unit-test-shared-secret-with-32-bytes-minimum",
        actor=actor,
        timestamp=timestamp,
        nonce=nonce,
        method="GET",
        path_and_query=target,
    )
    store = _MemoryNonceStore()

    first = await actor_id(
        _request(target),
        actor,
        timestamp,
        nonce,
        signature,
        _trusted_settings(),
        store,
    )
    with pytest.raises(HTTPException) as captured:
        await actor_id(
            _request(target),
            actor,
            timestamp,
            nonce,
            signature,
            _trusted_settings(),
            store,
        )

    assert first == uuid.UUID(actor)
    assert captured.value.status_code == 401


@pytest.mark.asyncio
async def test_trusted_gateway_fails_closed_when_nonce_store_is_unavailable() -> None:
    """Redis 不可用时返回 503，不得让合法 HMAC 绕过防重放。"""
    actor = str(uuid.uuid4())
    timestamp = str(int(time.time()))
    nonce = "unavailable_store_nonce_123456"
    target = "/api/v1/projects"
    signature = sign_gateway_identity(
        secret="unit-test-shared-secret-with-32-bytes-minimum",
        actor=actor,
        timestamp=timestamp,
        nonce=nonce,
        method="GET",
        path_and_query=target,
    )

    with pytest.raises(HTTPException) as captured:
        await actor_id(
            _request(target),
            actor,
            timestamp,
            nonce,
            signature,
            _trusted_settings(),
            _UnavailableNonceStore(),
        )

    assert captured.value.status_code == 503


def test_trusted_gateway_rejects_weak_hmac_secret() -> None:
    """可复制的联调配置也不能接受低熓 HMAC 共享密钥。"""
    with pytest.raises(ValidationError, match="32"):
        Settings(
            _env_file=None,
            auth_mode="trusted-gateway",
            gateway_hmac_secret="weak-secret",
        )


def test_production_rejects_unsigned_development_identity_mode() -> None:
    """生产配置不得退化为可由浏览器任意填写 Actor UUID 的开发模式。"""
    with pytest.raises(ValidationError, match="trusted-gateway"):
        Settings(_env_file=None, environment="production", auth_mode="dev-header")
