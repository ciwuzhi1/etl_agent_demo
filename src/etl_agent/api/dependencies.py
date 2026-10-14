import hashlib
import hmac
import re
import time
import uuid
from functools import lru_cache
from typing import Annotated, Protocol

from fastapi import Depends, Header, HTTPException, Request, status
from redis.asyncio import Redis
from redis.exceptions import RedisError

from etl_agent.config import Settings, get_settings

_SAFE_NONCE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")


class GatewayNonceStore(Protocol):
    """定义可信网关 Nonce 的跨进程一次性消费边界。"""

    async def consume(self, nonce: str, *, ttl_seconds: int) -> bool:
        """原子消费 Nonce；已存在返回 False，存储不可用时向上抛出异常。"""
        ...


class RedisGatewayNonceStore:
    """使用 Redis SET NX 让所有 API 实例共享同一防重放窗口。"""

    def __init__(self, client: Redis) -> None:
        """绑定长连接 Redis 客户端，避免每个 HTTP 请求重新建立连接池。"""
        self._client = client

    async def consume(self, nonce: str, *, ttl_seconds: int) -> bool:
        """只保存 Nonce 摘要并设置过期时间，避免 Redis 成为身份轨迹副本。"""
        nonce_sha256 = hashlib.sha256(nonce.encode("ascii")).hexdigest()
        result = await self._client.set(
            f"etl-agent:gateway-nonce:{nonce_sha256}",
            "1",
            ex=ttl_seconds,
            nx=True,
        )
        return bool(result)


@lru_cache
def get_gateway_nonce_store() -> GatewayNonceStore:
    """构造进程级 Redis Nonce Store；生产认证依赖不可用时必须失败关闭。"""
    settings = get_settings()
    return RedisGatewayNonceStore(Redis.from_url(settings.redis_url, decode_responses=True))


def gateway_signature_payload(
    *,
    actor: str,
    timestamp: str,
    nonce: str,
    method: str,
    path_and_query: str,
) -> bytes:
    """构造网关与控制面共享的规范签名文本，字段换行分隔以避免拼接歧义。"""
    return "\n".join(
        (actor, timestamp, nonce, method.upper(), path_and_query)
    ).encode("utf-8")


def sign_gateway_identity(
    *,
    secret: str,
    actor: str,
    timestamp: str,
    nonce: str,
    method: str,
    path_and_query: str,
) -> str:
    """使用 HMAC-SHA256 签署可信身份，供网关实现和契约测试复用。"""
    payload = gateway_signature_payload(
        actor=actor,
        timestamp=timestamp,
        nonce=nonce,
        method=method,
        path_and_query=path_and_query,
    )
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _request_target(request: Request) -> str:
    """返回不含主机的原始请求目标，防止代理改写 Host 导致合法签名失效。"""
    raw_path = request.scope.get("raw_path", request.url.path.encode("utf-8"))
    query = request.scope.get("query_string", b"")
    path = bytes(raw_path).decode("latin-1")
    return f"{path}?{bytes(query).decode('latin-1')}" if query else path


def _authentication_failed(detail: str) -> HTTPException:
    """生成统一 401 响应，避免不同失败分支泄漏签名校验内部细节。"""
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Gateway-HMAC"},
    )


def _authentication_unavailable() -> HTTPException:
    """在防重放存储不可用时返回 503，禁止降级成仅验 HMAC 的可重放认证。"""
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="可信网关认证暂时不可用",
        headers={"Retry-After": "5"},
    )


async def actor_id(
    request: Request,
    x_actor_id: Annotated[str, Header()],
    x_auth_timestamp: Annotated[str | None, Header()] = None,
    x_auth_nonce: Annotated[str | None, Header()] = None,
    x_auth_signature: Annotated[str | None, Header()] = None,
    settings: Settings = Depends(get_settings),
    nonce_store: GatewayNonceStore = Depends(get_gateway_nonce_store),
) -> uuid.UUID:
    """解析调用者身份；生产模式仅信任经过共享密钥签名的网关注入请求头。"""
    try:
        parsed_actor = uuid.UUID(x_actor_id)
    except ValueError as exc:
        raise _authentication_failed("调用者身份无效") from exc

    if settings.auth_mode == "dev-header":
        return parsed_actor

    if not x_auth_timestamp or not x_auth_nonce or not x_auth_signature:
        raise _authentication_failed("请求缺少可信网关签名")
    if not _SAFE_NONCE.fullmatch(x_auth_nonce):
        raise _authentication_failed("可信网关签名无效")
    try:
        signed_at = int(x_auth_timestamp)
    except ValueError as exc:
        raise _authentication_failed("可信网关签名无效") from exc
    if abs(int(time.time()) - signed_at) > settings.gateway_max_clock_skew_seconds:
        raise _authentication_failed("可信网关签名已过期")

    secret = settings.gateway_hmac_secret
    if secret is None:
        # Settings 已在启动时校验；保留运行时保护，避免测试或错误依赖覆盖绕过边界。
        raise _authentication_failed("可信网关认证未配置")
    expected = sign_gateway_identity(
        secret=secret.get_secret_value(),
        actor=x_actor_id,
        timestamp=x_auth_timestamp,
        nonce=x_auth_nonce,
        method=request.method,
        path_and_query=_request_target(request),
    )
    if not hmac.compare_digest(expected, x_auth_signature.casefold()):
        raise _authentication_failed("可信网关签名无效")
    try:
        consumed = await nonce_store.consume(
            x_auth_nonce,
            # 接受窗口覆盖时间戳前后两个方向；额外 5 秒避免边界时钟抖动
            # 让已接受请求在另一实例上重新获得一次机会。
            ttl_seconds=settings.gateway_max_clock_skew_seconds * 2 + 5,
        )
    except RedisError as exc:
        raise _authentication_unavailable() from exc
    if not consumed:
        raise _authentication_failed("可信网关签名已被使用")
    return parsed_actor


ActorId = Annotated[uuid.UUID, actor_id]
