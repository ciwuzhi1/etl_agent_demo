import asyncio
import json
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol

import httpx
from cryptography.fernet import Fernet, InvalidToken
from pydantic import BaseModel, Field, SecretStr
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.domain.errors import DomainError, ExternalServiceUnavailable, ResourceNotFound
from etl_agent.infrastructure.models import EncryptedSecret


class ConnectionCredentials(BaseModel):
    username: str = Field(min_length=1, max_length=255)
    password: SecretStr
    attributes: dict[str, SecretStr] = Field(default_factory=dict)

    def cleartext_payload(self) -> dict[str, object]:
        """仅在执行内存边界展开 SecretStr；调用方不得记录或持久化返回载荷。"""
        return {
            "username": self.username,
            "password": self.password.get_secret_value(),
            "attributes": {key: value.get_secret_value() for key, value in self.attributes.items()},
        }

    def redact_error(self, error: Exception) -> str:
        """用固定占位符替换本凭据对象中的全部敏感值，再限制错误长度。"""
        text = str(error)
        sensitive_values = [
            self.password.get_secret_value(),
            *(value.get_secret_value() for value in self.attributes.values()),
        ]
        for value in sensitive_values:
            if value:
                text = text.replace(value, "[REDACTED]")
        return text[:10_000]


class SecretProvider(Protocol):
    async def store(
        self,
        session: AsyncSession,
        value: ConnectionCredentials,
        *,
        secret_ref: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """加密保存连接凭据并返回不含明文的引用标识。"""
        ...

    async def resolve(self, session: AsyncSession, secret_ref: uuid.UUID) -> ConnectionCredentials:
        """在授权执行边界内按引用解析凭据，明文只短暂存在于内存。"""
        ...

    async def destroy(self, session: AsyncSession, secret_ref: uuid.UUID) -> None:
        """不可逆销毁凭据材料，同时保留不含秘密的本地审计墓碑。"""
        ...


class FernetDatabaseSecretProvider(SecretProvider):
    """仅供本地/单机部署；生产环境应以 Vault/KMS Adapter 替换。"""

    provider_name = "fernet-db"

    def __init__(self, master_key: str, *, key_version: str = "local-v1") -> None:
        """初始化实例依赖和运行参数；依赖显式保存以便替换、测试和生命周期管理。"""
        try:
            self._fernet = Fernet(master_key.encode())
        except (ValueError, TypeError) as exc:
            raise DomainError("ETL_AGENT_SECRET_MASTER_KEY 不是有效的 Fernet 密钥") from exc
        self._key_version = key_version

    async def store(
        self,
        session: AsyncSession,
        value: ConnectionCredentials,
        *,
        secret_ref: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """在密钥存储边界中执行“store”对应的内部步骤；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        if secret_ref is not None:
            existing = await session.get(EncryptedSecret, secret_ref)
            if existing is not None:
                if existing.destroyed_at is not None:
                    raise DomainError("已销毁的 Fernet 凭据引用不能重新写入")
                resolved = await self.resolve(session, secret_ref)
                if resolved.cleartext_payload() != value.cleartext_payload():
                    raise DomainError("预分配的 Fernet 凭据引用已被其他内容占用")
                return secret_ref
        cleartext = json.dumps(
            value.cleartext_payload(), ensure_ascii=False, separators=(",", ":")
        ).encode()
        secret = EncryptedSecret(
            id=secret_ref or uuid.uuid4(),
            provider=self.provider_name,
            ciphertext=self._fernet.encrypt(cleartext),
            external_ref=None,
            key_version=self._key_version,
        )
        session.add(secret)
        await session.flush()
        return secret.id

    async def resolve(self, session: AsyncSession, secret_ref: uuid.UUID) -> ConnectionCredentials:
        """在密钥存储边界中解析受控引用并验证所属边界；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        secret = await session.get(EncryptedSecret, secret_ref)
        if secret is None:
            raise ResourceNotFound("连接凭据不存在")
        if secret.destroyed_at is not None:
            raise ResourceNotFound("连接凭据已经销毁")
        if secret.provider != self.provider_name or secret.ciphertext is None:
            raise DomainError("连接凭据不属于当前 Fernet SecretProvider")
        try:
            payload = json.loads(self._fernet.decrypt(secret.ciphertext))
        except (InvalidToken, json.JSONDecodeError) as exc:
            raise DomainError("连接凭据无法解密，请检查主密钥版本") from exc
        return ConnectionCredentials.model_validate(payload)

    async def destroy(self, session: AsyncSession, secret_ref: uuid.UUID) -> None:
        """清除本地 Fernet 密文并写入销毁时间；重复调用保持幂等。"""
        secret = await session.get(EncryptedSecret, secret_ref)
        if secret is None:
            raise ResourceNotFound("连接凭据不存在")
        if secret.provider != self.provider_name or secret.external_ref is not None:
            raise DomainError("连接凭据不属于当前 Fernet SecretProvider")
        if secret.destroyed_at is not None:
            return
        # 保留 UUID、Provider 和密钥版本作为审计墓碑，但密文本体不可恢复地清空。
        secret.ciphertext = None
        secret.destroyed_at = datetime.now(UTC)
        await session.flush()


class VaultKvV2SecretProvider(SecretProvider):
    """把凭据保存在 Vault KV v2，PostgreSQL 只持久化不含秘密的路径引用。"""

    provider_name = "vault-kv-v2"
    _SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

    def __init__(
        self,
        address: str,
        *,
        token: SecretStr | None = None,
        token_file: Path | None = None,
        namespace: str | None = None,
        mount: str = "secret",
        path_prefix: str = "etl-agent/connections",
        timeout_seconds: float = 10.0,
        verify_tls: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """校验 Vault 地址、认证来源和 KV 路径，拒绝可能改变请求目标的路径片段。"""
        if (token is None) == (token_file is None):
            raise DomainError("Vault 必须且只能配置 ETL_AGENT_VAULT_TOKEN 或 TOKEN_FILE 之一")
        try:
            parsed_address = httpx.URL(address)
        except httpx.InvalidURL as exc:
            raise DomainError("Vault 地址不是合法 URL") from exc
        if (
            parsed_address.scheme not in {"http", "https"}
            or parsed_address.host is None
            or parsed_address.username
            or parsed_address.password
            or parsed_address.query
            or parsed_address.fragment
            or parsed_address.path not in {"", "/"}
        ):
            raise DomainError("Vault 地址只能包含 http(s) 协议、主机和端口")
        self._address = str(parsed_address).rstrip("/")
        self._token = token
        self._token_file = token_file
        normalized_namespace = namespace.strip().strip("/") if namespace else None
        self._namespace = (
            self._validate_path(normalized_namespace, field="Vault namespace", allow_nested=True)
            if normalized_namespace
            else None
        )
        self._mount = self._validate_path(mount, field="Vault KV mount", allow_nested=False)
        self._path_prefix = self._validate_path(
            path_prefix.strip("/"), field="Vault path prefix", allow_nested=True
        )
        self._timeout_seconds = timeout_seconds
        self._verify_tls = verify_tls
        self._transport = transport

    async def store(
        self,
        session: AsyncSession,
        value: ConnectionCredentials,
        *,
        secret_ref: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """先把凭据写入 Vault，再保存路径引用；数据库不会出现凭据副本。"""
        preallocated = secret_ref is not None
        secret_ref = secret_ref or uuid.uuid4()
        external_ref = self._secret_path(secret_ref)
        existing = await session.get(EncryptedSecret, secret_ref)
        if existing is not None:
            if existing.destroyed_at is not None:
                raise DomainError("已销毁的 Vault 凭据引用不能重新写入")
            resolved = await self.resolve(session, secret_ref)
            if resolved.cleartext_payload() != value.cleartext_payload():
                raise DomainError("预分配的 Vault 凭据引用已被其他内容占用")
            return secret_ref
        if preallocated:
            try:
                remote = await self._read_remote(external_ref)
            except ResourceNotFound:
                remote = None
            if remote is not None:
                if remote.cleartext_payload() != value.cleartext_payload():
                    raise DomainError("预分配的 Vault 路径已被其他凭据占用")
                self._add_locator(session, secret_ref, external_ref)
                await session.flush()
                return secret_ref
        # CAS=0 保证 UUID 路径只能首次创建，即使极端碰撞或事务重试也不会覆盖旧凭据。
        await self._request(
            "POST",
            external_ref,
            json_body={"options": {"cas": 0}, "data": value.cleartext_payload()},
        )
        self._add_locator(session, secret_ref, external_ref)
        await session.flush()
        return secret_ref

    async def resolve(self, session: AsyncSession, secret_ref: uuid.UUID) -> ConnectionCredentials:
        """按受控 UUID 解析 Vault 凭据，并验证数据库路径未被篡改或跨前缀替换。"""
        secret = await session.get(EncryptedSecret, secret_ref)
        if secret is None:
            raise ResourceNotFound("连接凭据不存在")
        if secret.destroyed_at is not None:
            raise ResourceNotFound("连接凭据已经销毁")
        expected_ref = self._secret_path(secret_ref)
        if (
            secret.provider != self.provider_name
            or secret.ciphertext is not None
            or secret.external_ref != expected_ref
        ):
            raise DomainError("Vault 凭据引用与当前 Provider 配置不一致")
        return await self._read_remote(expected_ref)

    async def destroy(self, session: AsyncSession, secret_ref: uuid.UUID) -> None:
        """删除 Vault KV v2 全部版本及元数据，再把本地定位记录标记为墓碑。"""
        secret = await session.get(EncryptedSecret, secret_ref)
        if secret is None:
            raise ResourceNotFound("连接凭据不存在")
        if secret.destroyed_at is not None:
            return
        expected_ref = self._secret_path(secret_ref)
        if (
            secret.provider != self.provider_name
            or secret.ciphertext is not None
            or secret.external_ref != expected_ref
        ):
            raise DomainError("Vault 凭据引用与当前 Provider 配置不一致")
        # 删除 metadata 才会同时移除 KV v2 的全部版本；404 视为此前远端删除已成功，
        # 这样 Vault 成功但 PostgreSQL 提交失败后的 Worker 重试仍能收敛。
        await self._request(
            "DELETE",
            expected_ref,
            endpoint="metadata",
            allow_not_found=True,
        )
        secret.destroyed_at = datetime.now(UTC)
        await session.flush()

    async def _read_remote(self, external_ref: str) -> ConnectionCredentials:
        """直接读取受控 Vault 路径，供正常解析和数据库回滚后的幂等恢复复用。"""
        response = await self._request("GET", external_ref)
        try:
            payload = response.json()["data"]["data"]
            return ConnectionCredentials.model_validate(payload)
        except (KeyError, TypeError, ValueError) as exc:
            # Vault 返回体可能包含业务凭据，因此异常中只保留固定文案，禁止拼接响应正文。
            raise DomainError("Vault 返回了无法识别的 KV v2 凭据结构") from exc

    def _add_locator(self, session: AsyncSession, secret_ref: uuid.UUID, external_ref: str) -> None:
        """只向 PostgreSQL 会话加入非敏感定位记录，实际提交仍由业务事务控制。"""
        session.add(
            EncryptedSecret(
                id=secret_ref,
                provider=self.provider_name,
                ciphertext=None,
                external_ref=external_ref,
                key_version=self.provider_name,
            )
        )

    async def _request(
        self,
        method: str,
        external_ref: str,
        *,
        json_body: dict[str, object] | None = None,
        endpoint: Literal["data", "metadata"] = "data",
        allow_not_found: bool = False,
    ) -> httpx.Response:
        """向 KV v2 发起禁止跨主机重定向的请求，并把远端错误收敛为脱敏语义。"""
        headers = await self._authentication_headers()
        url = f"/v1/{self._mount}/{endpoint}/{external_ref}"
        try:
            # 不跟随重定向，避免 Vault Token 因错误代理配置被转发给其他主机。
            async with httpx.AsyncClient(
                base_url=self._address,
                headers=headers,
                timeout=self._timeout_seconds,
                verify=self._verify_tls,
                follow_redirects=False,
                transport=self._transport,
            ) as client:
                response = await client.request(method, url, json=json_body)
        except httpx.HTTPError as exc:
            raise ExternalServiceUnavailable(
                "Vault 当前不可访问，请检查网络、TLS 和服务状态"
            ) from exc
        if response.status_code == 404 and not allow_not_found:
            raise ResourceNotFound("Vault 中的连接凭据不存在或已删除")
        if response.status_code in {401, 403}:
            raise DomainError("Vault 拒绝访问，请检查 Token、Policy 和 Namespace")
        if response.is_redirect:
            raise DomainError("Vault 返回了重定向；为防止 Token 泄露，平台已拒绝跟随")
        if response.status_code >= 500:
            raise ExternalServiceUnavailable(
                f"Vault KV v2 暂时不可用，HTTP 状态码 {response.status_code}"
            )
        if not response.is_success and not (allow_not_found and response.status_code == 404):
            raise DomainError(f"Vault KV v2 请求失败，HTTP 状态码 {response.status_code}")
        return response

    async def _authentication_headers(self) -> dict[str, str]:
        """每次请求重新读取 Token 文件，使 Vault Agent 轮换后无需重启服务。"""
        if self._token_file is not None:
            try:
                token = await asyncio.to_thread(self._token_file.read_text, encoding="utf-8")
            except OSError as exc:
                raise DomainError("无法读取 Vault Token 文件") from exc
        else:
            token = self._token.get_secret_value() if self._token is not None else ""
        token = token.strip()
        if not token:
            raise DomainError("Vault Token 为空")
        headers = {"X-Vault-Token": token}
        if self._namespace:
            headers["X-Vault-Namespace"] = self._namespace
        return headers

    def _secret_path(self, secret_ref: uuid.UUID) -> str:
        """从平台 UUID 确定性生成 Vault 路径，避免请求参数接受任意外部路径。"""
        return f"{self._path_prefix}/{secret_ref}"

    @classmethod
    def _validate_path(cls, value: str, *, field: str, allow_nested: bool) -> str:
        """限制 Vault mount 和前缀字符集，阻断空段、查询串和路径穿越。"""
        segments = value.split("/")
        if not value or (not allow_nested and len(segments) != 1):
            raise DomainError(f"{field} 配置不合法")
        if any(not cls._SAFE_SEGMENT.fullmatch(segment) for segment in segments):
            raise DomainError(f"{field} 只能包含安全的路径片段")
        return value


class RoutingSecretProvider(SecretProvider):
    """把新写入路由到主 Provider，同时按记录来源兼容解析历史凭据。"""

    def __init__(
        self,
        primary: SecretProvider,
        providers: dict[str, SecretProvider],
    ) -> None:
        """冻结主 Provider 与可读后端映射，防止运行中配置变化导致解析漂移。"""
        self._primary = primary
        self._providers = dict(providers)

    async def store(
        self,
        session: AsyncSession,
        value: ConnectionCredentials,
        *,
        secret_ref: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """所有新连接只写当前主 Provider，旧后端仅承担迁移期读取。"""
        return await self._primary.store(session, value, secret_ref=secret_ref)

    async def resolve(self, session: AsyncSession, secret_ref: uuid.UUID) -> ConnectionCredentials:
        """根据本地非敏感来源标识选择后端，未知或未配置的后端明确失败。"""
        secret = await session.get(EncryptedSecret, secret_ref)
        if secret is None:
            raise ResourceNotFound("连接凭据不存在")
        if secret.destroyed_at is not None:
            raise ResourceNotFound("连接凭据已经销毁")
        provider = self._providers.get(secret.provider)
        if provider is None:
            raise DomainError(f"凭据来源 {secret.provider} 当前未配置，无法解析历史连接")
        return await provider.resolve(session, secret_ref)

    async def destroy(self, session: AsyncSession, secret_ref: uuid.UUID) -> None:
        """根据墓碑记录中的来源 Provider 路由不可逆销毁，未知后端失败关闭。"""
        secret = await session.get(EncryptedSecret, secret_ref)
        if secret is None:
            raise ResourceNotFound("连接凭据不存在")
        if secret.destroyed_at is not None:
            return
        provider = self._providers.get(secret.provider)
        if provider is None:
            raise DomainError(f"凭据来源 {secret.provider} 当前未配置，无法执行销毁")
        await provider.destroy(session, secret_ref)
