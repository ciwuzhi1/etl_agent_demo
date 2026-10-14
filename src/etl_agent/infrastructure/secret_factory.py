from etl_agent.application.secrets import (
    FernetDatabaseSecretProvider,
    RoutingSecretProvider,
    SecretProvider,
    VaultKvV2SecretProvider,
)
from etl_agent.config import Settings, get_settings
from etl_agent.domain.errors import DomainError


def create_secret_provider() -> SecretProvider:
    """按配置创建主 SecretProvider，并保留已配置旧后端的只读迁移能力。"""
    settings = get_settings()
    providers: dict[str, SecretProvider] = {}

    # 切换到 Vault 后仍允许保留本地主密钥，使历史 Fernet 连接可以平滑读取和迁移。
    if settings.secret_master_key is not None:
        value = settings.secret_master_key.get_secret_value()
        if value:
            local = FernetDatabaseSecretProvider(value)
            providers[local.provider_name] = local

    if settings.secret_provider == "vault-kv-v2":
        vault = _create_vault_provider(settings)
        providers[vault.provider_name] = vault
        primary: SecretProvider = vault
    else:
        local_primary = providers.get("fernet-db")
        if local_primary is None:
            raise DomainError(
                "尚未配置 ETL_AGENT_SECRET_MASTER_KEY；请生成 Fernet 密钥后再创建连接"
            )
        primary = local_primary

    return RoutingSecretProvider(primary, providers)


def secret_provider_readiness() -> tuple[str, bool]:
    """返回不含 Token、地址和路径的 Provider 配置就绪状态，供健康检查展示。"""
    settings = get_settings()
    try:
        create_secret_provider()
    except DomainError:
        return settings.secret_provider, False
    return settings.secret_provider, True


def primary_secret_provider_name() -> str:
    """返回当前新凭据写入目标的公开标识，不创建 Provider 也不读取任何 Token。"""
    return get_settings().secret_provider


def _create_vault_provider(settings: Settings) -> VaultKvV2SecretProvider:
    """从受控配置构造 Vault Adapter，并在生产环境禁止明文 HTTP 传输 Token。"""
    if settings.vault_address is None:
        raise DomainError("Vault 模式缺少 ETL_AGENT_VAULT_ADDRESS")
    address = str(settings.vault_address)
    if settings.environment.lower() not in {"local", "test", "dev", "development"}:
        if not address.lower().startswith("https://"):
            raise DomainError("非开发环境的 Vault 地址必须使用 HTTPS")
    return VaultKvV2SecretProvider(
        address,
        token=settings.vault_token,
        token_file=settings.vault_token_file,
        namespace=settings.vault_namespace,
        mount=settings.vault_kv_mount,
        path_prefix=settings.vault_path_prefix,
        timeout_seconds=settings.vault_timeout_seconds,
        verify_tls=settings.vault_verify_tls,
    )
