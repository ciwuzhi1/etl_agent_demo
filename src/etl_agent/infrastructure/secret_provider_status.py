from cryptography.fernet import Fernet

from etl_agent.config import Settings, get_settings


def _fernet_db_ready(settings: Settings) -> bool:
    """Fernet 模式就绪 = 主密钥存在且是可用密钥；只构造对象，不解密任何凭据。"""
    if settings.secret_master_key is None:
        return False
    try:
        Fernet(settings.secret_master_key.get_secret_value().encode())
    except ValueError:
        # 异常消息不回显密钥内容，避免秘密进入日志。
        return False
    return True


def _vault_ready(settings: Settings) -> bool:
    """Vault 模式就绪 = 地址存在，且 Token 与 Token 文件恰好二选一；不读取 Token 值。"""
    if settings.vault_address is None:
        return False
    has_token = settings.vault_token is not None
    token_file = settings.vault_token_file
    if has_token and token_file is not None:
        return False
    if token_file is not None:
        return token_file.is_file()
    return has_token


def secret_provider_readiness() -> tuple[str, bool]:
    """返回 SecretProvider 名称与必要配置就绪状态，供健康检查展示。

    本函数不连接数据库、不读取任何凭据密文，也不返回秘密本身。
    """
    settings = get_settings()
    if settings.secret_provider == "vault-kv-v2":
        return settings.secret_provider, _vault_ready(settings)
    return settings.secret_provider, _fernet_db_ready(settings)
