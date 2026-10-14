from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AnyHttpUrl, Field, SecretStr, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ETL_AGENT_",
        env_ignore_empty=True,
        extra="ignore",
    )

    app_name: str = "SeaTunnel ETL Agent"
    environment: str = "local"
    api_prefix: str = "/api/v1"
    auth_mode: Literal["dev-header", "trusted-gateway"] = "dev-header"
    gateway_hmac_secret: SecretStr | None = None
    gateway_max_clock_skew_seconds: int = Field(default=60, ge=5, le=300)
    bootstrap_enabled: bool = False
    bootstrap_token: SecretStr | None = None
    otel_enabled: bool = False
    otel_service_name: str = Field(default="seatunnel-etl-agent", min_length=1, max_length=255)
    otel_traces_endpoint: AnyHttpUrl | None = None
    otel_export_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    otel_trace_sample_ratio: float = Field(default=1.0, ge=0, le=1)
    database_url: str = "postgresql+asyncpg://etl_agent:etl_agent@localhost:5432/etl_agent"
    checkpoint_database_url: str = (
        "postgresql://etl_checkpoint:etl_checkpoint@localhost:5432/etl_checkpoint"
    )
    redis_url: str = "redis://localhost:6379/0"
    seatunnel_api_url: AnyHttpUrl = Field(default=AnyHttpUrl("http://localhost:8080"))
    seatunnel_timezone: str = "UTC"
    seatunnel_llm_provider: str = "openai-compatible"
    generation_backend: Literal["direct", "seatunnel-cli"] = "direct"
    llm_api_key: SecretStr | None = None
    llm_base_url: AnyHttpUrl | None = None
    llm_model: str | None = Field(default=None, max_length=255)
    llm_fast_model: str | None = Field(default=None, max_length=255)
    llm_request_timeout_seconds: float = Field(default=180, gt=0, le=600)
    row_llm_transform_enabled: bool = False
    row_llm_api_key: SecretStr | None = None
    row_llm_allowed_api_hosts: str = ""
    row_llm_max_rows_per_second: int = Field(default=10, ge=1, le=100)
    secret_provider: Literal["fernet-db", "vault-kv-v2"] = "fernet-db"
    secret_master_key: SecretStr | None = None
    vault_address: AnyHttpUrl | None = None
    vault_token: SecretStr | None = None
    vault_token_file: Path | None = None
    vault_namespace: str | None = Field(default=None, max_length=255)
    vault_kv_mount: str = Field(default="secret", min_length=1, max_length=128)
    vault_path_prefix: str = Field(default="etl-agent/connections", min_length=1, max_length=384)
    vault_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    vault_verify_tls: bool = True
    secret_rotation_rollback_days: int = Field(default=7, ge=1, le=365)
    credential_alert_webhook_url: AnyHttpUrl | None = None
    credential_alert_webhook_signing_secret: SecretStr | None = None
    credential_alert_webhook_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    credential_alert_scan_interval_seconds: int = Field(default=3600, ge=60, le=86_400)
    credential_alert_warning_cooldown_hours: int = Field(default=24, ge=1, le=720)
    credential_alert_overdue_cooldown_hours: int = Field(default=6, ge=1, le=720)
    schedule_scan_interval_seconds: int = Field(default=30, ge=5, le=3600)
    schedule_misfire_grace_seconds: int = Field(default=120, ge=0, le=86_400)
    harness_policy_bundle_version: str = Field(
        default="harness-v2-20260808", min_length=1, max_length=128
    )
    harness_signing_key_id: str = Field(default="local-harness-v1", min_length=1, max_length=128)
    harness_private_key: SecretStr | None = None
    harness_public_keys: dict[str, str] = Field(default_factory=dict)
    harness_dev_key_path: Path = Path("var/harness/authorization-ed25519.key")
    harness_preparation_ttl_seconds: int = Field(default=86_400, ge=300, le=604_800)
    harness_capability_ttl_seconds: int = Field(default=300, ge=30, le=900)
    harness_outbox_scan_interval_seconds: int = Field(default=2, ge=1, le=60)
    harness_runtime_max_records: int = Field(default=10_000_000, ge=1, le=10_000_000_000)
    harness_runtime_max_bytes: int = Field(default=10 * 1024**3, ge=1024, le=10 * 1024**4)
    harness_runtime_timeout_seconds: int = Field(default=3600, ge=60, le=604_800)
    harness_runtime_max_output_ratio: float = Field(default=2.0, ge=1.0, le=1000.0)
    harness_runtime_max_reject_rate: float = Field(default=0.5, ge=0.0, le=1.0)
    harness_runtime_minimum_quality_rows: int = Field(default=100, ge=1, le=10_000_000_000)
    harness_runtime_warning_ratio: float = Field(default=0.8, gt=0.0, lt=1.0)
    artifact_root: Path = Path("var/artifacts")
    file_asset_root: Path = Path("var/file-assets")
    file_asset_engine_root: str = Field(default="/data/file-assets", min_length=2, max_length=512)
    file_asset_max_bytes: int = Field(default=25 * 1024 * 1024, ge=1024, le=1024**3)
    file_asset_max_rows: int = Field(default=100_000, ge=1, le=10_000_000)
    worker_heartbeat_interval_seconds: int = Field(default=10, ge=5, le=300)
    worker_heartbeat_stale_seconds: int = Field(default=35, ge=15, le=900)
    doris_rollback_retention_days: int = Field(default=7, ge=1, le=3650)
    doris_rollback_capacity_warning_count: int = Field(default=20, ge=1, le=1_000_000)

    @model_validator(mode="after")
    def require_production_authentication_boundary(self) -> "Settings":
        """生产环境必须启用可信网关签名，避免客户端自行伪造 Actor UUID。"""
        production = self.environment.strip().casefold() in {"prod", "production"}
        if production and self.auth_mode != "trusted-gateway":
            raise ValueError("生产环境必须设置 ETL_AGENT_AUTH_MODE=trusted-gateway")
        if self.auth_mode == "trusted-gateway" and self.gateway_hmac_secret is None:
            raise ValueError("可信网关认证必须配置 ETL_AGENT_GATEWAY_HMAC_SECRET")
        if (
            self.auth_mode == "trusted-gateway"
            and self.gateway_hmac_secret is not None
            and len(self.gateway_hmac_secret.get_secret_value().encode("utf-8")) < 32
        ):
            # HMAC 安全性直接受共享密钥熵约束；即使是非生产联调，也不能让弱密钥
            # 悄悄进入可复制的部署配置。
            raise ValueError("可信网关 HMAC 共享密钥至少需要 32 字节")
        if self.bootstrap_enabled and self.bootstrap_token is None:
            raise ValueError("启用首次初始化接口必须配置 ETL_AGENT_BOOTSTRAP_TOKEN")
        if (
            self.bootstrap_token is not None
            and len(self.bootstrap_token.get_secret_value().encode("utf-8")) < 32
        ):
            raise ValueError("首次初始化 Token 至少需要 32 字节")
        if self.row_llm_transform_enabled and self.row_llm_api_key is None:
            raise ValueError("启用逐行 LLM Transform 必须配置 ETL_AGENT_ROW_LLM_API_KEY")
        if production and not self.harness_public_keys:
            raise ValueError("生产环境必须配置 ETL_AGENT_HARNESS_PUBLIC_KEYS")
        return self


class ConfigurationError(RuntimeError):
    """配置不合法时使用的异常：消息只点名配置项与违反的规则，绝不携带任何取值。"""


def _describe_validation_error(exc: ValidationError) -> str:
    """把 Pydantic 校验错误转换为脱敏说明。

    Pydantic 的 ValidationError 字符串与 traceback 会带上 ``input_value``，也就是
    整份配置字典的原始值，其中包含网关 HMAC 密钥等秘密；因此这里只取规则说明与
    字段名，显式丢弃 input 与 ctx。
    """
    lines: list[str] = []
    for error in exc.errors():
        location = error["loc"]
        if location:
            item = "ETL_AGENT_" + "_".join(str(part) for part in location).upper()
        else:
            # 模型级校验器（字段之间的关系）没有 loc，规则文本本身已点名配置项。
            item = "配置组合"
        lines.append(f"{item}: {error['msg']}")
    return "配置校验失败，进程拒绝启动：\n  - " + "\n  - ".join(lines)


@lru_cache
def get_settings() -> Settings:
    """在配置入口中读取全部环境变量并执行生产边界校验；错误信息只点名配置项，
    秘密字段一律是 SecretStr，不进入消息与日志。
    """
    try:
        return Settings()
    except ValidationError as exc:
        # from None：不保留原始异常链，避免 Pydantic 自带的 input_value 进入 traceback。
        raise ConfigurationError(_describe_validation_error(exc)) from None
