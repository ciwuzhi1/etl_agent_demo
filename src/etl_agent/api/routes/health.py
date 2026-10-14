from fastapi import APIRouter

from etl_agent.api.schemas import HealthResponse
from etl_agent.infrastructure.secret_provider_status import secret_provider_readiness

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """返回服务存活状态及脱敏的 SecretProvider 配置状态，不主动读取任何凭据。"""
    provider, configured = secret_provider_readiness()
    return HealthResponse(
        status="ok",
        service="seatunnel-etl-agent",
        secret_provider=provider,
        secret_provider_configured=configured,
    )
