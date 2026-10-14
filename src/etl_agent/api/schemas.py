"""API 请求与响应模型。本章只声明健康检查响应，后续章节按路由逐个补齐。"""

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str
    secret_provider: str
    secret_provider_configured: bool
