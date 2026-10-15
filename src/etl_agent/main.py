from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from etl_agent.api.routes import connections, file_assets, health, projects
from etl_agent.config import get_settings
from etl_agent.domain.errors import (
    DomainError,
    ExternalServiceUnavailable,
    PermissionDenied,
    ResourceNotFound,
    VersionConflict,
)

settings = get_settings()
app = FastAPI(title=settings.app_name, version="0.1.0")

app.include_router(health.router)
app.include_router(projects.router, prefix=settings.api_prefix)
app.include_router(connections.router, prefix=settings.api_prefix)
app.include_router(file_assets.router, prefix=settings.api_prefix)
# 后续章节的业务路由按章节顺序在此追加。


@app.exception_handler(DomainError)
async def domain_error_handler(_request: Request, exc: DomainError) -> JSONResponse:
    """把领域异常映射为稳定 HTTP 错误结构。

    Trace ID 关联要等第 10 章的 observability 落地后补入；本章只保证状态码与错误语义。
    """
    status_code = 400
    if isinstance(exc, PermissionDenied):
        status_code = 403
    elif isinstance(exc, ExternalServiceUnavailable):
        status_code = 503
    elif isinstance(exc, ResourceNotFound):
        status_code = 404
    elif isinstance(exc, VersionConflict):
        status_code = 409
    return JSONResponse(status_code=status_code, content={"detail": str(exc)})


def run() -> None:
    """控制台入口 etl-agent-api：以 API 进程启动 Uvicorn。"""
    import uvicorn

    uvicorn.run("etl_agent.main:app", host="0.0.0.0", port=8000)
