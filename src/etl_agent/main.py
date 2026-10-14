from fastapi import FastAPI

from etl_agent.api.routes import health
from etl_agent.config import get_settings

settings = get_settings()
app = FastAPI(title=settings.app_name, version="0.1.0")

# 本章只注册无需身份的 /health。领域异常 → HTTP 状态映射随第 3 章 domain/errors.py
# 落地后加入，其余业务路由按章节顺序在此处追加注册。
app.include_router(health.router)
# 后续章节的业务路由统一挂在 settings.api_prefix 下：
# app.include_router(projects.router, prefix=settings.api_prefix)


def run() -> None:
    """控制台入口 etl-agent-api：以 API 进程启动 Uvicorn。"""
    import uvicorn

    uvicorn.run("etl_agent.main:app", host="0.0.0.0", port=8000)
