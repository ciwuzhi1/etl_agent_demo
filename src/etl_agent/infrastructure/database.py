from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from etl_agent.config import get_settings


class Base(DeclarativeBase):
    pass


def create_engine(database_url: str | None = None) -> AsyncEngine:
    """按传入 URL 创建异步 Engine；省略参数时使用业务库 URL。

    本模块只服务业务真相库 ``ETL_AGENT_DATABASE_URL``。Checkpoint 库
    ``ETL_AGENT_CHECKPOINT_DATABASE_URL`` 不经过 SQLAlchemy，第 8 章由 LangGraph 的
    AsyncPostgresSaver（psycopg）直接持有，两套连接不共享连接池。
    """
    url = database_url or get_settings().database_url
    return create_async_engine(url, pool_pre_ping=True)


engine = create_engine()
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """为单个请求提供独立 Session，请求结束即关闭；不自动提交，
    应用服务必须显式 commit 或在异常时 rollback。
    """
    async with SessionFactory() as session:
        yield session
