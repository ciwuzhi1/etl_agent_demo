from collections.abc import AsyncIterator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from etl_agent.infrastructure.database import Base


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    """构造或模拟测试所需的 session 行为，使断言聚焦于被测业务边界。"""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as value:
        yield value
    await engine.dispose()
