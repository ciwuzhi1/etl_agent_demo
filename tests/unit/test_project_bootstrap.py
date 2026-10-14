import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.config import Settings, get_settings
from etl_agent.infrastructure.database import get_session
from etl_agent.main import app


@pytest.mark.asyncio
async def test_bootstrap_requires_strong_token_and_is_single_use(
    session: AsyncSession,
) -> None:
    """未授权初始化失败，正确 Token 只能创建一个企业工作区。"""
    token = "bootstrap-token-with-at-least-32-random-bytes"
    settings = Settings(
        _env_file=None,
        bootstrap_enabled=True,
        bootstrap_token=token,
    )

    async def override_session():  # type: ignore[no-untyped-def]
        """让 ASGI 请求复用测试事务会话。"""
        yield session

    app.dependency_overrides[get_session] = override_session
    app.dependency_overrides[get_settings] = lambda: settings
    payload = {
        "project_name": f"secure-bootstrap-{uuid.uuid4()}",
        "user_subject": f"admin-{uuid.uuid4()}@company.local",
        "user_display_name": "首位管理员",
    }
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            denied = await client.post(
                "/api/v1/projects/bootstrap",
                headers={"X-Bootstrap-Token": "wrong-token-value-with-at-least-32-bytes"},
                json=payload,
            )
            created = await client.post(
                "/api/v1/projects/bootstrap",
                headers={"X-Bootstrap-Token": token},
                json=payload,
            )
            repeated = await client.post(
                "/api/v1/projects/bootstrap",
                headers={"X-Bootstrap-Token": token},
                json={**payload, "project_name": "不应创建的第二个初始化项目"},
            )
    finally:
        app.dependency_overrides.pop(get_session, None)
        app.dependency_overrides.pop(get_settings, None)

    assert denied.status_code == 403
    assert created.status_code == 201
    assert repeated.status_code == 409
