"""一次性 Bootstrap 的验收测试。课件 §3.10 场景表第 1–3 条。

一个用例连发三次 POST，覆盖三条边界：
错 Token → 403、首次正确 → 201、再次调用 → 409。

手法上值得记的三点：
- 用 `app.dependency_overrides` 把 `get_session` 换成 fixture 的内存库会话，
  应用才不会去连真实 PostgreSQL；
- `Settings(_env_file=None, ...)` 关掉读 `.env`，否则换台机器结论就变了；
- 走 ASGI 传输（`ASGITransport`）而不是起 HTTP 服务器：跑得快，
  且异常映射、依赖注入都还是真实链路。

⚠️ 本用例**不**覆盖 `pg_advisory_xact_lock` 的并发互斥——SQLite 上那个分支会被跳过，
409 走的是"已提交过项目再查一次"的串行路径。
"""

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
    # 长度 ≥32 字节：与 Settings 的启动期校验保持一致，别用会被拒的短 Token。
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
    # 用 uuid4 拼 subject 和 project_name：万一有状态残留，也不会让本用例互相影响。
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
            # 三次请求的顺序本身就是断言的一部分：
            # 第一次被拒后**不能**留下项目，否则第二次就不是"首次"了。
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
            # 故意换个项目名：证明 409 来自"已有项目"这个服务端事实，
            # 而不是因为请求体和第一次一模一样。
            repeated = await client.post(
                "/api/v1/projects/bootstrap",
                headers={"X-Bootstrap-Token": token},
                json={**payload, "project_name": "不应创建的第二个初始化项目"},
            )
    finally:
        # 必须清理：dependency_overrides 是 app 上的全局字典，
        # 不清会泄漏到同进程后续测试文件，让别的用例莫名其妙连到这个内存库。
        app.dependency_overrides.pop(get_session, None)
        app.dependency_overrides.pop(get_settings, None)

    assert denied.status_code == 403
    assert created.status_code == 201
    assert repeated.status_code == 409
