"""执行取消端点的状态机契约（第 10/15 章）。

覆盖：
- QUEUED（还没投递到引擎）取消必须直接进 CANCELLED —— 早期实现无条件写 CANCELLING，
  撞上状态机"QUEUED 不允许 → CANCELLING"，让最想撤销的那一刻反而拿到 400；
- SUBMITTED / RUNNING（已交给引擎）取消进 CANCELLING，由 Worker 轮询代执行停作业；
- 终态运行取消被拒（409），迟到指令不得改写已确认终态；
- 非本项目成员无权取消（403）。
"""

import uuid
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.domain.enums import ExecutionStatus, PipelineVersionStatus, ProjectRole
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import (
    ExecutionRun,
    Pipeline,
    PipelineVersion,
    Project,
    ProjectMembership,
    User,
)
from etl_agent.main import app


async def _seed_run(
    session: AsyncSession, status: ExecutionStatus
) -> tuple[ExecutionRun, User, User]:
    """建项目/流水线/版本/运行与两名成员，返回 (run, 工程师, 非成员)。"""
    suffix = uuid.uuid4()
    project = Project(name=f"cancel-{suffix}")
    engineer = User(subject=f"cancel-engineer-{suffix}", display_name="取消工程师")
    outsider = User(subject=f"cancel-outsider-{suffix}", display_name="外部人员")
    session.add_all([project, engineer, outsider])
    await session.flush()
    session.add(
        ProjectMembership(
            project_id=project.id, user_id=engineer.id, role=ProjectRole.DATA_ENGINEER
        )
    )
    pipeline = Pipeline(project_id=project.id, name=f"pipeline-{suffix}")
    session.add(pipeline)
    await session.flush()
    version = PipelineVersion(
        pipeline_id=pipeline.id,
        version_number=1,
        status=PipelineVersionStatus.APPROVED,
        requirement="取消测试",
        plan={},
        created_by=engineer.id,
    )
    session.add(version)
    await session.flush()
    run = ExecutionRun(
        pipeline_version_id=version.id,
        status=status,
        idempotency_key=f"exec-{suffix.hex[:16]}",
        requested_by=engineer.id,
    )
    session.add(run)
    await session.commit()
    return run, engineer, outsider


async def _post_cancellation(
    session: AsyncSession, run_id: uuid.UUID, actor: User
) -> tuple[int, dict]:
    """以指定身份调用取消端点，复用当前 SQLite 会话。"""

    async def override_session() -> AsyncIterator[AsyncSession]:
        yield session

    app.dependency_overrides[get_session] = override_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                f"/api/v1/execution-runs/{run_id}/cancellation",
                headers={"X-Actor-ID": str(actor.id)},
            )
            return response.status_code, response.json()
    finally:
        app.dependency_overrides.pop(get_session, None)


@pytest.mark.asyncio
async def test_queued_run_is_cancelled_outright(session: AsyncSession) -> None:
    """QUEUED 还没投递到引擎，取消直接进 CANCELLED，不走 CANCELLING 中间态。"""
    run, engineer, _ = await _seed_run(session, ExecutionStatus.QUEUED)
    status, body = await _post_cancellation(session, run.id, engineer)
    assert status == 202
    assert body["status"] == ExecutionStatus.CANCELLED.value


@pytest.mark.asyncio
async def test_submitted_run_enters_cancelling(session: AsyncSession) -> None:
    """已交给引擎的运行取消进 CANCELLING，真正的停止由 Worker 代执行。"""
    run, engineer, _ = await _seed_run(session, ExecutionStatus.SUBMITTED)
    status, body = await _post_cancellation(session, run.id, engineer)
    assert status == 202
    assert body["status"] == ExecutionStatus.CANCELLING.value


@pytest.mark.asyncio
async def test_terminal_run_cannot_be_cancelled(session: AsyncSession) -> None:
    """终态运行不能再被改写，取消返回 409。"""
    run, engineer, _ = await _seed_run(session, ExecutionStatus.FAILED)
    status, body = await _post_cancellation(session, run.id, engineer)
    assert status == 409
    assert "终态" in body["detail"]


@pytest.mark.asyncio
async def test_non_member_cannot_cancel(session: AsyncSession) -> None:
    """非本项目成员无权取消，失败关闭。"""
    run, _, outsider = await _seed_run(session, ExecutionStatus.RUNNING)
    status, _ = await _post_cancellation(session, run.id, outsider)
    assert status == 403
