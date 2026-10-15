"""AgentRun API：自然语言需求驱动的生成任务入口（第 8 章）。

五个端点：
- POST /projects/{project_id}/agent-runs   排队一次生成任务（202，Agent 图由 Worker 接管）
- GET  /projects/{project_id}/agent-runs   列出项目最近的生成任务
- GET  /agent-runs/{run_id}                读取单个任务
- GET  /agent-runs/{run_id}/steps          LangGraph 节点迁移轨迹（checkpoint 尚未持久化，
  先按 run 状态推导 mock 步骤，第 8 章 Checkpoint 后端落地后替换为真实读取）
- POST /agent-runs/{run_id}/answers        提交澄清答案（WAITING_INPUT → QUEUED 回边）

权限：读操作要求项目内任一成员角色；发起任务与提交澄清答案要求 ADMIN/DATA_ENGINEER。
"""

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.agent_tasks import run_agent_generation
from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    AgentRunAnswerRequest,
    AgentRunCreateRequest,
    AgentRunResponse,
)
from etl_agent.application.agent_runs import AgentRunService
from etl_agent.application.generation_context import build_generation_context
from etl_agent.application.profiling import require_project_role
from etl_agent.domain.enums import AgentRunStatus, ProjectRole
from etl_agent.domain.errors import ResourceNotFound, VersionConflict
from etl_agent.domain.state_machine import require_agent_run_transition
from etl_agent.infrastructure.agent_run_model import AgentRun
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.generation_factory import create_generation_adapter

router = APIRouter(tags=["agent-runs"])

_VIEWER_ROLES = {
    ProjectRole.ADMIN,
    ProjectRole.DATA_ENGINEER,
    ProjectRole.APPROVER,
    ProjectRole.VIEWER,
}
_ENGINEER_ROLES = {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER}

# LangGraph 生成图的节点顺序；mock 步骤按 run 状态推导到这里为止的位置。
_AGENT_NODES = ["intake", "context_build", "plan", "generate", "validate", "await_approval"]
_NODE_PROGRESS: dict[AgentRunStatus, int] = {
    AgentRunStatus.QUEUED: 0,
    AgentRunStatus.RUNNING: 3,
    AgentRunStatus.WAITING_INPUT: 3,
    AgentRunStatus.WAITING_APPROVAL: 5,
    AgentRunStatus.FAILED: 3,
}


@router.post(
    "/projects/{project_id}/agent-runs",
    response_model=AgentRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_agent_run(
    project_id: uuid.UUID,
    request: AgentRunCreateRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> AgentRunResponse:
    """创建 QUEUED 状态的 AgentRun；实际图执行由 Worker 领取，API 不阻塞等待生成。

    ``metadata_context`` 必须在建 run 之前就带上完整的 ``profiles`` 描述符：policy 节点
    的确定性门禁与模型提示都读它。若只落 ``profile_ids``，门禁会按"零个端点"校验，
    永远产不出被接受的候选。
    """
    service = AgentRunService(create_generation_adapter())
    async with session.begin():
        # 角色校验与写入共用一个事务；先查后写，非法角色在落库前被挡下。
        await require_project_role(session, project_id, actor, _ENGINEER_ROLES)
        profiles = await build_generation_context(session, request.profile_ids)
        metadata_context: dict[str, Any] = {
            "profiles": profiles,
            "profile_ids": [str(profile_id) for profile_id in request.profile_ids],
            "trace_id": request.trace_id or trace_id,
        }
        run = await service.start(
            session,
            project_id=project_id,
            requirement=request.requirement,
            metadata_context=metadata_context,
            created_by=actor,
        )
        response = AgentRunResponse.model_validate(run)
    # 事务提交后再投递生成任务：Worker 一定读得到刚建的 run 行，避免"入队早于可见"。
    # 之前这里只落库不投递，AgentRun 会永远停在 QUEUED，没有任何 Worker 会领。
    run_agent_generation.delay(str(run.id))
    return response


@router.get(
    "/projects/{project_id}/agent-runs",
    response_model=list[AgentRunResponse],
)
async def list_agent_runs(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[AgentRunResponse]:
    """列出项目最近一百次生成任务，按创建时间倒序。"""
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    runs = (
        await session.scalars(
            select(AgentRun)
            .where(AgentRun.project_id == project_id)
            .order_by(AgentRun.created_at.desc())
            .limit(100)
        )
    ).all()
    return [AgentRunResponse.model_validate(run) for run in runs]


@router.get("/agent-runs/{run_id}", response_model=AgentRunResponse)
async def get_agent_run(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> AgentRunResponse:
    """读取单个 AgentRun，供前端轮询 WAITING_APPROVAL/FAILED 等终态信号。"""
    run = await _load_run(session, run_id)
    await require_project_role(session, run.project_id, actor, _VIEWER_ROLES)
    return AgentRunResponse.model_validate(run)


@router.post("/agent-runs/{run_id}/answers", response_model=AgentRunResponse)
async def answer_agent_run(
    run_id: uuid.UUID,
    request: AgentRunAnswerRequest,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> AgentRunResponse:
    """提交澄清答案，把 WAITING_INPUT 的 run 重新排队给 Worker。

    流程：角色校验 → 行锁读取 run → 校验当前必须是 WAITING_INPUT → 走领域状态机回边
    WAITING_INPUT→QUEUED → 清空 error_message → 把答案追加进
    ``metadata_context["conversation"]`` 并**整体重新赋值**（JSON 列不会检测原地修改）→
    提交事务 → 投递生成任务。这样下次跑图时模型能看到"追问 + 回答"的上下文。
    """
    async with session.begin():
        # 先按项目范围校验权限，再进入需要行锁的状态变更，避免越权者探测 run 状态。
        scope = await _load_run(session, run_id)
        await require_project_role(session, scope.project_id, actor, _ENGINEER_ROLES)
        run = await _load_run_locked(session, run_id)
        if run.status != AgentRunStatus.WAITING_INPUT:
            raise VersionConflict("AgentRun 当前不在等待澄清输入状态，无法提交答案")
        require_agent_run_transition(AgentRunStatus.WAITING_INPUT, AgentRunStatus.QUEUED)
        run.status = AgentRunStatus.QUEUED
        run.error_message = None
        metadata_context = dict(run.metadata_context or {})
        conversation = list(metadata_context.get("conversation", []))
        conversation.append({"role": "user", "content": [{"type": "text", "text": request.answer}]})
        metadata_context["conversation"] = conversation
        # JSON 列只在整体赋值时才被 SQLAlchemy 记为变更，必须重新绑定新 dict。
        run.metadata_context = metadata_context
        await session.flush()
    # 事务提交后再投递：Worker 一定读得到 QUEUED 的 run 与刚追加的答案。
    run_agent_generation.delay(str(run_id))
    return AgentRunResponse.model_validate(run)


@router.get("/agent-runs/{run_id}/steps")
async def list_agent_run_steps(
    run_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """返回 LangGraph 节点迁移轨迹。

    Checkpoint 持久化（PostgresSaver）尚未落地，这里先根据 run 状态推导 mock 步骤；
    等第 8 章 checkpoint 接通后，本端点改为读取真实的节点转换记录。
    """
    run = await _load_run(session, run_id)
    await require_project_role(session, run.project_id, actor, _VIEWER_ROLES)
    # 数据库列是 String(32)，重载后可能是纯字符串；先归一化为枚举再做集合/字典判断。
    status_ = AgentRunStatus(run.status)
    reached = _NODE_PROGRESS.get(status_, 0)
    terminal = status_ in {AgentRunStatus.WAITING_APPROVAL, AgentRunStatus.FAILED}
    steps: list[dict[str, Any]] = []
    for index, node in enumerate(_AGENT_NODES):
        if index < reached:
            step_status = "SUCCEEDED"
        elif index == reached and not terminal:
            step_status = "RUNNING"
        else:
            step_status = "PENDING"
        steps.append({"step": index + 1, "node": node, "status": step_status})
    return steps


async def _load_run(session: AsyncSession, run_id: uuid.UUID) -> AgentRun:
    """按 ID 读取 AgentRun；不存在时统一返回 404 语义。"""
    run = await session.get(AgentRun, run_id)
    if run is None:
        raise ResourceNotFound("AgentRun 不存在")
    return run


async def _load_run_locked(session: AsyncSession, run_id: uuid.UUID) -> AgentRun:
    """带行锁读取 AgentRun，串行化同一 run 的并发状态变更。"""
    run = await session.scalar(select(AgentRun).where(AgentRun.id == run_id).with_for_update())
    if run is None:
        raise ResourceNotFound("AgentRun 不存在")
    return run
