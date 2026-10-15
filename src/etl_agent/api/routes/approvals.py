"""审批 API：提交、查询与四眼分离决策（第 9 章）。

端点：
- POST /pipeline-versions/{version_id}/approvals           提交审批（版本进入 WAITING_APPROVAL）
- GET  /pipeline-versions/{version_id}/approvals/latest     最近一张审批单（页面刷新恢复上下文）
- POST /approvals/{approval_id}/decisions                  APPROVE/REJECT 决策

角色边界全部由 ApprovalService 在服务层强制执行：
提交者不能有 APPROVER 之外的越权写入，决策人不能与提交人相同。
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    ApprovalDecisionRequest,
    ApprovalRequestResponse,
    ApprovalSubmitRequest,
)
from etl_agent.application.approvals import ApprovalService
from etl_agent.domain.errors import ResourceNotFound
from etl_agent.infrastructure.database import get_session

router = APIRouter(tags=["approvals"])

approval_service = ApprovalService()


@router.post(
    "/pipeline-versions/{version_id}/approvals",
    response_model=ApprovalRequestResponse,
    status_code=status.HTTP_201_CREATED,
)
async def submit_approval(
    version_id: uuid.UUID,
    request: ApprovalSubmitRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> ApprovalRequestResponse:
    """把已冻结产物（有 SHA-256 摘要）的版本推进到 WAITING_APPROVAL 并建审批单。"""
    async with session.begin():
        approval = await approval_service.submit(
            session,
            version_id=version_id,
            actor_id=actor,
            reason=request.reason,
            trace_id=trace_id,
        )
        response = ApprovalRequestResponse.model_validate(approval)
    return response


@router.get(
    "/pipeline-versions/{version_id}/approvals/latest",
    response_model=ApprovalRequestResponse,
)
async def latest_approval(
    version_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> ApprovalRequestResponse:
    """读取版本最近一张审批单；从未提交过时返回 404。"""
    approval = await approval_service.latest_for_version(
        session, version_id=version_id, actor_id=actor
    )
    if approval is None:
        raise ResourceNotFound("该版本尚未提交审批")
    return ApprovalRequestResponse.model_validate(approval)


@router.post(
    "/approvals/{approval_id}/decisions",
    response_model=ApprovalRequestResponse,
)
async def decide_approval(
    approval_id: uuid.UUID,
    request: ApprovalDecisionRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> ApprovalRequestResponse:
    """审批决策：绑定产物哈希、拒绝重复决策、强制审批人与提交人分离。"""
    async with session.begin():
        approval = await approval_service.decide(
            session,
            approval_id=approval_id,
            actor_id=actor,
            decision=request.decision,
            reason=request.reason,
            trace_id=trace_id,
        )
        response = ApprovalRequestResponse.model_validate(approval)
    return response
