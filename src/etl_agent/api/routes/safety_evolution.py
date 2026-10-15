"""受控安全进化 API（第 14 章）。

这些端点只驱动不可越级的状态机：创建/提交/裁决提案，进入并验证影子与小流量阶段，
签署推广授权或显式回滚。接口不解析、不执行候选正文，也不热修改策略、Prompt 或运行中的任务。
所有写操作都在单个事务内提交，Actor 身份来自统一认证依赖。
"""

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.application.safety_evolution import SafetyEvolutionService
from etl_agent.domain.safety_evolution import (
    SafetyProposalKind,
    SafetyProposalStatus,
    SafetyReleaseStage,
    SafetyReleaseStatus,
    SafetyStageObservation,
)
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import SafetyProposal

router = APIRouter(tags=["safety-evolution"])


class CreateSafetyProposalRequest(BaseModel):
    """创建改进提案的入参；候选正文只作为可离线审查的结构化事实。"""

    kind: SafetyProposalKind
    payload: dict[str, Any] = Field(default_factory=dict)
    title: str | None = Field(default=None, max_length=255)
    rationale: str | None = Field(default=None, max_length=2000)


class DecideSafetyProposalRequest(BaseModel):
    """独立审批人的批准或拒绝决定。"""

    approve: bool
    reason: str | None = Field(default=None, max_length=2000)


class StartCanaryRequest(BaseModel):
    """小流量阶段的比例，服务端再约束到 1%~10%。"""

    percentage: int = Field(ge=1, le=10)


class StageObservationRequest(BaseModel):
    """影子或小流量验证提交的脱敏聚合事实。"""

    sample_count: int = Field(ge=1, le=10_000_000_000)
    p0_p1_regressions: int = Field(default=0, ge=0)
    policy_false_positives: int = Field(default=0, ge=0)
    execution_errors: int = Field(default=0, ge=0)
    notes: list[str] = Field(default_factory=list, max_length=50)


class RollbackRequest(BaseModel):
    """显式回滚原因。"""

    reason: str = Field(min_length=1, max_length=2000)


class SafetyProposalResponse(BaseModel):
    """提案视图：状态与摘要，不回显候选正文中可能的敏感字段。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    kind: SafetyProposalKind
    status: SafetyProposalStatus
    title: str | None
    payload_sha256: str
    created_by: uuid.UUID
    reviewed_by: uuid.UUID | None
    submitted_at: datetime | None
    decided_at: datetime | None


class SafetyReleaseResponse(BaseModel):
    """发布视图：阶段状态、粗阶段、可选签名摘要与观察，不含私钥材料。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    proposal_id: uuid.UUID
    status: SafetyReleaseStatus
    stage: SafetyReleaseStage
    canary_traffic_percent: int | None
    manifest_sha256: str | None
    started_at: datetime | None
    verified_at: datetime | None
    rollback_reason: str | None


@router.post(
    "/safety-proposals",
    response_model=SafetyProposalResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_safety_proposal(
    request: CreateSafetyProposalRequest,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyProposalResponse:
    """创建 DRAFT 提案；候选疑似含凭据时服务失败关闭。"""
    service = SafetyEvolutionService()
    async with session.begin():
        proposal = await service.create_proposal(
            session,
            request.kind,
            request.payload,
            actor,
            title=request.title,
            rationale=request.rationale,
        )
    return SafetyProposalResponse.model_validate(proposal)


@router.get("/safety-proposals", response_model=list[SafetyProposalResponse])
async def list_safety_proposals(
    _actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[SafetyProposalResponse]:
    """按创建时间倒序列出全部改进提案。"""
    proposals = (
        await session.scalars(select(SafetyProposal).order_by(SafetyProposal.created_at.desc()))
    ).all()
    return [SafetyProposalResponse.model_validate(item) for item in proposals]


@router.post("/safety-proposals/{proposal_id}/submit", response_model=SafetyProposalResponse)
async def submit_safety_proposal(
    proposal_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyProposalResponse:
    """由创建者把 DRAFT 提案提交到 IN_REVIEW。"""
    service = SafetyEvolutionService()
    async with session.begin():
        proposal = await service.submit_proposal(session, proposal_id, actor)
    return SafetyProposalResponse.model_validate(proposal)


@router.post("/safety-proposals/{proposal_id}/decide", response_model=SafetyProposalResponse)
async def decide_safety_proposal(
    proposal_id: uuid.UUID,
    request: DecideSafetyProposalRequest,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyProposalResponse:
    """由独立审批人批准或拒绝；创建者裁决自己的提案将被拒绝。"""
    service = SafetyEvolutionService()
    async with session.begin():
        proposal = await service.decide_proposal(
            session, proposal_id, actor, request.approve, reason=request.reason
        )
    return SafetyProposalResponse.model_validate(proposal)


@router.post(
    "/safety-proposals/{proposal_id}/shadow-start",
    response_model=SafetyReleaseResponse,
    status_code=status.HTTP_201_CREATED,
)
async def start_shadow(
    proposal_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyReleaseResponse:
    """把已批准提案签名后进入只读影子阶段。"""
    service = SafetyEvolutionService()
    async with session.begin():
        release = await service.start_shadow(session, proposal_id, actor_id=actor)
    return SafetyReleaseResponse.model_validate(release)


@router.post("/safety-proposals/{proposal_id}/shadow-verify", response_model=SafetyReleaseResponse)
async def verify_shadow(
    proposal_id: uuid.UUID,
    request: StageObservationRequest,
    _actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyReleaseResponse:
    """记录影子结果；命中回归、误报越线或执行错误会自动回滚。"""
    service = SafetyEvolutionService()
    async with session.begin():
        release = await service.verify_shadow(
            session, proposal_id, SafetyStageObservation.model_validate(request.model_dump())
        )
    return SafetyReleaseResponse.model_validate(release)


@router.post("/safety-proposals/{proposal_id}/canary-start", response_model=SafetyReleaseResponse)
async def start_canary(
    proposal_id: uuid.UUID,
    request: StartCanaryRequest,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyReleaseResponse:
    """把影子通过的候选提升到受控小流量阶段。"""
    service = SafetyEvolutionService()
    async with session.begin():
        release = await service.start_canary(
            session, proposal_id, request.percentage, actor_id=actor
        )
    return SafetyReleaseResponse.model_validate(release)


@router.post("/safety-proposals/{proposal_id}/canary-verify", response_model=SafetyReleaseResponse)
async def verify_canary(
    proposal_id: uuid.UUID,
    request: StageObservationRequest,
    _actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyReleaseResponse:
    """记录小流量结果；失败即自动撤销候选授权。"""
    service = SafetyEvolutionService()
    async with session.begin():
        release = await service.verify_canary(
            session, proposal_id, SafetyStageObservation.model_validate(request.model_dump())
        )
    return SafetyReleaseResponse.model_validate(release)


@router.post("/safety-proposals/{proposal_id}/promote", response_model=SafetyReleaseResponse)
async def promote(
    proposal_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyReleaseResponse:
    """签署推广授权；该动作不修改代码、策略文件或运行中的任务。"""
    service = SafetyEvolutionService()
    async with session.begin():
        release = await service.approve_promotion(session, proposal_id, actor)
    return SafetyReleaseResponse.model_validate(release)


@router.post("/safety-proposals/{proposal_id}/rollback", response_model=SafetyReleaseResponse)
async def rollback(
    proposal_id: uuid.UUID,
    request: RollbackRequest,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SafetyReleaseResponse:
    """显式撤销候选授权，同时保留失败历史。"""
    service = SafetyEvolutionService()
    async with session.begin():
        release = await service.rollback(session, proposal_id, request.reason, actor_id=actor)
    return SafetyReleaseResponse.model_validate(release)


__all__ = ["SafetyProposalResponse", "SafetyReleaseResponse", "router"]
