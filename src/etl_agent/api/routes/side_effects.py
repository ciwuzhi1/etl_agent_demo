"""副作用治理 API：Prepare、审批与 Commit（第 12 章）。

端点：
- POST /projects/{project_id}/side-effect-preparations      冻结事实 + PDP 裁决
- GET  /projects/{project_id}/side-effect-preparations      项目副作用门禁列表
- GET  /side-effect-preparations/{preparation_id}           单个 Preparation
- POST /side-effect-preparations/{preparation_id}/decisions 具名职责审批槽
- POST /side-effect-preparations/{preparation_id}/commit    签发一次性能力并投递 Outbox

角色边界由 SideEffectHarnessService 在服务层强制执行：申请人不能自批，同一
自然人不能占多个职责槽，审批不齐不能 Commit。响应只回状态与摘要，不含冻结
事实正文或任何令牌材料。
"""

import uuid
from functools import lru_cache
from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    CreateSideEffectPreparationRequest,
    SideEffectApprovalRequest,
    SideEffectCommitResponse,
    SideEffectPreparationResponse,
)
from etl_agent.application.harness_policy import EmbeddedPolicyDecisionPoint
from etl_agent.application.side_effect_harness import (
    SideEffectHarnessService,
    load_signer,
)
from etl_agent.config import get_settings
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.harness_signing import CapabilityTokenIssuer
from etl_agent.side_effect_tasks import execute_side_effect

router = APIRouter(tags=["side-effects"])


@lru_cache
def get_side_effect_harness_service() -> SideEffectHarnessService:
    """按配置装配签名与策略组件；进程内共享同一签发密钥。"""
    settings = get_settings()
    signer = load_signer(
        harness_private_key=(
            settings.harness_private_key.get_secret_value()
            if settings.harness_private_key is not None
            else None
        ),
        key_id=settings.harness_signing_key_id,
        dev_key_path=settings.harness_dev_key_path,
    )
    return SideEffectHarnessService(
        pdp=EmbeddedPolicyDecisionPoint(),
        token_issuer=CapabilityTokenIssuer(signer, settings.harness_signing_key_id),
        environment=settings.environment,
        policy_bundle_version=settings.harness_policy_bundle_version,
        preparation_ttl_seconds=settings.harness_preparation_ttl_seconds,
        capability_ttl_seconds=settings.harness_capability_ttl_seconds,
    )


@router.post(
    "/projects/{project_id}/side-effect-preparations",
    response_model=SideEffectPreparationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def prepare_side_effect(
    project_id: uuid.UUID,
    request: CreateSideEffectPreparationRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> SideEffectPreparationResponse:
    """冻结副作用事实并调用 PDP；策略拒绝时返回 403 且不留可提交状态。"""
    service = get_side_effect_harness_service()
    async with session.begin():
        preparation = await service.prepare(
            session,
            project_id=project_id,
            operation=request.operation,
            aggregate_id=request.aggregate_id,
            context=request.context,
            requested_by=actor,
            idempotency_key=request.idempotency_key,
            trace_id=trace_id,
        )
        response = SideEffectPreparationResponse.model_validate(preparation)
    return response


@router.get(
    "/projects/{project_id}/side-effect-preparations",
    response_model=list[SideEffectPreparationResponse],
)
async def list_side_effect_preparations(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[SideEffectPreparationResponse]:
    """按新到旧列出项目副作用 Preparation，供控制台审查统一门禁。"""
    service = get_side_effect_harness_service()
    preparations = await service.list_for_project(
        session,
        project_id=project_id,
        actor_id=actor,
    )
    return [SideEffectPreparationResponse.model_validate(item) for item in preparations]


@router.get(
    "/side-effect-preparations/{preparation_id}",
    response_model=SideEffectPreparationResponse,
)
async def get_side_effect_preparation(
    preparation_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> SideEffectPreparationResponse:
    """读取单个副作用 Preparation；非项目成员返回 403。"""
    service = get_side_effect_harness_service()
    preparation = await service.get(session, preparation_id=preparation_id, actor_id=actor)
    return SideEffectPreparationResponse.model_validate(preparation)


@router.post(
    "/side-effect-preparations/{preparation_id}/decisions",
    response_model=SideEffectPreparationResponse,
)
async def decide_side_effect(
    preparation_id: uuid.UUID,
    request: SideEffectApprovalRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> SideEffectPreparationResponse:
    """占用一个职责审批槽；全部必需职责批准后 Preparation 进入 READY。"""
    service = get_side_effect_harness_service()
    async with session.begin():
        preparation = await service.decide(
            session,
            preparation_id=preparation_id,
            actor_id=actor,
            role=request.role,
            decision=request.decision,
            reason=request.reason,
            trace_id=trace_id,
        )
        response = SideEffectPreparationResponse.model_validate(preparation)
    return response


@router.post(
    "/side-effect-preparations/{preparation_id}/commit",
    response_model=SideEffectCommitResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def commit_side_effect(
    preparation_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> SideEffectCommitResponse:
    """签发一次性能力令牌并在同一事务创建 Outbox 命令；外部执行由 Worker 完成。"""
    service = get_side_effect_harness_service()
    async with session.begin():
        result = await service.commit(
            session,
            preparation_id=preparation_id,
            committed_by=actor,
            trace_id=trace_id,
        )
    # 事务提交后再投递：命令此时才对外可见，避免 Worker 读到尚未提交的 Outbox 行。
    # command_id 即 commit 返回的 execution_id（Outbox 行 ID），与 Worker 回执口径一致。
    execute_side_effect.delay(result.execution_id)
    return SideEffectCommitResponse(
        execution_id=result.execution_id,
        preparation_id=result.preparation_id,
        token_id=result.token_id,
        command_payload_sha256=result.command_payload_sha256,
    )
