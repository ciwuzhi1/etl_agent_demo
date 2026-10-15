"""执行 Harness API：Prepare、职责审批与 Commit（第 11 章）。

端点：
- POST /pipeline-versions/{version_id}/execution-preparations  冻结执行意图 + PDP 裁决
- POST /execution-preparations/{preparation_id}/decisions      具名 Harness 职责审批槽
- POST /execution-preparations/{preparation_id}/commit         签发一次性能力并落排队 Run

安全边界：
* 项目与被调用者身份一律由服务端从 Pipeline 版本回溯并强制写入 ``ToolIntent``，
  请求体只能影响工具名与副作用，不能伪造 ``actor_id`` / ``project_id``；
* 审批前用 ``HarnessAccessService.require_any_role`` 校验调用者确实持有该职责，
  同一自然人不能自批由服务层（``ExecutionHarnessService``）保证；
* Commit 只对 ``OPERATOR`` 开放，响应只回排队中的执行记录 ID，绝不回显令牌明文；
* 所有写操作落在 ``async with session.begin():`` 单事务内，PDP 拒绝时整体回滚。
"""

import uuid
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    CommitPreparationResponse,
    DecidePreparationRequest,
    ExecutionPreparationResponse,
    PrepareExecutionRequest,
)
from etl_agent.application.execution_harness import ExecutionHarnessService
from etl_agent.application.harness_access import HarnessAccessService
from etl_agent.application.harness_factory import build_execution_harness
from etl_agent.application.profiling import require_project_role
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import DomainError, ResourceNotFound
from etl_agent.domain.harness import (
    DataClassification,
    HarnessRole,
    ResourceScope,
    ToolEffect,
    ToolIntent,
)
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import ExecutionPreparation, Pipeline, PipelineVersion

router = APIRouter(tags=["execution-preparations"])

harness_access = HarnessAccessService()

_PREPARE_ROLES = {ProjectRole.ADMIN, ProjectRole.DATA_ENGINEER}
_DEFAULT_TOOL_NAME = "seatunnel.execute_pipeline"
_DEFAULT_ACTION = "execute"
_DEFAULT_EFFECTS = (ToolEffect.SECRET_USE, ToolEffect.WRITE)


@lru_cache
def get_execution_harness_service() -> ExecutionHarnessService:
    """进程内共享同一签发密钥的执行 Harness 服务，避免每次请求重建签名字。"""
    return build_execution_harness()


@router.post(
    "/pipeline-versions/{version_id}/execution-preparations",
    response_model=ExecutionPreparationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def prepare_execution(
    version_id: uuid.UUID,
    request: PrepareExecutionRequest,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> ExecutionPreparationResponse:
    """为已审批版本冻结执行事实并调用 PDP；项目与调用者身份由服务端解析。"""
    service = get_execution_harness_service()
    async with session.begin():
        project_id = await _project_id_of_version(session, version_id)
        await require_project_role(session, project_id, actor, _PREPARE_ROLES)
        intent = _build_intent(
            request.intent, project_id=project_id, actor_id=actor, version_id=version_id
        )
        preparation_id = await service.prepare(
            session,
            project_id=str(project_id),
            pipeline_version_id=str(version_id),
            intent=intent,
            requested_by=str(actor),
        )
        preparation = await _load_preparation(session, uuid.UUID(preparation_id))
        response = ExecutionPreparationResponse.model_validate(preparation)
    return response


@router.post(
    "/execution-preparations/{preparation_id}/decisions",
    response_model=ExecutionPreparationResponse,
)
async def decide_execution_preparation(
    preparation_id: uuid.UUID,
    request: DecidePreparationRequest,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> ExecutionPreparationResponse:
    """占用一个职责审批槽并返回准备单最新状态；调用者必须实际持有该 Harness 职责。"""
    service = get_execution_harness_service()
    async with session.begin():
        preparation = await _load_preparation(session, preparation_id)
        await harness_access.require_any_role(
            session,
            project_id=preparation.project_id,
            user_id=actor,
            allowed={request.role},
        )
        await service.decide(
            session,
            str(preparation_id),
            str(actor),
            request.role.value,
            request.decision.value,
            request.reason,
        )
        response = ExecutionPreparationResponse.model_validate(preparation)
    return response


@router.post(
    "/execution-preparations/{preparation_id}/commit",
    response_model=CommitPreparationResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def commit_execution_preparation(
    preparation_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> CommitPreparationResponse:
    """签发一次性能力令牌并在同一事务创建排队 Run；仅 OPERATOR 职责可提交。"""
    service = get_execution_harness_service()
    async with session.begin():
        preparation = await _load_preparation(session, preparation_id)
        await harness_access.require_any_role(
            session,
            project_id=preparation.project_id,
            user_id=actor,
            allowed={HarnessRole.OPERATOR},
        )
        execution_run_id = await service.commit(
            session,
            str(preparation_id),
            str(actor),
        )
    return CommitPreparationResponse(execution_run_id=execution_run_id)


async def _load_preparation(
    session: AsyncSession, preparation_id: uuid.UUID
) -> ExecutionPreparation:
    """按 ID 读取执行准备单，缺失时返回 404 语义。"""
    preparation = await session.get(ExecutionPreparation, preparation_id)
    if preparation is None:
        raise ResourceNotFound("执行准备单不存在")
    return preparation


async def _project_id_of_version(session: AsyncSession, version_id: uuid.UUID) -> uuid.UUID:
    """经 PipelineVersion -> Pipeline 回溯版本所属项目，缺失即 404。"""
    project_id = await session.scalar(
        select(Pipeline.project_id)
        .join(PipelineVersion, PipelineVersion.pipeline_id == Pipeline.id)
        .where(PipelineVersion.id == version_id)
    )
    if project_id is None:
        raise ResourceNotFound("Pipeline 版本不存在")
    return project_id


def _build_intent(
    payload: dict[str, Any] | None,
    *,
    project_id: uuid.UUID,
    actor_id: uuid.UUID,
    version_id: uuid.UUID,
) -> ToolIntent:
    """构造工具意图：缺省时用固定执行契约，显式提供时仍强制服务端身份与项目。

    客户端只能影响工具名、动作与副作用等“意图描述”，``project_id`` / ``actor_id``
    以及 ResourceScope 的项目归属一律以服务端解析结果为准，避免伪造身份或跨项目
    资源范围。
    """
    if payload is None:
        return ToolIntent(
            tool_name=_DEFAULT_TOOL_NAME,
            action=_DEFAULT_ACTION,
            project_id=project_id,
            actor_id=actor_id,
            resource_scopes=[
                ResourceScope(
                    project_id=project_id,
                    resource_type="pipeline_version",
                    resource_id=str(version_id),
                )
            ],
            effects=list(_DEFAULT_EFFECTS),
            bounded=True,
        )
    return ToolIntent(
        tool_name=str(payload.get("tool_name") or _DEFAULT_TOOL_NAME),
        action=str(payload.get("action") or _DEFAULT_ACTION),
        project_id=project_id,
        actor_id=actor_id,
        resource_scopes=[
            ResourceScope(
                project_id=project_id,
                resource_type=_optional_str(scope.get("resource_type")),
                resource_id=_optional_str(scope.get("resource_id")),
            )
            for scope in _dict_items(payload.get("resource_scopes"), "resource_scopes")
        ],
        effects=_coerce_enum(payload.get("effects"), ToolEffect, "intent.effects"),
        data_classifications=_coerce_enum(
            payload.get("data_classifications"),
            DataClassification,
            "intent.data_classifications",
        ),
        external_destination=_optional_str(payload.get("external_destination")),
        bounded=bool(payload.get("bounded", True)),
        artifact_sha256=_optional_str(payload.get("artifact_sha256")),
        metadata=_coerce_metadata(payload.get("metadata")),
    )


def _dict_items(raw: Any, label: str) -> list[dict[str, Any]]:
    """把请求体中的对象数组规整化；非法结构直接失败关闭，不猜测调用方意图。"""
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise DomainError(f"intent.{label} 必须是对象数组")
    items: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise DomainError(f"intent.{label} 的每一项都必须是对象")
        items.append({str(key): value for key, value in item.items()})
    return items


def _coerce_enum(raw: Any, enum_type: Any, label: str) -> list[Any]:
    """把请求体中的字符串列表转换为领域枚举；非法取值返回 400 而不是 500。"""
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise DomainError(f"{label} 必须是字符串数组")
    try:
        return [enum_type(str(item)) for item in raw]
    except ValueError as exc:
        raise DomainError(f"{label} 含非法取值") from exc


def _coerce_metadata(raw: Any) -> dict[str, Any]:
    """只接受键值对形式的元数据；其它形态按空对象处理，不把任意载荷带进意图。"""
    if not isinstance(raw, dict):
        return {}
    return {str(key): value for key, value in raw.items()}


def _optional_str(value: Any) -> str | None:
    """把可选字段规整为字符串或 None，避免 None 被渲染成字面量 'None'。"""
    return None if value is None else str(value)
