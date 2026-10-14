import hmac
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import (
    BootstrapProjectRequest,
    BootstrapProjectResponse,
    CreateProjectMemberRequest,
    GrantHarnessRolesRequest,
    HarnessRoleGrantResponse,
    ProjectMemberResponse,
)
from etl_agent.application.harness_access import (
    HarnessAccessService,
    legacy_harness_roles,
    seed_legacy_role_grants,
)
from etl_agent.application.project_members import ProjectMemberService
from etl_agent.config import Settings, get_settings
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import PermissionDenied, VersionConflict
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import Project, ProjectMembership, User

router = APIRouter(prefix="/projects", tags=["projects"])
member_service = ProjectMemberService()
harness_access = HarnessAccessService()


@router.post(
    "/bootstrap", response_model=BootstrapProjectResponse, status_code=status.HTTP_201_CREATED
)
async def bootstrap_project(
    request: BootstrapProjectRequest,
    bootstrap_token: Annotated[str | None, Header(alias="X-Bootstrap-Token")] = None,
    settings: Settings = Depends(get_settings),
    session: AsyncSession = Depends(get_session),
) -> BootstrapProjectResponse:
    """使用一次性运维窗口初始化首个管理员；已有项目时永久拒绝再次执行。"""
    configured_token = settings.bootstrap_token
    if (
        not settings.bootstrap_enabled
        or configured_token is None
        or bootstrap_token is None
        or not hmac.compare_digest(configured_token.get_secret_value(), bootstrap_token)
    ):
        raise PermissionDenied("首次初始化接口未开启或 Token 无效")
    async with session.begin():
        bind = session.get_bind()
        if bind.dialect.name == "postgresql":
            # 空表无法通过 SELECT FOR UPDATE 建立互斥。事务级 advisory lock 让两个
            # 同时持有正确 Token 的请求也只能有一个创建首个管理员。
            await session.execute(text("SELECT pg_advisory_xact_lock(1163154259)"))
        existing_project = await session.scalar(select(Project.id).limit(1))
        if existing_project is not None:
            raise VersionConflict("企业工作区已经初始化，首次初始化接口不可重复使用")
        user = await session.scalar(select(User).where(User.subject == request.user_subject))
        if user is None:
            user = User(subject=request.user_subject, display_name=request.user_display_name)
            session.add(user)
            await session.flush()
        project = Project(name=request.project_name)
        session.add(project)
        await session.flush()
        session.add(
            ProjectMembership(project_id=project.id, user_id=user.id, role=ProjectRole.ADMIN)
        )
        await session.flush()
        await seed_legacy_role_grants(
            session,
            project_id=project.id,
            user_id=user.id,
            legacy_role=ProjectRole.ADMIN,
            granted_by=user.id,
        )
    # Token 只存在于请求内存，不写入审计、响应或浏览器持久化状态。
    return BootstrapProjectResponse(project_id=project.id, actor_id=user.id)


@router.get("/{project_id}/members", response_model=list[ProjectMemberResponse])
async def list_project_members(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[ProjectMemberResponse]:
    """返回项目成员与职责角色，供控制台展示当前操作身份和审批交接。"""
    rows = await member_service.list(session, project_id=project_id, actor_id=actor)
    responses: list[ProjectMemberResponse] = []
    for user, membership in rows:
        roles = await harness_access.roles_for(
            session,
            project_id=project_id,
            user_id=user.id,
        )
        responses.append(
            ProjectMemberResponse(
                user_id=user.id,
                subject=user.subject,
                display_name=user.display_name,
                role=membership.role,
                harness_roles=sorted(roles, key=lambda value: value.value),
            )
        )
    return responses


@router.post(
    "/{project_id}/members",
    response_model=ProjectMemberResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_project_member(
    project_id: uuid.UUID,
    request: CreateProjectMemberRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> ProjectMemberResponse:
    """由项目管理员添加具名成员；成员身份用于后续独立审批和审计归属。"""
    async with session.begin():
        user, membership = await member_service.add(
            session,
            project_id=project_id,
            actor_id=actor,
            subject=request.subject,
            display_name=request.display_name,
            role=request.role,
            trace_id=trace_id,
        )
    return ProjectMemberResponse(
        user_id=user.id,
        subject=user.subject,
        display_name=user.display_name,
        role=membership.role,
        harness_roles=sorted(legacy_harness_roles(membership.role), key=lambda value: value.value),
    )


@router.post(
    "/{project_id}/members/{user_id}/harness-roles",
    response_model=HarnessRoleGrantResponse,
)
async def grant_harness_roles(
    project_id: uuid.UUID,
    user_id: uuid.UUID,
    request: GrantHarnessRolesRequest,
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> HarnessRoleGrantResponse:
    """由项目管理员显式授予职责；管理员身份本身不会获得安全审批能力。"""
    async with session.begin():
        await harness_access.grant(
            session,
            project_id=project_id,
            user_id=user_id,
            roles=set(request.roles),
            actor_id=actor,
            trace_id=trace_id,
        )
    roles = await harness_access.roles_for(
        session,
        project_id=project_id,
        user_id=user_id,
    )
    return HarnessRoleGrantResponse(
        user_id=user_id,
        roles=sorted(roles, key=lambda value: value.value),
    )
