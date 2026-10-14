"""项目与身份相关的 HTTP 路由。课件 §3.6–§3.8。

这一层**只做翻译**：HTTP 请求 ↔ 应用服务的调用与返回。
业务规则（谁能加人、职责怎么映射、状态能不能跳）全在 `application/` 和 `domain/`，
所以换成 gRPC 或者加个新入口时，规则不会被绕过。

四个端点：
- POST /projects/bootstrap                     一次性初始化首位管理员
- GET  /projects/{id}/members                  成员与职责列表
- POST /projects/{id}/members                  管理员添加成员
- POST /projects/{id}/members/{uid}/harness-roles  显式授予 Harness 职责

`DomainError` 由 `main.py` 的 exception_handler 统一翻译成 403/404/409/503，
所以这里看不到任何 HTTPException。
"""

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

# prefix 写死 /projects；settings.api_prefix（/api/v1）由 main.py 挂载时再加一层。
router = APIRouter(prefix="/projects", tags=["projects"])
# 服务类本身无状态（session 每次由调用方传入），所以可以在模块级复用单例。
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
    # 四种失败（开关没开 / 配置里没 Token / 请求没带 Token / Token 不匹配）
    # 合并成同一个 PermissionDenied：不能让调用者靠错误信息探测"开关是不是开着"。
    # compare_digest 而不是 ==，避免逐字节比较的时序侧信道。
    if (
        not settings.bootstrap_enabled
        or configured_token is None
        or bootstrap_token is None
        or not hmac.compare_digest(configured_token.get_secret_value(), bootstrap_token)
    ):
        raise PermissionDenied("首次初始化接口未开启或 Token 无效")
    # 整段放进一个事务：用户、项目、成员关系要么全成，要么全不成，
    # 不能留下"有项目但没管理员"这种没人能授权别人的中间态。
    async with session.begin():
        bind = session.get_bind()
        if bind.dialect.name == "postgresql":
            # 空表无法通过 SELECT FOR UPDATE 建立互斥。事务级 advisory lock 让两个
            # 同时持有正确 Token 的请求也只能有一个创建首个管理员。
            # 这也是测试用 SQLite 时**不会**覆盖到的分支——单测只验"已有项目→409"。
            await session.execute(text("SELECT pg_advisory_xact_lock(1163154259)"))
        existing_project = await session.scalar(select(Project.id).limit(1))
        if existing_project is not None:
            # 用 VersionConflict 而不是 PermissionDenied：Token 是对的，
            # 冲突的是"这个工作区已经被初始化过"这个事实。
            raise VersionConflict("企业工作区已经初始化，首次初始化接口不可重复使用")
        # 按 subject 复用已存在的账号，避免同一个人被建成两个 User 各带一份权限。
        user = await session.scalar(select(User).where(User.subject == request.user_subject))
        if user is None:
            user = User(subject=request.user_subject, display_name=request.user_display_name)
            session.add(user)
            # flush 而不是 commit：把行写进事务拿到数据库生成的 UUID，
            # 但还不提交，好让上面任何一步失败时整段回滚。
            await session.flush()
        project = Project(name=request.project_name)
        session.add(project)
        await session.flush()
        session.add(
            ProjectMembership(project_id=project.id, user_id=user.id, role=ProjectRole.ADMIN)
        )
        await session.flush()
        # 把 V1 的 ADMIN 映射成最小 V2 职责集；注意**不含** SECURITY_APPROVER，
        # 安全审批权必须走下面的显式授予端点。
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
    # 读操作也走权限：member_service.list 内部会 require_project_role，
    # 不是项目成员就拿不到名单。
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
                # 排序保证响应稳定，避免同一份数据两次请求顺序不同。
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
    # 一个事务包住"加成员 + 播初始职责 + 写审计"，三者不能只成功一半。
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
        # 这里直接算 legacy 映射，不去查库：刚创建的成员其职责就是映射结果，
        # 省一次查询；显式授予过的成员要读真实集合时走 GET /members。
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
        # grant 内部依次判：调用者是 ADMIN → 目标是项目成员 → 重复授予幂等 → 写审计。
        await harness_access.grant(
            session,
            project_id=project_id,
            user_id=user_id,
            roles=set(request.roles),
            actor_id=actor,
            trace_id=trace_id,
        )
    # 提交后再读，返回的是**完整**职责集合（含本次之前已有的），
    # 而不是本次请求带了哪几个——避免调用方误以为授予会替换旧集合。
    roles = await harness_access.roles_for(
        session,
        project_id=project_id,
        user_id=user_id,
    )
    return HarnessRoleGrantResponse(
        user_id=user_id,
        roles=sorted(roles, key=lambda value: value.value),
    )
