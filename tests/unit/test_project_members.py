"""项目成员与角色边界的验收测试。

三个用例测的都是**失败路径**和它的副作用：
- 管理员加审批人 → 成功，且必须落下审计事件；
- 重复添加并试图改角色 → VersionConflict（不能借这个接口静默提权）；
- 非管理员加人 → PermissionDenied。

这里直接调应用服务、不走 HTTP：测的是权限规则本身，套一层 ASGI 只会让失败更难定位。
HTTP 层的映射由 test_project_bootstrap.py 和 main.py 的异常处理器负责。
"""

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.project_members import ProjectMemberService
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import PermissionDenied, VersionConflict
from etl_agent.infrastructure.models import AuditEvent, Project, ProjectMembership, User


async def _seed_admin(session: AsyncSession) -> tuple[Project, User]:
    """创建带唯一管理员的项目，供成员权限边界测试复用。"""
    # 名字带 uuid：每个用例一套独立数据，避免唯一约束互相撞。
    project = Project(name=f"members-{uuid.uuid4()}")
    admin = User(subject=f"admin-{uuid.uuid4()}", display_name="项目管理员")
    session.add_all([project, admin])
    await session.flush()
    session.add(ProjectMembership(project_id=project.id, user_id=admin.id, role=ProjectRole.ADMIN))
    await session.commit()
    return project, admin


@pytest.mark.asyncio
async def test_admin_adds_named_approver_and_audit_event(session: AsyncSession) -> None:
    """验证管理员可添加独立审批人，并为权限变化留下结构化审计记录。"""
    project, admin = await _seed_admin(session)

    # 故意传 "Approver@Company.Local"：服务端要归一化成小写去空格再落库，
    # 否则 Admin@x.com 和 admin@x.com 会变成两个账号各带一份权限。
    user, membership = await ProjectMemberService().add(
        session,
        project_id=project.id,
        actor_id=admin.id,
        subject="Approver@Company.Local",
        display_name="数据审批人",
        role=ProjectRole.APPROVER,
    )
    await session.commit()

    assert user.subject == "approver@company.local"
    assert membership.role is ProjectRole.APPROVER
    # 审计不是副产品，是被测行为：权限变更没有审计记录就等于没被管住。
    event = await session.scalar(select(AuditEvent).where(AuditEvent.resource_id == membership.id))
    assert event is not None
    assert event.action == "project_member.added"


@pytest.mark.asyncio
async def test_existing_member_role_cannot_be_silently_overwritten(session: AsyncSession) -> None:
    """验证重复添加不能被滥用为无审批的角色变更通道。"""
    project, admin = await _seed_admin(session)
    service = ProjectMemberService()
    await service.add(
        session,
        project_id=project.id,
        actor_id=admin.id,
        subject="member@company.local",
        display_name="项目成员",
        role=ProjectRole.VIEWER,
    )
    await session.commit()

    # 第二次用同一个人但换成 APPROVER：如果实现把"重复添加"当成"更新角色"，
    # 管理员就能绕过审批流程自行提权，四眼分离当场失效。
    with pytest.raises(VersionConflict, match="角色变更"):
        await service.add(
            session,
            project_id=project.id,
            actor_id=admin.id,
            subject="member@company.local",
            display_name="项目成员",
            role=ProjectRole.APPROVER,
        )


@pytest.mark.asyncio
async def test_non_admin_cannot_add_project_member(session: AsyncSession) -> None:
    """验证普通查看者不能自行创建审批身份。"""
    project, admin = await _seed_admin(session)
    viewer = User(subject=f"viewer-{uuid.uuid4()}", display_name="查看者")
    session.add(viewer)
    await session.flush()
    session.add(
        ProjectMembership(project_id=project.id, user_id=viewer.id, role=ProjectRole.VIEWER)
    )
    await session.commit()

    # VIEWER 想给自己项目里加一个 APPROVER——必须被服务端拒掉，而不是靠前端藏按钮。
    with pytest.raises(PermissionDenied):
        await ProjectMemberService().add(
            session,
            project_id=project.id,
            actor_id=viewer.id,
            subject="fake-approver@company.local",
            display_name="伪造审批人",
            role=ProjectRole.APPROVER,
        )

    # 保留管理员引用，避免测试夹具被误读成没有合法管理员的项目。
    assert admin.id is not None
