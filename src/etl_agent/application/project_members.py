import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.harness_access import seed_legacy_role_grants
from etl_agent.application.profiling import require_project_role
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import ResourceNotFound, VersionConflict
from etl_agent.infrastructure.models import AuditEvent, Project, ProjectMembership, User


class ProjectMemberService:
    """管理项目成员与角色，为审批职责分离提供服务端事实来源。"""

    async def list(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        actor_id: uuid.UUID,
    ) -> list[tuple[User, ProjectMembership]]:
        """列出当前项目成员；所有项目角色均可读取姓名和角色，不暴露认证材料。"""
        await require_project_role(
            session,
            project_id,
            actor_id,
            {
                ProjectRole.ADMIN,
                ProjectRole.DATA_ENGINEER,
                ProjectRole.APPROVER,
                ProjectRole.VIEWER,
            },
        )
        rows = (
            await session.execute(
                select(User, ProjectMembership)
                .join(ProjectMembership, ProjectMembership.user_id == User.id)
                .where(ProjectMembership.project_id == project_id)
                .order_by(ProjectMembership.created_at, User.display_name)
            )
        ).all()
        return [(row[0], row[1]) for row in rows]

    async def add(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        actor_id: uuid.UUID,
        subject: str,
        display_name: str,
        role: ProjectRole,
        trace_id: str | None = None,
    ) -> tuple[User, ProjectMembership]:
        """由管理员添加项目成员；重复成员显式报冲突，避免静默改写既有权限。"""
        await require_project_role(
            session,
            project_id,
            actor_id,
            {ProjectRole.ADMIN},
        )
        if await session.get(Project, project_id) is None:
            raise ResourceNotFound("项目不存在")

        normalized_subject = subject.strip().casefold()
        user = await session.scalar(select(User).where(User.subject == normalized_subject))
        if user is None:
            user = User(subject=normalized_subject, display_name=display_name.strip())
            session.add(user)
            await session.flush()
        existing = await session.scalar(
            select(ProjectMembership).where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.user_id == user.id,
            )
        )
        if existing is not None:
            raise VersionConflict("该用户已经是项目成员；角色变更需要独立权限变更流程")

        membership = ProjectMembership(project_id=project_id, user_id=user.id, role=role)
        session.add(membership)
        await session.flush()
        await seed_legacy_role_grants(
            session,
            project_id=project_id,
            user_id=user.id,
            legacy_role=role,
            granted_by=actor_id,
        )
        session.add(
            AuditEvent(
                actor_id=actor_id,
                project_id=project_id,
                action="project_member.added",
                resource_type="ProjectMembership",
                resource_id=membership.id,
                before=None,
                after={
                    "user_id": str(user.id),
                    "subject": user.subject,
                    "display_name": user.display_name,
                    "role": membership.role.value,
                },
                trace_id=trace_id,
            )
        )
        await session.flush()
        return user, membership
