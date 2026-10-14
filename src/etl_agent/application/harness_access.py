import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.profiling import require_project_role
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import PermissionDenied, ResourceNotFound
from etl_agent.domain.harness import HarnessRole
from etl_agent.infrastructure.models import (
    AuditEvent,
    ProjectMembership,
    ProjectRoleGrant,
)


def legacy_harness_roles(role: ProjectRole) -> set[HarnessRole]:
    """把 V1 单角色映射为最小 V2 职责集合，管理员绝不隐式获得安全审批权。"""
    mappings = {
        ProjectRole.ADMIN: {
            HarnessRole.DATA_ENGINEER,
            HarnessRole.OPERATOR,
            HarnessRole.AUDITOR,
        },
        ProjectRole.DATA_ENGINEER: {
            HarnessRole.DATA_ENGINEER,
            HarnessRole.OPERATOR,
        },
        ProjectRole.APPROVER: {HarnessRole.DATA_APPROVER},
        ProjectRole.VIEWER: {HarnessRole.AUDITOR},
    }
    return set(mappings[role])


class HarnessAccessService:
    """管理可叠加的 Harness 项目职责，并提供所有安全流程共用的权限判断。"""

    async def roles_for(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
    ) -> set[HarnessRole]:
        """读取成员显式职责；非项目成员始终返回空集合，避免孤立授权生效。"""
        membership = await session.scalar(
            select(ProjectMembership.id).where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.user_id == user_id,
            )
        )
        if membership is None:
            return set()
        values = (
            await session.scalars(
                select(ProjectRoleGrant.role).where(
                    ProjectRoleGrant.project_id == project_id,
                    ProjectRoleGrant.user_id == user_id,
                )
            )
        ).all()
        return set(values)

    async def require_any_role(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
        allowed: set[HarnessRole],
    ) -> set[HarnessRole]:
        """要求用户至少拥有一个指定职责，并返回其完整职责供策略上下文冻结。"""
        roles = await self.roles_for(session, project_id=project_id, user_id=user_id)
        if not roles.intersection(allowed):
            expected = "、".join(sorted(role.value for role in allowed))
            raise PermissionDenied(f"需要以下 Harness 职责之一：{expected}")
        return roles

    async def grant(
        self,
        session: AsyncSession,
        *,
        project_id: uuid.UUID,
        user_id: uuid.UUID,
        roles: set[HarnessRole],
        actor_id: uuid.UUID,
        trace_id: str | None = None,
    ) -> list[ProjectRoleGrant]:
        """由项目管理员授予显式职责；重复授予幂等，安全审批权必须明确选择。"""
        await require_project_role(session, project_id, actor_id, {ProjectRole.ADMIN})
        membership = await session.scalar(
            select(ProjectMembership.id).where(
                ProjectMembership.project_id == project_id,
                ProjectMembership.user_id == user_id,
            )
        )
        if membership is None:
            raise ResourceNotFound("目标用户不是项目成员")
        existing = {
            grant.role: grant
            for grant in (
                await session.scalars(
                    select(ProjectRoleGrant).where(
                        ProjectRoleGrant.project_id == project_id,
                        ProjectRoleGrant.user_id == user_id,
                    )
                )
            ).all()
        }
        added: list[ProjectRoleGrant] = []
        for role in sorted(roles, key=lambda value: value.value):
            if role in existing:
                added.append(existing[role])
                continue
            grant = ProjectRoleGrant(
                project_id=project_id,
                user_id=user_id,
                role=role,
                granted_by=actor_id,
            )
            session.add(grant)
            added.append(grant)
        await session.flush()
        session.add(
            AuditEvent(
                actor_id=actor_id,
                project_id=project_id,
                action="harness_roles.granted",
                resource_type="User",
                resource_id=user_id,
                before=None,
                after={"roles": sorted(role.value for role in roles)},
                trace_id=trace_id,
            )
        )
        await session.flush()
        return added


async def seed_legacy_role_grants(
    session: AsyncSession,
    *,
    project_id: uuid.UUID,
    user_id: uuid.UUID,
    legacy_role: ProjectRole,
    granted_by: uuid.UUID | None,
) -> list[ProjectRoleGrant]:
    """为新建 V1 成员同步初始 V2 职责；仅用于创建事务，不替代后续显式授权。"""
    grants = [
        ProjectRoleGrant(
            project_id=project_id,
            user_id=user_id,
            role=role,
            granted_by=granted_by,
        )
        for role in sorted(legacy_harness_roles(legacy_role), key=lambda value: value.value)
    ]
    session.add_all(grants)
    await session.flush()
    return grants
