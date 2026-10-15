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

# V1 到 V2 角色映射常量：将旧版 ProjectRole 枚举值映射为新版 HarnessRole 列表
V1_TO_V2_ROLE_MAP: dict[str, list[str]] = {
    "ADMIN": [
        HarnessRole.DATA_ENGINEER.value,
        HarnessRole.DATA_APPROVER.value,
        HarnessRole.SECURITY_APPROVER.value,
        HarnessRole.OPERATOR.value,
    ],
    "DATA_ENGINEER": [
        HarnessRole.DATA_ENGINEER.value,
        HarnessRole.OPERATOR.value,
    ],
    "APPROVER": [HarnessRole.DATA_APPROVER.value],
    "VIEWER": [HarnessRole.AUDITOR.value],
}


def map_v1_role_to_v2(v1_role: str) -> list[str]:
    """将遗留角色转换为 harness 角色列表。

    Args:
        v1_role: V1 角色字符串（如 "ADMIN", "DATA_ENGINEER"）

    Returns:
        授予的 harness 角色值列表

    Raises:
        KeyError: 如果 v1_role 不在映射表中
    """
    if v1_role not in V1_TO_V2_ROLE_MAP:
        raise KeyError(f"未知的 V1 角色: {v1_role}")
    return list(V1_TO_V2_ROLE_MAP[v1_role])


async def require_any_role(
    session: AsyncSession,
    project_id: str,
    actor_id: str,
    required_roles: list[str],
) -> bool:
    """检查参与者是否拥有任何所需的 harness 角色。

    查询 project_role_grants 表，如果用户拥有任一所需角色则返回 True，
    否则抛出 PermissionDenied 异常（fail-closed 策略）。

    Args:
        session: 数据库会话
        project_id: 项目 ID
        actor_id: 参与者用户 ID
        required_roles: 所需的 harness 角色列表

    Returns:
        True 如果授权成功

    Raises:
        PermissionDenied: 如果用户没有任何所需角色
    """
    # 验证所有请求的角色都是有效的 HarnessRole
    valid_roles = {role.value for role in HarnessRole}
    for role in required_roles:
        if role not in valid_roles:
            raise ValueError(f"无效的 Harness 角色: {role}")

    # 查询用户的角色授予
    granted_roles = await session.scalars(
        select(ProjectRoleGrant.role).where(
            ProjectRoleGrant.project_id == uuid.UUID(project_id),
            ProjectRoleGrant.user_id == uuid.UUID(actor_id),
        )
    )
    granted_values = {role.value for role in granted_roles.all()}

    # 检查是否有交集
    required_set = set(required_roles)
    if not granted_values.intersection(required_set):
        expected = ", ".join(sorted(required_roles))
        msg = (
            f"需要以下 Harness 职责之一：{expected}，"
            f"但用户 {actor_id} 在项目 {project_id} 中未获得任何授权"
        )
        raise PermissionDenied(msg)

    return True


async def grant_harness_role(
    session: AsyncSession,
    project_id: str,
    user_id: str,
    role: str,
    granted_by: str,
) -> None:
    """向 project_role_grants 表插入角色授予记录。

    Args:
        session: 数据库会话
        project_id: 项目 ID
        user_id: 用户 ID
        role: 要授予的 Harness 角色
        granted_by: 授权者用户 ID

    Raises:
        ValueError: 如果角色不是有效的 HarnessRole
    """
    # 验证角色有效性
    try:
        harness_role = HarnessRole(role)
    except ValueError:
        valid_roles = ", ".join(r.value for r in HarnessRole)
        raise ValueError(f"无效的角色 '{role}'。有效角色: {valid_roles}") from None

    # 创建授予记录
    grant = ProjectRoleGrant(
        project_id=uuid.UUID(project_id),
        user_id=uuid.UUID(user_id),
        role=harness_role,
        granted_by=uuid.UUID(granted_by),
    )
    session.add(grant)
    await session.flush()


async def revoke_harness_role(
    session: AsyncSession,
    project_id: str,
    user_id: str,
    role: str,
) -> None:
    """从 project_role_grants 表删除角色授予记录。

    Args:
        session: 数据库会话
        project_id: 项目 ID
        user_id: 用户 ID
        role: 要撤销的 Harness 角色

    Raises:
        ValueError: 如果角色不是有效的 HarnessRole
    """
    # 验证角色有效性
    try:
        harness_role = HarnessRole(role)
    except ValueError:
        valid_roles = ", ".join(r.value for r in HarnessRole)
        raise ValueError(f"无效的角色 '{role}'。有效角色: {valid_roles}") from None

    # 删除授予记录
    result = await session.execute(
        select(ProjectRoleGrant).where(
            ProjectRoleGrant.project_id == uuid.UUID(project_id),
            ProjectRoleGrant.user_id == uuid.UUID(user_id),
            ProjectRoleGrant.role == harness_role,
        )
    )
    grants = result.scalars().all()

    for grant in grants:
        await session.delete(grant)

    await session.flush()


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
