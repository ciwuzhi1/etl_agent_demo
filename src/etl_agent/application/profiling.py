import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import PermissionDenied
from etl_agent.infrastructure.models import ProjectMembership


async def require_project_role(
    session: AsyncSession,
    project_id: uuid.UUID,
    actor_id: uuid.UUID,
    allowed: set[ProjectRole],
) -> None:
    """校验调用者在该项目内持有允许的角色，否则拒绝访问。

    本文件按参考实现的位置提前建立，当前只承载这一个权限函数；第 4 章的数据探查服务
    （DataProfile）会在同一模块内追加，`harness_access` 的导入路径因此保持不变。
    """
    role = await session.scalar(
        select(ProjectMembership.role).where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.user_id == actor_id,
        )
    )
    if role not in allowed:
        raise PermissionDenied("当前用户没有访问该数据连接的项目角色")
