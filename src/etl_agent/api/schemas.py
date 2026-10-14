"""API 请求与响应模型。后续章节按路由逐个补齐。"""

import uuid

from pydantic import BaseModel, Field

from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.harness import HarnessRole


class HealthResponse(BaseModel):
    status: str
    service: str
    secret_provider: str
    secret_provider_configured: bool


class BootstrapProjectRequest(BaseModel):
    project_name: str = Field(min_length=1, max_length=255)
    user_subject: str = Field(min_length=1, max_length=255)
    user_display_name: str = Field(min_length=1, max_length=255)


class BootstrapProjectResponse(BaseModel):
    project_id: uuid.UUID
    actor_id: uuid.UUID


class CreateProjectMemberRequest(BaseModel):
    subject: str = Field(min_length=1, max_length=255)
    display_name: str = Field(min_length=1, max_length=255)
    role: ProjectRole


class ProjectMemberResponse(BaseModel):
    user_id: uuid.UUID
    subject: str
    display_name: str
    role: ProjectRole
    harness_roles: list[HarnessRole] = Field(default_factory=list)


class GrantHarnessRolesRequest(BaseModel):
    """描述管理员要显式授予现有项目成员的可叠加 Harness 职责。"""

    roles: list[HarnessRole] = Field(min_length=1, max_length=5)


class HarnessRoleGrantResponse(BaseModel):
    """返回成员当前完整 Harness 职责集合，不暴露认证或签名材料。"""

    user_id: uuid.UUID
    roles: list[HarnessRole]
