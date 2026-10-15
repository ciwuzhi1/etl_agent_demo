"""API 请求与响应模型（Pydantic）。课件 §3.8 / §4.8。

这些类是对外契约，承担三件事：
1. 入参校验（长度、范围、枚举），让路由里不必再手写 if；
2. 出参**白名单**——只有列在这里的字段才会返回，这本身就是防泄漏手段；
3. 生成 OpenAPI 文档。

一条硬规矩：响应模型里**绝不出现** password、username、secret_ref、Vault 路径。
`DataConnectionResponse` 少列一个字段是特性，不是遗漏。
"""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from etl_agent.domain.enums import (
    ConnectionStatus,
    ConnectorType,
    DataEndpointRole,
    DataProfileStatus,
    FileAssetFormat,
    ProjectRole,
)
from etl_agent.domain.harness import HarnessRole


class HealthResponse(BaseModel):
    """就绪探针：只报「活着吗、配置齐了吗」，不读也不回显任何秘密值。"""

    status: str
    service: str
    secret_provider: str
    secret_provider_configured: bool


class BootstrapProjectRequest(BaseModel):
    """一次性初始化的入参。Token 不在这里——它走 X-Bootstrap-Token 请求头。"""

    project_name: str = Field(min_length=1, max_length=255)
    user_subject: str = Field(min_length=1, max_length=255)
    user_display_name: str = Field(min_length=1, max_length=255)


class BootstrapProjectResponse(BaseModel):
    """只返回两个 UUID。Token 不进响应，也不写审计（课件 §3.6）。"""

    project_id: uuid.UUID
    actor_id: uuid.UUID


class CreateProjectMemberRequest(BaseModel):
    """管理员添加成员的入参；role 由调用方指定，服务端再判一次权限。"""

    subject: str = Field(min_length=1, max_length=255)
    display_name: str = Field(min_length=1, max_length=255)
    role: ProjectRole


class ProjectMemberResponse(BaseModel):
    """成员视图：项目角色 + 可叠加的 Harness 职责，不含任何认证材料。"""

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


class ConnectionCredentialsRequest(BaseModel):
    """凭据入参。用 SecretStr 是为了 repr/日志里显示成掩码，降低意外回显风险。

    注意：这是**进**来的方向，允许带密码；**出**去的 DataConnectionResponse 没有这些字段。
    """

    username: str = Field(min_length=1, max_length=255)
    password: SecretStr
    attributes: dict[str, SecretStr] = Field(default_factory=dict)


class CreateConnectionRequest(BaseModel):
    """登记一个数据源。host/port/database_name 可空——文件类或非网络连接器不需要。

    这里没有 secret_ref：密文由服务端交给 SecretProvider 后自行生成引用，
    不能让调用方指定要覆盖哪条秘密记录。
    """

    project_id: uuid.UUID
    name: str = Field(min_length=1, max_length=255)
    connector_type: ConnectorType
    host: str | None = Field(default=None, max_length=255)
    port: int | None = Field(default=None, ge=1, le=65535)
    database_name: str | None = Field(default=None, max_length=255)
    options: dict[str, Any] = Field(default_factory=dict)
    credentials: ConnectionCredentialsRequest


class DataConnectionResponse(BaseModel):
    """连接响应模型：没有 credentials、password 或 secret_ref 字段。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    connector_type: ConnectorType
    host: str | None
    port: int | None
    database_name: str | None
    options: dict[str, Any]
    status: ConnectionStatus
    test_error: str | None
    last_tested_at: datetime | None
    credential_issued_at: datetime
    credential_rotation_enabled: bool
    credential_max_age_days: int
    credential_warning_days: int


class CreateProfileRequest(BaseModel):
    """发起一次探查。sample_limit 上限 100：样本只是给模型看的样例，不该变成抽样导出。

    schema_name / table_name 会先过安全标识符白名单才拼进元数据查询，
    `orders; DROP TABLE users` 这类输入应在到 MySQL 之前就被拒掉。
    """

    schema_name: str = Field(min_length=1, max_length=128)
    table_name: str = Field(min_length=1, max_length=128)
    sample_limit: int = Field(default=20, ge=1, le=100)
    endpoint_role: DataEndpointRole = DataEndpointRole.SOURCE


class DataProfileResponse(BaseModel):
    """Profile 响应只暴露结构化元数据、统计与脱敏样本，不回显连接凭据。"""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: uuid.UUID
    connection_id: uuid.UUID
    schema_name: str
    table_name: str
    endpoint_role: DataEndpointRole
    status: DataProfileStatus
    # 模型属性叫 schema_definition（schema 是 Pydantic 的保留词，不能直接用作字段名），
    # 但对外 JSON 字段要保持与数据库列一致叫 schema_json，所以读写各设一个别名。
    schema_definition: dict[str, Any] = Field(
        validation_alias="schema_json", serialization_alias="schema_json"
    )
    primary_keys: list[str]
    statistics: dict[str, Any]
    masked_sample: list[dict[str, Any]]
    sample_size: int
    expires_at: datetime | None
    error: str | None


class FileAssetResponse(BaseModel):
    """文件资产响应：物理上不含 source_uri/normalized_uri/engine_path/file-asset:// URI。

    课件 §5.5（第 5 章）要求响应模型只回文件名、格式、字节、SHA-256、状态、Schema、
    统计、脱敏样本、行数、过期时间、错误；内部定位由存储 Adapter 和 Worker 认识。
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    original_filename: str
    format: FileAssetFormat = Field(validation_alias="file_format")
    byte_size: int | None
    sha256: str | None
    status: DataProfileStatus
    schema_definition: dict[str, Any] = Field(validation_alias="schema_json")
    statistics: dict[str, Any]
    masked_sample: list[dict[str, Any]]
    row_count: int | None
    expires_at: datetime | None
    error: str | None
