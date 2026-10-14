"""ORM 模型：PostgreSQL 业务库里 13 张表的唯一定义处。

课件 §3.4（第 3 章的 10 张）+ §4.5（第 4 章补的 3 张）。
本文件与 `alembic/versions/` 里的迁移必须一致——`alembic check` 就是用来盯这个漂移的。

四组表的分工：
- 身份：users / projects / project_memberships / project_role_grants
- 证据：data_connections / encrypted_secrets / data_profiles（第 4 章元数据链路）
- 版本与审批：pipelines / pipeline_versions / pipeline_artifacts / approval_requests
- 运行与审计：execution_runs / audit_events

贯穿全表的一条纪律：**不覆盖同一行**。需求变了开 V+1，重跑新建 ExecutionRun，
销毁凭据留墓碑。覆盖 = 毁掉审计证据。
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from etl_agent.domain.enums import (
    ApprovalStatus,
    ConnectionStatus,
    ConnectorType,
    DataEndpointRole,
    DataProfileStatus,
    ExecutionStatus,
    PipelineVersionStatus,
    ProjectRole,
)
from etl_agent.domain.harness import HarnessRole
from etl_agent.infrastructure.database import Base


def enum_column(enum_type: type[Any]) -> Enum:
    """将领域枚举保存为非原生字符串枚举，降低 PostgreSQL 枚举升级风险。"""
    return Enum(enum_type, native_enum=False, validate_strings=True)


class TimestampMixin:
    """给每张表配 created/updated 两个时间戳。

    `server_default=func.now()` 让数据库负责填，避免"应用机器时钟不一致"；
    `updated_at` 的 `onupdate` 由 SQLAlchemy 在 UPDATE 时维护。
    AuditEvent 故意不用这个 Mixin——审计记录只关心"何时发生"，不该被更新。
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class User(Base, TimestampMixin):
    """全局的"人"。不属于任何项目，一个 subject 在整套系统里只有一个人。"""

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # subject 来自企业身份源（SSO/网关）的稳定标识，不是显示名；唯一约束防止
    # 同一个人被建成两个账号后各带一份权限。
    subject: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)


class Project(Base, TimestampMixin):
    """一个企业工作区：项目是权限、连接、Pipeline 和审计的归属边界（租户边界）。"""

    __tablename__ = "projects"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)


class ProjectMembership(Base, TimestampMixin):
    """「这个人在这个项目里是什么角色」。

    单值 role：V1 的粗粒度访问判断。可叠加的细职责在 ProjectRoleGrant，
    两者不能互相替代。
    """

    __tablename__ = "project_memberships"
    # 同一人在同一项目只能有一条成员关系；否则"他到底是什么"会有多个答案。
    __table_args__ = (UniqueConstraint("project_id", "user_id"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # ondelete="CASCADE"：项目删了，成员关系跟着删（归属边界没了，成员无意义）。
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[ProjectRole] = mapped_column(enum_column(ProjectRole), nullable=False)


class ProjectRoleGrant(Base, TimestampMixin):
    """保存一个成员可同时拥有的 Harness 职责，不把管理员隐式升级为安全审批人。"""

    __tablename__ = "project_role_grants"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "user_id",
            "role",
            name="uq_project_role_grants_identity",
        ),
        Index("ix_project_role_grants_project_user", "project_id", "user_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[HarnessRole] = mapped_column(enum_column(HarnessRole), nullable=False)
    granted_by: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )


class EncryptedSecret(Base, TimestampMixin):
    """凭据存储记录：本地 Provider 保存密文，外部 Provider 只保存不含秘密的定位引用。"""

    __tablename__ = "encrypted_secrets"
    __table_args__ = (
        CheckConstraint(
            "(destroyed_at IS NULL AND provider = 'fernet-db' "
            "AND ciphertext IS NOT NULL AND external_ref IS NULL) "
            "OR (destroyed_at IS NULL AND provider <> 'fernet-db' "
            "AND ciphertext IS NULL AND external_ref IS NOT NULL) "
            "OR (destroyed_at IS NOT NULL AND ciphertext IS NULL)",
            name="ck_encrypted_secrets_storage_shape",
        ),
        UniqueConstraint("provider", "external_ref"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    provider: Mapped[str] = mapped_column(String(32), default="fernet-db", nullable=False)
    ciphertext: Mapped[bytes | None] = mapped_column(LargeBinary)
    external_ref: Mapped[str | None] = mapped_column(String(512))
    key_version: Mapped[str] = mapped_column(String(64), default="local-v1", nullable=False)
    # 销毁后保留不含秘密的墓碑记录，历史轮换和审计外键因此仍可追溯。
    destroyed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DataConnection(Base, TimestampMixin):
    """连接元数据与 secret 引用；表内不保存用户名、密码或 Vault 路径。"""

    __tablename__ = "data_connections"
    __table_args__ = (
        UniqueConstraint("project_id", "name"),
        CheckConstraint(
            "credential_max_age_days >= 1 AND credential_max_age_days <= 3650",
            name="ck_data_connections_credential_max_age",
        ),
        CheckConstraint(
            "credential_warning_days >= 0 AND credential_warning_days < credential_max_age_days",
            name="ck_data_connections_credential_warning",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    connector_type: Mapped[ConnectorType] = mapped_column(
        enum_column(ConnectorType), nullable=False
    )
    host: Mapped[str | None] = mapped_column(String(255))
    port: Mapped[int | None] = mapped_column(Integer)
    database_name: Mapped[str | None] = mapped_column(String(255))
    options: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    secret_ref: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("encrypted_secrets.id"), nullable=False
    )
    status: Mapped[ConnectionStatus] = mapped_column(
        enum_column(ConnectionStatus), default=ConnectionStatus.UNTESTED, nullable=False
    )
    test_error: Mapped[str | None] = mapped_column(Text)
    last_tested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 凭据年龄独立于 Secret 记录：Provider 迁移复制的是同一凭据，不能借此重置轮换时钟。
    credential_issued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    credential_rotation_enabled: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true", nullable=False
    )
    credential_max_age_days: Mapped[int] = mapped_column(
        Integer, default=90, server_default="90", nullable=False
    )
    credential_warning_days: Mapped[int] = mapped_column(
        Integer, default=14, server_default="14", nullable=False
    )
    created_by: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))


class DataProfile(Base, TimestampMixin):
    """一次探查的有时效结果：只保存结构化元数据、统计和脱敏样本。"""

    __tablename__ = "data_profiles"
    __table_args__ = (
        Index("ix_data_profiles_connection_table", "connection_id", "schema_name", "table_name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("data_connections.id", ondelete="CASCADE"), nullable=False
    )
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    endpoint_role: Mapped[DataEndpointRole] = mapped_column(
        enum_column(DataEndpointRole),
        default=DataEndpointRole.SOURCE,
        nullable=False,
    )
    status: Mapped[DataProfileStatus] = mapped_column(
        enum_column(DataProfileStatus), default=DataProfileStatus.PENDING, nullable=False
    )
    schema_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    primary_keys: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    statistics: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    masked_sample: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    sample_size: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)
    requested_by: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))


class Pipeline(Base, TimestampMixin):
    """长期存在的业务流水线本身（"订单同步到 Doris"）。

    它不持有状态和内容——那些都在 PipelineVersion 上，因为需求会变而这条业务线不变。
    """

    __tablename__ = "pipelines"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class PipelineVersion(Base, TimestampMixin):
    """这条流水线的一次**不可变**修订。

    改需求就开 V+1，不许覆盖同一行——覆盖等于毁掉"当时批的到底是什么"的证据。
    """

    __tablename__ = "pipeline_versions"
    # 同一条 Pipeline 下版本号唯一；并发创建撞号时靠这个约束报错，而不是靠代码判断。
    __table_args__ = (UniqueConstraint("pipeline_id", "version_number"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[PipelineVersionStatus] = mapped_column(
        enum_column(PipelineVersionStatus), default=PipelineVersionStatus.DRAFT, nullable=False
    )
    # requirement 保留原始需求文本，plan 是结构化后的计划；两者分开才能复盘"理解偏了没"。
    requirement: Mapped[str] = mapped_column(Text, nullable=False)
    plan: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    # 冻结制品的摘要。审批单会引用它，用来证明"批的就是这份内容"。
    artifact_sha256: Mapped[str | None] = mapped_column(String(64))
    # 乐观锁：更新时带上版本号比对，挡住两个编辑者互相覆盖。
    lock_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))


class PipelineArtifact(Base, TimestampMixin):
    """某个版本生成的冻结制品（SeaTunnel 配置等）。

    只增不改：内容变了就换新行 + 新 sha256。
    """

    __tablename__ = "pipeline_artifacts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_version_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pipeline_versions.id", ondelete="CASCADE"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    uri: Mapped[str] = mapped_column(String(1024), nullable=False)
    # 摘要才是证据；uri 只是去哪儿取，文件可能被移动或替换。
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class ApprovalRequest(Base, TimestampMixin):
    """一条审批记录：把"谁批的"绑定到"版本 + 制品摘要"这个确切内容上。

    requested_by 与 decided_by 是两个独立字段，四眼分离靠它们不相等来审计。
    """

    __tablename__ = "approval_requests"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_version_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pipeline_versions.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[ApprovalStatus] = mapped_column(
        enum_column(ApprovalStatus), default=ApprovalStatus.PENDING, nullable=False
    )
    # 审批针对的是这份摘要，不是"版本当前内容"：批完又改了，旧审批不能算数。
    artifact_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    requested_by: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))
    decided_by: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))
    reason: Mapped[str | None] = mapped_column(Text)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ExecutionRun(Base, TimestampMixin):
    """每一次运行一行。

    重跑不修改原行，而是新建一行并带上 retry_of_run_id——原失败诊断、耗时和指标要留着。
    """

    __tablename__ = "execution_runs"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # 注意这里没有 ondelete="CASCADE"：版本删了也不能删掉运行历史，审计要能追溯。
    pipeline_version_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pipeline_versions.id"), nullable=False
    )
    status: Mapped[ExecutionStatus] = mapped_column(
        enum_column(ExecutionStatus), default=ExecutionStatus.QUEUED, nullable=False
    )
    # 幂等键唯一：同一意图被重复提交（网络重试、用户连点）只会产生一行。
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    seatunnel_job_id: Mapped[str | None] = mapped_column(String(255))
    requested_by: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))
    error: Mapped[str | None] = mapped_column(Text)


class AuditEvent(Base):
    """审计：记"变化"，不记秘密。

    故意不继承 TimestampMixin——只需要 occurred_at，不需要被更新。
    before / after 是结构化差分，绝不写入密码、Token 或密钥明文。
    """

    __tablename__ = "audit_events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    actor_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))
    project_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("projects.id"))
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    before: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    after: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    reason: Mapped[str | None] = mapped_column(Text)
    # 与请求日志关联的追踪号；没有也不影响审计成立，所以可空。
    trace_id: Mapped[str | None] = mapped_column(String(64))
