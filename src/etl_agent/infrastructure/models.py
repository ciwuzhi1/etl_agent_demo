"""ORM 模型：PostgreSQL 业务库里 13 张表的唯一定义处。

课件 §3.4（第 3 章的 10 张）+ §4.5（第 4 章补的 3 张）。
本文件与 `alembic/versions/` 里的迁移必须一致——`alembic check` 就是用来盯这个漂移的。

各组表的分工：
- 身份：users / projects / project_memberships / project_role_grants
- 证据：data_connections / encrypted_secrets / data_profiles（第 4 章元数据链路）
- 版本与审批：pipelines / pipeline_versions / pipeline_artifacts / approval_requests
- 运行与审计：execution_runs / audit_events
- 副作用治理（第 12 章）：harness_side_effect_preparations / _approvals / _capability_grants

贯穿全表的一条纪律：**不覆盖同一行**。需求变了开 V+1，重跑新建 ExecutionRun，
销毁凭据留墓碑。覆盖 = 毁掉审计证据。
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
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
    FileAssetFormat,
    PipelineVersionStatus,
    ProjectRole,
)
from etl_agent.domain.harness import HarnessApprovalValue, HarnessOutboxStatus, HarnessRole
from etl_agent.domain.safety_evolution import (
    SafetyProposalKind,
    SafetyProposalStatus,
    SafetyReleaseStage,
    SafetyReleaseStatus,
)
from etl_agent.domain.side_effects import (
    SideEffectExecutionStatus,
    SideEffectOperation,
    SideEffectPreparationStatus,
)
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
    # 执行生命周期时间戳
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 失败诊断字段
    failure_category: Mapped[str | None] = mapped_column(String(64))
    failure_stage: Mapped[str | None] = mapped_column(String(32))
    retryable: Mapped[bool | None] = mapped_column(Boolean)
    # 指标字段（第 10 章启用）
    source_rows: Mapped[int | None] = mapped_column(BigInteger)
    sink_rows: Mapped[int | None] = mapped_column(BigInteger)
    source_bytes: Mapped[int | None] = mapped_column(BigInteger)
    sink_bytes: Mapped[int | None] = mapped_column(BigInteger)
    # 运行时监督字段（第 13 章启用）：监督层只减权，这里记录最新动作、契约摘要与隔离事实。
    supervision_action: Mapped[str | None] = mapped_column(String(32))
    supervision_contract_sha256: Mapped[str | None] = mapped_column(String(64))
    quarantined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    quarantine_reason: Mapped[str | None] = mapped_column(Text)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RuntimeSupervisionSnapshot(Base, TimestampMixin):
    """运行时监督快照：每次引擎轮询一行，带 SHA-256 哈希链与减损副作用状态。

    只增不改（覆盖 = 毁掉"当时观察到什么"的证据）；`snapshot_sha256` 串起同一 run 的
    前序快照，构成可验证的哈希链。`mitigation_status` 记录需要外部取消/隔离的动作
    从判定到应用的状态机，保证重复投递不会二次调用引擎。
    """

    __tablename__ = "runtime_supervision_snapshots"
    __table_args__ = (
        UniqueConstraint("run_id", "sequence", name="uq_runtime_supervision_run_sequence"),
        Index("ix_runtime_supervision_run_observed", "run_id", "observed_at"),
        Index("ix_runtime_supervision_mitigation_status", "mitigation_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("execution_runs.id", ondelete="CASCADE"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    engine_status: Mapped[str] = mapped_column(String(32), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    risk_level: Mapped[str] = mapped_column(String(8), nullable=False)
    cancel_required: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    block_publish: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    next_poll_seconds: Mapped[int] = mapped_column(Integer, default=2, nullable=False)
    observed: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    budget: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    rule_matches: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    contract_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    snapshot_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    mitigation_status: Mapped[str] = mapped_column(
        String(32),
        default="NOT_REQUIRED",
        nullable=False,
    )
    mitigation_error: Mapped[str | None] = mapped_column(Text)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FileAsset(Base, TimestampMixin):
    """受管文件资产：上传身份、内部定位、引擎路径与解析结果。

    浏览器给的文件名/编码/表头/行数/内容全都不可信，所以平台先把它变成"受管资产"：
    - original_filename 给人看（显示别名）
    - source_uri / normalized_uri 是 file-asset://<uuid>/… 的内部定位
    - engine_path 是给 SeaTunnel 容器的只读路径 /data/file-assets/<uuid>/normalized.jsonl
    - 解析结果：状态/Schema/统计/样本/行数/过期时间
    """

    __tablename__ = "file_assets"
    __table_args__ = (
        Index("ix_file_assets_project_created", "project_id", "created_at"),
        CheckConstraint("byte_size >= 0", name="ck_file_assets_byte_size"),
        CheckConstraint("row_count >= 0", name="ck_file_assets_row_count"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    # 原始文件名（显示用，可能带目录段/控制字符，已净化）
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    # 格式枚举：csv/json/jsonl/xlsx/parquet/orc/xml/markdown/pdf
    file_format: Mapped[FileAssetFormat] = mapped_column(
        enum_column(FileAssetFormat), nullable=False
    )
    # MIME 类型（上传时记录）
    media_type: Mapped[str | None] = mapped_column(String(255))
    # 字节数与 SHA-256（上传时计算）
    byte_size: Mapped[int | None] = mapped_column(BigInteger)
    sha256: Mapped[str | None] = mapped_column(String(64))
    # 内部定位 URI：file-asset://<uuid>/source.<ext>
    source_uri: Mapped[str | None] = mapped_column(String(1024))
    # 规范化后的 JSONL URI：file-asset://<uuid>/normalized.jsonl
    normalized_uri: Mapped[str | None] = mapped_column(String(1024))
    # 引擎容器内路径：/data/file-assets/<uuid>/normalized.jsonl
    engine_path: Mapped[str | None] = mapped_column(String(1024))
    # 解析状态：PENDING/RUNNING/READY/FAILED
    status: Mapped[DataProfileStatus] = mapped_column(
        enum_column(DataProfileStatus), default=DataProfileStatus.PENDING, nullable=False
    )
    # Schema 定义（字段名/类型/是否主键）
    schema_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    # 统计信息（行数/列数/规范化映射等）
    statistics: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    # 脱敏样本（进入 Agent 上下文前的几十行）
    masked_sample: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    # 实际行数（解析后统计）
    row_count: Mapped[int | None] = mapped_column(BigInteger)
    # 过期时间（+24h TTL，与 DataProfile 同一口径）
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 失败时的错误文案（不把主机路径或文件内容写进库）
    error: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))


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


class HarnessPolicyDecisionRecord(Base, TimestampMixin):
    """策略决策记录：保存 PDP 评估结果供审批和证据链引用。"""

    __tablename__ = "harness_policy_decisions"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    actor_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    risk_level: Mapped[str] = mapped_column(String(16), nullable=False)
    policy_bundle_version: Mapped[str] = mapped_column(String(64), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    rule_matches: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list, nullable=False)
    required_roles: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    required_approvals: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    artifact_sha256: Mapped[str | None] = mapped_column(String(64))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    trace_id: Mapped[str | None] = mapped_column(String(64))


class HarnessEvidenceStream(Base, TimestampMixin):
    """保存证据流唯一身份和受行锁保护的当前哈希头。"""

    __tablename__ = "harness_evidence_streams"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "stream_type",
            "stream_key",
            name="uq_harness_evidence_stream_identity",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    stream_type: Mapped[str] = mapped_column(String(64), nullable=False)
    stream_key: Mapped[str] = mapped_column(String(255), nullable=False)
    next_sequence: Mapped[int] = mapped_column(BigInteger, default=1, nullable=False)
    head_hash: Mapped[str | None] = mapped_column(String(64))


class HarnessEvidenceEvent(Base):
    """保存只追加的规范事件、前序哈希和可选 Ed25519 检查点签名。"""

    __tablename__ = "harness_evidence_events"
    __table_args__ = (
        UniqueConstraint("stream_id", "sequence", name="uq_harness_evidence_event_sequence"),
        UniqueConstraint("event_hash", name="uq_harness_evidence_event_hash"),
        CheckConstraint("sequence > 0", name="ck_harness_evidence_event_sequence_positive"),
        Index("ix_harness_evidence_events_stream_occurred", "stream_id", "occurred_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    stream_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("harness_evidence_streams.id", ondelete="CASCADE"),
        nullable=False,
    )
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    actor_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    risk_level: Mapped[str] = mapped_column(String(8), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    previous_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    event_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("harness_policy_decisions.id", ondelete="SET NULL"),
    )
    signature_algorithm: Mapped[str | None] = mapped_column(String(32))
    signature_key_id: Mapped[str | None] = mapped_column(String(128))
    signature_value: Mapped[str | None] = mapped_column(Text)
    trace_id: Mapped[str | None] = mapped_column(String(64))


class ExecutionPreparation(Base, TimestampMixin):
    """保存不可变执行输入、影响摘要和审批要求，作为 Commit 的唯一授权依据。"""

    __tablename__ = "execution_preparations"
    __table_args__ = (
        Index("ix_execution_preparations_project_created", "project_id", "created_at"),
        Index("ix_execution_preparations_status_expires", "status", "expires_at"),
        UniqueConstraint(
            "project_id",
            "idempotency_key",
            name="uq_execution_preparations_project_idempotency",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    pipeline_version_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pipeline_versions.id", ondelete="RESTRICT"), nullable=False
    )
    policy_decision_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("harness_policy_decisions.id", ondelete="RESTRICT"),
        unique=True,
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    risk_level: Mapped[str] = mapped_column(String(8), nullable=False)
    policy_bundle_version: Mapped[str] = mapped_column(String(128), nullable=False)
    environment: Mapped[str] = mapped_column(String(64), nullable=False)
    tool_name: Mapped[str] = mapped_column(String(128), nullable=False)
    tool_version: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    intent: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    policy_context: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    prepared_input: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    impact_report: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    rollback_plan: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    required_roles: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    required_approvals: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    requested_by: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    committed_by: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    committed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lock_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


class HarnessApprovalDecision(Base):
    """保存具名审批人对指定职责槽位的决定，并由唯一约束保证自然人分离。"""

    __tablename__ = "harness_approval_decisions"
    __table_args__ = (
        UniqueConstraint(
            "preparation_id",
            "role",
            name="uq_harness_approval_decisions_role",
        ),
        UniqueConstraint(
            "preparation_id",
            "decided_by",
            name="uq_harness_approval_decisions_person",
        ),
        Index("ix_harness_approval_decisions_preparation", "preparation_id", "decided_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    preparation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("execution_preparations.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    decided_by: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class HarnessCapabilityGrant(Base):
    """保存能力令牌摘要、受签名声明和一次性消费事实，不持久化原始紧凑令牌。"""

    __tablename__ = "harness_capability_grants"
    __table_args__ = (
        UniqueConstraint("preparation_id", name="uq_harness_capability_grants_preparation"),
        Index("ix_harness_capability_grants_execution", "execution_run_id"),
    )

    token_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    preparation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("execution_preparations.id", ondelete="CASCADE"),
        nullable=False,
    )
    token_sha256: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    key_id: Mapped[str] = mapped_column(String(128), nullable=False)
    claims: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    claims_signature_algorithm: Mapped[str] = mapped_column(String(32), nullable=False)
    claims_signature_value: Mapped[str] = mapped_column(Text, nullable=False)
    subject_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    consumed_by: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    execution_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("execution_runs.id", ondelete="SET NULL")
    )


class HarnessCommandOutbox(Base, TimestampMixin):
    """Harness 命令的 Outbox 表：实现至少一次可靠投递。

    工作流：PENDING → SENDING（认领加锁）→ SENT（成功）或 FAILED（失败等待重试）。
    失败后按指数退避重新变为可认领状态；超过最大尝试次数则标记为 DEAD。
    """

    __tablename__ = "harness_command_outbox"
    __table_args__ = (
        Index("ix_harness_command_outbox_status_available", "status", "available_at"),
        Index("ix_harness_command_outbox_locked", "locked_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # 命令内容：JSON 序列化的 Harness 命令载荷
    command_payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    # 目标执行引擎或队列标识
    target: Mapped[str] = mapped_column(String(128), nullable=False)
    # 当前投递状态
    status: Mapped[HarnessOutboxStatus] = mapped_column(
        enum_column(HarnessOutboxStatus), default=HarnessOutboxStatus.PENDING, nullable=False
    )
    # 最早可处理时间：用于延迟投递和失败重试退避
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    # 租约锁定时间：SENDING 状态时记录，用于检测崩溃 worker
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 成功发送时间
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 尝试次数：每次认领递增
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # 最后一次错误摘要（已脱敏）
    last_error: Mapped[str | None] = mapped_column(Text)
    # 关联的执行 run ID（可选）
    execution_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("execution_runs.id", ondelete="SET NULL")
    )
    # 关联的项目 ID
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    # 发起者
    created_by: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("users.id"))


class HarnessSideEffectPreparation(Base, TimestampMixin):
    """第 12 章：非 Pipeline 副作用的冻结事实、审批门禁与执行状态。

    Prepare 写入 WAITING_APPROVAL；PDP DENY 落 DENIED 终态；全部必需职责批准后
    READY；Commit 只推进 status=COMMITTED，外部执行进度由 execution_status 与
    Worker 租约列（lease_owner / lease_expires_at）单独跟踪，两层状态互不覆盖。
    """

    __tablename__ = "harness_side_effect_preparations"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "idempotency_key",
            name="uq_side_effect_preparations_project_idempotency",
        ),
        Index("ix_side_effect_preparations_project_created", "project_id", "created_at"),
        Index("ix_side_effect_preparations_operation_aggregate", "operation", "aggregate_id"),
        Index("ix_side_effect_preparations_status_execution", "status", "execution_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    operation: Mapped[SideEffectOperation] = mapped_column(
        enum_column(SideEffectOperation), nullable=False
    )
    # 被操作的业务对象（ExecutionRun / RollbackRequest / Secret 申请等）主键。
    aggregate_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    status: Mapped[SideEffectPreparationStatus] = mapped_column(
        enum_column(SideEffectPreparationStatus),
        default=SideEffectPreparationStatus.WAITING_APPROVAL,
        nullable=False,
    )
    execution_status: Mapped[SideEffectExecutionStatus] = mapped_column(
        enum_column(SideEffectExecutionStatus),
        default=SideEffectExecutionStatus.PENDING,
        nullable=False,
    )
    risk_level: Mapped[str] = mapped_column(String(8), nullable=False)
    # 冻结事实的规范 JSON SHA-256：审批与 Commit 都必须绑定这一摘要。
    facts_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    frozen_facts: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    command_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    required_roles: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    required_approvals: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    requested_by: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    committed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Worker 执行租约：防止至少一次投递让两个 Worker 并发进入外部临界区。
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    execution_error: Mapped[str | None] = mapped_column(Text)
    result_sha256: Mapped[str | None] = mapped_column(String(64))


class HarnessSideEffectApproval(Base):
    """第 12 章：具名职责对副作用 Preparation 的批准或驳回决定。

    两个唯一约束共同实现职责分离：同一职责槽只能有一个决定，同一自然人不能
    同时占据多个职责槽。
    """

    __tablename__ = "harness_side_effect_approvals"
    __table_args__ = (
        UniqueConstraint("preparation_id", "role", name="uq_side_effect_approvals_role"),
        UniqueConstraint("preparation_id", "decided_by", name="uq_side_effect_approvals_person"),
        Index("ix_side_effect_approvals_preparation", "preparation_id", "decided_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    preparation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("harness_side_effect_preparations.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[HarnessRole] = mapped_column(enum_column(HarnessRole), nullable=False)
    decision: Mapped[HarnessApprovalValue] = mapped_column(
        enum_column(HarnessApprovalValue), nullable=False
    )
    decided_by: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class HarnessSideEffectCapabilityGrant(Base):
    """第 12 章：Commit 签发的一次性能力授权；只存令牌摘要，不存原始紧凑令牌。"""

    __tablename__ = "harness_side_effect_capability_grants"
    __table_args__ = (
        UniqueConstraint("preparation_id", name="uq_side_effect_grants_preparation"),
        Index("ix_side_effect_grants_execution_command", "execution_command_id"),
    )

    # 主键即 claims 里的 token_id；消费与重放判定都围绕它进行。
    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    preparation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("harness_side_effect_preparations.id", ondelete="CASCADE"),
        nullable=False,
    )
    token_sha256: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    claims_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    subject_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 指向 harness_command_outbox.id；Outbox 表由第 11 章引入，本迁移链尚未建表，
    # 因此这里保持为不带数据库外键的引用列，由应用层保证一致性。
    execution_command_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))


class SafetyProposal(Base, TimestampMixin):
    """第 14 章：受控安全进化的改进提案。

    只承载可离线审查的候选正文（不含可执行代码/部署命令/凭据），并通过
    DRAFT→IN_REVIEW→APPROVED/REJECTED→RELEASED 的不可跳跃状态推进。
    created_by 与 reviewed_by 分离以实现"提案人不能裁决自己"的四眼约束。
    """

    __tablename__ = "safety_proposals"
    __table_args__ = (Index("ix_safety_proposals_status_created", "status", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    kind: Mapped[SafetyProposalKind] = mapped_column(
        enum_column(SafetyProposalKind), nullable=False
    )
    status: Mapped[SafetyProposalStatus] = mapped_column(
        enum_column(SafetyProposalStatus),
        default=SafetyProposalStatus.DRAFT,
        nullable=False,
    )
    # 候选正文只以结构化 JSON 保存，摘要另存于 payload_sha256 供发布清单签名引用。
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str | None] = mapped_column(String(255))
    rationale: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    reviewed_by: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    decision_reason: Mapped[str | None] = mapped_column(Text)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 乐观锁，防止并发裁决跨越状态机。
    lock_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


class SafetyRelease(Base, TimestampMixin):
    """第 14 章：已批准提案的签名发布候选，沿影子→小流量→推广/回滚推进。

    每行记录一个阶段的当前细粒度状态、粗阶段、脱敏聚合观察与可选签名清单；
    一旦 P0/P1 回归、误报越线或执行错误命中，即自动转为 ROLLED_BACK 并保留失败原因。
    """

    __tablename__ = "safety_releases"
    __table_args__ = (
        UniqueConstraint("proposal_id", name="uq_safety_releases_proposal"),
        Index("ix_safety_releases_status", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    proposal_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("safety_proposals.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[SafetyReleaseStatus] = mapped_column(
        enum_column(SafetyReleaseStatus),
        default=SafetyReleaseStatus.SHADOW_RUNNING,
        nullable=False,
    )
    stage: Mapped[SafetyReleaseStage] = mapped_column(
        enum_column(SafetyReleaseStage),
        default=SafetyReleaseStage.SHADOW,
        nullable=False,
    )
    canary_traffic_percent: Mapped[int | None] = mapped_column(Integer)
    # 冻结的签名发布清单与规范摘要；改写数据库即可被验证发现。
    manifest: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    manifest_sha256: Mapped[str | None] = mapped_column(String(64))
    signature_algorithm: Mapped[str | None] = mapped_column(String(32))
    signature_key_id: Mapped[str | None] = mapped_column(String(128))
    signature_value: Mapped[str | None] = mapped_column(Text)
    # 当前阶段的脱敏聚合观察（样本量、P0/P1 回归、误报、执行错误）。
    observation: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rolled_back_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rollback_reason: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )


class BenchmarkRun(Base, TimestampMixin):
    """第 14 章：一次冻结 Benchmark 运行的签名证据记录。

    报告正文、门槛与逐案例结论整体存入 report；manifest_sha256 把结论绑定到确切
    的场景清单内容，gates_passed 让发布门禁可以直接读取而不必重算。
    """

    __tablename__ = "benchmark_runs"
    __table_args__ = (Index("ix_benchmark_runs_manifest_started", "manifest_sha256", "started_at"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    suite_id: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_bundle_version: Mapped[str] = mapped_column(String(128), nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    report: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    report_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    gates_passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    gate_failures: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )


class WorkerHeartbeat(Base, TimestampMixin):
    """Worker 心跳：Beat 周期任务按 Celery 节点名 upsert 一行。

    控制台用它回答"现在有哪些 Worker 活着"：`last_seen_at` 是唯一事实，`online`
    由读侧按 `worker_heartbeat_stale_seconds` 现算，而不是让写入方自己写死一个
    ONLINE/OFFLINE 结论——进程崩溃时没有机会把行改成 OFFLINE。

    `worker_id` 唯一：同一节点重复上报只更新一行，不产生心跳历史（历史只会让
    "最新一次心跳"的查询变慢，且没有审计价值）。
    """

    __tablename__ = "worker_heartbeats"
    __table_args__ = (UniqueConstraint("worker_id", name="uq_worker_heartbeats_worker_id"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    active_tasks: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="ONLINE", nullable=False)
