"""API 请求与响应模型（Pydantic）。

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

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from etl_agent.domain.enums import (
    AgentRunStatus,
    ApprovalDecisionValue,
    ApprovalStatus,
    ConnectionStatus,
    ConnectorType,
    DataEndpointRole,
    DataProfileStatus,
    ExecutionStatus,
    FileAssetFormat,
    PipelineVersionStatus,
    ProjectRole,
    QualityStatus,
)
from etl_agent.domain.harness import HarnessApprovalValue, HarnessRole
from etl_agent.domain.side_effects import (
    SideEffectExecutionStatus,
    SideEffectOperation,
    SideEffectPreparationStatus,
)


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
    """只返回两个 UUID。Token 不进响应，也不写审计。"""

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

    响应模型只回文件名、格式、字节、SHA-256、状态、Schema、统计、脱敏样本、行数、
    过期时间、错误；内部定位由存储 Adapter 和 Worker 认识。
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


class AgentRunCreateRequest(BaseModel):
    """发起一次自然语言驱动的生成任务。

    profile_ids 只是给 Agent 的元数据线索，是否可用由服务端重新校验；
    trace_id 允许为空，路由会退回 X-Trace-ID 请求头。
    """

    requirement: str = Field(min_length=1, max_length=4000)
    profile_ids: list[uuid.UUID] = Field(default_factory=list, max_length=20)
    trace_id: str | None = Field(default=None, max_length=64)


class AgentRunAnswerRequest(BaseModel):
    """澄清恢复入参：把用户对模型追问的回答追加进会话历史。

    只接受非空纯文本，长度上限与需求文本同级（4000），避免把大段内容塞进会话历史；
    答案不含凭据校验，安全边界由后续确定性门禁负责。
    """

    answer: str = Field(min_length=1, max_length=4000)


class AgentRunResponse(BaseModel):
    """AgentRun 会话层视图：不含 metadata_context 之外可能泄漏的内部字段。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    status: AgentRunStatus
    requirement: str
    candidate_hocon: str | None
    error_message: str | None
    started_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    created_by: uuid.UUID


class PipelineCreateRequest(BaseModel):
    """创建 Pipeline 并同时冻结首个 DRAFT 版本的需求文本。"""

    name: str = Field(min_length=1, max_length=255)
    requirement: str = Field(min_length=1, max_length=4000)


class PipelineVersionResponse(BaseModel):
    """版本视图：plan 只回结构化计划，不返回制品文件内容本身。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    pipeline_id: uuid.UUID
    version_number: int
    status: PipelineVersionStatus
    requirement: str
    plan: dict[str, Any]
    artifact_sha256: str | None
    lock_version: int


class RecordGenerationRequest(BaseModel):
    """外部生成器把结果回写平台的入参；契约门禁在服务端重算，不信任调用方。"""

    plan: dict[str, Any]
    hocon: str = Field(min_length=1, max_length=200_000)
    artifact_uri: str = Field(min_length=1, max_length=1024)
    upstream_version: str = Field(min_length=1, max_length=255)


class ApprovalSubmitRequest(BaseModel):
    """提交审批：reason 仅进审计事件，不改变产物哈希绑定。"""

    reason: str | None = Field(default=None, max_length=1000)


class ApprovalDecisionRequest(BaseModel):
    """审批决策入参；reason 必填，方便日后复盘"为什么批/为什么拒"。"""

    decision: ApprovalDecisionValue
    reason: str = Field(min_length=1, max_length=1000)


class ApprovalRequestResponse(BaseModel):
    """审批单视图：decision 由 status 推导，PENDING 时为空。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    pipeline_version_id: uuid.UUID
    status: ApprovalStatus
    requested_by: uuid.UUID
    decided_by: uuid.UUID | None
    decision: ApprovalDecisionValue | None = None
    reason: str | None
    artifact_sha256: str

    @model_validator(mode="after")
    def _derive_decision(self) -> "ApprovalRequestResponse":
        if self.decision is None:
            if self.status is ApprovalStatus.APPROVED:
                self.decision = ApprovalDecisionValue.APPROVE
            elif self.status is ApprovalStatus.REJECTED:
                self.decision = ApprovalDecisionValue.REJECT
        return self


class ExecutionRunResponse(BaseModel):
    """执行视图：engine_status 等引擎侧字段尚未持久化时保持为空，不回显内部错误细节。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: ExecutionStatus
    engine_status: str | None = None
    source_rows: int | None
    sink_rows: int | None
    failure_category: str | None
    failure_stage: str | None
    retryable: bool | None
    submitted_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None


class ExecutionEventResponse(BaseModel):
    """执行事件行：details 只放结构化摘要，禁止写入堆栈或凭据。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    occurred_at: datetime
    level: str
    stage: str
    event_type: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class QualityReportResponse(BaseModel):
    """质量报告视图：结论沿用 QualityStatus，INCOMPLETE 不等于通过。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    run_id: uuid.UUID
    status: QualityStatus
    total_rows: int | None
    passed_rows: int | None
    rejected_rows: int | None
    reject_rate: float | None
    valid_sink: int | None
    error_sink: int | None


class CreateSideEffectPreparationRequest(BaseModel):
    """发起一次受治理副作用。

    context 是要冻结的业务事实（如 run_id、job_id、目标表名），响应不回显它；
    幂等键可省略，服务端按「操作类型:聚合根 ID」自动派生。
    """

    operation: SideEffectOperation
    aggregate_id: uuid.UUID
    context: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)


class SideEffectApprovalRequest(BaseModel):
    """占用一个职责审批槽；role 必须是该 Preparation 要求的职责之一。"""

    role: HarnessRole
    decision: HarnessApprovalValue
    reason: str = Field(min_length=1, max_length=2_000)


class SideEffectPreparationResponse(BaseModel):
    """副作用 Preparation 视图：只回状态、摘要与审批要求，不含冻结事实正文。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    operation: SideEffectOperation
    aggregate_id: uuid.UUID
    status: SideEffectPreparationStatus
    execution_status: SideEffectExecutionStatus
    risk_level: str
    facts_sha256: str
    required_roles: list[str]
    required_approvals: int
    requested_by: uuid.UUID
    expires_at: datetime
    committed_at: datetime | None


class SideEffectCommitResponse(BaseModel):
    """Commit 凭证：Outbox 命令 ID、能力令牌 ID 与命令载荷摘要。"""

    execution_id: str
    preparation_id: str
    token_id: str
    command_payload_sha256: str


class OverviewMetricsResponse(BaseModel):
    """总览页顶部的四个计数指标；全部来自库内事实，不做推测。"""

    today_succeeded: int
    today_failed: int
    pending_preparations: int
    ready_connections: int
    file_assets: int


class FlowStageResponse(BaseModel):
    """受管执行主线的单个阶段，仅用于展示静态流程说明。"""

    key: str
    label: str
    hint: str


class GuardrailResponse(BaseModel):
    """确定性护栏条目；由代码硬编码，任何模型输出都不能绕过。"""

    code: str
    label: str


class OverviewResponse(BaseModel):
    """控制台总览：指标 + 就绪连接 + 静态流程与护栏说明。"""

    metrics: OverviewMetricsResponse
    ready_connections: list[DataConnectionResponse]
    flow_stages: list[FlowStageResponse]
    guardrails: list[GuardrailResponse]


class SecretLifecycleResponse(BaseModel):
    """凭据生命周期视图：只描述到期窗口，不含任何密码或密文。"""

    id: uuid.UUID
    connection_id: uuid.UUID
    connection_name: str
    status: str
    issued_at: datetime
    expires_at: datetime
    days_to_expiry: int


class WorkerStatusResponse(BaseModel):
    """Worker 心跳视图；心跳表尚未落库时列表为空，而不虚构在线节点。"""

    id: str
    name: str
    online: bool
    last_heartbeat_at: datetime | None
    active_runs: int


class SupervisionSnapshotResponse(BaseModel):
    """运行时监督快照：动作、风险与规则命中原因，不含原始样本或日志正文。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    run_id: uuid.UUID
    sequence: int
    observed_at: datetime
    engine_status: str
    action: str
    risk_level: str
    cancel_required: bool
    block_publish: bool
    reasons: list[str] = Field(default_factory=list)


class BenchmarkCaseResponse(BaseModel):
    """Benchmark 单个用例的期望/实测与可复现结果。"""

    id: str
    name: str
    expected: dict[str, Any] = Field(default_factory=dict)
    observed: dict[str, Any] = Field(default_factory=dict)
    passed: bool
    evidence_hash: str


class BenchmarkRunResponse(BaseModel):
    """Benchmark 运行视图：门槛结论与完整报告，报告本身不含生产数据。"""

    id: uuid.UUID
    suite_id: str
    version: str
    policy_bundle_version: str
    gates_passed: bool
    gate_failures: list[str] = Field(default_factory=list)
    started_at: datetime
    finished_at: datetime
    report: dict[str, Any] = Field(default_factory=dict)


class ExecutionPreparationResponse(BaseModel):
    """执行准备单视图：冻结事实与审批要求，不含能力令牌明文。"""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    pipeline_version_id: uuid.UUID
    status: str
    risk_level: str
    policy_bundle_version: str
    environment: str
    tool_name: str
    tool_version: str
    artifact_sha256: str
    input_fingerprint: str
    resource_fingerprint: str
    impact_report: dict[str, Any]
    rollback_plan: dict[str, Any]
    required_roles: list[str]
    required_approvals: int
    requested_by: uuid.UUID
    expires_at: datetime
    committed_at: datetime | None
    created_at: datetime


class WorkspaceMemberResponse(BaseModel):
    """工作区成员：项目角色 + 可叠加的 Harness 职责，不含认证材料。"""

    user_id: uuid.UUID
    subject: str
    display_name: str
    role: ProjectRole
    harness_roles: list[HarnessRole] = Field(default_factory=list)


class WorkspaceContextResponse(BaseModel):
    """本地演示控制台的会话上下文：首个工作区与成员名单。

    仅 dev-header 身份模式可用；生产 trusted-gateway 模式不暴露此端点。
    """

    project_id: uuid.UUID
    project_name: str
    members: list[WorkspaceMemberResponse]


class PrepareExecutionRequest(BaseModel):
    """从 Pipeline 版本发起一次执行准备。

    intent 可以省略：省略时服务端按固定执行契约构造 ToolIntent。即使显式提供，
    也只能描述工具名与副作用，``actor_id`` / ``project_id`` 一律由服务端从版本回溯，
    客户端不能伪造身份或跨项目资源范围。
    """

    intent: dict[str, Any] | None = None


class DecidePreparationRequest(BaseModel):
    """占用一个 Harness 职责审批槽；role 必须是该准备单要求的职责之一。"""

    role: HarnessRole
    decision: HarnessApprovalValue
    reason: str | None = Field(default=None, max_length=2_000)


class CommitPreparationResponse(BaseModel):
    """Commit 凭证：只回排队中的执行记录 ID 与状态，绝不返回能力令牌明文。"""

    execution_run_id: str
    status: str = "QUEUED"


class PipelineArtifactResponse(BaseModel):
    """Pipeline 版本制品视图：库内摘要 + 取件位置 + 按需读取的 HOCON 正文。

    正文只是展示材料，证据以 ``sha256`` 为准；文件缺失时 ``hocon`` 为空字符串。
    """

    kind: str
    uri: str
    sha256: str
    hocon: str
