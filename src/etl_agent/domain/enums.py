"""领域枚举：全系统共用的"固定词汇表"。课件 §3.3。

这里只回答"有哪些合法取值"，不回答"取值之间怎样变化"——后者在
`domain/state_machine.py`。API 入参、数据库列、Worker 判断三处必须引用同一批
名字，否则同一个状态会被写成 `WAITING_APPROVAL` / `WAIT_APPROVE` / `PENDING`
三种样子，等到跨进程比对时才暴露。

统一用 `StrEnum`：写进数据库就是普通字符串（迁移和手写 SQL 都能看懂），
读出来仍能用枚举成员做 `is` / `==` 比较，不用到处 `str()`。

类顺序沿用参考实现，不承载语义；读的时候按主题找：ProjectRole 与 HarnessRole
（domain/harness.py）是两套东西——前者回答"你在这个项目里算什么"，后者回答
"你能不能承担审批/提交这个动作"，分开才能保住四眼分离。

⚠️ 本文件是课件给出的**最终形态**（28 个类）。带 `Secret*`、`CredentialAlert*`、
`Schedule*`、`FileAsset*`、`Rollback*`、`Cleanup*`、`AgentRun*` 的枚举要到
第 4–13 章才真正启用。现在只是把词汇钉住，**不代表那些功能已经可用**。
"""

from enum import StrEnum


class ProjectRole(StrEnum):
    """V1 项目角色：单值，用于常规资源访问判断。"""

    ADMIN = "ADMIN"
    DATA_ENGINEER = "DATA_ENGINEER"
    APPROVER = "APPROVER"
    VIEWER = "VIEWER"


class PipelineVersionStatus(StrEnum):
    """一个版本从草稿到定稿的生命周期。

    必须依次 DRAFT→PROFILING→GENERATED→VALIDATED→WAITING_APPROVAL→终态；
    APPROVED / REJECTED 之后不可再动，改需求要开新版本而不是覆盖这一行。
    """

    DRAFT = "DRAFT"
    PROFILING = "PROFILING"
    GENERATED = "GENERATED"
    VALIDATED = "VALIDATED"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class ApprovalStatus(StrEnum):
    """审批单自身的状态。与版本状态分开记，因为「版本在等」和「谁批了」是两件事。"""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class ApprovalDecisionValue(StrEnum):
    """审批人提交的二选一**动作**，不是状态；落库后体现为 ApprovalStatus 的变化。"""

    APPROVE = "APPROVE"
    REJECT = "REJECT"


class RollbackStatus(StrEnum):
    """回滚工单状态（第 10 章之后启用）。"""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class CleanupStatus(StrEnum):
    """清理工单状态（第 10 章之后启用）。"""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretMigrationStatus(StrEnum):
    """秘密在不同 Provider 之间迁移的工单状态（第 12 章）。"""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretRotationStatus(StrEnum):
    """凭据轮换。比其他工单多了 VALIDATING / ACTIVATING：新密码必须先验证可用，
    才能切换生效，避免把整套系统换成一组坏凭据。
    """

    VALIDATING = "VALIDATING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ACTIVATING = "ACTIVATING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretRotationRollbackStatus(StrEnum):
    """轮换出问题时回退到旧凭据的工单状态（第 12 章）。"""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretDestructionStatus(StrEnum):
    """秘密销毁的工单状态（第 12 章）。销毁要留墓碑，所以是工单而不是 delete。"""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretDestructionTarget(StrEnum):
    """销毁哪一份凭据：轮换留下的旧密码，还是回滚后被搁置的那一份。"""

    ROTATION_SOURCE = "ROTATION_SOURCE"
    ROLLED_BACK_STAGED = "ROLLED_BACK_STAGED"


class CredentialAlertLevel(StrEnum):
    """凭据到期告警级别：临期提醒 vs 已经过期。"""

    WARNING = "WARNING"
    OVERDUE = "OVERDUE"


class CredentialAlertStatus(StrEnum):
    """告警在业务上是否已处理完（与「消息发出去没有」是两件事）。"""

    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class CredentialAlertDeliveryStatus(StrEnum):
    """告警向 webhook 投递的传输状态。SKIPPED = 按策略不发，不是发送失败。"""

    PENDING = "PENDING"
    SENDING = "SENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class ExecutionStatus(StrEnum):
    """每次 ExecutionRun 的状态。

    SUCCEEDED / FAILED / CANCELLED 都是终态：重试要新建一行（带 retry_of_run_id），
    不许把原行改回 QUEUED，否则失败诊断和耗时就没了。
    """

    QUEUED = "QUEUED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class ExecutionTrigger(StrEnum):
    """这次运行是人手动点的还是定时触发的；影响归因和重跑策略。"""

    MANUAL = "MANUAL"
    SCHEDULED = "SCHEDULED"


class ScheduleMode(StrEnum):
    """调度模式：全量批次，或按水位线做增量（第 11 章之后）。"""

    BATCH = "BATCH"
    WATERMARK_INCREMENTAL = "WATERMARK_INCREMENTAL"


class ScheduleMisfirePolicy(StrEnum):
    """错过触发时刻怎么办：跳过这一轮，还是补跑一次（第 11 章之后）"""

    SKIP = "SKIP"
    FIRE_ONCE = "FIRE_ONCE"


class FailureCategory(StrEnum):
    """失败**归因**：为什么失败。给诊断和告警分流用，不是给用户看堆栈。"""

    CONFIGURATION = "CONFIGURATION"
    CREDENTIALS = "CREDENTIALS"
    CONNECTIVITY = "CONNECTIVITY"
    SOURCE_SCHEMA = "SOURCE_SCHEMA"
    SINK_WRITE = "SINK_WRITE"
    ENGINE = "ENGINE"
    OBSERVABILITY = "OBSERVABILITY"
    CANCELLATION = "CANCELLATION"
    UNKNOWN = "UNKNOWN"


class FailureStage(StrEnum):
    """失败**位置**：链路哪一段出的事。与 Category 正交，两者要一起看。"""

    SUBMISSION = "SUBMISSION"
    SOURCE = "SOURCE"
    TRANSFORM = "TRANSFORM"
    SINK = "SINK"
    MONITORING = "MONITORING"
    CANCELLATION = "CANCELLATION"
    PUBLISH = "PUBLISH"
    UNKNOWN = "UNKNOWN"


class QualityStatus(StrEnum):
    """质量报告结论。INCOMPLETE = 样本不足以下结论，**不等于通过**。"""

    PASSED = "PASSED"
    WARNING = "WARNING"
    FAILED = "FAILED"
    INCOMPLETE = "INCOMPLETE"


class GenerationKind(StrEnum):
    """向模型请求的三类内容。注意值是小写字符串，与其余枚举风格不同（照抄课件）。"""

    QUESTION = "question"
    CONFIG = "config"
    ERROR = "error"


class ConnectorType(StrEnum):
    """可登记的数据源类型；探查侧按这个值去 ProbeRegistry 查 Adapter。"""

    MYSQL = "MYSQL"
    DORIS = "DORIS"
    POSTGRESQL = "POSTGRESQL"
    ORACLE = "ORACLE"
    SQL_SERVER = "SQL_SERVER"
    HIVE = "HIVE"
    HDFS = "HDFS"
    LOCAL_FILE = "LOCAL_FILE"


class ConnectionStatus(StrEnum):
    """连接自身的连通性状态。新建默认 UNTESTED——没测过就不该直接拿去探查。"""

    UNTESTED = "UNTESTED"
    TESTING = "TESTING"
    READY = "READY"
    FAILED = "FAILED"


class DataProfileStatus(StrEnum):
    """一次探查任务的进度。Profile 是**快照**，带 expires_at，不能当永久 Schema。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    READY = "READY"
    FAILED = "FAILED"


class DataEndpointRole(StrEnum):
    """这个连接在当前项目里当源还是当目标；同一份库可以既被读又被写。"""

    SOURCE = "SOURCE"
    SINK = "SINK"


class FileAssetFormat(StrEnum):
    """受管文件资产的格式白名单（第 5 章启用）。"""

    CSV = "CSV"
    JSON = "JSON"
    JSONL = "JSONL"
    XLSX = "XLSX"
    PARQUET = "PARQUET"
    ORC = "ORC"
    XML = "XML"
    MARKDOWN = "MARKDOWN"
    PDF = "PDF"


class AgentRunStatus(StrEnum):
    """Agent **会话层**状态，与版本层、执行层分属不同表，不能互相冒充。

    WAITING_INPUT → QUEUED 是合法回边（人补了输入就重新排队），
    这与「终态原地重启」不是一回事。
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING_INPUT = "WAITING_INPUT"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    FAILED = "FAILED"
