from enum import StrEnum


class ProjectRole(StrEnum):
    ADMIN = "ADMIN"
    DATA_ENGINEER = "DATA_ENGINEER"
    APPROVER = "APPROVER"
    VIEWER = "VIEWER"


class PipelineVersionStatus(StrEnum):
    DRAFT = "DRAFT"
    PROFILING = "PROFILING"
    GENERATED = "GENERATED"
    VALIDATED = "VALIDATED"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class ApprovalStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class ApprovalDecisionValue(StrEnum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"


class RollbackStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class CleanupStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretMigrationStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretRotationStatus(StrEnum):
    VALIDATING = "VALIDATING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ACTIVATING = "ACTIVATING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretRotationRollbackStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretDestructionStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class SecretDestructionTarget(StrEnum):
    ROTATION_SOURCE = "ROTATION_SOURCE"
    ROLLED_BACK_STAGED = "ROLLED_BACK_STAGED"


class CredentialAlertLevel(StrEnum):
    WARNING = "WARNING"
    OVERDUE = "OVERDUE"


class CredentialAlertStatus(StrEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class CredentialAlertDeliveryStatus(StrEnum):
    PENDING = "PENDING"
    SENDING = "SENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class ExecutionStatus(StrEnum):
    QUEUED = "QUEUED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class ExecutionTrigger(StrEnum):
    MANUAL = "MANUAL"
    SCHEDULED = "SCHEDULED"


class ScheduleMode(StrEnum):
    BATCH = "BATCH"
    WATERMARK_INCREMENTAL = "WATERMARK_INCREMENTAL"


class ScheduleMisfirePolicy(StrEnum):
    SKIP = "SKIP"
    FIRE_ONCE = "FIRE_ONCE"


class FailureCategory(StrEnum):
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
    SUBMISSION = "SUBMISSION"
    SOURCE = "SOURCE"
    TRANSFORM = "TRANSFORM"
    SINK = "SINK"
    MONITORING = "MONITORING"
    CANCELLATION = "CANCELLATION"
    PUBLISH = "PUBLISH"
    UNKNOWN = "UNKNOWN"


class QualityStatus(StrEnum):
    PASSED = "PASSED"
    WARNING = "WARNING"
    FAILED = "FAILED"
    INCOMPLETE = "INCOMPLETE"


class GenerationKind(StrEnum):
    QUESTION = "question"
    CONFIG = "config"
    ERROR = "error"


class ConnectorType(StrEnum):
    MYSQL = "MYSQL"
    DORIS = "DORIS"
    POSTGRESQL = "POSTGRESQL"
    ORACLE = "ORACLE"
    SQL_SERVER = "SQL_SERVER"
    HIVE = "HIVE"
    HDFS = "HDFS"
    LOCAL_FILE = "LOCAL_FILE"


class ConnectionStatus(StrEnum):
    UNTESTED = "UNTESTED"
    TESTING = "TESTING"
    READY = "READY"
    FAILED = "FAILED"


class DataProfileStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    READY = "READY"
    FAILED = "FAILED"


class DataEndpointRole(StrEnum):
    SOURCE = "SOURCE"
    SINK = "SINK"


class FileAssetFormat(StrEnum):
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
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING_INPUT = "WAITING_INPUT"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    FAILED = "FAILED"
