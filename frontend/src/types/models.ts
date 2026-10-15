/**
 * 全应用共享的领域类型集中在此模块，避免散落在各组件里。
 * 字段命名对齐 docs/prd.md / docs/spec.md 的业务事实，仅用于前端展示（数据来自 mock）。
 */

export type ProjectRole = 'ADMIN' | 'DATA_ENGINEER' | 'APPROVER' | 'VIEWER'
export type HarnessRole =
  | 'DATA_ENGINEER'
  | 'DATA_APPROVER'
  | 'SECURITY_APPROVER'
  | 'OPERATOR'
  | 'AUDITOR'

export interface Actor {
  id: string
  name: string
  title: string
  roles: ProjectRole[]
  harnessRoles: HarnessRole[]
}

/** 工作区成员：来自后端 GET /context，供控制台选择真实身份。 */
export interface WorkspaceMember {
  user_id: string
  subject: string
  display_name: string
  role: ProjectRole
  harness_roles: HarnessRole[]
}

export interface WorkspaceContext {
  project_id: string
  project_name: string
  members: WorkspaceMember[]
}

/** 与后端 `domain/enums.py::ConnectorType` 逐字对齐，避免写库时被枚举拒绝。 */
export type ConnectorType =
  | 'MYSQL'
  | 'POSTGRESQL'
  | 'ORACLE'
  | 'SQL_SERVER'
  | 'HIVE'
  | 'HDFS'
  | 'DORIS'
  | 'LOCAL_FILE'

export type ReadinessStatus = 'READY' | 'TESTING' | 'FAILED' | 'UNKNOWN'
export type ConnectionDirection = 'SOURCE' | 'SINK'

export interface Connection {
  id: string
  name: string
  connectorType: ConnectorType
  host: string
  port: number
  database: string
  direction: ConnectionDirection
  status: ReadinessStatus
  lastTestedAt: string | null
}

export interface DataProfile {
  id: string
  connectionId: string
  table: string
  direction: ConnectionDirection
  sampleRows: number
  columnCount: number
  expiresAt: string
  status: ReadinessStatus
}

export type FileFormat = 'CSV' | 'JSON' | 'JSONL' | 'XLSX' | 'PARQUET' | 'ORC' | 'XML' | 'MARKDOWN' | 'PDF'
export type FileAssetStatus = 'PARSED' | 'PARSING' | 'FAILED'

export interface FileAsset {
  id: string
  name: string
  format: FileFormat
  rows: number
  sizeBytes: number
  status: FileAssetStatus
  uploadedAt: string
}

export type SecretStatus = 'PENDING' | 'STAGED' | 'ACTIVE' | 'ROLLED_BACK'

export interface SecretLifecycle {
  id: string
  connectionId: string
  status: SecretStatus
  issuedAt: string
  expiresAt: string
  daysToExpiry: number
}

export type RiskLevel = 'P0' | 'P1' | 'P2' | 'P3'

export type PipelineVersionStatus =
  | 'DRAFT'
  | 'PROFILING'
  | 'GENERATED'
  | 'VALIDATED'
  | 'WAITING_APPROVAL'
  | 'APPROVED'
  | 'REJECTED'

export interface PipelineVersion {
  id: string
  pipelineId: string
  versionNumber: number
  status: PipelineVersionStatus
  artifactSha256: string
  createdAt: string
  createdBy: string
}

export type AgentRunStatus = 'QUEUED' | 'RUNNING' | 'WAITING_INPUT' | 'WAITING_APPROVAL' | 'FAILED'

export interface AgentRun {
  id: string
  pipelineVersionId: string
  status: AgentRunStatus
  repairCount: number
  startedAt: string
  message: string
}

export type AgentStepKind = string
export type AgentStepStatus = 'DONE' | 'ACTIVE' | 'PENDING'

export interface AgentStep {
  id: string
  kind: AgentStepKind
  title: string
  detail: string
  status: AgentStepStatus
  at: string
}

export interface DesignNode {
  id: string
  kind: 'SOURCE' | 'TRANSFORM' | 'SINK'
  connectorType: string
  label: string
  route: string | null
}

export interface SchemaDiff {
  target: string
  addedColumns: string[]
  removedColumns: string[]
  typeChanges: { column: string; sourceType: string; targetType: string }[]
}

export interface DesignRisk {
  code: string
  level: 'HIGH' | 'MEDIUM' | 'LOW'
  message: string
}

export interface DesignReview {
  summary: string
  nodes: DesignNode[]
  schemaDiffs: SchemaDiff[]
  risks: DesignRisk[]
  samplePreview: { before: Record<string, string>[]; after: Record<string, string>[] }
  previewSemantics: string
}

export interface PipelineDesign {
  version: PipelineVersion
  hocon: string
  designReview: DesignReview
}

export type PreparationStatus =
  | 'PENDING'
  | 'WAITING_APPROVAL'
  | 'READY'
  | 'APPROVED'
  | 'REJECTED'
  | 'COMMITTED'
  | 'EXPIRED'
  | 'DENIED'

export interface ExecutionPreparation {
  id: string
  pipelineVersionId: string
  pipelineVersionNumber: number
  status: PreparationStatus
  riskLevel: RiskLevel
  inputFingerprint: string
  resourceFingerprint: string
  artifactSha256: string
  requiredRoles: HarnessRole[]
  impactReport: string
  rollbackPlan: string
  createdAt: string
  expiresAt: string
}

export type ExecutionStatus =
  | 'QUEUED'
  | 'SUBMITTED'
  | 'RUNNING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'CANCELLING'
  | 'CANCELLED'

export type ExecutionTrigger = 'MANUAL' | 'SCHEDULE' | 'RETRY'

export interface ExecutionMetrics {
  readRows: number
  writeRows: number
  rejectRows: number
}

export interface ExecutionRun {
  id: string
  preparationId: string
  pipelineVersionNumber: number
  status: ExecutionStatus
  idempotencyKey: string
  seatunnelJobId: number | null
  trigger: ExecutionTrigger
  startedAt: string
  finishedAt: string | null
  durationSeconds: number | null
  metrics: ExecutionMetrics
  errorSummary: string | null
}

export interface ExecutionEvent {
  id: string
  runId: string
  level: 'INFO' | 'WARN' | 'ERROR'
  eventType: string
  message: string
  occurredAt: string
}

export interface QualityCheck {
  code: string
  label: string
  passed: boolean
  detail: string
}

export interface QualityReport {
  id: string
  runId: string
  status: 'PASSED' | 'WARNING' | 'FAILED'
  passRate: number
  rejectRate: number
  checks: QualityCheck[]
}

export type SupervisionAction = 'CONTINUE' | 'THROTTLE' | 'PAUSE' | 'CANCEL' | 'QUARANTINE'

export interface SupervisionSnapshot {
  id: string
  runId: string
  action: SupervisionAction
  riskLevel: RiskLevel
  reasons: string[]
  createdAt: string
}

export interface WorkerStatus {
  id: string
  name: string
  online: boolean
  lastHeartbeatAt: string
  activeRuns: number
}

export interface BenchmarkMetric {
  key: string
  label: string
  value: number
  unit: '%' | 'ms' | ''
  target: string
  passed: boolean
  delta: number
}

export interface BenchmarkRun {
  id: string
  suiteVersion: string
  status: 'PASSED' | 'FAILED' | 'RUNNING'
  startedAt: string
  p0p1Regressions: number
  metrics: BenchmarkMetric[]
}

export interface BenchmarkCase {
  id: string
  name: string
  expected: string
  observed: string
  passed: boolean
  evidenceHash: string
}

export type ProposalType = 'POLICY' | 'PROMPT' | 'BENCHMARK'
export type ProposalStatus = 'DRAFT' | 'IN_REVIEW' | 'APPROVED' | 'REJECTED' | 'RELEASED'

export interface SafetyProposal {
  id: string
  type: ProposalType
  status: ProposalStatus
  title: string
  createdBy: string
  createdAt: string
  summary: string
}

export type ReleaseStage = 'SHADOW' | 'CANARY' | 'PROMOTION' | 'ROLLED_BACK'
export type ReleaseStatus = 'RUNNING' | 'VERIFIED' | 'ROLLED_BACK' | 'APPROVED'

export interface SafetyRelease {
  id: string
  stage: ReleaseStage
  status: ReleaseStatus
  benchmarkRunId: string
  progress: number
  updatedAt: string
}

export interface OverviewMetrics {
  todaySucceeded: number
  todayFailed: number
  pendingPreparations: number
  readyConnections: number
  fileAssets: number
}
