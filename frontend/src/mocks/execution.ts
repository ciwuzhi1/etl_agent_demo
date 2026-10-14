import type {
  ExecutionEvent,
  ExecutionRun,
  QualityReport,
  SupervisionSnapshot,
  WorkerStatus,
} from '@/types/models'

export const mockRuns: ExecutionRun[] = [
  {
    id: 'run-20261014-3f2a',
    preparationId: 'prep-20261014-01',
    pipelineVersionNumber: 12,
    status: 'SUCCEEDED',
    idempotencyKey: 'exec:orders-clean:2026-10-14',
    seatunnelJobId: 90431,
    trigger: 'MANUAL',
    startedAt: '2026-10-14T02:40:00Z',
    finishedAt: '2026-10-14T02:42:18Z',
    durationSeconds: 138,
    metrics: { readRows: 4, writeRows: 4, rejectRows: 4 },
    errorSummary: null,
  },
  {
    id: 'run-20261014-71bd',
    preparationId: 'prep-20261014-01',
    pipelineVersionNumber: 12,
    status: 'RUNNING',
    idempotencyKey: 'exec:orders-clean:2026-10-14:retry',
    seatunnelJobId: 90455,
    trigger: 'RETRY',
    startedAt: '2026-10-14T03:10:00Z',
    finishedAt: null,
    durationSeconds: null,
    metrics: { readRows: 1_240_000, writeRows: 1_180_500, rejectRows: 59_500 },
    errorSummary: null,
  },
  {
    id: 'run-20261013-a08c',
    preparationId: 'prep-20261013-04',
    pipelineVersionNumber: 11,
    status: 'FAILED',
    idempotencyKey: 'exec:orders-clean:2026-10-13',
    seatunnelJobId: 90388,
    trigger: 'SCHEDULE',
    startedAt: '2026-10-13T18:00:00Z',
    finishedAt: '2026-10-13T18:04:52Z',
    durationSeconds: 292,
    metrics: { readRows: 1_102_300, writeRows: 0, rejectRows: 1_102_300 },
    errorSummary: '质量门禁未通过：拒绝率 100.0% 超过上限 50.0%，已阻断 Doris 发布。',
  },
  {
    id: 'run-20261013-c114',
    preparationId: 'prep-20261013-02',
    pipelineVersionNumber: 10,
    status: 'CANCELLED',
    idempotencyKey: 'exec:orders-clean:2026-10-13:manual',
    seatunnelJobId: 90351,
    trigger: 'MANUAL',
    startedAt: '2026-10-13T09:20:00Z',
    finishedAt: '2026-10-13T09:26:11Z',
    durationSeconds: 371,
    metrics: { readRows: 640_000, writeRows: 0, rejectRows: 0 },
    errorSummary: null,
  },
]

export const mockEvents: ExecutionEvent[] = [
  { id: 'evt-1', runId: 'run-20261014-3f2a', level: 'INFO', eventType: 'QUEUED', message: 'Commit 成功，创建 Run 与 Outbox 命令。', occurredAt: '2026-10-14T02:40:00Z' },
  { id: 'evt-2', runId: 'run-20261014-3f2a', level: 'INFO', eventType: 'STAGING_PREPARED', message: 'Doris 影子表已就绪，写入幂等发布标记。', occurredAt: '2026-10-14T02:40:22Z' },
  { id: 'evt-3', runId: 'run-20261014-3f2a', level: 'INFO', eventType: 'ENGINE_STATUS_CHANGED', message: 'SeaTunnel 状态 RUNNING → FINISHED（JobId 90431）。', occurredAt: '2026-10-14T02:41:50Z' },
  { id: 'evt-4', runId: 'run-20261014-3f2a', level: 'INFO', eventType: 'QUALITY_REPORT_GENERATED', message: '质量报告生成：通过率 50.0%，拒绝 4 行。', occurredAt: '2026-10-14T02:42:05Z' },
  { id: 'evt-5', runId: 'run-20261014-3f2a', level: 'INFO', eventType: 'ATOMIC_PUBLISHED', message: 'Doris 有效表原子发布完成，旧版保留在影子表名下。', occurredAt: '2026-10-14T02:42:18Z' },
  { id: 'evt-6', runId: 'run-20261013-a08c', level: 'ERROR', eventType: 'ENGINE_STATUS_CHANGED', message: 'SeaTunnel 状态 RUNNING → FAILED（JobId 90388）。', occurredAt: '2026-10-13T18:04:40Z' },
  { id: 'evt-7', runId: 'run-20261013-a08c', level: 'ERROR', eventType: 'QUALITY_GATE_BLOCKED', message: '拒绝率超限，禁止发布。', occurredAt: '2026-10-13T18:04:52Z' },
]

export const mockQualityReports: QualityReport[] = [
  {
    id: 'qr-run-3f2a',
    runId: 'run-20261014-3f2a',
    status: 'WARNING',
    passRate: 0.5,
    rejectRate: 0.5,
    checks: [
      { code: 'AMOUNT_POSITIVE', label: '金额必须为正', passed: false, detail: '1 行命中：order_id=1002 amount=-5.00' },
      { code: 'NAME_TRIMMED', label: '客户名称去空格', passed: true, detail: 'Alice 两侧空格已删除' },
      { code: 'EMAIL_MASKED', label: '邮箱脱敏', passed: true, detail: 'alice@example.com → a***@example.com' },
      { code: 'ROW_COUNT', label: '有效行数下限', passed: true, detail: '有效 4 行 ≥ 下限 1 行' },
    ],
  },
  {
    id: 'qr-run-a08c',
    runId: 'run-20261013-a08c',
    status: 'FAILED',
    passRate: 0,
    rejectRate: 1,
    checks: [
      { code: 'SOURCE_SCHEMA', label: '源表结构一致', passed: false, detail: '源表缺少 amount 列，全部行被拒绝' },
      { code: 'ROW_COUNT', label: '有效行数下限', passed: false, detail: '有效 0 行 < 下限 1 行' },
    ],
  },
]

export const mockSupervision: SupervisionSnapshot[] = [
  {
    id: 'sup-3f2a-1',
    runId: 'run-20261014-3f2a',
    action: 'CONTINUE',
    riskLevel: 'P3',
    reasons: ['六维预算均在阈值内'],
    createdAt: '2026-10-14T02:41:00Z',
  },
  {
    id: 'sup-71bd-1',
    runId: 'run-20261014-71bd',
    action: 'THROTTLE',
    riskLevel: 'P1',
    reasons: ['输出放大比 1.05× 接近警告线', '拒绝率 4.8% 持续上升'],
    createdAt: '2026-10-14T03:18:00Z',
  },
  {
    id: 'sup-a08c-1',
    runId: 'run-20261013-a08c',
    action: 'QUARANTINE',
    riskLevel: 'P0',
    reasons: ['SUPERVISOR-P0-RECORD-BUDGET 未命中，但拒绝率 100% 触发隔离', '阻断 Doris 发布'],
    createdAt: '2026-10-13T18:04:52Z',
  },
]

export const mockWorkers: WorkerStatus[] = [
  { id: 'worker-01', name: 'worker-01@etl-agent', online: true, lastHeartbeatAt: '2026-10-14T03:19:40Z', activeRuns: 1 },
  { id: 'worker-02', name: 'worker-02@etl-agent', online: true, lastHeartbeatAt: '2026-10-14T03:19:38Z', activeRuns: 0 },
  { id: 'worker-03', name: 'worker-03@etl-agent', online: false, lastHeartbeatAt: '2026-10-13T22:10:02Z', activeRuns: 0 },
]
