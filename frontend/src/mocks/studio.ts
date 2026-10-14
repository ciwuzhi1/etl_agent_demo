import type { AgentRun, AgentStep, ExecutionPreparation, PipelineDesign } from '@/types/models'

export const mockDesign: PipelineDesign = {
  version: {
    id: 'pv-12',
    pipelineId: 'pipe-orders-clean',
    versionNumber: 12,
    status: 'WAITING_APPROVAL',
    artifactSha256: 'a1f9c40b7d2e5831f0c6a9b4d7e2801536c4ab90ff21d35e70839bc4a61d02e7',
    createdAt: '2026-10-14T02:26:00Z',
    createdBy: '林知远',
  },
  hocon: [
    'env {',
    '  job.mode = "BATCH"',
    '  job.name = "orders_clean_${now}"',
    '}',
    '',
    'source {',
    '  JdbcSource {',
    '    url = "jdbc:mysql://${MYSQL_HOST}:3308/source_db"',
    '    query = "SELECT * FROM orders WHERE updated_at > \'${watermark_lower}\'"',
    '    connector.jdbc.driver = "com.mysql.cj.jdbc.Driver"',
    '  }',
    '}',
    '',
    'transform {',
    '  Sql {',
    '    query = """',
    '      SELECT order_id, trim(customer_name) AS customer_name, amount,',
    '             regexp_replace(email, \'(.).*@\', \'$1***@\') AS email',
    '      FROM orders WHERE amount > 0',
    '    """',
    '  }',
    '}',
    '',
    'sink {',
    '  JdbcSink {',
    '    url = "jdbc:mysql://${DORIS_HOST}:9030/analytics"',
    '    table = "clean_orders"',
    '    support_upsert_by_key = true',
    '  }',
    '}',
  ].join('\n'),
  designReview: {
    summary:
      '候选方案将 MySQL orders 清洗后写入 Doris clean_orders：裁剪字段、去空格、金额过滤、邮箱脱敏，并输出拒绝行到错误表。',
    nodes: [
      { id: 'n-source', kind: 'SOURCE', connectorType: 'MYSQL', label: 'source_db.orders', route: null },
      { id: 'n-transform', kind: 'TRANSFORM', connectorType: 'SQL', label: '清洗与脱敏', route: 'orders → clean_orders' },
      { id: 'n-sink', kind: 'SINK', connectorType: 'DORIS', label: 'analytics.clean_orders', route: null },
    ],
    schemaDiffs: [
      {
        target: 'analytics.clean_orders',
        addedColumns: ['error_code'],
        removedColumns: ['raw_name'],
        typeChanges: [{ column: 'amount', sourceType: 'decimal(12,2)', targetType: 'decimal(14,2)' }],
      },
    ],
    risks: [
      { code: 'HARNESS-P1-OUTPUT-AMPLIFICATION', level: 'MEDIUM', message: '输出放大比预估 1.4×，超过 1.0× 警告线。' },
      { code: 'HARNESS-P2-NULL-STRICTNESS', level: 'LOW', message: 'customer_name 允许空值，将按契约写入错误表。' },
    ],
    samplePreview: {
      before: [
        { order_id: '1001', customer_name: '  Alice ', amount: '120.00', email: 'alice@example.com' },
        { order_id: '1002', customer_name: 'Bob', amount: '-5.00', email: 'bob@example.com' },
      ],
      after: [
        { order_id: '1001', customer_name: 'Alice', amount: '120.00', email: 'a***@example.com' },
        { order_id: '1002', customer_name: 'Bob', amount: '-5.00', email: 'b***@example.com' },
      ],
    },
    previewSemantics: 'Before/After 均为脱敏样本，仅用于审查结构变化，不包含真实业务数据。',
  },
}

export const mockAgentRun: AgentRun = {
  id: 'run-agent-20261014-02',
  pipelineVersionId: 'pv-12',
  status: 'WAITING_APPROVAL',
  repairCount: 1,
  startedAt: '2026-10-14T02:20:00Z',
  message: '候选已通过确定性门禁与真实引擎检查，等待具名审批。',
}

export const mockAgentSteps: AgentStep[] = [
  { id: 'step-1', kind: 'GENERATE', title: '生成候选', detail: '基于受限 Profile 生成 direct 路径候选（单次调用）。', status: 'DONE', at: '2026-10-14T02:20:12Z' },
  { id: 'step-2', kind: 'REPAIR', title: '修复 1/2', detail: '质量契约缺少机器可判定谓词，已按反馈修复。', status: 'DONE', at: '2026-10-14T02:23:40Z' },
  { id: 'step-3', kind: 'REVIEW', title: '门禁与引擎检查', detail: '静态门禁 + SeaTunnel 真实检查通过，未发现敏感上下文。', status: 'DONE', at: '2026-10-14T02:25:30Z' },
  { id: 'step-4', kind: 'CLARIFY', title: '等待审批', detail: '等待数据审批人裁决，申请人不能自批。', status: 'ACTIVE', at: '2026-10-14T02:26:00Z' },
]

export const mockPreparations: ExecutionPreparation[] = [
  {
    id: 'prep-20261014-01',
    pipelineVersionId: 'pv-12',
    pipelineVersionNumber: 12,
    status: 'READY',
    riskLevel: 'P1',
    inputFingerprint: 'sha256:6b1c9d2f4a7e0835b6c1d9e2f4a7083b6c1d9e2f4a7083b6c1d9e2f4a7083b6c',
    resourceFingerprint: 'sha256:2f4a7083b6c1d9e2f4a7083b6c1d9e2f4a7083b6c1d9e2f4a7083b6c1d9e2f4',
    artifactSha256: 'a1f9c40b7d2e5831f0c6a9b4d7e2801536c4ab90ff21d35e70839bc4a61d02e7',
    requiredRoles: ['DATA_APPROVER'],
    impactReport: '读取 orders 预估 4 行，写入 clean_orders 4 行，输出放大比 1.0×。',
    rollbackPlan: 'Doris 影子表 + 原子交换，保留旧版本 7 天供回滚。',
    createdAt: '2026-10-14T02:30:00Z',
    expiresAt: '2026-10-15T02:30:00Z',
  },
  {
    id: 'prep-20261014-02',
    pipelineVersionId: 'pv-09',
    pipelineVersionNumber: 9,
    status: 'PENDING',
    riskLevel: 'P0',
    inputFingerprint: 'sha256:9e2f4a7083b6c1d9e2f4a7083b6c1d9e2f4a7083b6c1d9e2f4a7083b6c1d9e2f',
    resourceFingerprint: 'sha256:1d9e2f4a7083b6c1d9e2f4a7083b6c1d9e2f4a7083b6c1d9e2f4a7083b6c1d9e',
    artifactSha256: 'b7d2e5831f0c6a9b4d7e2801536c4ab90ff21d35e70839bc4a61d02e7a1f9c40',
    requiredRoles: ['DATA_APPROVER', 'SECURITY_APPROVER'],
    impactReport: '清理 Doris 保留表，属不可逆 P0 破坏性操作。',
    rollbackPlan: '不可回滚；执行前二次确认保留期与来源 Run。',
    createdAt: '2026-10-14T03:02:00Z',
    expiresAt: '2026-10-15T03:02:00Z',
  },
]
