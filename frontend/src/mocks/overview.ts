import type { Connection, OverviewMetrics } from '@/types/models'

import { mockConnections } from './connections'

export const mockOverviewMetrics: OverviewMetrics = {
  todaySucceeded: 12,
  todayFailed: 1,
  pendingPreparations: 3,
  readyConnections: mockConnections.filter((connection) => connection.status === 'READY').length,
  fileAssets: 3,
}

export const mockReadyConnections: Connection[] = mockConnections.filter(
  (connection) => connection.status === 'READY',
)

export const mockFlowStages: { key: string; label: string; hint: string }[] = [
  { key: 'profile', label: '受限 Profile', hint: '只暴露结构与脱敏样本' },
  { key: 'generate', label: '生成候选', hint: '单次调用，最多 3 份' },
  { key: 'gate', label: '确定性门禁', hint: '静态规则 + 真实引擎检查' },
  { key: 'approve', label: '具名审批', hint: '申请人 ≠ 审批人' },
  { key: 'commit', label: 'Harness 提交', hint: '一次性授权 + Outbox' },
  { key: 'execute', label: 'SeaTunnel 执行', hint: '数据面独立进程' },
]

export const mockGuardrails: { code: string; label: string }[] = [
  { code: 'HARNESS-P0-CROSS-PROJECT', label: '跨项目资源访问直接拒绝' },
  { code: 'HARNESS-P0-DESTRUCTIVE-DUAL', label: '破坏性操作强制双职责双人' },
  { code: 'HARNESS-P1-OUTPUT-AMPLIFICATION', label: '输出放大超阈值要求审批' },
  { code: 'HARNESS-P1-NO-UNBOUNDED-SCAN', label: '无界全表扫描阻断' },
  { code: 'HARNESS-P0-SECRET-EGRESS', label: '凭据外发直接拒绝' },
]
