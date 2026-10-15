import { apiRequest } from './http'
import { getProjectId, whenSessionReady } from './sessionStore'

import type {
  ExecutionEvent,
  ExecutionRun,
  QualityReport,
  SupervisionSnapshot,
  WorkerStatus,
} from '@/types/models'

/**
 * 执行、观测与失败诊断域接口。第 15 章：切换到真实后端 API 调用。
 * 后端路由：/api/v1/execution-runs、/execution-runs/{id}/events、/supervision 等。
 */

interface ExecutionRunDTO {
  id: string
  status: string
  engine_status: string | null
  source_rows: number | null
  sink_rows: number | null
  failure_category: string | null
  failure_stage: string | null
  retryable: boolean | null
  submitted_at: string | null
  started_at: string | null
  finished_at: string | null
}

interface EventDTO {
  id: string
  occurred_at: string
  level: string
  stage: string
  event_type: string
  message: string
}

interface QualityDTO {
  id: string
  run_id: string
  status: string
  total_rows: number | null
  passed_rows: number | null
  rejected_rows: number | null
  reject_rate: number | null
}

interface SupervisionDTO {
  id: string
  run_id: string
  sequence: number
  observed_at: string
  engine_status: string
  action: string
  risk_level: string
  cancel_required: boolean
  block_publish: boolean
  reasons: string[]
}

interface WorkerDTO {
  id: string
  name: string
  online: boolean
  last_heartbeat_at: string | null
  active_runs: number
}

function durationSeconds(startedAt: string | null, finishedAt: string | null): number | null {
  if (!startedAt || !finishedAt) return null
  const start = Date.parse(startedAt)
  const end = Date.parse(finishedAt)
  if (Number.isNaN(start) || Number.isNaN(end)) return null
  return Math.max(0, Math.round((end - start) / 1000))
}

function toRun(dto: ExecutionRunDTO): ExecutionRun {
  return {
    id: dto.id,
    preparationId: '',
    pipelineVersionNumber: 0,
    status: dto.status as ExecutionRun['status'],
    idempotencyKey: '',
    seatunnelJobId: null,
    trigger: 'MANUAL',
    startedAt: dto.started_at ?? '',
    finishedAt: dto.finished_at,
    durationSeconds: durationSeconds(dto.started_at, dto.finished_at),
    metrics: {
      readRows: dto.source_rows ?? 0,
      writeRows: dto.sink_rows ?? 0,
      rejectRows: 0,
    },
    errorSummary: dto.failure_category ?? null,
  }
}

function toSupervision(dto: SupervisionDTO): SupervisionSnapshot {
  return {
    id: dto.id,
    runId: dto.run_id,
    action: dto.action as SupervisionSnapshot['action'],
    riskLevel: dto.risk_level as SupervisionSnapshot['riskLevel'],
    reasons: dto.reasons ?? [],
    createdAt: dto.observed_at,
  }
}

function toQuality(dto: QualityDTO): QualityReport {
  const total = dto.total_rows ?? 0
  const passed = dto.passed_rows ?? 0
  const passRate = total > 0 ? passed / total : 0
  return {
    id: dto.id,
    runId: dto.run_id,
    status: dto.status as QualityReport['status'],
    passRate,
    rejectRate: dto.reject_rate ?? 0,
    checks: [
      {
        code: 'ROWS',
        label: '有效 / 拒绝行',
        passed: dto.status === 'PASSED',
        detail: `有效 ${passed} · 拒绝 ${dto.rejected_rows ?? 0}`,
      },
    ],
  }
}

export const executionService = {
  /** 列出项目的执行记录 */
  async listRuns(projectId?: string): Promise<ExecutionRun[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<ExecutionRunDTO[]>(`/projects/${pid}/execution-runs`)
    return rows.map(toRun)
  },

  /** 获取执行详情 */
  async getRun(runId: string): Promise<ExecutionRun> {
    return toRun(await apiRequest<ExecutionRunDTO>(`/execution-runs/${runId}`))
  },

  /** 列出执行事件（append-only 日志） */
  async listEvents(runId: string): Promise<ExecutionEvent[]> {
    const rows = await apiRequest<EventDTO[]>(`/execution-runs/${runId}/events`)
    return rows.map((dto) => ({
      id: dto.id,
      runId,
      level: dto.level as ExecutionEvent['level'],
      eventType: dto.event_type,
      message: dto.message,
      occurredAt: dto.occurred_at,
    }))
  },

  /** 获取质量报告 */
  async getQualityReport(runId: string): Promise<QualityReport | null> {
    const dto = await apiRequest<QualityDTO | null>(`/execution-runs/${runId}/quality-report`)
    return dto ? toQuality(dto) : null
  },

  /** 获取最新监督快照 */
  async getSupervision(runId: string): Promise<SupervisionSnapshot | null> {
    const dto = await apiRequest<SupervisionDTO | null>(
      `/execution-runs/${runId}/supervision/latest`,
    )
    return dto ? toSupervision(dto) : null
  },

  /** 列出监督快照历史 */
  async listSupervisionSnapshots(runId: string): Promise<SupervisionSnapshot[]> {
    const rows = await apiRequest<SupervisionDTO[]>(`/execution-runs/${runId}/supervision`)
    return rows.map(toSupervision)
  },

  /** 请求取消执行 */
  async requestCancellation(runId: string, reason: string): Promise<ExecutionRun> {
    return toRun(
      await apiRequest<ExecutionRunDTO>(`/execution-runs/${runId}/cancellation`, {
        method: 'POST',
        body: JSON.stringify({ reason }),
      }),
    )
  },

  /** 重试准备：后端返回新的 QUEUED 运行，这里回传它的 id。 */
  async retryPreparation(runId: string): Promise<{ preparation_id: string }> {
    const dto = await apiRequest<ExecutionRunDTO>(`/execution-runs/${runId}/retry-preparations`, {
      method: 'POST',
    })
    return { preparation_id: dto.id }
  },

  /** 列出 worker 心跳状态 */
  async listWorkers(): Promise<WorkerStatus[]> {
    const rows = await apiRequest<WorkerDTO[]>('/workers')
    return rows.map((dto) => ({
      id: dto.id,
      name: dto.name,
      online: dto.online,
      lastHeartbeatAt: dto.last_heartbeat_at ?? '',
      activeRuns: dto.active_runs,
    }))
  },
}
