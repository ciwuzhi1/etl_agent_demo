import { apiRequest } from './http'
import { getProjectId, whenSessionReady } from './sessionStore'

import type { Connection, ConnectorType, OverviewMetrics, ReadinessStatus } from '@/types/models'

export interface FlowStage {
  key: string
  label: string
  hint: string
}

export interface Guardrail {
  code: string
  label: string
}

export interface OverviewPayload {
  metrics: OverviewMetrics
  readyConnections: Connection[]
  flowStages: FlowStage[]
  guardrails: Guardrail[]
}

interface OverviewMetricsDTO {
  today_succeeded: number
  today_failed: number
  pending_preparations: number
  ready_connections: number
  file_assets: number
}

interface ConnectionDTO {
  id: string
  name: string
  connector_type: string
  host: string | null
  port: number | null
  database_name: string | null
  status: string
  last_tested_at: string | null
}

interface OverviewDTO {
  metrics: OverviewMetricsDTO
  ready_connections: ConnectionDTO[]
  flow_stages: FlowStage[]
  guardrails: Guardrail[]
}

function toConnection(dto: ConnectionDTO): Connection {
  const status: ReadinessStatus =
    dto.status === 'READY' || dto.status === 'TESTING' || dto.status === 'FAILED'
      ? dto.status
      : 'UNKNOWN'
  return {
    id: dto.id,
    name: dto.name,
    connectorType: dto.connector_type as ConnectorType,
    host: dto.host ?? '',
    port: dto.port ?? 0,
    database: dto.database_name ?? '',
    direction: 'SOURCE',
    status,
    lastTestedAt: dto.last_tested_at,
  }
}

/** 总览域接口。第 15 章：切换到真实后端 API 调用。 */
export const overviewService = {
  /** 获取项目总览指标（GET /projects/{id}/overview） */
  async getOverview(projectId?: string): Promise<OverviewPayload> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const dto = await apiRequest<OverviewDTO>(`/projects/${pid}/overview`)
    return {
      metrics: {
        todaySucceeded: dto.metrics.today_succeeded,
        todayFailed: dto.metrics.today_failed,
        pendingPreparations: dto.metrics.pending_preparations,
        readyConnections: dto.metrics.ready_connections,
        fileAssets: dto.metrics.file_assets,
      },
      readyConnections: dto.ready_connections.map(toConnection),
      flowStages: dto.flow_stages,
      guardrails: dto.guardrails,
    }
  },

  /** 健康探针（GET /health） */
  getHealth(): Promise<Record<string, unknown>> {
    return fetch('/health').then((response) => {
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      return response.json() as Promise<Record<string, unknown>>
    })
  },
}
