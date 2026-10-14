import { mockFlowStages, mockGuardrails, mockOverviewMetrics, mockReadyConnections } from '@/mocks/overview'

import { clone, mockDelay } from './mock'

import type { Connection, OverviewMetrics } from '@/types/models'

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

/** 总览域接口。 */
export const overviewService = {
  getOverview(): Promise<OverviewPayload> {
    return mockDelay(
      clone({
        metrics: mockOverviewMetrics,
        readyConnections: mockReadyConnections,
        flowStages: mockFlowStages,
        guardrails: mockGuardrails,
      }),
    )
  },
}
