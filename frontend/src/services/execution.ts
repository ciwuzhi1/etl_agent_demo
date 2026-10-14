import { mockEvents, mockQualityReports, mockRuns, mockSupervision, mockWorkers } from '@/mocks/execution'

import { clone, mockDelay } from './mock'

import type {
  ExecutionEvent,
  ExecutionRun,
  QualityReport,
  SupervisionSnapshot,
  WorkerStatus,
} from '@/types/models'

/** 执行、观测与失败诊断域接口。 */
export const executionService = {
  listRuns(): Promise<ExecutionRun[]> {
    return mockDelay(clone(mockRuns))
  },
  listEvents(runId: string): Promise<ExecutionEvent[]> {
    return mockDelay(
      clone(
        mockEvents
          .filter((event) => event.runId === runId)
          .sort((left, right) => left.occurredAt.localeCompare(right.occurredAt)),
      ),
    )
  },
  getQualityReport(runId: string): Promise<QualityReport | null> {
    return mockDelay(clone(mockQualityReports.find((report) => report.runId === runId) ?? null))
  },
  getSupervision(runId: string): Promise<SupervisionSnapshot | null> {
    return mockDelay(
      clone(
        mockSupervision
          .filter((snapshot) => snapshot.runId === runId)
          .sort((left, right) => right.createdAt.localeCompare(left.createdAt))[0] ?? null,
      ),
    )
  },
  listWorkers(): Promise<WorkerStatus[]> {
    return mockDelay(clone(mockWorkers))
  },
}
