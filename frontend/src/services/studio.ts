import { mockAgentRun, mockAgentSteps, mockDesign, mockPreparations } from '@/mocks/studio'

import { clone, mockDelay } from './mock'

import type { AgentRun, AgentStep, ExecutionPreparation, PipelineDesign } from '@/types/models'

/** Agent 生成与冻结域接口。 */
export const studioService = {
  getDesign(): Promise<PipelineDesign> {
    return mockDelay(clone(mockDesign))
  },
  getAgentRun(): Promise<AgentRun> {
    return mockDelay(clone(mockAgentRun))
  },
  listAgentSteps(): Promise<AgentStep[]> {
    return mockDelay(clone(mockAgentSteps))
  },
  listPreparations(): Promise<ExecutionPreparation[]> {
    return mockDelay(clone(mockPreparations))
  },
}
