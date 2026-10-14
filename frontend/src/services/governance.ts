import { mockBenchmark, mockBenchmarkCases, mockProposals, mockReleases } from '@/mocks/governance'

import { clone, mockDelay } from './mock'

import type { BenchmarkCase, BenchmarkRun, SafetyProposal, SafetyRelease } from '@/types/models'

/** 安全治理域接口。 */
export const governanceService = {
  getBenchmark(): Promise<BenchmarkRun> {
    return mockDelay(clone(mockBenchmark))
  },
  listBenchmarkCases(): Promise<BenchmarkCase[]> {
    return mockDelay(clone(mockBenchmarkCases))
  },
  listProposals(): Promise<SafetyProposal[]> {
    return mockDelay(clone(mockProposals))
  },
  listReleases(): Promise<SafetyRelease[]> {
    return mockDelay(clone(mockReleases))
  },
}
