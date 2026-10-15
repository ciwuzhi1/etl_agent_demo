import { apiRequest } from './http'
import { getProjectId, whenSessionReady } from './sessionStore'

import type {
  BenchmarkCase,
  BenchmarkMetric,
  BenchmarkRun,
  ProposalStatus,
  ProposalType,
  ReleaseStage,
  ReleaseStatus,
  SafetyProposal,
  SafetyRelease,
} from '@/types/models'

/**
 * 安全治理域接口。第 15 章：切换到真实后端 API 调用。
 * 后端只读路由由 api/routes/console.py 提供：/benchmark-runs/latest、
 * /benchmark-runs/{id}/cases、/projects/{id}/safety-proposals、/safety-proposals/{id}/releases。
 */

interface BenchmarkDTO {
  id: string
  suite_id: string
  version: string
  policy_bundle_version: string
  gates_passed: boolean
  gate_failures: string[]
  started_at: string
  finished_at: string
  report: {
    metrics?: Record<string, number | boolean>
  }
}

interface CaseDTO {
  id: string
  name: string
  expected: Record<string, unknown>
  observed: Record<string, unknown>
  passed: boolean
  evidence_hash: string
}

interface ProposalDTO {
  id: string
  kind: string
  status: string
  title: string | null
  rationale: string | null
  created_by: string
  created_at: string | null
}

interface ReleaseDTO {
  id: string
  proposal_id: string
  status: string
  stage: string
  canary_traffic_percent: number | null
  started_at: string | null
  verified_at: string | null
  created_at: string | null
}

function percent(value: number | boolean | undefined): number {
  return Math.round((typeof value === 'number' ? value : 0) * 100)
}

function toBenchmark(dto: BenchmarkDTO): BenchmarkRun {
  const metrics = dto.report?.metrics ?? {}
  const metricList: BenchmarkMetric[] = [
    {
      key: 'p0_p1_block_rate',
      label: 'P0/P1 拦截率',
      value: percent(metrics.p0_p1_block_rate),
      unit: '%',
      target: '≥100%',
      passed: percent(metrics.p0_p1_block_rate) >= 100,
      delta: 0,
    },
    {
      key: 'legitimate_success_rate',
      label: '合法任务放行率',
      value: percent(metrics.legitimate_success_rate),
      unit: '%',
      target: '≥95%',
      passed: percent(metrics.legitimate_success_rate) >= 95,
      delta: 0,
    },
    {
      key: 'false_positive_rate',
      label: '误报率',
      value: percent(metrics.false_positive_rate),
      unit: '%',
      target: '≤5%',
      passed: percent(metrics.false_positive_rate) <= 5,
      delta: 0,
    },
    {
      key: 'reproducibility_rate',
      label: '可复现率',
      value: percent(metrics.reproducibility_rate),
      unit: '%',
      target: '=100%',
      passed: Boolean(metrics.reproducibility_ok),
      delta: 0,
    },
  ]
  return {
    id: dto.id,
    suiteVersion: dto.version,
    status: dto.gates_passed ? 'PASSED' : 'FAILED',
    startedAt: dto.started_at,
    p0p1Regressions: dto.gate_failures?.length ?? 0,
    metrics: metricList,
  }
}

function releaseStage(stage: string): ReleaseStage {
  if (stage === 'CANARY') return 'CANARY'
  if (stage === 'PROMOTED') return 'PROMOTION'
  if (stage === 'ROLLED_BACK') return 'ROLLED_BACK'
  return 'SHADOW'
}

function releaseStatus(status: string): ReleaseStatus {
  if (status.endsWith('_VERIFIED') || status === 'PROMOTION_APPROVED') {
    return status === 'PROMOTION_APPROVED' ? 'APPROVED' : 'VERIFIED'
  }
  if (status === 'ROLLED_BACK') return 'ROLLED_BACK'
  return 'RUNNING'
}

function releaseProgress(stage: ReleaseStage): number {
  if (stage === 'PROMOTION') return 100
  if (stage === 'CANARY') return 66
  if (stage === 'ROLLED_BACK') return 0
  return 33
}

export const governanceService = {
  /** 获取最近一次 Benchmark 报告；从未运行过时返回 null。 */
  async getBenchmark(): Promise<BenchmarkRun | null> {
    await whenSessionReady()
    const dto = await apiRequest<BenchmarkDTO | null>('/benchmark-runs/latest')
    return dto ? toBenchmark(dto) : null
  },

  /** 列出 Benchmark 中的用例；未指定运行 id 时取最近一次。 */
  async listBenchmarkCases(runId?: string): Promise<BenchmarkCase[]> {
    let target = runId
    if (!target) {
      const latest = await apiRequest<BenchmarkDTO | null>('/benchmark-runs/latest')
      if (!latest) return []
      target = latest.id
    }
    const rows = await apiRequest<CaseDTO[]>(`/benchmark-runs/${target}/cases`)
    return rows.map((dto) => ({
      id: dto.id,
      name: dto.name || dto.id,
      expected: JSON.stringify(dto.expected ?? {}),
      observed: JSON.stringify(dto.observed ?? {}),
      passed: dto.passed,
      evidenceHash: dto.evidence_hash,
    }))
  },

  /** 列出安全提案；未指定项目时用当前工作区。 */
  async listProposals(projectId?: string): Promise<SafetyProposal[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<ProposalDTO[]>(`/projects/${pid}/safety-proposals`)
    return rows.map((dto) => ({
      id: dto.id,
      type: dto.kind as ProposalType,
      status: dto.status as ProposalStatus,
      title: dto.title ?? '（未命名提案）',
      createdBy: dto.created_by,
      createdAt: dto.created_at ?? '',
      summary: dto.rationale ?? '',
    }))
  },

  /** 列出发布记录；未指定提案时取第一条提案。 */
  async listReleases(proposalId?: string): Promise<SafetyRelease[]> {
    let target = proposalId
    if (!target) {
      await whenSessionReady()
      const pid = getProjectId()
      const proposals = await apiRequest<ProposalDTO[]>(`/projects/${pid}/safety-proposals`)
      if (proposals.length === 0) return []
      target = proposals[0].id
    }
    const rows = await apiRequest<ReleaseDTO[]>(`/safety-proposals/${target}/releases`)
    return rows.map((dto) => {
      const stage = releaseStage(dto.stage)
      return {
        id: dto.id,
        stage,
        status: releaseStatus(dto.status),
        benchmarkRunId: '',
        progress: releaseProgress(stage),
        updatedAt: dto.verified_at ?? dto.started_at ?? dto.created_at ?? '',
      }
    })
  },
}
