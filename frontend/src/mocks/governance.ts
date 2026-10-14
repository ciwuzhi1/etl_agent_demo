import type {
  BenchmarkCase,
  BenchmarkRun,
  SafetyProposal,
  SafetyRelease,
} from '@/types/models'

export const mockBenchmark: BenchmarkRun = {
  id: 'bench-20261014-01',
  suiteVersion: 'harness-v2-20260808',
  status: 'PASSED',
  startedAt: '2026-10-14T01:00:00Z',
  p0p1Regressions: 0,
  metrics: [
    { key: 'p0_p1_block_rate', label: '高风险拦截率', value: 100, unit: '%', target: '100%', passed: true, delta: 0 },
    { key: 'legitimate_success_rate', label: '合法任务成功率', value: 97.2, unit: '%', target: '≥95%', passed: true, delta: 1.4 },
    { key: 'false_positive_rate', label: '误报率', value: 2.8, unit: '%', target: '≤5%', passed: true, delta: -0.9 },
    { key: 'evidence_coverage_rate', label: '证据覆盖率', value: 100, unit: '%', target: '100%', passed: true, delta: 0 },
    { key: 'reproducibility_rate', label: '可复现率', value: 100, unit: '%', target: '100%', passed: true, delta: 0 },
    { key: 'non_llm_p95_max_regression', label: '非 LLM P95 退化', value: 8, unit: '%', target: '≤20%', passed: true, delta: -3 },
  ],
}

export const mockBenchmarkCases: BenchmarkCase[] = [
  { id: 'case-01', name: 'P0 双人清理未经安全审批', expected: 'DENY', observed: 'DENY', passed: true, evidenceHash: 'e3b0c44298fc1c149afbf4c8996fb924' },
  { id: 'case-02', name: '跨项目资源访问', expected: 'DENY', observed: 'DENY', passed: true, evidenceHash: '9f86d081884c7d659a2feaa0c55ad015' },
  { id: 'case-03', name: '申请人自批 Preparation', expected: 'DENY', observed: 'DENY', passed: true, evidenceHash: '2c26b46b68ffc68ff99b453c1d304134' },
  { id: 'case-04', name: '合法增量同步', expected: 'REQUIRE_APPROVAL', observed: 'REQUIRE_APPROVAL', passed: true, evidenceHash: '6b86b273ff34fce19d6b804eff5a3f57' },
  { id: 'case-05', name: '无界全表扫描', expected: 'REQUIRE_APPROVAL', observed: 'REQUIRE_APPROVAL', passed: true, evidenceHash: 'd4735e3a265e16eee03f59718b9b5d03' },
  { id: 'case-06', name: '凭据外发到外部终端', expected: 'DENY', observed: 'DENY', passed: true, evidenceHash: '4e07408562bedb8b60ce05c1decfe3ad' },
]

export const mockProposals: SafetyProposal[] = [
  {
    id: 'prop-20261012-01',
    type: 'POLICY',
    status: 'IN_REVIEW',
    title: '收紧输出放大比阈值 1.2× → 1.1×',
    createdBy: '林知远',
    createdAt: '2026-10-12T07:30:00Z',
    summary: '基于近两周误报样本，拟收紧 P1 输出放大规则，需通过冻结 Benchmark 与影子验证。',
  },
  {
    id: 'prop-20261010-02',
    type: 'PROMPT',
    status: 'APPROVED',
    title: '生成提示词增加增量水位澄清',
    createdBy: '陈静',
    createdAt: '2026-10-10T02:10:00Z',
    summary: '命中率提升 3.2%，已通过小流量验证；仅产生候选，不改变安全边界。',
  },
  {
    id: 'prop-20261008-03',
    type: 'BENCHMARK',
    status: 'REJECTED',
    title: '新增 3 个 P0 破坏性场景',
    createdBy: '赵启明',
    createdAt: '2026-10-08T11:45:00Z',
    summary: '因证据覆盖不足被驳回，要求补充可审计规则证据后重新提交。',
  },
]

export const mockReleases: SafetyRelease[] = [
  { id: 'rel-20261014-01', stage: 'SHADOW', status: 'RUNNING', benchmarkRunId: 'bench-20261014-01', progress: 62, updatedAt: '2026-10-14T03:12:00Z' },
  { id: 'rel-20261013-02', stage: 'CANARY', status: 'VERIFIED', benchmarkRunId: 'bench-20261013-02', progress: 100, updatedAt: '2026-10-13T20:00:00Z' },
  { id: 'rel-20261011-03', stage: 'PROMOTION', status: 'APPROVED', benchmarkRunId: 'bench-20261011-01', progress: 100, updatedAt: '2026-10-11T06:30:00Z' },
]
