<script setup lang="ts">
import { FlaskConical, Rocket, ShieldCheck, TrendingDown, TrendingUp } from 'lucide-vue-next'
import { onMounted } from 'vue'

import EmptyState from '@/components/ui/EmptyState.vue'
import ErrorState from '@/components/ui/ErrorState.vue'
import LoadingState from '@/components/ui/LoadingState.vue'
import PageHeader from '@/components/ui/PageHeader.vue'
import Panel from '@/components/ui/Panel.vue'
import StatusTag from '@/components/ui/StatusTag.vue'
import { useAsyncState } from '@/composables/useAsyncState'
import { governanceService } from '@/services/governance'
import type { ReleaseStage } from '@/types/models'

const benchmarkState = useAsyncState(() => governanceService.getBenchmark())
const casesState = useAsyncState(() => governanceService.listBenchmarkCases())
const proposalsState = useAsyncState(() => governanceService.listProposals())
const releasesState = useAsyncState(() => governanceService.listReleases())

const STAGE_LABEL: Record<ReleaseStage, string> = {
  SHADOW: '影子验证',
  CANARY: '5% 小流量',
  PROMOTION: '推广授权',
}

function formatValue(value: number, unit: string): string {
  return unit === '%' ? `${value}%` : unit === 'ms' ? `${value}ms` : `${value}`
}

onMounted(() => {
  void benchmarkState.run()
  void casesState.run()
  void proposalsState.run()
  void releasesState.run()
})
</script>

<template>
  <div>
    <PageHeader
      eyebrow="SAFETY GOVERNANCE"
      title="安全治理"
      description="策略改动先经过冻结 Benchmark 与影子/小流量验证；出现 P0/P1 回归自动回滚授权。"
    />

    <LoadingState v-if="benchmarkState.loading.value && !benchmarkState.data.value" :rows="4" />
    <ErrorState
      v-else-if="benchmarkState.error.value && !benchmarkState.data.value"
      :message="benchmarkState.error.value"
      @retry="benchmarkState.run()"
    />
    <div v-else class="flex flex-col gap-4">
      <Panel
        v-if="benchmarkState.data.value"
        title="Benchmark 门禁"
        :description="`套件版本 ${benchmarkState.data.value.suiteVersion} · P0/P1 回归 ${benchmarkState.data.value.p0p1Regressions}`"
      >
        <template #actions>
          <StatusTag :status="benchmarkState.data.value.status" />
        </template>
        <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
          <article
            v-for="metric in benchmarkState.data.value.metrics"
            :key="metric.key"
            class="flex flex-col gap-1 rounded-lg border border-line p-3"
          >
            <div class="flex items-center justify-between gap-2">
              <small class="text-[10px] text-muted">{{ metric.label }}</small>
              <StatusTag :status="metric.passed ? 'PASSED' : 'FAILED'" :label="metric.passed ? 'PASS' : 'FAIL'" />
            </div>
            <div class="flex items-baseline gap-2">
              <strong class="text-xl font-bold text-ink">{{ formatValue(metric.value, metric.unit) }}</strong>
              <span class="font-mono text-[10px] text-muted">目标 {{ metric.target }}</span>
            </div>
            <span
              class="flex items-center gap-1 font-mono text-[10px]"
              :class="metric.delta === 0 ? 'text-muted' : metric.delta > 0 ? 'text-ok' : 'text-warn'"
            >
              <component :is="metric.delta >= 0 ? TrendingUp : TrendingDown" :size="12" aria-hidden="true" />
              {{ metric.delta >= 0 ? '+' : '' }}{{ metric.delta }} 环比
            </span>
          </article>
        </div>
      </Panel>

      <div class="grid grid-cols-1 gap-4 lg:grid-cols-[0.9fr_1.1fr]">
        <Panel title="冻结场景" description="同一案例连续执行两次，观察结果必须完全一致。">
          <LoadingState v-if="casesState.loading.value && !casesState.data.value" :rows="3" />
          <ul v-else-if="casesState.data.value?.length" class="flex flex-col gap-2">
            <li
              v-for="item in casesState.data.value"
              :key="item.id"
              class="flex items-start justify-between gap-3 rounded-lg border border-line bg-bg/40 p-3"
            >
              <div class="min-w-0">
                <strong class="block text-[11px] text-ink">{{ item.name }}</strong>
                <small class="font-mono text-[9px] text-muted">
                  期望 {{ item.expected }} · 实测 {{ item.observed }}
                </small>
                <small class="block font-mono text-[9px] text-muted/80">{{ item.evidenceHash }}</small>
              </div>
              <StatusTag :status="item.passed ? 'PASSED' : 'FAILED'" :label="item.passed ? 'PASS' : 'FAIL'" />
            </li>
          </ul>
          <EmptyState v-else title="暂无案例" />
        </Panel>

        <Panel title="提案审查" description="安全进化只产生候选与评测报告，不会自动热更新。">
          <LoadingState v-if="proposalsState.loading.value && !proposalsState.data.value" :rows="3" />
          <ul v-else-if="proposalsState.data.value?.length" class="flex flex-col gap-2">
            <li
              v-for="proposal in proposalsState.data.value"
              :key="proposal.id"
              class="flex flex-col gap-2 rounded-lg border border-line p-3"
            >
              <div class="flex items-center justify-between gap-2">
                <div class="flex items-center gap-2">
                  <span class="rounded bg-ink/5 px-1.5 py-1 font-mono text-[9px] text-muted">{{ proposal.type }}</span>
                  <strong class="text-[11px] text-ink">{{ proposal.title }}</strong>
                </div>
                <StatusTag :status="proposal.status" />
              </div>
              <p class="text-[10px] leading-relaxed text-muted">{{ proposal.summary }}</p>
              <small class="font-mono text-[9px] text-muted/80">
                {{ proposal.createdBy }} · {{ proposal.createdAt.slice(0, 16).replace('T', ' ') }}
              </small>
            </li>
          </ul>
          <EmptyState v-else title="暂无提案" />
        </Panel>
      </div>

      <Panel title="签名运行" description="推广授权 ≠ 已自动更新生产；仅产出签名授权，由部署系统执行。">
        <LoadingState v-if="releasesState.loading.value && !releasesState.data.value" :rows="2" />
        <div v-else-if="releasesState.data.value?.length" class="grid grid-cols-1 gap-3 md:grid-cols-3">
          <article
            v-for="release in releasesState.data.value"
            :key="release.id"
            class="flex flex-col gap-3 rounded-lg border border-line p-4"
          >
            <div class="flex items-center justify-between gap-2">
              <div class="flex items-center gap-2">
                <component
                  :is="release.stage === 'PROMOTION' ? Rocket : release.stage === 'CANARY' ? FlaskConical : ShieldCheck"
                  :size="16"
                  class="text-accent"
                  aria-hidden="true"
                />
                <strong class="text-xs text-ink">{{ STAGE_LABEL[release.stage] }}</strong>
              </div>
              <StatusTag :status="release.status" />
            </div>
            <div class="h-1.5 w-full overflow-hidden rounded-full bg-ink/5">
              <div class="h-full rounded-full bg-accent" :style="{ width: `${release.progress}%` }" />
            </div>
            <small class="font-mono text-[9px] text-muted">
              {{ release.id }} · 基准 {{ release.benchmarkRunId }}
            </small>
          </article>
        </div>
        <EmptyState v-else title="暂无签名运行" />
      </Panel>
    </div>
  </div>
</template>
