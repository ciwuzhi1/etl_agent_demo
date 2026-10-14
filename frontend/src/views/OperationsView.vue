<script setup lang="ts">
import { Activity, CheckCircle2, RefreshCw, Server, XCircle } from 'lucide-vue-next'
import { NButton, useMessage } from 'naive-ui'
import { computed, onMounted, ref } from 'vue'

import EmptyState from '@/components/ui/EmptyState.vue'
import ErrorState from '@/components/ui/ErrorState.vue'
import LoadingState from '@/components/ui/LoadingState.vue'
import MetricCard from '@/components/ui/MetricCard.vue'
import PageHeader from '@/components/ui/PageHeader.vue'
import Panel from '@/components/ui/Panel.vue'
import RiskBadge from '@/components/ui/RiskBadge.vue'
import StatusTag from '@/components/ui/StatusTag.vue'
import { useAsyncState } from '@/composables/useAsyncState'
import { executionService } from '@/services/execution'
import type { ExecutionRun } from '@/types/models'

const message = useMessage()

const runsState = useAsyncState(() => executionService.listRuns())
const workersState = useAsyncState(() => executionService.listWorkers())

const selectedRunId = ref<string | null>(null)
const eventsState = useAsyncState(() => executionService.listEvents(selectedRunId.value ?? ''))
const qualityState = useAsyncState(() => executionService.getQualityReport(selectedRunId.value ?? ''))
const supervisionState = useAsyncState(() => executionService.getSupervision(selectedRunId.value ?? ''))

const runs = computed(() => runsState.data.value ?? [])
const selectedRun = computed<ExecutionRun | null>(
  () => runs.value.find((run) => run.id === selectedRunId.value) ?? null,
)

const summary = computed(() => ({
  succeeded: runs.value.filter((run) => run.status === 'SUCCEEDED').length,
  failed: runs.value.filter((run) => run.status === 'FAILED').length,
  running: runs.value.filter((run) => run.status === 'RUNNING' || run.status === 'SUBMITTED').length,
  onlineWorkers: (workersState.data.value ?? []).filter((worker) => worker.online).length,
}))

function formatClock(iso: string | null): string {
  return iso ? iso.slice(11, 19) : '—'
}

function formatDuration(seconds: number | null): string {
  if (seconds === null) {
    return '进行中'
  }
  const minutes = Math.floor(seconds / 60)
  const rest = seconds % 60
  return minutes > 0 ? `${minutes}m ${rest}s` : `${rest}s`
}

async function selectRun(runId: string): Promise<void> {
  selectedRunId.value = runId
  await Promise.all([eventsState.run(), qualityState.run(), supervisionState.run()])
}

async function refresh(): Promise<void> {
  await Promise.all([runsState.run(), workersState.run()])
  if (selectedRunId.value) {
    await selectRun(selectedRunId.value)
  }
}

function requestAction(label: string): void {
  message.info(`${label}：已进入 Harness 统一副作用治理（演示环境不产生真实副作用）`)
}

onMounted(async () => {
  await Promise.all([runsState.run(), workersState.run()])
  if (runs.value[0]) {
    await selectRun(runs.value[0].id)
  }
})
</script>

<template>
  <div>
    <PageHeader
      eyebrow="EXECUTION LEDGER"
      title="运行中心"
      description="以 PostgreSQL 为执行真相源，持续对账 SeaTunnel 状态与行数指标。"
    >
      <template #actions>
        <NButton quaternary :loading="runsState.loading.value" @click="refresh">
          <template #icon><RefreshCw :size="15" /></template>
          刷新运行记录
        </NButton>
      </template>
    </PageHeader>

    <div class="mb-4 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <MetricCard :icon="CheckCircle2" label="成功" :value="summary.succeeded" unit="次" tone="ok" />
      <MetricCard :icon="XCircle" label="失败" :value="summary.failed" unit="次" tone="danger" />
      <MetricCard :icon="Activity" label="执行中" :value="summary.running" unit="次" tone="warn" />
      <MetricCard :icon="Server" label="在线 Worker" :value="summary.onlineWorkers" unit="个" />
    </div>

    <LoadingState v-if="runsState.loading.value && !runs.length" :rows="3" />
    <ErrorState v-else-if="runsState.error.value && !runs.length" :message="runsState.error.value" @retry="refresh" />
    <EmptyState v-else-if="!runs.length" title="暂无执行记录" description="提交 Commit 后在此查看台账。" />
    <div v-else class="flex flex-col gap-4">
      <Panel title="执行台账" description="Commit 成功 ≠ ETL 成功；节流 ≠ 降低读取速率。">
        <div class="overflow-x-auto">
          <table class="w-full min-w-[920px] border-collapse text-left">
            <thead>
              <tr class="border-b border-line">
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">Run ID</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">版本</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">状态</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">触发</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">开始</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">耗时</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">读取/写入/拒绝</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="run in runs"
                :key="run.id"
                class="cursor-pointer border-b border-line/70 transition-colors last:border-b-0 hover:bg-accent-soft/40"
                :class="selectedRunId === run.id ? 'bg-accent-soft/60' : ''"
                @click="selectRun(run.id)"
              >
                <td class="px-3 py-3">
                  <strong class="block font-mono text-[11px] text-ink">{{ run.id }}</strong>
                  <small class="font-mono text-[9px] text-muted">
                    job {{ run.seatunnelJobId ?? '—' }} · key {{ run.idempotencyKey.slice(0, 18) }}…
                  </small>
                </td>
                <td class="px-3 py-3 font-mono text-[11px] text-ink">v{{ run.pipelineVersionNumber }}</td>
                <td class="px-3 py-3"><StatusTag :status="run.status" /></td>
                <td class="px-3 py-3 font-mono text-[10px] text-muted">{{ run.trigger }}</td>
                <td class="px-3 py-3 font-mono text-[10px] text-muted">{{ formatClock(run.startedAt) }}</td>
                <td class="px-3 py-3 font-mono text-[10px] text-muted">{{ formatDuration(run.durationSeconds) }}</td>
                <td class="px-3 py-3 font-mono text-[10px] text-muted">
                  {{ run.metrics.readRows.toLocaleString() }} /
                  {{ run.metrics.writeRows.toLocaleString() }} /
                  {{ run.metrics.rejectRows.toLocaleString() }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </Panel>

      <div class="grid grid-cols-1 gap-4 lg:grid-cols-[1.45fr_0.75fr]">
        <Panel
          :title="selectedRun ? `运行详情 · ${selectedRun.id}` : '运行详情'"
          description="状态由引擎对账写入；迟到的错误不得改写已确认终态。"
        >
          <template #actions>
            <NButton size="tiny" quaternary @click="requestAction('申请回滚')">申请回滚</NButton>
            <NButton size="tiny" quaternary @click="requestAction('取消执行')">取消</NButton>
          </template>

          <div v-if="!selectedRun" class="py-6 text-center text-xs text-muted">请选择一条执行记录</div>
          <div v-else class="flex flex-col gap-4">
            <div
              v-if="selectedRun.errorSummary"
              class="rounded-lg border border-danger/30 bg-danger/5 p-3"
            >
              <strong class="text-[11px] text-danger">失败诊断</strong>
              <p class="mt-1 text-[11px] leading-relaxed text-muted">{{ selectedRun.errorSummary }}</p>
            </div>

            <div v-if="supervisionState.data.value" class="rounded-lg border border-line p-3">
              <div class="flex items-center justify-between gap-3">
                <strong class="text-[11px] text-ink">监督快照</strong>
                <div class="flex items-center gap-2">
                  <span class="font-mono text-[10px] text-muted">{{ supervisionState.data.value.action }}</span>
                  <RiskBadge :level="supervisionState.data.value.riskLevel" />
                </div>
              </div>
              <ul class="mt-2 flex flex-col gap-1">
                <li
                  v-for="reason in supervisionState.data.value.reasons"
                  :key="reason"
                  class="text-[10px] text-muted"
                >
                  · {{ reason }}
                </li>
              </ul>
            </div>

            <div v-if="qualityState.data.value" class="rounded-lg border border-ok/30 bg-ok/5 p-3">
              <div class="flex items-center justify-between gap-3">
                <strong class="text-[11px] text-ink">质量报告</strong>
                <div class="flex items-center gap-2">
                  <span class="font-mono text-[10px] text-muted">
                    通过率 {{ (qualityState.data.value.passRate * 100).toFixed(1) }}% · 拒绝率
                    {{ (qualityState.data.value.rejectRate * 100).toFixed(1) }}%
                  </span>
                  <StatusTag :status="qualityState.data.value.status" />
                </div>
              </div>
              <ul class="mt-2 flex flex-col gap-1.5">
                <li
                  v-for="check in qualityState.data.value.checks"
                  :key="check.code"
                  class="flex items-start gap-2"
                >
                  <StatusTag :status="check.passed ? 'PASSED' : 'FAILED'" :label="check.passed ? 'PASS' : 'FAIL'" />
                  <div class="min-w-0">
                    <strong class="block font-mono text-[10px] text-ink">{{ check.code }} · {{ check.label }}</strong>
                    <span class="text-[10px] text-muted">{{ check.detail }}</span>
                  </div>
                </li>
              </ul>
            </div>

            <div>
              <strong class="mb-2 block text-[11px] text-ink">事件时间线</strong>
              <LoadingState v-if="eventsState.loading.value" :rows="2" />
              <EmptyState v-else-if="!eventsState.data.value?.length" title="暂无事件" />
              <ol v-else class="flex flex-col">
                <li
                  v-for="event in eventsState.data.value"
                  :key="event.id"
                  class="relative flex gap-3 border-l border-line pb-4 pl-4 last:pb-0"
                >
                  <span
                    class="absolute top-1 -left-[5px] h-2.5 w-2.5 rounded-full"
                    :class="{
                      'bg-accent': event.level === 'INFO',
                      'bg-warn': event.level === 'WARN',
                      'bg-danger': event.level === 'ERROR',
                    }"
                    aria-hidden="true"
                  />
                  <div class="min-w-0">
                    <time class="font-mono text-[9px] text-muted">{{ event.occurredAt.slice(11, 19) }}</time>
                    <strong class="block text-[11px] text-ink">{{ event.eventType }}</strong>
                    <span class="text-[10px] text-muted">{{ event.message }}</span>
                  </div>
                </li>
              </ol>
            </div>
          </div>
        </Panel>

        <Panel title="Worker 状态" description="心跳默认 10s，陈旧阈值 35s。">
          <ul v-if="workersState.data.value?.length" class="flex flex-col">
            <li
              v-for="worker in workersState.data.value"
              :key="worker.id"
              class="flex items-center justify-between gap-3 border-t border-line py-3 first:border-t-0"
            >
              <div class="flex min-w-0 items-center gap-2">
                <span
                  class="h-2 w-2 shrink-0 rounded-full"
                  :class="worker.online ? 'bg-ok' : 'bg-muted'"
                  aria-hidden="true"
                />
                <div class="min-w-0">
                  <strong class="block truncate text-[11px] text-ink">{{ worker.name }}</strong>
                  <small class="font-mono text-[9px] text-muted">{{ worker.lastHeartbeatAt.slice(11, 19) }}</small>
                </div>
              </div>
              <StatusTag :status="worker.online ? 'ONLINE' : 'OFFLINE'" />
            </li>
          </ul>
          <EmptyState v-else title="暂无 Worker" />
        </Panel>
      </div>
    </div>
  </div>
</template>
