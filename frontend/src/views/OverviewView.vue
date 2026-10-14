<script setup lang="ts">
import { ArrowRight, CheckCircle2, ClipboardList, Database, ShieldCheck, XCircle } from 'lucide-vue-next'
import { computed, onMounted } from 'vue'

import EmptyState from '@/components/ui/EmptyState.vue'
import ErrorState from '@/components/ui/ErrorState.vue'
import LoadingState from '@/components/ui/LoadingState.vue'
import MetricCard from '@/components/ui/MetricCard.vue'
import Panel from '@/components/ui/Panel.vue'
import StatusTag from '@/components/ui/StatusTag.vue'
import { useAsyncState } from '@/composables/useAsyncState'
import { overviewService } from '@/services/overview'

const { data, error, loading, run } = useAsyncState(() => overviewService.getOverview())

const metrics = computed(() => data.value?.metrics ?? null)

onMounted(run)
</script>

<template>
  <LoadingState v-if="loading" :rows="4" />
  <ErrorState v-else-if="error" :message="error" @retry="run" />
  <EmptyState v-else-if="!data" title="暂无总览数据" description="请稍后重试。" />
  <div v-else class="flex flex-col gap-5">
    <section
      class="relative overflow-hidden rounded-2xl border border-line bg-linear-to-br from-surface to-accent-soft p-8 sm:p-10"
    >
      <p class="font-mono text-[11px] tracking-[1.7px] text-muted">GOOD MORNING, DATA TEAM</p>
      <h1 class="mt-3 max-w-2xl text-3xl leading-tight font-bold text-ink sm:text-4xl">
        把 ETL 从“能跑”<br />升级为<span class="text-accent">可信生产流程</span>
      </h1>
      <p class="mt-4 max-w-xl text-sm leading-relaxed text-muted">
        从元数据理解到审批执行，每一步都有确定性边界；模型只负责提建议。
      </p>
      <RouterLink
        to="/studio"
        class="mt-6 inline-flex items-center gap-2 rounded-lg bg-accent px-4 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-accent/90"
      >
        创建 Pipeline <ArrowRight :size="16" aria-hidden="true" />
      </RouterLink>
    </section>

    <div v-if="metrics" class="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <MetricCard
        :icon="CheckCircle2"
        label="今日成功"
        :value="metrics.todaySucceeded"
        unit="次"
        tone="ok"
      />
      <MetricCard
        :icon="XCircle"
        label="今日失败"
        :value="metrics.todayFailed"
        unit="次"
        tone="danger"
      />
      <MetricCard
        :icon="ClipboardList"
        label="待办 Preparation"
        :value="metrics.pendingPreparations"
        unit="份"
        tone="warn"
      />
      <MetricCard
        :icon="Database"
        label="就绪连接"
        :value="metrics.readyConnections"
        unit="个"
        :hint="`受管文件 ${metrics.fileAssets} 个`"
      />
    </div>

    <div class="grid grid-cols-1 gap-4 lg:grid-cols-[1.4fr_0.8fr]">
      <div class="flex flex-col gap-4">
        <Panel title="受管执行主线" description="需求 → 探查 → 生成 → 门禁 → 审批 → 授权 → 执行 → 留证">
          <ol class="flex flex-col gap-2 sm:flex-row sm:items-stretch">
            <li
              v-for="(stage, index) in data.flowStages"
              :key="stage.key"
              class="flex flex-1 flex-col gap-1 rounded-lg border border-line bg-bg/60 p-3"
            >
              <span class="font-mono text-[10px] text-accent">
                {{ String(index + 1).padStart(2, '0') }}
              </span>
              <strong class="text-xs text-ink">{{ stage.label }}</strong>
              <small class="text-[10px] leading-snug text-muted">{{ stage.hint }}</small>
            </li>
          </ol>
        </Panel>

        <Panel title="确定性护栏" description="这些规则由代码硬编码，任何模型输出都不能绕过。">
          <ul class="flex flex-col">
            <li
              v-for="rule in data.guardrails"
              :key="rule.code"
              class="flex items-center gap-3 border-t border-line py-3 first:border-t-0 text-xs"
            >
              <ShieldCheck :size="15" class="shrink-0 text-accent" aria-hidden="true" />
              <span class="font-mono text-[10px] text-muted">{{ rule.code }}</span>
              <span class="text-ink">{{ rule.label }}</span>
            </li>
          </ul>
        </Panel>
      </div>

      <Panel title="就绪连接" description="READY 且未过期的 Profile 才会暴露给 Pipeline Studio。">
        <ul v-if="data.readyConnections.length" class="flex flex-col gap-2">
          <li
            v-for="connection in data.readyConnections"
            :key="connection.id"
            class="flex items-center justify-between gap-3 rounded-lg border border-line bg-bg/40 px-3 py-2.5"
          >
            <div class="min-w-0">
              <strong class="block truncate text-xs text-ink">{{ connection.name }}</strong>
              <small class="font-mono text-[10px] text-muted">
                {{ connection.connectorType }} · {{ connection.host }}:{{ connection.port }}
              </small>
            </div>
            <StatusTag :status="connection.status" />
          </li>
        </ul>
        <EmptyState v-else title="暂无就绪连接" description="请先在「数据连接」登记并测试。" />
      </Panel>
    </div>
  </div>
</template>
