<script setup lang="ts">
import {
  FlaskConical,
  Rocket,
  ShieldAlert,
  ShieldCheck,
  TrendingDown,
  TrendingUp,
} from 'lucide-vue-next'
import { NButton, useMessage } from 'naive-ui'
import { computed, onMounted, ref } from 'vue'

import EmptyState from '@/components/ui/EmptyState.vue'
import ErrorState from '@/components/ui/ErrorState.vue'
import LoadingState from '@/components/ui/LoadingState.vue'
import PageHeader from '@/components/ui/PageHeader.vue'
import Panel from '@/components/ui/Panel.vue'
import RiskBadge from '@/components/ui/RiskBadge.vue'
import StatusTag from '@/components/ui/StatusTag.vue'
import { useAsyncState } from '@/composables/useAsyncState'
import { useSession } from '@/composables/useSession'
import { governanceService } from '@/services/governance'
import { describeApiError } from '@/services/http'
import { sideEffectService } from '@/services/sideEffects'
import type {
  HarnessRole,
  ReleaseStage,
  SideEffectPreparation,
} from '@/types/models'

const message = useMessage()
const session = useSession()

const benchmarkState = useAsyncState(() => governanceService.getBenchmark())
const casesState = useAsyncState(() => governanceService.listBenchmarkCases())
const proposalsState = useAsyncState(() => governanceService.listProposals())
const releasesState = useAsyncState(() => governanceService.listReleases())
const sideEffectsState = useAsyncState(() => sideEffectService.list())

const STAGE_LABEL: Record<ReleaseStage, string> = {
  SHADOW: '影子验证',
  CANARY: '5% 小流量',
  PROMOTION: '推广授权',
  ROLLED_BACK: '已回滚',
}

const actingId = ref<string | null>(null)
const committingId = ref<string | null>(null)

const OPERATION_LABEL: Record<string, string> = {
  CANCEL_EXECUTION: '取消执行',
  ROLLBACK_DORIS: 'Doris 回滚',
  CLEANUP_DORIS: 'Doris 清理',
  MIGRATE_SECRET: 'Secret 迁移',
  STAGE_SECRET_ROTATION: 'Secret 暂存轮换',
  VALIDATE_SECRET_ROTATION: 'Secret 轮换验证',
  ACTIVATE_SECRET_ROTATION: 'Secret 轮换激活',
  ROLLBACK_SECRET_ROTATION: 'Secret 轮换回退',
  DESTROY_SECRET: 'Secret 销毁',
}

function formatValue(value: number, unit: string): string {
  return unit === '%' ? `${value}%` : unit === 'ms' ? `${value}ms` : `${value}`
}

/** 当前身份在该准备单要求的职责里实际持有的那一个；一个都不持有则为 null。 */
function heldRequiredRole(preparation: SideEffectPreparation): HarnessRole | null {
  return preparation.requiredRoles.find((role) => session.hasHarnessRole(role)) ?? null
}

/** 申请人不能自批；只有等待审批的准备单能占槽。 */
function canDecide(preparation: SideEffectPreparation): boolean {
  return (
    preparation.status === 'WAITING_APPROVAL' &&
    preparation.requestedBy !== session.currentActor.value?.id &&
    heldRequiredRole(preparation) !== null
  )
}

function decideBlockedReason(preparation: SideEffectPreparation): string | undefined {
  if (preparation.status !== 'WAITING_APPROVAL') {
    return `准备单已处于 ${preparation.status}，无需审批`
  }
  if (preparation.requestedBy === session.currentActor.value?.id) {
    return '四眼分离：申请人不能审批自己发起的副作用'
  }
  if (heldRequiredRole(preparation) === null) {
    const roles = preparation.requiredRoles.join('、') || '审批人'
    return `当前身份缺少所需职责（${roles}），请切换身份后审批`
  }
  return undefined
}

/** 只有审批齐备（READY）且调用者持有 OPERATOR 时才能提交。 */
const commitBlocked = computed(() => !session.hasHarnessRole('OPERATOR'))

async function handleDecide(
  preparation: SideEffectPreparation,
  decision: 'APPROVE' | 'REJECT',
): Promise<void> {
  const role = heldRequiredRole(preparation)
  if (!role) {
    message.warning(decideBlockedReason(preparation) ?? '当前身份无法审批该准备单')
    return
  }
  actingId.value = preparation.id
  try {
    await sideEffectService.decide(
      preparation.id,
      role,
      decision,
      decision === 'APPROVE' ? '控制台审批通过' : '控制台驳回',
    )
    await sideEffectsState.run()
    message.success(decision === 'APPROVE' ? `已占用审批槽（${role}）` : '已驳回该副作用')
  } catch (caught) {
    message.error(describeApiError(caught))
  } finally {
    actingId.value = null
  }
}

async function handleCommit(preparation: SideEffectPreparation): Promise<void> {
  if (commitBlocked.value) {
    message.warning('当前身份缺少 OPERATOR 职责，无法提交副作用')
    return
  }
  committingId.value = preparation.id
  try {
    const result = await sideEffectService.commit(preparation.id)
    message.success(`已签发一次性能力并投递命令 ${result.executionId.slice(0, 8)}，由 Worker 执行`)
    await sideEffectsState.run()
  } catch (caught) {
    message.error(describeApiError(caught))
  } finally {
    committingId.value = null
  }
}

onMounted(() => {
  void benchmarkState.run()
  void casesState.run()
  void proposalsState.run()
  void releasesState.run()
  void sideEffectsState.run()
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

      <Panel
        title="副作用治理"
        description="9 类副作用共用同一条通道：发起只创建 Preparation，占满全部职责槽并签发一次性能力后才由 Worker 执行。"
      >
        <template #actions>
          <NButton
            size="tiny"
            quaternary
            :loading="sideEffectsState.loading.value"
            @click="sideEffectsState.run()"
          >
            刷新
          </NButton>
        </template>

        <LoadingState
          v-if="sideEffectsState.loading.value && !sideEffectsState.data.value"
          :rows="2"
        />
        <div v-else-if="sideEffectsState.data.value?.length" class="overflow-x-auto">
          <table class="w-full min-w-[1040px] border-collapse text-left">
            <thead>
              <tr class="border-b border-line">
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">
                  Preparation
                </th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">
                  操作
                </th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">
                  风险
                </th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">
                  必备职责
                </th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">
                  审批 / 状态
                </th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">
                  操作
                </th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="preparation in sideEffectsState.data.value"
                :key="preparation.id"
                class="border-b border-line/70 last:border-b-0"
              >
                <td class="px-3 py-3">
                  <strong class="block font-mono text-[11px] text-ink">
                    {{ preparation.id.slice(0, 8) }}
                  </strong>
                  <small class="font-mono text-[10px] text-muted">
                    聚合根 {{ preparation.aggregateId.slice(0, 8) }}
                  </small>
                </td>
                <td class="px-3 py-3 text-[11px] text-ink">
                  {{ OPERATION_LABEL[preparation.operation] ?? preparation.operation }}
                </td>
                <td class="px-3 py-3"><RiskBadge :level="preparation.riskLevel" /></td>
                <td class="px-3 py-3">
                  <span
                    v-for="role in preparation.requiredRoles"
                    :key="role"
                    class="mr-1 inline-block rounded bg-ink/5 px-1.5 py-1 font-mono text-[9px] text-muted"
                  >
                    {{ role }}
                  </span>
                </td>
                <td class="px-3 py-3">
                  <StatusTag :status="preparation.status" />
                  <small class="mt-1 block font-mono text-[9px] text-muted">
                    需 {{ preparation.requiredApprovals }} 槽 · 执行 {{ preparation.executionStatus }}
                  </small>
                </td>
                <td class="px-3 py-3">
                  <div class="flex flex-wrap items-center gap-1.5">
                    <NButton
                      size="tiny"
                      type="primary"
                      :disabled="!canDecide(preparation)"
                      :loading="actingId === preparation.id"
                      :title="decideBlockedReason(preparation)"
                      @click="handleDecide(preparation, 'APPROVE')"
                    >
                      通过
                    </NButton>
                    <NButton
                      size="tiny"
                      quaternary
                      :disabled="!canDecide(preparation)"
                      :loading="actingId === preparation.id"
                      :title="decideBlockedReason(preparation)"
                      @click="handleDecide(preparation, 'REJECT')"
                    >
                      驳回
                    </NButton>
                    <NButton
                      size="tiny"
                      secondary
                      :disabled="preparation.status !== 'READY' || commitBlocked"
                      :loading="committingId === preparation.id"
                      :title="
                        preparation.status !== 'READY'
                          ? '审批未齐备，不能提交'
                          : commitBlocked
                            ? '当前身份缺少 OPERATOR 职责'
                            : undefined
                      "
                      @click="handleCommit(preparation)"
                    >
                      提交执行
                    </NButton>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <EmptyState
          v-else
          title="暂无副作用准备单"
          description="在「运行中心」对已成功的运行点「申请回滚」即可发起一张。"
        >
          <template #icon><ShieldAlert :size="18" class="text-muted" /></template>
        </EmptyState>
      </Panel>
    </div>
  </div>
</template>
