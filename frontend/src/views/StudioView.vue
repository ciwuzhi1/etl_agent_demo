<script setup lang="ts">
import { CheckCircle2, CircleDashed, Loader2, Sparkles, Wand2 } from 'lucide-vue-next'
import { NButton, NCheckbox, NInput, NSelect, useMessage } from 'naive-ui'
import { computed, onMounted, ref } from 'vue'

import EmptyState from '@/components/ui/EmptyState.vue'
import ErrorState from '@/components/ui/ErrorState.vue'
import FormField from '@/components/ui/FormField.vue'
import LoadingState from '@/components/ui/LoadingState.vue'
import PageHeader from '@/components/ui/PageHeader.vue'
import Panel from '@/components/ui/Panel.vue'
import RiskBadge from '@/components/ui/RiskBadge.vue'
import StatusTag from '@/components/ui/StatusTag.vue'
import { useAsyncState } from '@/composables/useAsyncState'
import { useSession } from '@/composables/useSession'
import { describeApiError } from '@/services/http'
import { studioService } from '@/services/studio'
import type { AgentRunStatus, AgentStep, ExecutionPreparation, HarnessRole } from '@/types/models'

const message = useMessage()
const session = useSession()

// 定向到最近一次「生成候选」创建的 run，未指定时 service 会退回后端最近一次。
const activeRunId = ref<string | null>(null)
const hocon = ref('')

const designState = useAsyncState(() => studioService.getDesign())
const stepsState = useAsyncState(() => studioService.listAgentSteps(activeRunId.value ?? undefined))
const runState = useAsyncState(() => studioService.getAgentRun(activeRunId.value ?? undefined))
const preparationsState = useAsyncState(() => studioService.listPreparations())
// 准备单只带 pipeline_version_id，版本号要靠版本列表还原，否则表格永远显示 v0。
const versionsState = useAsyncState(() => studioService.listDesigns())

const versionNumberById = computed(() => {
  const lookup = new Map<string, number>()
  for (const design of versionsState.data.value ?? []) {
    lookup.set(design.version.id, design.version.versionNumber)
  }
  return lookup
})

const requirement = ref(
  '把 MySQL 的订单同步到 Doris：清理客户名称两侧空格，金额必须为正，邮箱脱敏，错误数据单独保存，每天凌晨执行。',
)

const contracts = ref<{ key: string; label: string; desc: string; enabled: boolean }[]>([
  { key: 'quality', label: '质量规则', desc: '金额为正、名称去空格、邮箱脱敏', enabled: true },
  { key: 'incremental', label: '增量水位', desc: '窗口 (lower, upper]，只有成功后推进', enabled: true },
  { key: 'delivery', label: '交付模式', desc: '有效表原子替换 + 错误表追加审计', enabled: true },
])

const profileOptions = [
  { label: 'source_db.orders（MySQL）', value: 'prof-orders-source' },
  { label: 'source_db.customers（MySQL）', value: 'prof-customers-source' },
]
const sinkOptions = [{ label: 'analytics.clean_orders（Doris）', value: 'prof-clean-orders-sink' }]

const sourceProfile = ref('prof-orders-source')
const sinkProfile = ref('prof-clean-orders-sink')

const commitBlocked = computed(() => !session.hasHarnessRole('OPERATOR'))
const generating = ref(false)
const committing = ref(false)
const decidingId = ref<string | null>(null)
const answerText = ref('')
const answering = ref(false)

// AgentRun 是异步图任务：QUEUED/RUNNING 期间有界轮询，直到进入终态信号。
const RUN_TERMINAL = new Set<AgentRunStatus>(['WAITING_APPROVAL', 'WAITING_INPUT', 'FAILED'])
const RUN_POLL_LIMIT = 12
const RUN_POLL_INTERVAL_MS = 1500

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })
}

function stepIcon(step: AgentStep) {
  if (step.status === 'ACTIVE') {
    return Loader2
  }
  return step.status === 'DONE' ? CheckCircle2 : CircleDashed
}

function stepClass(step: AgentStep): string {
  if (step.status === 'ACTIVE') {
    return 'text-warn'
  }
  return step.status === 'DONE' ? 'text-ok' : 'text-muted'
}

/** 刷新最新版本，并把该版本冻结的 HOCON 制品填进审查区；无版本/制品时保持空态。 */
async function refreshDesign(): Promise<void> {
  await designState.run()
  const versionId = designState.data.value?.version.id
  if (!versionId) {
    hocon.value = ''
    return
  }
  try {
    const artifact = await studioService.getArtifact(versionId)
    hocon.value = artifact?.hocon ?? ''
  } catch {
    // 制品读取失败不改写页面事实，展示空态即可。
    hocon.value = ''
  }
}

async function pollActiveRun(runId: string): Promise<AgentRunStatus> {
  /** 轮询到 run 终态，并同步刷新六节点步骤与候选方案状态。 */
  activeRunId.value = runId
  await runState.run()
  await stepsState.run()
  let status: AgentRunStatus = runState.data.value?.status ?? 'QUEUED'
  for (let attempt = 0; attempt < RUN_POLL_LIMIT && !RUN_TERMINAL.has(status); attempt += 1) {
    await sleep(RUN_POLL_INTERVAL_MS)
    await runState.run()
    await stepsState.run()
    status = runState.data.value?.status ?? status
  }
  await refreshDesign()
  await preparationsState.run()
  return status
}

async function handleGenerate(): Promise<void> {
  const text = requirement.value.trim()
  if (!text) {
    message.warning('请先描述你的 ETL 需求')
    return
  }
  generating.value = true
  try {
    await session.ensureLoaded()
    const projectId = session.projectId.value
    if (!projectId) {
      throw new Error('工作区尚未就绪，请重新登录')
    }
    const run = await studioService.startAgentRun(projectId, text, [])
    const status = await pollActiveRun(run.id)

    if (status === 'FAILED') {
      message.error(runState.data.value?.message || 'Agent 生成失败')
    } else if (status === 'WAITING_INPUT') {
      message.info('Agent 需要补充澄清，请在下方回答后继续')
    } else if (RUN_TERMINAL.has(status)) {
      message.success('Agent 生成完成，已刷新候选方案')
    } else {
      message.warning('Agent 仍在执行，请稍后刷新查看结果')
    }
  } catch (caught) {
    message.error(describeApiError(caught))
  } finally {
    generating.value = false
  }
}

/** 提交澄清回答：run 重新排队，图带着「追问 + 回答」继续跑。 */
async function handleAnswer(): Promise<void> {
  const runId = runState.data.value?.id
  const text = answerText.value.trim()
  if (!runId) {
    message.warning('没有正在等待澄清的生成任务')
    return
  }
  if (!text) {
    message.warning('请先输入澄清内容')
    return
  }
  answering.value = true
  try {
    await studioService.answerAgentRun(runId, text)
    answerText.value = ''
    const status = await pollActiveRun(runId)
    if (status === 'WAITING_INPUT') {
      message.info('Agent 仍有新问题，请继续澄清')
    } else {
      message.success('已提交回答，Agent 继续执行')
    }
  } catch (caught) {
    message.error(describeApiError(caught))
  } finally {
    answering.value = false
  }
}

/** 当前身份在该准备单要求的职责里实际持有的那一个；一个都不持有则为 null。 */
function heldRequiredRole(preparation: ExecutionPreparation): HarnessRole | null {
  return preparation.requiredRoles.find((role) => session.hasHarnessRole(role)) ?? null
}

/** 只有等待审批的准备单可以占审批槽，其余状态后端会拒绝。 */
function canDecide(preparation: ExecutionPreparation): boolean {
  return preparation.status === 'WAITING_APPROVAL' && heldRequiredRole(preparation) !== null
}

function decisionBlockedReason(preparation: ExecutionPreparation): string | undefined {
  if (preparation.status !== 'WAITING_APPROVAL') {
    return `准备单已处于 ${preparation.status}，无需审批`
  }
  if (heldRequiredRole(preparation) === null) {
    const roles = preparation.requiredRoles.join('、') || '审批人'
    return `当前身份缺少所需职责（${roles}），请切换身份后审批`
  }
  return undefined
}

/** 具名职责审批：占用一个审批槽，理由进审计。审批满额后准备单转 READY。 */
async function handleDecision(
  preparation: ExecutionPreparation,
  decision: 'APPROVE' | 'REJECT',
): Promise<void> {
  const role = heldRequiredRole(preparation)
  if (!role) {
    message.warning(decisionBlockedReason(preparation) ?? '当前身份无法审批该准备单')
    return
  }
  decidingId.value = preparation.id
  try {
    await studioService.decidePreparation(
      preparation.id,
      role,
      decision,
      decision === 'APPROVE' ? '控制台审批通过' : '控制台驳回',
    )
    await preparationsState.run()
    if (decision === 'APPROVE') {
      const latest = (preparationsState.data.value ?? []).find((item) => item.id === preparation.id)
      message.success(
        latest?.status === 'READY'
          ? '审批已完成，准备单 READY；切换到 OPERATOR 身份即可提交 Commit'
          : '已占用一个审批槽，仍需其他职责批准',
      )
    } else {
      message.info('已驳回该准备单')
    }
  } catch (caught) {
    message.error(describeApiError(caught))
  } finally {
    decidingId.value = null
  }
}

async function handleCommit(): Promise<void> {
  if (commitBlocked.value) {
    message.warning('当前身份缺少 OPERATOR 职责，无法 Commit')
    return
  }
  const versionId = designState.data.value?.version.id
  if (!versionId) {
    message.warning('请先生成候选版本，再进行 Commit')
    return
  }
  // 已 READY 的准备单直接复用：否则每次点击都会新建一张待审批准备单，审批永远追不上点击。
  const reusable = (preparationsState.data.value ?? []).find(
    (item) => item.pipelineVersionId === versionId && item.status === 'READY',
  )
  committing.value = true
  try {
    // Prepare → （审批）→ Commit；后端在同一事务里冻结事实并按 PDP 决定是否需要审批。
    const preparation = reusable ?? (await studioService.prepare(versionId, null))
    if (preparation.status === 'WAITING_APPROVAL') {
      const roles = preparation.requiredRoles.join('、') || '审批人'
      message.warning(`准备单 ${preparation.id} 等待审批，需 ${roles} 批准后才能 Commit`)
      await preparationsState.run()
      return
    }
    if (preparation.status === 'COMMITTED') {
      message.warning('该准备单已提交过，请到运行中心查看执行记录')
      return
    }
    const result = await studioService.commitPreparation(preparation.id)
    message.success(`已提交执行，执行编号 ${result.executionRunId}（${result.status}）`)
    await preparationsState.run()
  } catch (caught) {
    message.error(describeApiError(caught))
  } finally {
    committing.value = false
  }
}

onMounted(() => {
  void refreshDesign()
  void stepsState.run()
  void runState.run()
  void preparationsState.run()
  void versionsState.run()
})
</script>

<template>
  <div>
    <PageHeader
      eyebrow="AGENT WORKBENCH"
      title="Pipeline Studio"
      description="描述目标、选择数据库或受管文件 Profile，由 Agent 生成并解释 SeaTunnel 配置。"
    >
      <template #actions>
        <NButton secondary :loading="generating" @click="handleGenerate">
          <template #icon><Wand2 :size="15" /></template>
          生成候选
        </NButton>
      </template>
    </PageHeader>

    <LoadingState v-if="designState.loading.value && !designState.data.value" :rows="4" />
    <ErrorState
      v-else-if="designState.error.value && !designState.data.value"
      :message="designState.error.value"
      @retry="refreshDesign"
    />
    <EmptyState v-else-if="!designState.data.value" title="尚无候选方案" description="输入需求后点击「生成候选」。" />
    <div v-else class="flex flex-col gap-4">
      <div class="grid grid-cols-1 gap-4 lg:grid-cols-[0.85fr_1.15fr]">
        <Panel title="需求与契约" description="自然语言只作为意图，冻结的契约才是可执行事实。">
          <div class="flex flex-col gap-4">
            <FormField label="需求描述" required :hint="`${requirement.length} / 4000 字符`">
              <NInput
                v-model:value="requirement"
                type="textarea"
                :autosize="{ minRows: 4, maxRows: 8 }"
                placeholder="例如：把 MySQL 的订单同步到 Doris……"
              />
            </FormField>

            <FormField label="Source Profile" required>
              <NSelect v-model:value="sourceProfile" :options="profileOptions" />
            </FormField>
            <FormField label="Sink Profile" required>
              <NSelect v-model:value="sinkProfile" :options="sinkOptions" />
            </FormField>

            <fieldset class="flex flex-col gap-2">
              <legend class="mb-1 text-xs font-semibold text-ink">契约</legend>
              <label
                v-for="contract in contracts"
                :key="contract.key"
                class="flex cursor-pointer items-start gap-3 rounded-lg border border-line p-3 transition-colors hover:border-accent/50"
              >
                <NCheckbox v-model:checked="contract.enabled" />
                <span class="flex flex-col">
                  <strong class="text-xs text-ink">{{ contract.label }}</strong>
                  <small class="text-[10px] text-muted">{{ contract.desc }}</small>
                </span>
              </label>
            </fieldset>
          </div>
        </Panel>

        <Panel
          title="Agent 控制台"
          :description="`修复预算：已用 ${runState.data.value?.repairCount ?? 0} / 2`"
        >
          <template #actions>
            <StatusTag v-if="runState.data.value" :status="runState.data.value.status" />
          </template>

          <LoadingState v-if="stepsState.loading.value && !stepsState.data.value" :rows="2" />
          <ol v-else class="flex flex-col gap-3">
            <li
              v-for="step in stepsState.data.value ?? []"
              :key="step.id"
              class="flex gap-3 rounded-lg border border-line bg-bg/40 p-3"
            >
              <component :is="stepIcon(step)" :size="16" class="mt-0.5 shrink-0" :class="stepClass(step)" aria-hidden="true" />
              <div class="min-w-0">
                <div class="flex items-center gap-2">
                  <strong class="text-xs text-ink">{{ step.title }}</strong>
                  <span class="font-mono text-[10px] text-muted">{{ step.kind }}</span>
                </div>
                <p class="mt-1 text-[11px] leading-relaxed text-muted">{{ step.detail }}</p>
                <time class="mt-1 block font-mono text-[10px] text-muted/80">{{ step.at }}</time>
              </div>
            </li>
          </ol>
          <p v-if="runState.data.value" class="mt-4 rounded-lg bg-accent-soft px-3 py-2 text-[11px] text-accent">
            {{ runState.data.value.message }}
          </p>

          <div
            v-if="runState.data.value?.status === 'WAITING_INPUT'"
            class="mt-3 flex flex-col gap-2 rounded-lg border border-warn/40 bg-warn/5 p-3"
          >
            <strong class="text-[11px] text-warn">Agent 需要澄清</strong>
            <NInput
              v-model:value="answerText"
              type="textarea"
              :autosize="{ minRows: 2, maxRows: 4 }"
              placeholder="补充说明后继续生成，例如：订单表按 created_at 增量，水位窗口左开右闭。"
            />
            <div class="flex justify-end">
              <NButton type="primary" size="small" :loading="answering" @click="handleAnswer">
                提交回答并继续
              </NButton>
            </div>
          </div>
        </Panel>
      </div>

      <Panel title="方案审查" description="审批绑定内容摘要，而不是数据库 ID。">
        <div class="flex flex-col gap-4">
          <div class="flex flex-col gap-2">
            <span class="font-mono text-[10px] tracking-wide text-muted">DAG</span>
            <div class="flex flex-col gap-2 sm:flex-row sm:items-stretch">
              <template v-for="(node, index) in designState.data.value.designReview.nodes" :key="node.id">
                <article
                  class="flex flex-1 flex-col gap-1 rounded-lg border p-3"
                  :class="{
                    'border-accent/30 bg-accent-soft': node.kind === 'SOURCE',
                    'border-line bg-bg/60': node.kind === 'TRANSFORM',
                    'border-ok/30 bg-ok/5': node.kind === 'SINK',
                  }"
                >
                  <small class="font-mono text-[10px] text-muted">{{ node.kind }} · {{ node.connectorType }}</small>
                  <strong class="text-xs text-ink">{{ node.label }}</strong>
                  <span class="text-[10px] text-muted">{{ node.route ?? 'implicit route' }}</span>
                </article>
                <span
                  v-if="index < designState.data.value.designReview.nodes.length - 1"
                  class="hidden self-center font-mono text-accent sm:block"
                  aria-hidden="true"
                >
                  →
                </span>
              </template>
            </div>
          </div>

          <div class="grid grid-cols-1 gap-4 md:grid-cols-2">
            <div>
              <h3 class="mb-2 text-xs font-semibold text-ink">Schema Diff</h3>
              <article
                v-for="diff in designState.data.value.designReview.schemaDiffs"
                :key="diff.target"
                class="rounded-lg border border-line p-3"
              >
                <strong class="text-[11px] text-ink">{{ diff.target }}</strong>
                <p class="mt-1 text-[10px] text-muted">
                  新增：{{ diff.addedColumns.join('、') || '无' }}
                </p>
                <p class="text-[10px] text-muted">移除：{{ diff.removedColumns.join('、') || '无' }}</p>
                <p class="text-[10px] text-muted">
                  类型变化：{{
                    diff.typeChanges.map((item) => `${item.column}: ${item.sourceType} → ${item.targetType}`).join('；') ||
                    '无'
                  }}
                </p>
              </article>
            </div>

            <div>
              <h3 class="mb-2 text-xs font-semibold text-ink">风险项</h3>
              <ul class="flex flex-col gap-2">
                <li
                  v-for="risk in designState.data.value.designReview.risks"
                  :key="risk.code"
                  class="flex items-start gap-2 rounded-lg border border-line p-3"
                >
                  <RiskBadge :level="risk.level === 'HIGH' ? 'P0' : risk.level === 'MEDIUM' ? 'P1' : 'P3'" />
                  <div class="min-w-0">
                    <strong class="block font-mono text-[10px] text-ink">{{ risk.code }}</strong>
                    <span class="text-[10px] text-muted">{{ risk.message }}</span>
                  </div>
                </li>
              </ul>
            </div>
          </div>

          <div class="grid grid-cols-1 gap-3 md:grid-cols-2">
            <div>
              <h3 class="mb-2 text-xs font-semibold text-ink">脱敏样本 · Before</h3>
              <pre class="max-h-[240px] overflow-auto rounded-lg bg-sidebar p-3 font-mono text-[10px] leading-relaxed text-sidebar-muted">{{ JSON.stringify(designState.data.value.designReview.samplePreview.before, null, 2) }}</pre>
            </div>
            <div>
              <h3 class="mb-2 text-xs font-semibold text-ink">结构预览 · After</h3>
              <pre class="max-h-[240px] overflow-auto rounded-lg bg-sidebar p-3 font-mono text-[10px] leading-relaxed text-sidebar-muted">{{ JSON.stringify(designState.data.value.designReview.samplePreview.after, null, 2) }}</pre>
            </div>
          </div>
          <p class="text-[10px] text-muted">{{ designState.data.value.designReview.previewSemantics }}</p>

          <details class="rounded-lg border border-line p-3">
            <summary class="cursor-pointer text-xs font-semibold text-ink">查看冻结 HOCON</summary>
            <pre v-if="hocon" class="mt-3 max-h-[320px] overflow-auto rounded-lg bg-sidebar p-3 font-mono text-[10px] leading-relaxed text-sidebar-muted">{{ hocon }}</pre>
            <p v-else class="mt-3 text-[10px] text-muted">当前版本尚未冻结 HOCON 制品。</p>
          </details>
        </div>
      </Panel>

      <Panel
        title="执行 Preparation"
        description="审批分离：申请人不能自批；P0 破坏性操作需要双职责双人。切换身份后在此占用审批槽，READY 再由 OPERATOR Commit。"
      >
        <template #actions>
          <NButton
            type="primary"
            size="small"
            :disabled="commitBlocked"
            :loading="committing"
            :title="commitBlocked ? '当前身份缺少 OPERATOR 职责' : undefined"
            @click="handleCommit"
          >
            提交 Commit
          </NButton>
        </template>

        <LoadingState v-if="preparationsState.loading.value && !preparationsState.data.value" :rows="2" />
        <div v-else class="overflow-x-auto">
          <table class="w-full min-w-[1040px] border-collapse text-left">
            <thead>
              <tr class="border-b border-line">
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">Preparation</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">版本</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">风险</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">必备职责</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">状态</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">到期</th>
                <th class="px-3 py-2 font-mono text-[9px] tracking-wider text-muted uppercase">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="preparation in preparationsState.data.value ?? []"
                :key="preparation.id"
                class="border-b border-line/70 last:border-b-0"
              >
                <td class="px-3 py-3">
                  <strong class="block font-mono text-[11px] text-ink">{{ preparation.id }}</strong>
                  <small class="text-[10px] text-muted">{{ preparation.impactReport }}</small>
                </td>
                <td class="px-3 py-3 font-mono text-[11px] text-ink">
                  v{{ versionNumberById.get(preparation.pipelineVersionId) ?? '?' }}
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
                <td class="px-3 py-3"><StatusTag :status="preparation.status" /></td>
                <td class="px-3 py-3 font-mono text-[10px] text-muted">
                  {{ preparation.expiresAt.slice(0, 16).replace('T', ' ') }}
                </td>
                <td class="px-3 py-3">
                  <div class="flex items-center gap-1.5">
                    <NButton
                      size="tiny"
                      type="primary"
                      :disabled="!canDecide(preparation)"
                      :loading="decidingId === preparation.id"
                      :title="decisionBlockedReason(preparation)"
                      @click="handleDecision(preparation, 'APPROVE')"
                    >
                      通过
                    </NButton>
                    <NButton
                      size="tiny"
                      quaternary
                      :disabled="!canDecide(preparation)"
                      :loading="decidingId === preparation.id"
                      :title="decisionBlockedReason(preparation)"
                      @click="handleDecision(preparation, 'REJECT')"
                    >
                      驳回
                    </NButton>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </Panel>

      <p class="flex items-center gap-2 text-[10px] text-muted">
        <Sparkles :size="13" class="text-accent" aria-hidden="true" />
        生成候选、方案审查与 Commit 均调用真实后端；Commit 仅签发一次性能力并投递 Outbox，由 Worker 执行。
      </p>
    </div>
  </div>
</template>
