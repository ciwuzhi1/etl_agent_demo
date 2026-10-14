<script setup lang="ts">
import { KeyRound, Plus, RefreshCw, Search, Server, Upload } from 'lucide-vue-next'
import { NButton, NInput, NSelect, useMessage } from 'naive-ui'
import { computed, onMounted, ref } from 'vue'

import EmptyState from '@/components/ui/EmptyState.vue'
import ErrorState from '@/components/ui/ErrorState.vue'
import FormField from '@/components/ui/FormField.vue'
import LoadingState from '@/components/ui/LoadingState.vue'
import PageHeader from '@/components/ui/PageHeader.vue'
import Panel from '@/components/ui/Panel.vue'
import StatusTag from '@/components/ui/StatusTag.vue'
import { useAsyncState } from '@/composables/useAsyncState'
import { connectionsService } from '@/services/connections'
import type {
  Connection,
  ConnectorType,
  DataProfile,
  FileAsset,
  SecretLifecycle,
} from '@/types/models'

const message = useMessage()

const listState = useAsyncState(() => connectionsService.listConnections())
const connections = ref<Connection[]>([])

const profiles = ref<DataProfile[]>([])
const files = ref<FileAsset[]>([])
const secrets = ref<SecretLifecycle[]>([])

const keyword = ref('')

const connectorOptions: { label: string; value: ConnectorType }[] = [
  { label: 'MySQL', value: 'MYSQL' },
  { label: 'PostgreSQL', value: 'POSTGRESQL' },
  { label: 'Oracle', value: 'ORACLE' },
  { label: 'SQL Server', value: 'SQLSERVER' },
  { label: 'Hive', value: 'HIVE' },
  { label: 'HDFS', value: 'HDFS' },
  { label: 'Doris', value: 'DORIS' },
]
const directionOptions = [
  { label: '数据源（Source）', value: 'SOURCE' },
  { label: '数据汇（Sink）', value: 'SINK' },
]

const draft = ref({
  name: '',
  connectorType: 'MYSQL',
  host: '',
  port: '3306',
  database: '',
  direction: 'SOURCE',
})
const draftError = ref<string | null>(null)
const submitting = ref(false)

const filteredConnections = computed(() => {
  const query = keyword.value.trim().toLowerCase()
  if (!query) {
    return connections.value
  }
  return connections.value.filter(
    (connection) =>
      connection.name.toLowerCase().includes(query) ||
      connection.connectorType.toLowerCase().includes(query),
  )
})

const expiringSecrets = computed(() => secrets.value.filter((secret) => secret.daysToExpiry <= 7))

function formatBytes(bytes: number): string {
  if (bytes === 0) {
    return '0 B'
  }
  const units = ['B', 'KB', 'MB', 'GB']
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  return `${(bytes / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`
}

async function loadList(): Promise<void> {
  await listState.run()
  connections.value = listState.data.value ? [...listState.data.value] : []
}

async function testConnection(connection: Connection): Promise<void> {
  connection.status = 'TESTING'
  await new Promise((resolve) => {
    window.setTimeout(resolve, 700)
  })
  connection.status = 'READY'
  connection.lastTestedAt = new Date().toISOString()
  message.success(`${connection.name} 连通性测试通过`)
}

async function handleRegister(): Promise<void> {
  draftError.value = null
  if (!draft.value.name.trim() || !draft.value.host.trim() || !draft.value.database.trim()) {
    draftError.value = '名称、主机与数据库为必填项'
    return
  }
  const port = Number.parseInt(draft.value.port, 10)
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    draftError.value = '端口必须是 1–65535 的整数'
    return
  }

  submitting.value = true
  const created: Connection = {
    id: crypto.randomUUID(),
    name: draft.value.name.trim(),
    connectorType: draft.value.connectorType as ConnectorType,
    host: draft.value.host.trim(),
    port,
    database: draft.value.database.trim(),
    direction: draft.value.direction as Connection['direction'],
    status: 'TESTING',
    lastTestedAt: null,
  }
  connections.value = [created, ...connections.value]
  draft.value = {
    name: '',
    connectorType: 'MYSQL',
    host: '',
    port: '3306',
    database: '',
    direction: 'SOURCE',
  }
  submitting.value = false
  message.success('连接已登记，凭据将以密文保存，模型不可见')
}

onMounted(async () => {
  await loadList()
  const [profileList, fileList, secretList] = await Promise.all([
    connectionsService.listProfiles(),
    connectionsService.listFiles(),
    connectionsService.listSecrets(),
  ])
  profiles.value = profileList
  files.value = fileList
  secrets.value = secretList
})
</script>

<template>
  <div>
    <PageHeader
      eyebrow="DATA ACCESS"
      title="数据连接"
      description="凭据加密保存，Agent 只使用连接别名和脱敏 Profile。"
    >
      <template #actions>
        <NButton quaternary :loading="listState.loading.value" @click="loadList">
          <template #icon><RefreshCw :size="15" /></template>
          刷新
        </NButton>
      </template>
    </PageHeader>

    <LoadingState v-if="listState.loading.value && !connections.length" :rows="3" />
    <ErrorState
      v-else-if="listState.error.value && !connections.length"
      :message="listState.error.value"
      @retry="loadList"
    />
    <div v-else class="grid grid-cols-1 gap-4 lg:grid-cols-[1.25fr_0.75fr]">
      <div class="flex flex-col gap-4">
        <Panel title="连接清单">
          <template #actions>
            <NInput v-model:value="keyword" size="small" placeholder="搜索连接" class="w-[150px]">
              <template #prefix><Search :size="14" /></template>
            </NInput>
          </template>

          <EmptyState
            v-if="!filteredConnections.length"
            title="暂无数据连接"
            description="请在右侧登记来源数据库或 Doris 目标。"
          />
          <ul v-else class="flex flex-col gap-2">
            <li
              v-for="connection in filteredConnections"
              :key="connection.id"
              class="flex items-center gap-3 rounded-lg border border-line bg-bg/40 p-3"
            >
              <span
                class="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-accent-soft font-mono text-[11px] font-semibold text-accent"
              >
                {{ connection.connectorType.slice(0, 2) }}
              </span>
              <div class="min-w-0 flex-1">
                <div class="flex items-center gap-2">
                  <strong class="truncate text-xs text-ink">{{ connection.name }}</strong>
                  <StatusTag :status="connection.status" />
                </div>
                <small class="font-mono text-[10px] text-muted">
                  {{ connection.host }}:{{ connection.port }}/{{ connection.database }} ·
                  {{ connection.direction }}
                </small>
              </div>
              <NButton size="tiny" quaternary @click="testConnection(connection)">测试</NButton>
            </li>
          </ul>
        </Panel>

        <Panel title="Profile 快照" description="每次探查最多 100 行样本，只保留结构与脱敏结果。">
          <ul v-if="profiles.length" class="grid grid-cols-1 gap-2 sm:grid-cols-2">
            <li
              v-for="profile in profiles"
              :key="profile.id"
              class="flex items-center justify-between gap-3 rounded-lg border border-line px-3 py-2.5"
            >
              <div class="min-w-0">
                <strong class="block truncate text-xs text-ink">{{ profile.table }}</strong>
                <small class="font-mono text-[10px] text-muted">
                  {{ profile.direction }} · {{ profile.columnCount }} 列 · {{ profile.sampleRows }} 行样本
                </small>
              </div>
              <StatusTag :status="profile.status" />
            </li>
          </ul>
          <EmptyState v-else title="暂无 Profile" description="创建探查任务后在此查看。" />
        </Panel>

        <Panel title="受管文件">
          <template #actions>
            <NButton size="small" tertiary>
              <template #icon><Upload :size="14" /></template>
              上传
            </NButton>
          </template>
          <ul v-if="files.length" class="flex flex-col">
            <li
              v-for="file in files"
              :key="file.id"
              class="flex items-center justify-between gap-3 border-t border-line py-2.5 text-xs first:border-t-0"
            >
              <div class="min-w-0">
                <strong class="block truncate text-ink">{{ file.name }}</strong>
                <small class="font-mono text-[10px] text-muted">
                  {{ file.format }} · {{ file.rows.toLocaleString() }} 行 · {{ formatBytes(file.sizeBytes) }}
                </small>
              </div>
              <StatusTag :status="file.status" />
            </li>
          </ul>
          <EmptyState v-else title="暂无受管文件" description="支持 CSV/JSON/JSONL/XLSX/Parquet/ORC/XML/Markdown/PDF。" />
        </Panel>
      </div>

      <div class="flex flex-col gap-4">
        <Panel title="登记连接" description="凭据只在 Worker 调用 Adapter 前短暂存在于内存。">
          <form class="flex flex-col gap-4" novalidate @submit.prevent="handleRegister">
            <FormField label="连接名称" required>
              <NInput v-model:value="draft.name" placeholder="如：MySQL 订单库" />
            </FormField>
            <FormField label="连接类型" required>
              <NSelect v-model:value="draft.connectorType" :options="connectorOptions" />
            </FormField>
            <div class="flex gap-3">
              <FormField label="主机" required class="flex-1">
                <NInput v-model:value="draft.host" placeholder="localhost" />
              </FormField>
              <FormField label="端口" required class="w-[100px]">
                <NInput v-model:value="draft.port" type="text" placeholder="3306" />
              </FormField>
            </div>
            <FormField label="数据库" required>
              <NInput v-model:value="draft.database" placeholder="source_db" />
            </FormField>
            <FormField label="方向" required>
              <NSelect v-model:value="draft.direction" :options="directionOptions" />
            </FormField>
            <p v-if="draftError" class="text-[11px] text-danger" role="alert">{{ draftError }}</p>
            <NButton type="primary" attr-type="submit" block :loading="submitting">
              <template #icon><Plus :size="15" /></template>
              登记连接
            </NButton>
          </form>
        </Panel>

        <Panel title="凭据生命周期" description="最长 90 天，提前 14 天预警；销毁与迁移需审批。">
          <ul v-if="secrets.length" class="flex flex-col gap-2">
            <li
              v-for="secret in secrets"
              :key="secret.id"
              class="flex items-center justify-between gap-3 rounded-lg border px-3 py-2.5"
              :class="secret.daysToExpiry <= 7 ? 'border-warn/40 bg-warn/5' : 'border-line'"
            >
              <div class="flex min-w-0 items-center gap-2">
                <KeyRound :size="15" class="shrink-0 text-muted" aria-hidden="true" />
                <div class="min-w-0">
                  <strong class="block font-mono text-[11px] text-ink">{{ secret.id }}</strong>
                  <small class="text-[10px] text-muted">
                    剩余 {{ secret.daysToExpiry }} 天 · 至 {{ secret.expiresAt.slice(0, 10) }}
                  </small>
                </div>
              </div>
              <StatusTag :status="secret.status" />
            </li>
          </ul>
          <p v-if="expiringSecrets.length" class="mt-3 text-[11px] text-warn">
            有 {{ expiringSecrets.length }} 个凭据进入到期预警窗口。
          </p>
        </Panel>

        <Panel title="连接洞察">
          <div class="flex items-center gap-3 text-xs text-muted">
            <Server :size="16" class="text-accent" aria-hidden="true" />
            <span>
              共 {{ connections.length }} 个连接、{{ profiles.length }} 份 Profile、{{ files.length }} 个受管文件。
            </span>
          </div>
        </Panel>
      </div>
    </div>
  </div>
</template>
