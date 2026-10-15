import { computed, ref } from 'vue'

import { getWorkspaceContext } from '@/services/context'
import { markSessionReady, setActorId, setProjectId } from '@/services/sessionStore'

import type { Actor, HarnessRole, WorkspaceMember } from '@/types/models'

/**
 * 会话：从后端 `/context` 拉取首个工作区与成员，把真实成员映射为可切换身份。
 *
 * 本地 dev-header 模式下 `Actor.id` 就是后端 `users.id`，请求头 `X-Actor-Id` 直接使用它；
 * 后端不可用时回退到演示身份，保证登录页仍能渲染（随后请求会失败并展示错误态）。
 */
const FALLBACK_ACTORS: readonly Actor[] = [
  { id: 'u-1001', name: '林知远', title: '数据工程师', roles: ['DATA_ENGINEER'], harnessRoles: ['DATA_ENGINEER'] },
  { id: 'u-1002', name: '陈静', title: '数据审批人', roles: ['APPROVER'], harnessRoles: ['DATA_APPROVER'] },
  { id: 'u-1003', name: '赵启明', title: '安全审批人', roles: ['APPROVER'], harnessRoles: ['SECURITY_APPROVER'] },
  { id: 'u-1004', name: '孙可', title: '运行操作员', roles: ['ADMIN'], harnessRoles: ['OPERATOR'] },
  { id: 'u-1005', name: '周宁', title: '审计员', roles: ['VIEWER'], harnessRoles: ['AUDITOR'] },
]

const actors = ref<Actor[]>([...FALLBACK_ACTORS])
const currentActorId = ref<string | null>(null)
const projectId = ref<string | null>(null)
const loaded = ref(false)
let loadPromise: Promise<void> | null = null

function toActor(member: WorkspaceMember): Actor {
  return {
    id: member.user_id,
    name: member.display_name,
    title: member.role,
    roles: [member.role],
    harnessRoles: member.harness_roles,
  }
}

async function load(): Promise<void> {
  try {
    const context = await getWorkspaceContext()
    const mapped = context.members.map(toActor)
    if (mapped.length > 0) {
      actors.value = mapped
    }
    projectId.value = context.project_id
    setProjectId(context.project_id)
  } catch {
    // 后端不可用：保留演示身份，projectId 为空，后续请求将以错误态呈现。
    actors.value = [...FALLBACK_ACTORS]
    projectId.value = null
    setProjectId(null)
  }
  loaded.value = true
  markSessionReady()
}

export function useSession() {
  /** 幂等加载一次工作区上下文；路由守卫与页面都复用它。 */
  function ensureLoaded(): Promise<void> {
    if (!loadPromise) {
      loadPromise = load()
    }
    return loadPromise
  }

  const currentActor = computed<Actor | null>(
    () => actors.value.find((actor) => actor.id === currentActorId.value) ?? null,
  )
  const isAuthenticated = computed(() => currentActor.value !== null)

  function signIn(actorId: string): void {
    currentActorId.value = actorId
    setActorId(actorId)
  }

  function signOut(): void {
    currentActorId.value = null
    setActorId(null)
  }

  function switchActor(actorId: string): void {
    if (actors.value.some((actor) => actor.id === actorId)) {
      currentActorId.value = actorId
      setActorId(actorId)
    }
  }

  function hasHarnessRole(role: HarnessRole): boolean {
    return currentActor.value?.harnessRoles.includes(role) ?? false
  }

  return {
    actors,
    currentActor,
    isAuthenticated,
    loaded,
    projectId,
    ensureLoaded,
    signIn,
    signOut,
    switchActor,
    hasHarnessRole,
  }
}
