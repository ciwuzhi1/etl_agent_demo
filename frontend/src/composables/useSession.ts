import { computed, ref } from 'vue'

import type { Actor, HarnessRole } from '@/types/models'

/**
 * 演示用的自然人身份清单。真实环境身份来自可信网关，
 * 此处仅用于前端展示职责分离（申请人 ≠ 审批人）。
 */
const ACTORS: readonly Actor[] = [
  { id: 'u-1001', name: '林知远', title: '数据工程师', roles: ['DATA_ENGINEER'], harnessRoles: [] },
  { id: 'u-1002', name: '陈静', title: '数据审批人', roles: ['APPROVER'], harnessRoles: ['DATA_APPROVER'] },
  { id: 'u-1003', name: '赵启明', title: '安全审批人', roles: ['APPROVER'], harnessRoles: ['SECURITY_APPROVER'] },
  { id: 'u-1004', name: '孙可', title: '运行操作员', roles: ['ADMIN'], harnessRoles: ['OPERATOR'] },
  { id: 'u-1005', name: '周宁', title: '审计员', roles: ['VIEWER'], harnessRoles: [] },
]

const currentActorId = ref<string | null>(null)

export function useSession() {
  const actors = ACTORS
  const currentActor = computed<Actor | null>(
    () => ACTORS.find((actor) => actor.id === currentActorId.value) ?? null,
  )
  const isAuthenticated = computed(() => currentActor.value !== null)

  function signIn(actorId: string): void {
    currentActorId.value = actorId
  }

  function signOut(): void {
    currentActorId.value = null
  }

  function switchActor(actorId: string): void {
    if (ACTORS.some((actor) => actor.id === actorId)) {
      currentActorId.value = actorId
    }
  }

  function hasHarnessRole(role: HarnessRole): boolean {
    return currentActor.value?.harnessRoles.includes(role) ?? false
  }

  return {
    actors,
    currentActor,
    isAuthenticated,
    signIn,
    signOut,
    switchActor,
    hasHarnessRole,
  }
}
