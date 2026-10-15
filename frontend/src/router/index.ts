import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'

import { useSession } from '@/composables/useSession'

const routes: RouteRecordRaw[] = [
  {
    path: '/login',
    component: () => import('@/layouts/AuthLayout.vue'),
    children: [
      {
        path: '',
        name: 'login',
        component: () => import('@/views/LoginView.vue'),
        meta: { title: '登录', section: 'SIGN IN' },
      },
    ],
  },
  {
    path: '/',
    component: () => import('@/layouts/AppShell.vue'),
    children: [
      {
        path: '',
        name: 'overview',
        component: () => import('@/views/OverviewView.vue'),
        meta: { title: '总览', section: 'OVERVIEW' },
      },
      {
        path: 'connections',
        name: 'connections',
        component: () => import('@/views/ConnectionsView.vue'),
        meta: { title: '数据连接', section: 'DATA ACCESS' },
      },
      {
        path: 'studio',
        name: 'studio',
        component: () => import('@/views/StudioView.vue'),
        meta: { title: 'Pipeline Studio', section: 'AGENT WORKBENCH' },
      },
      {
        path: 'operations',
        name: 'operations',
        component: () => import('@/views/OperationsView.vue'),
        meta: { title: '运行中心', section: 'EXECUTION LEDGER' },
      },
      {
        path: 'governance',
        name: 'governance',
        component: () => import('@/views/GovernanceView.vue'),
        meta: { title: '安全治理', section: 'SAFETY GOVERNANCE' },
      },
    ],
  },
]

export const router = createRouter({
  history: createWebHistory(),
  routes,
  scrollBehavior: () => ({ top: 0 }),
})

router.beforeEach(async (to) => {
  const session = useSession()
  // 先加载工作区成员，登录页的下拉才能列出真实身份（而不是演示占位名单）。
  await session.ensureLoaded()
  if (to.name !== 'login' && !session.isAuthenticated.value) {
    return { name: 'login' }
  }
  if (to.name === 'login' && session.isAuthenticated.value) {
    return { name: 'overview' }
  }
  return true
})
