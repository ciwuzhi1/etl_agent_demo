<script setup lang="ts">
import { Activity, Database, LayoutDashboard, ShieldCheck, Workflow } from 'lucide-vue-next'
import type { Component } from 'vue'
import { computed } from 'vue'
import { RouterLink, useRoute } from 'vue-router'

interface NavItem {
  name: string
  label: string
  icon: Component
}

const items: NavItem[] = [
  { name: 'overview', label: '总览', icon: LayoutDashboard },
  { name: 'connections', label: '数据连接', icon: Database },
  { name: 'studio', label: 'Pipeline Studio', icon: Workflow },
  { name: 'operations', label: '运行中心', icon: Activity },
  { name: 'governance', label: '安全治理', icon: ShieldCheck },
]

const route = useRoute()
const activeName = computed(() => route.name)
</script>

<template>
  <aside class="sticky top-0 flex h-screen flex-col bg-sidebar px-3 py-6 lg:px-4">
    <div class="flex items-center gap-3 border-b border-white/10 px-1 pb-6 lg:px-2">
      <span class="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-accent text-sm font-extrabold text-sidebar">
        FP
      </span>
      <div class="hidden flex-col lg:flex">
        <strong class="text-[15px] font-semibold text-white">ETL Agent</strong>
        <small class="font-mono text-[9px] tracking-[1.4px] text-sidebar-muted">CONTROL PLANE</small>
      </div>
    </div>

    <nav class="mt-5 flex flex-col gap-1.5" aria-label="主导航">
      <RouterLink
        v-for="item in items"
        :key="item.name"
        :to="{ name: item.name }"
        class="flex items-center gap-3 rounded-lg border-l-[3px] px-3 py-3 text-[13px] transition-colors"
        :class="
          activeName === item.name
            ? 'border-accent bg-sidebar-active text-white'
            : 'border-transparent text-sidebar-muted hover:bg-sidebar-active/60 hover:text-white'
        "
      >
        <component :is="item.icon" :size="18" class="shrink-0" aria-hidden="true" />
        <span class="hidden lg:inline">{{ item.label }}</span>
      </RouterLink>
    </nav>

    <div class="mt-auto flex items-center gap-2 border-t border-white/10 px-1 pt-5 lg:px-2">
      <span
        class="h-2 w-2 shrink-0 rounded-full bg-ok shadow-[0_0_0_4px_rgba(21,128,61,0.18)]"
        aria-hidden="true"
      />
      <div class="hidden flex-col lg:flex">
        <strong class="text-[11px] text-sidebar-muted">Worker 在线</strong>
        <small class="font-mono text-[9px] text-sidebar-muted/70">heartbeat 10s</small>
      </div>
    </div>
  </aside>
</template>
