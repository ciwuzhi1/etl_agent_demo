<script setup lang="ts">
import { Bell } from 'lucide-vue-next'
import { useRoute } from 'vue-router'

import IdentitySwitcher from '@/components/shell/IdentitySwitcher.vue'
import { useSession } from '@/composables/useSession'

const route = useRoute()
const session = useSession()
</script>

<template>
  <header
    class="sticky top-0 z-10 flex h-[76px] items-center justify-between gap-4 border-b border-line bg-surface px-4 sm:px-6 lg:px-9"
  >
    <div class="flex min-w-0 flex-col">
      <p class="font-mono text-[10px] tracking-[1.5px] text-muted">{{ route.meta.section }}</p>
      <strong class="truncate text-sm text-ink">{{ route.meta.title }}</strong>
    </div>

    <div class="flex items-center gap-3">
      <span class="hidden rounded-md bg-bg px-2.5 py-1.5 font-mono text-[10px] text-muted sm:inline">
        DEMO
      </span>
      <IdentitySwitcher />
      <button
        type="button"
        class="grid h-8 w-8 place-items-center rounded-full text-muted transition-colors hover:bg-bg"
        aria-label="通知"
      >
        <Bell :size="17" aria-hidden="true" />
      </button>
      <span
        class="grid h-9 w-9 shrink-0 place-items-center rounded-full bg-accent-soft text-sm font-bold text-accent"
        :title="session.currentActor.value?.name"
      >
        {{ session.currentActor.value?.name.slice(0, 1) ?? '—' }}
      </span>
    </div>
  </header>
</template>
