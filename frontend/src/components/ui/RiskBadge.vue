<script setup lang="ts">
import { computed } from 'vue'

import type { RiskLevel } from '@/types/models'

const props = defineProps<{ level: RiskLevel }>()

const LEVEL_CLASS: Record<RiskLevel, string> = {
  P0: 'border-danger/30 bg-danger/10 text-danger',
  P1: 'border-warn/30 bg-warn/10 text-warn',
  P2: 'border-accent/30 bg-accent/10 text-accent',
  P3: 'border-ok/30 bg-ok/10 text-ok',
}

const levelClass = computed(() => LEVEL_CLASS[props.level])
</script>

<template>
  <span
    class="inline-grid min-w-[34px] place-items-center rounded-md border px-1.5 py-1 font-mono text-[10px] font-bold"
    :class="levelClass"
    :title="`风险等级 ${level}`"
  >
    {{ level }}
  </span>
</template>
