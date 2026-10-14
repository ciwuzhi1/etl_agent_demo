<script setup lang="ts">
import type { Component } from 'vue'
import { computed } from 'vue'

type Tone = 'accent' | 'ok' | 'warn' | 'danger' | 'neutral'

const props = withDefaults(
  defineProps<{
    icon: Component
    label: string
    value: number | string
    unit?: string
    hint?: string
    tone?: Tone
  }>(),
  { tone: 'accent', unit: undefined, hint: undefined },
)

const TONE_CLASS: Record<Tone, string> = {
  accent: 'bg-accent-soft text-accent',
  ok: 'bg-ok/10 text-ok',
  warn: 'bg-warn/10 text-warn',
  danger: 'bg-danger/10 text-danger',
  neutral: 'bg-ink/5 text-muted',
}

const toneClass = computed(() => TONE_CLASS[props.tone])
</script>

<template>
  <article class="flex items-center gap-4 rounded-xl border border-line bg-surface p-5 shadow-sm">
    <span class="grid h-11 w-11 shrink-0 place-items-center rounded-xl" :class="toneClass">
      <component :is="icon" :size="20" aria-hidden="true" />
    </span>
    <div class="min-w-0">
      <p class="text-[11px] text-muted">{{ label }}</p>
      <p class="mt-1 flex items-baseline gap-1">
        <strong class="text-2xl font-bold text-ink">{{ value }}</strong>
        <span v-if="unit" class="text-[11px] text-muted">{{ unit }}</span>
      </p>
      <p v-if="hint" class="mt-0.5 text-[10px] text-muted">{{ hint }}</p>
    </div>
  </article>
</template>
