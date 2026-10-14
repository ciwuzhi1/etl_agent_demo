<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ status: string; label?: string }>()

type Tone = 'ok' | 'warn' | 'info' | 'danger' | 'neutral'

const TONE_CLASS: Record<Tone, string> = {
  ok: 'bg-ok/10 text-ok',
  warn: 'bg-warn/10 text-warn',
  info: 'bg-accent/10 text-accent',
  danger: 'bg-danger/10 text-danger',
  neutral: 'bg-ink/5 text-muted',
}

const STATUS_TONE: Record<string, Tone> = {
  READY: 'ok',
  SUCCEEDED: 'ok',
  PASSED: 'ok',
  APPROVED: 'ok',
  PARSED: 'ok',
  ACTIVE: 'ok',
  VERIFIED: 'ok',
  ONLINE: 'ok',
  RELEASED: 'ok',
  WAITING_INPUT: 'warn',
  WAITING_APPROVAL: 'warn',
  PROFILING: 'warn',
  TESTING: 'warn',
  PENDING: 'warn',
  WARNING: 'warn',
  IN_REVIEW: 'warn',
  SHADOW: 'warn',
  CANARY: 'warn',
  THROTTLE: 'warn',
  RUNNING: 'info',
  QUEUED: 'info',
  SUBMITTED: 'info',
  STAGED: 'info',
  PARSING: 'info',
  FAILED: 'danger',
  REJECTED: 'danger',
  ERROR: 'danger',
  DENIED: 'danger',
  EXPIRED: 'danger',
  ROLLED_BACK: 'danger',
  QUARANTINE: 'danger',
  OFFLINE: 'danger',
  CANCELLED: 'neutral',
  CANCELLING: 'neutral',
  UNKNOWN: 'neutral',
  DRAFT: 'neutral',
}

const toneClass = computed(() => TONE_CLASS[STATUS_TONE[props.status] ?? 'neutral'])
const text = computed(() => props.label ?? props.status)
</script>

<template>
  <span
    class="inline-flex items-center rounded-md px-2 py-1 font-mono text-[10px] tracking-wide whitespace-nowrap"
    :class="toneClass"
  >
    {{ text }}
  </span>
</template>
