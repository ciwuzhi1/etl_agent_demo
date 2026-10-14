<script setup lang="ts">
defineProps<{ label: string; hint?: string; error?: string; required?: boolean }>()
</script>

<template>
  <label class="flex flex-col gap-1.5">
    <span class="text-xs font-semibold text-ink">
      {{ label }}<span v-if="required" class="text-danger"> *</span>
    </span>
    <slot />
    <span v-if="error" class="text-[11px] text-danger">{{ error }}</span>
    <span v-else-if="hint" class="text-[11px] text-muted">{{ hint }}</span>
  </label>
</template>
