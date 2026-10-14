<script setup lang="ts">
withDefaults(defineProps<{ title?: string; description?: string }>(), {
  title: '暂无数据',
  description: undefined,
})
</script>

<template>
  <div class="rounded-xl border border-dashed border-line px-6 py-10 text-center">
    <p class="text-sm font-medium text-ink">{{ title }}</p>
    <p v-if="description" class="mt-1 text-xs text-muted">{{ description }}</p>
    <div v-if="$slots.default" class="mt-4">
      <slot />
    </div>
  </div>
</template>
