<script setup lang="ts">
withDefaults(defineProps<{ rows?: number }>(), { rows: 3 })
</script>

<template>
  <div class="flex flex-col gap-3" aria-busy="true" aria-live="polite">
    <span class="sr-only">加载中</span>
    <div v-for="row in rows" :key="row" class="h-12 animate-pulse rounded-xl bg-ink/5" />
  </div>
</template>
