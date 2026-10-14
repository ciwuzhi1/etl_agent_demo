<script setup lang="ts">
defineProps<{ message: string }>()
defineEmits<{ retry: [] }>()
</script>

<template>
  <div class="rounded-xl border border-danger/30 bg-danger/5 px-6 py-6 text-center" role="alert">
    <p class="text-sm font-medium text-danger">{{ message }}</p>
    <button
      type="button"
      class="mt-3 rounded-lg border border-danger/40 px-3 py-1.5 text-xs font-medium text-danger transition-colors hover:bg-danger/10"
      @click="$emit('retry')"
    >
      重试
    </button>
  </div>
</template>
