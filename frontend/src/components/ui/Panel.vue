<script setup lang="ts">
defineProps<{ title?: string; description?: string }>()
</script>

<template>
  <section class="rounded-xl border border-line bg-surface p-5 shadow-sm">
    <header
      v-if="title || description || $slots.actions"
      class="mb-4 flex items-start justify-between gap-4"
    >
      <div class="min-w-0">
        <h2 v-if="title" class="text-sm font-semibold text-ink">{{ title }}</h2>
        <p v-if="description" class="mt-1 text-xs leading-relaxed text-muted">{{ description }}</p>
      </div>
      <div v-if="$slots.actions" class="flex shrink-0 items-center gap-2">
        <slot name="actions" />
      </div>
    </header>
    <slot />
  </section>
</template>
