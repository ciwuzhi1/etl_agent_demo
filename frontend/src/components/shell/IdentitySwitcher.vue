<script setup lang="ts">
import { NSelect } from 'naive-ui'
import { computed } from 'vue'

import { useSession } from '@/composables/useSession'

const session = useSession()

const options = computed(() =>
  session.actors.value.map((actor) => ({ label: `${actor.name} · ${actor.title}`, value: actor.id })),
)

const value = computed(() => session.currentActor.value?.id ?? null)

function handleUpdate(next: string | null): void {
  if (next !== null) {
    session.switchActor(next)
  }
}
</script>

<template>
  <label class="flex items-center">
    <span class="sr-only">切换当前身份</span>
    <NSelect
      :value="value"
      :options="options"
      size="small"
      placeholder="选择身份"
      class="w-[180px] sm:w-[200px]"
      @update:value="handleUpdate"
    />
  </label>
</template>
