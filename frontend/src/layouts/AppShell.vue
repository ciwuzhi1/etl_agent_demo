<script setup lang="ts">
import Sidebar from '@/components/shell/Sidebar.vue'
import Topbar from '@/components/shell/Topbar.vue'
</script>

<template>
  <div class="grid min-h-screen grid-cols-[68px_1fr] lg:grid-cols-[235px_1fr]">
    <Sidebar />
    <div class="flex min-w-0 flex-col">
      <Topbar />
      <main class="mx-auto w-full max-w-[1500px] flex-1 px-4 py-6 sm:px-6 lg:px-9 lg:py-9">
        <RouterView />
      </main>
    </div>
  </div>
</template>
