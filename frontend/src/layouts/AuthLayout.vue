<script setup lang="ts">
import { ShieldCheck } from 'lucide-vue-next'

const trustItems = ['四眼审批', '一次性授权', '证据链留痕'] as const
</script>

<template>
  <div class="grid min-h-screen grid-cols-1 bg-sidebar lg:grid-cols-[1.05fr_0.95fr]">
    <section class="relative hidden flex-col justify-center overflow-hidden px-16 py-16 lg:flex lg:px-20">
      <div
        class="pointer-events-none absolute -right-32 -bottom-40 h-[520px] w-[520px] rounded-full border border-white/10"
        aria-hidden="true"
      />
      <div class="z-[1] relative mb-14 flex items-center gap-3">
        <span class="grid h-11 w-11 place-items-center rounded-xl bg-accent text-lg font-extrabold text-sidebar">
          FP
        </span>
        <div class="flex flex-col">
          <strong class="text-base font-semibold text-white">ETL Agent</strong>
          <small class="font-mono text-[10px] tracking-[1.5px] text-sidebar-muted">CONTROL PLANE</small>
        </div>
      </div>

      <p class="z-[1] relative font-mono text-[11px] tracking-[1.7px] text-accent">
        SEATUNNEL-NATIVE GOVERNANCE
      </p>
      <h1 class="z-[1] relative mt-4 max-w-xl text-4xl leading-tight font-bold text-white xl:text-5xl">
        把 ETL 从“能跑”<br />升级为<span class="text-accent">可信生产流程</span>
      </h1>
      <p class="z-[1] relative mt-6 max-w-lg text-base leading-relaxed text-sidebar-muted">
        从元数据理解到审批执行，每一步都有确定性边界：模型只负责提建议，副作用由确定性代码和具名的人把守。
      </p>
      <ul class="z-[1] relative mt-10 flex flex-wrap gap-6 text-[11px] font-medium text-sidebar-muted">
        <li v-for="item in trustItems" :key="item" class="flex items-center gap-2">
          <ShieldCheck :size="14" class="text-accent" aria-hidden="true" />{{ item }}
        </li>
      </ul>
    </section>

    <section class="flex items-center justify-center px-5 py-10 lg:px-10">
      <div class="w-full max-w-md rounded-2xl bg-surface p-8 shadow-2xl sm:p-10">
        <RouterView />
      </div>
    </section>
  </div>
</template>
