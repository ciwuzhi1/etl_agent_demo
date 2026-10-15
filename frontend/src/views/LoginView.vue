<script setup lang="ts">
import { NButton, NInput, NSelect } from 'naive-ui'
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'

import FormField from '@/components/ui/FormField.vue'
import { useSession } from '@/composables/useSession'

const router = useRouter()
const session = useSession()

const email = ref('engineer@atguigu.com')
const password = ref('')
const actorId = ref<string>(session.actors.value[0]?.id ?? '')
const submitting = ref(false)
const error = ref<string | null>(null)

const actorOptions = computed(() =>
  session.actors.value.map((actor) => ({ label: `${actor.name} · ${actor.title}`, value: actor.id })),
)

async function handleSubmit(): Promise<void> {
  error.value = null
  if (!email.value.trim()) {
    error.value = '请输入工作邮箱'
    return
  }
  if (!password.value.trim()) {
    error.value = '演示环境请输入任意密码'
    return
  }

  submitting.value = true
  try {
    session.signIn(actorId.value)
    await router.push({ name: 'overview' })
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <form class="flex flex-col gap-5" novalidate @submit.prevent="handleSubmit">
    <div>
      <p class="font-mono text-[11px] tracking-[1.7px] text-accent">SIGN IN</p>
      <h1 class="mt-2 text-2xl font-bold text-ink">登录控制台</h1>
      <p class="mt-2 text-sm leading-relaxed text-muted">
        使用企业身份进入 etl_agent 控制面。演示环境可从下方选择自然人身份。
      </p>
    </div>

    <FormField label="工作邮箱" required>
      <NInput v-model:value="email" placeholder="you@company.com" />
    </FormField>

    <FormField label="密码" required hint="演示环境不校验真实密码，请输入任意字符。">
      <NInput
        v-model:value="password"
        type="password"
        show-password-on="click"
        placeholder="········"
      />
    </FormField>

    <FormField label="演示身份" required>
      <NSelect v-model:value="actorId" :options="actorOptions" />
    </FormField>

    <p v-if="error" class="text-[11px] text-danger" role="alert">{{ error }}</p>

    <NButton type="primary" attr-type="submit" block :loading="submitting">登录</NButton>

    <p class="text-center text-[10px] leading-relaxed text-muted">
      演示数据不含真实凭据；生产环境身份必须来自可信网关（HMAC + Nonce）。
    </p>
  </form>
</template>
