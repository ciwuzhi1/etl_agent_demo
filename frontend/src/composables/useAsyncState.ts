import { ref, shallowRef, type Ref, type ShallowRef } from 'vue'

export interface AsyncState<T> {
  data: ShallowRef<T | null>
  error: Ref<string | null>
  loading: Ref<boolean>
  run: () => Promise<void>
}

/**
 * 统一的异步加载状态机，供各页面复用 loading / error / empty 四态。
 * 加载失败时把错误收敛为可展示的字符串，并清空数据。
 */
export function useAsyncState<T>(loader: () => Promise<T>): AsyncState<T> {
  const data = shallowRef<T | null>(null)
  const error = ref<string | null>(null)
  const loading = ref(false)

  async function run(): Promise<void> {
    loading.value = true
    error.value = null
    try {
      data.value = await loader()
    } catch (caught) {
      error.value = caught instanceof Error ? caught.message : '未知错误'
      data.value = null
    } finally {
      loading.value = false
    }
  }

  return { data, error, loading, run }
}
