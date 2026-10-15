import { getActorId } from './sessionStore'

/**
 * HTTP 封装：指向 /api/v1，统一注入本地演示身份头 X-Actor-Id。
 *
 * 生产环境身份来自可信网关（HMAC + Nonce），本地演示用 dev-header 模式直接声明调用者；
 * Actor 由 `sessionStore` 提供，/context 首次调用时它还是 null，因此不带该头。
 */
export class ApiError extends Error {
  readonly status: number
  readonly detail: unknown

  constructor(message: string, status: number, detail?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
  }
}

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api/v1'

/** 允许调用方把某个头显式置为 undefined 来“删除”它（multipart 上传靠这个去掉 JSON 头）。 */
type HeaderBag = Record<string, string | undefined>

export async function apiRequest<T>(path: string, init: RequestInit = {}): Promise<T> {
  const actorId = getActorId()
  const headers: HeaderBag = {
    ...((init.headers as HeaderBag | undefined) ?? {}),
  }
  // 先抹掉调用方显式置空的头（如 uploadFile 传的 Content-Type: undefined），
  // 否则 fetch 会把它序列化成字面量 "undefined"，multipart 边界就会丢失。
  for (const key of Object.keys(headers)) {
    if (headers[key] === undefined) {
      delete headers[key]
    }
  }
  // 只有调用方没指定、且不是 FormData 时才补默认 JSON 头：
  // FormData 的 Content-Type 必须由浏览器生成，才能带上正确的 multipart 边界。
  const isMultipart = typeof FormData !== 'undefined' && init.body instanceof FormData
  if (!('Content-Type' in headers) && !isMultipart) {
    headers['Content-Type'] = 'application/json'
  }
  if (actorId) {
    headers['X-Actor-Id'] = actorId
  }
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: headers as Record<string, string>,
  })

  if (!response.ok) {
    let detail: unknown
    try {
      detail = await response.json()
    } catch {
      detail = await response.text()
    }
    throw new ApiError(`请求失败（HTTP ${response.status}）`, response.status, detail)
  }

  if (response.status === 204) {
    return undefined as T
  }
  return (await response.json()) as T
}

/**
 * 把后端错误整理成中文可读提示。
 *
 * FastAPI 的 detail 可能是字符串，也可能是 `{detail: "..."}` 或
 * `[{loc, msg, ...}]` 校验错误数组；这里统一抽取第一条可用信息，绝不吞错。
 */
function firstMessage(value: unknown): string | null {
  if (typeof value === 'string') {
    return value.trim() ? value : null
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const message = firstMessage(item)
      if (message) {
        return message
      }
    }
    return null
  }
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>
    for (const key of ['msg', 'detail', 'message', 'error']) {
      const message = firstMessage(record[key])
      if (message) {
        return message
      }
    }
  }
  return null
}

export function describeApiError(error: unknown): string {
  if (error instanceof ApiError) {
    return firstMessage(error.detail) ?? error.message
  }
  if (error instanceof Error) {
    return error.message
  }
  return '请求失败，请稍后重试'
}
