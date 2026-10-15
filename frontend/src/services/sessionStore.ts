/**
 * 会话上下文叶子模块：只保存 projectId / actorId 两个值，供 services 直接读取。
 *
 * 刻意不 import 任何 composable 或 service，避免 "composable ↔ service" 循环依赖；
 * `useSession` 负责在拿到后端 `/context` 后写入这里的值，并放行 `whenSessionReady`。
 */

let projectId: string | null = null
let actorId: string | null = null

let resolveReady: () => void = () => {}
const readyPromise = new Promise<void>((resolve) => {
  resolveReady = resolve
})
let ready = false

export function setProjectId(next: string | null): void {
  projectId = next
}

export function setActorId(next: string | null): void {
  actorId = next
}

export function getActorId(): string | null {
  return actorId
}

export function getProjectId(): string {
  if (!projectId) {
    throw new Error('工作区尚未就绪')
  }
  return projectId
}

/** 标记上下文加载完成（无论成功与否），放行所有等待中的请求。 */
export function markSessionReady(): void {
  if (!ready) {
    ready = true
    resolveReady()
  }
}

/** 等待会话上下文就绪；services 在真正发请求前先 await 它。 */
export function whenSessionReady(): Promise<void> {
  return readyPromise
}
