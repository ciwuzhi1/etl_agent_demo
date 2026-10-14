/** 把静态 mock 数据包装成带延迟的 Promise，模拟真实网络请求。 */
export function mockDelay<T>(payload: T, ms = 320): Promise<T> {
  return new Promise((resolve) => {
    window.setTimeout(() => resolve(payload), ms)
  })
}

/** 返回一份深拷贝，避免调用方直接修改共享的 mock 源数据。 */
export function clone<T>(value: T): T {
  return structuredClone(value)
}
