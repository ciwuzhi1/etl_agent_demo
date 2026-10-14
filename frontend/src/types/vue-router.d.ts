import 'vue-router'

declare module 'vue-router' {
  interface RouteMeta {
    /** 顶栏标题 */
    title: string
    /** 顶栏眉标（英文小标题） */
    section: string
  }
}
