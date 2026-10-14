/**
 * 设计 token 的单一来源，取自原型 docs/etl.pen 的 variables。
 * TS 逻辑引用这里；Tailwind 与 Naive UI 主题从同一组值派生，避免多处硬编码。
 */
export const tokens = {
  bg: '#f1f3f4',
  surface: '#ffffff',
  sidebar: '#0a2427',
  sidebarMuted: '#7e9b9c',
  sidebarActive: '#123a3d',
  accent: '#0e9488',
  accentHover: '#0fa79a',
  accentPressed: '#0b7f75',
  accentSoft: '#e7f4f1',
  text: '#101a1c',
  muted: '#66757a',
  mutedLight: '#8a969b',
  border: '#e3e7e8',
  ok: '#15803d',
  warn: '#b45309',
  danger: '#b91c1c',
  fontBody: '"Inter", "Segoe UI", "PingFang SC", "Microsoft YaHei", system-ui, sans-serif',
} as const

export type DesignTokens = typeof tokens
