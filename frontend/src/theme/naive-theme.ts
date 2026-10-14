import type { GlobalThemeOverrides } from 'naive-ui'

import { tokens } from './tokens'

/** 把设计 token 映射到 Naive UI 主题，保证组件库与原型配色一致。 */
export const naiveThemeOverrides: GlobalThemeOverrides = {
  common: {
    primaryColor: tokens.accent,
    primaryColorHover: tokens.accentHover,
    primaryColorPressed: tokens.accentPressed,
    primaryColorSuppl: tokens.accentHover,
    successColor: tokens.ok,
    warningColor: tokens.warn,
    errorColor: tokens.danger,
    infoColor: tokens.muted,
    bodyColor: tokens.bg,
    cardColor: tokens.surface,
    modalColor: tokens.surface,
    popoverColor: tokens.surface,
    borderColor: tokens.border,
    dividerColor: tokens.border,
    textColorBase: tokens.text,
    textColor1: tokens.text,
    textColor2: tokens.muted,
    textColor3: tokens.mutedLight,
    fontFamily: tokens.fontBody,
    borderRadius: '10px',
    borderRadiusSmall: '8px',
  },
}
