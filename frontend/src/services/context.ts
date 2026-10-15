import { apiRequest } from './http'

import type { WorkspaceContext } from '@/types/models'

/**
 * 本地演示会话上下文：拿到首个工作区与其成员，用于选择真实身份。
 * 仅在 dev-header 身份模式下由后端提供。
 */
export function getWorkspaceContext(): Promise<WorkspaceContext> {
  return apiRequest<WorkspaceContext>('/context')
}
