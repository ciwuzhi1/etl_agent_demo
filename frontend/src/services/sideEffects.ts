import { apiRequest } from './http'
import { getProjectId, whenSessionReady } from './sessionStore'

import type { SideEffectPreparation } from '@/types/models'

/**
 * 统一副作用治理域接口（第 12 章）。
 *
 * 9 类副作用（取消执行、Doris 回滚/清理、Secret 迁移/轮换/销毁等）共用同一条通道：
 * **发起只创建 Preparation，不产生任何真实副作用**；占满全部职责审批槽后才签发一次性
 * 能力令牌，并由 Worker 完成外部执行。所有端点都在 api/routes/side_effects.py。
 */

interface SideEffectPreparationDTO {
  id: string
  project_id: string
  operation: string
  aggregate_id: string
  status: string
  execution_status: string
  risk_level: string
  facts_sha256: string
  required_roles: string[]
  required_approvals: number
  requested_by: string
  expires_at: string
  committed_at: string | null
}

interface SideEffectCommitDTO {
  execution_id: string
  preparation_id: string
  token_id: string
  command_payload_sha256: string
}

function toPreparation(dto: SideEffectPreparationDTO): SideEffectPreparation {
  return {
    id: dto.id,
    operation: dto.operation as SideEffectPreparation['operation'],
    aggregateId: dto.aggregate_id,
    status: dto.status as SideEffectPreparation['status'],
    executionStatus: dto.execution_status as SideEffectPreparation['executionStatus'],
    riskLevel: dto.risk_level as SideEffectPreparation['riskLevel'],
    factsSha256: dto.facts_sha256,
    requiredRoles: dto.required_roles as SideEffectPreparation['requiredRoles'],
    requiredApprovals: dto.required_approvals,
    requestedBy: dto.requested_by,
    expiresAt: dto.expires_at,
    committedAt: dto.committed_at,
  }
}

export const sideEffectService = {
  /** 列出项目的副作用 Preparation（新到旧）。 */
  async list(projectId?: string): Promise<SideEffectPreparation[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<SideEffectPreparationDTO[]>(
      `/projects/${pid}/side-effect-preparations`,
    )
    return rows.map(toPreparation)
  },

  /** 获取单个 Preparation 详情。 */
  async get(preparationId: string): Promise<SideEffectPreparation> {
    return toPreparation(
      await apiRequest<SideEffectPreparationDTO>(
        `/side-effect-preparations/${preparationId}`,
      ),
    )
  },

  /**
   * 占用一个具名职责审批槽。role 必须是该 Preparation 要求的职责之一，
   * 且申请人不能自批（后端双重校验）；理由必填并进审计。
   */
  async decide(
    preparationId: string,
    role: string,
    decision: 'APPROVE' | 'REJECT',
    reason: string,
  ): Promise<SideEffectPreparation> {
    return toPreparation(
      await apiRequest<SideEffectPreparationDTO>(
        `/side-effect-preparations/${preparationId}/decisions`,
        { method: 'POST', body: JSON.stringify({ role, decision, reason }) },
      ),
    )
  },

  /** 提交：签发一次性能力令牌并把命令投递到 Outbox，由 Worker 执行外部副作用。 */
  async commit(
    preparationId: string,
  ): Promise<{ executionId: string; tokenId: string }> {
    const dto = await apiRequest<SideEffectCommitDTO>(
      `/side-effect-preparations/${preparationId}/commit`,
      { method: 'POST' },
    )
    return { executionId: dto.execution_id, tokenId: dto.token_id }
  },

  /**
   * 发起一次副作用 Preparation（当前控制台只用到 Doris 回滚）。
   * 只创建待审批的准备单，不产生副作用。
   */
  async requestRollback(
    runId: string,
    context: Record<string, unknown> = {},
  ): Promise<SideEffectPreparation> {
    await whenSessionReady()
    const pid = getProjectId()
    return toPreparation(
      await apiRequest<SideEffectPreparationDTO>(
        `/projects/${pid}/side-effect-preparations`,
        {
          method: 'POST',
          body: JSON.stringify({
            operation: 'ROLLBACK_DORIS',
            aggregate_id: runId,
            context,
          }),
        },
      ),
    )
  },
}
