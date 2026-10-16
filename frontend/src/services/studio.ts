import { ApiError, apiRequest } from './http'
import { getProjectId, whenSessionReady } from './sessionStore'

import type {
  AgentRun,
  AgentStep,
  ExecutionPreparation,
  HarnessRole,
  PipelineDesign,
  PipelineVersion,
} from '@/types/models'

/**
 * Agent 生成与冻结域接口。第 15 章：切换到真实后端 API 调用。
 * 后端路由：/projects/{id}/pipeline-versions、/projects/{id}/agent-runs、
 * /projects/{id}/execution-preparations。
 *
 * 说明：后端目前不提供独立的“设计评审正文”，`designReview` 由 plan 缺失时收敛为空结构；
 * 冻结的 HOCON 制品通过 `/pipeline-versions/{id}/artifact` 单独读取，页面据此展示空态
 * 而不是伪造数据。
 */

interface PipelineVersionDTO {
  id: string
  pipeline_id: string
  version_number: number
  status: string
  requirement: string
  plan: Record<string, unknown>
  artifact_sha256: string | null
  lock_version: number
}

interface AgentRunDTO {
  id: string
  project_id: string
  status: string
  requirement: string
  candidate_hocon: string | null
  error_message: string | null
  started_at: string | null
  completed_at: string | null
  failed_at: string | null
  created_by: string
}

interface AgentStepDTO {
  step: number
  node: string
  status: string
}

interface PreparationDTO {
  id: string
  pipeline_version_id: string
  status: string
  risk_level: string
  input_fingerprint: string
  resource_fingerprint: string
  artifact_sha256: string
  required_roles: string[]
  impact_report: Record<string, unknown>
  rollback_plan: Record<string, unknown>
  expires_at: string
  created_at: string
}

interface ArtifactDTO {
  kind: string
  uri: string
  sha256: string
  hocon: string
}

interface CommitResultDTO {
  execution_run_id: string
  status: string
}

function toVersion(dto: PipelineVersionDTO): PipelineVersion {
  return {
    id: dto.id,
    pipelineId: dto.pipeline_id,
    versionNumber: dto.version_number,
    status: dto.status as PipelineVersion['status'],
    artifactSha256: dto.artifact_sha256 ?? '',
    createdAt: '',
    createdBy: '',
  }
}

function toDesign(dto: PipelineVersionDTO): PipelineDesign {
  return {
    version: toVersion(dto),
    hocon: '',
    designReview: {
      summary: dto.requirement,
      nodes: [],
      schemaDiffs: [],
      risks: [],
      samplePreview: { before: [], after: [] },
      previewSemantics: '后端尚未提供设计评审正文，此处仅展示版本事实。',
    },
  }
}

function toAgentRun(dto: AgentRunDTO): AgentRun {
  return {
    id: dto.id,
    pipelineVersionId: '',
    status: dto.status as AgentRun['status'],
    repairCount: 0,
    startedAt: dto.started_at ?? '',
    message: dto.error_message ?? dto.status,
  }
}

function toAgentStep(dto: AgentStepDTO): AgentStep {
  const status = dto.status === 'SUCCEEDED' ? 'DONE' : dto.status === 'RUNNING' ? 'ACTIVE' : 'PENDING'
  return {
    id: String(dto.step),
    kind: dto.node,
    title: dto.node,
    detail: '',
    status,
    at: '',
  }
}

function toPreparation(dto: PreparationDTO): ExecutionPreparation {
  return {
    id: dto.id,
    pipelineVersionId: dto.pipeline_version_id,
    pipelineVersionNumber: 0,
    status: dto.status as ExecutionPreparation['status'],
    riskLevel: dto.risk_level as ExecutionPreparation['riskLevel'],
    inputFingerprint: dto.input_fingerprint,
    resourceFingerprint: dto.resource_fingerprint,
    artifactSha256: dto.artifact_sha256,
    requiredRoles: dto.required_roles as HarnessRole[],
    impactReport: JSON.stringify(dto.impact_report),
    rollbackPlan: JSON.stringify(dto.rollback_plan),
    createdAt: dto.created_at,
    expiresAt: dto.expires_at,
  }
}

export const studioService = {
  /** 列出项目的 Pipeline 版本 */
  async listDesigns(projectId?: string): Promise<PipelineDesign[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<PipelineVersionDTO[]>(`/projects/${pid}/pipeline-versions`)
    return rows.map(toDesign)
  },

  /** 获取单个 Pipeline 版本详情；未指定 id 时取最新一个版本，无版本时返回 null。 */
  async getDesign(versionId?: string): Promise<PipelineDesign | null> {
    if (versionId) {
      return toDesign(await apiRequest<PipelineVersionDTO>(`/pipeline-versions/${versionId}`))
    }
    await whenSessionReady()
    const pid = getProjectId()
    const rows = await apiRequest<PipelineVersionDTO[]>(`/projects/${pid}/pipeline-versions`)
    return rows.length > 0 ? toDesign(rows[0]) : null
  },

  /** 创建一个 Pipeline 及其首个 DRAFT 版本（写入生成事实）。 */
  async createPipeline(name: string, requirement: string): Promise<PipelineDesign> {
    await whenSessionReady()
    const pid = getProjectId()
    const dto = await apiRequest<PipelineVersionDTO>(`/projects/${pid}/pipelines`, {
      method: 'POST',
      body: JSON.stringify({ name, requirement }),
    })
    return toDesign(dto)
  },

  /** 触发 AgentRun 生成 */
  async startAgentRun(
    projectId: string,
    requirement: string,
    profileIds: string[],
  ): Promise<AgentRun> {
    const dto = await apiRequest<AgentRunDTO>(`/projects/${projectId}/agent-runs`, {
      method: 'POST',
      body: JSON.stringify({ requirement, profile_ids: profileIds }),
    })
    return toAgentRun(dto)
  },

  /** 提交澄清回答：把 WAITING_INPUT 的 run 重新排队，图会带着追问与回答继续跑。 */
  async answerAgentRun(runId: string, answer: string): Promise<AgentRun> {
    const dto = await apiRequest<AgentRunDTO>(`/agent-runs/${runId}/answers`, {
      method: 'POST',
      body: JSON.stringify({ answer }),
    })
    return toAgentRun(dto)
  },

  /** 查询 AgentRun 状态；未指定 id 时取最近一次。 */
  async getAgentRun(runId?: string): Promise<AgentRun> {
    if (runId) {
      return toAgentRun(await apiRequest<AgentRunDTO>(`/agent-runs/${runId}`))
    }
    await whenSessionReady()
    const pid = getProjectId()
    const rows = await apiRequest<AgentRunDTO[]>(`/projects/${pid}/agent-runs`)
    if (rows.length === 0) {
      throw new Error('暂无 AgentRun')
    }
    return toAgentRun(rows[0])
  },

  /** 列出 AgentRun 的步骤；未指定 id 时取最近一次。 */
  async listAgentSteps(runId?: string): Promise<AgentStep[]> {
    let target = runId
    if (!target) {
      await whenSessionReady()
      const pid = getProjectId()
      const rows = await apiRequest<AgentRunDTO[]>(`/projects/${pid}/agent-runs`)
      if (rows.length === 0) {
        return []
      }
      target = rows[0].id
    }
    const steps = await apiRequest<AgentStepDTO[]>(`/agent-runs/${target}/steps`)
    return steps.map(toAgentStep)
  },

  /** 列出项目的执行准备单 */
  async listPreparations(projectId?: string): Promise<ExecutionPreparation[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<PreparationDTO[]>(`/projects/${pid}/execution-preparations`)
    return rows.map(toPreparation)
  },

  /** 读取冻结的 HOCON 制品；版本尚无制品时后端返回 404，这里收敛为 null（页面展示空态）。 */
  async getArtifact(versionId: string): Promise<{ hocon: string; sha256: string } | null> {
    try {
      const dto = await apiRequest<ArtifactDTO>(`/pipeline-versions/${versionId}/artifact`)
      return { hocon: dto.hocon, sha256: dto.sha256 }
    } catch (error) {
      if (error instanceof ApiError && error.status === 404) {
        return null
      }
      throw error
    }
  },

  /** 准备执行：冻结事实并由 PDP 裁决，产物可能是 READY 或 WAITING_APPROVAL 准备单。 */
  prepare(versionId: string, intent: unknown = null): Promise<ExecutionPreparation> {
    return apiRequest<PreparationDTO>(`/pipeline-versions/${versionId}/execution-preparations`, {
      method: 'POST',
      body: JSON.stringify({ intent }),
    }).then(toPreparation)
  },

  /** 审批决定：具名职责占用一个审批槽，reason 一并进审计。 */
  decidePreparation(
    preparationId: string,
    role: string,
    decision: string,
    reason = '',
  ): Promise<ExecutionPreparation> {
    return apiRequest<PreparationDTO>(`/execution-preparations/${preparationId}/decisions`, {
      method: 'POST',
      body: JSON.stringify({ role, decision, reason }),
    }).then(toPreparation)
  },

  /** 提交执行：签发一次性能力并投递 Outbox，返回新建的执行编号。 */
  async commitPreparation(
    preparationId: string,
  ): Promise<{ executionRunId: string; status: string }> {
    const dto = await apiRequest<CommitResultDTO>(
      `/execution-preparations/${preparationId}/commit`,
      { method: 'POST' },
    )
    return { executionRunId: dto.execution_run_id, status: dto.status }
  },
}
