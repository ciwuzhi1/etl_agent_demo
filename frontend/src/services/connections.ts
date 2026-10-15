import { apiRequest } from './http'
import { getProjectId, whenSessionReady } from './sessionStore'

import type {
  Connection,
  ConnectorType,
  DataProfile,
  FileAsset,
  FileAssetStatus,
  ReadinessStatus,
  SecretLifecycle,
} from '@/types/models'

/**
 * 数据连接域接口。第 15 章：切换到真实后端 API 调用。
 * 后端路由：/api/v1/projects/{id}/connections、/data-profiles、/file-assets、/secret-lifecycles。
 *
 * 后端返回 snake_case，前端模型用 camelCase，因此每个方法都做一次 DTO → view model 映射，
 * 把列名差异收敛在这一层，组件不必知道后端字段名。
 */

interface ConnectionDTO {
  id: string
  name: string
  connector_type: string
  host: string | null
  port: number | null
  database_name: string | null
  status: string
  last_tested_at: string | null
}

interface ProfileDTO {
  id: string
  connection_id: string
  table_name: string
  endpoint_role: string
  schema_json: { columns?: unknown[] } | null
  sample_size: number
  expires_at: string | null
  status: string
}

interface FileDTO {
  id: string
  original_filename: string
  format: string
  byte_size: number | null
  row_count: number | null
  status: string
}

interface SecretDTO {
  id: string
  connection_id: string
  status: string
  issued_at: string
  expires_at: string
  days_to_expiry: number
}

function toConnectionStatus(raw: string): ReadinessStatus {
  return raw === 'READY' || raw === 'TESTING' || raw === 'FAILED' ? raw : 'UNKNOWN'
}

function toProfileStatus(raw: string): ReadinessStatus {
  if (raw === 'READY') return 'READY'
  if (raw === 'FAILED') return 'FAILED'
  return 'TESTING'
}

function toFileStatus(raw: string): FileAssetStatus {
  if (raw === 'READY') return 'PARSED'
  if (raw === 'FAILED') return 'FAILED'
  return 'PARSING'
}

function toConnection(dto: ConnectionDTO): Connection {
  return {
    id: dto.id,
    name: dto.name,
    connectorType: dto.connector_type as ConnectorType,
    host: dto.host ?? '',
    port: dto.port ?? 0,
    database: dto.database_name ?? '',
    direction: 'SOURCE',
    status: toConnectionStatus(dto.status),
    lastTestedAt: dto.last_tested_at,
  }
}

function toProfile(dto: ProfileDTO): DataProfile {
  return {
    id: dto.id,
    connectionId: dto.connection_id,
    table: dto.table_name,
    direction: dto.endpoint_role === 'SINK' ? 'SINK' : 'SOURCE',
    sampleRows: dto.sample_size,
    columnCount: dto.schema_json?.columns?.length ?? 0,
    expiresAt: dto.expires_at ?? '',
    status: toProfileStatus(dto.status),
  }
}

function toFile(dto: FileDTO): FileAsset {
  return {
    id: dto.id,
    name: dto.original_filename,
    format: dto.format as FileAsset['format'],
    rows: dto.row_count ?? 0,
    sizeBytes: dto.byte_size ?? 0,
    status: toFileStatus(dto.status),
    uploadedAt: '',
  }
}

function toSecret(dto: SecretDTO): SecretLifecycle {
  return {
    id: dto.id,
    connectionId: dto.connection_id,
    status: dto.status === 'ACTIVE' || dto.status === 'PENDING' ? dto.status : 'PENDING',
    issuedAt: dto.issued_at,
    expiresAt: dto.expires_at,
    daysToExpiry: dto.days_to_expiry,
  }
}

export const connectionsService = {
  /** 列出项目的连接 */
  async listConnections(projectId?: string): Promise<Connection[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<ConnectionDTO[]>(`/projects/${pid}/connections`)
    return rows.map(toConnection)
  },

  /** 创建连接：把前端字段映射为后端契约（database_name + credentials）。 */
  async createConnection(
    projectId: string,
    payload: {
      name: string
      connector_type: string
      host: string
      port: number
      database: string
      username: string
      password: string
    },
  ): Promise<Connection> {
    const dto = await apiRequest<ConnectionDTO>('/connections', {
      method: 'POST',
      body: JSON.stringify({
        project_id: projectId,
        name: payload.name,
        connector_type: payload.connector_type,
        host: payload.host,
        port: payload.port,
        database_name: payload.database,
        credentials: { username: payload.username, password: payload.password },
      }),
    })
    return toConnection(dto)
  },

  /** 测试连接 */
  async testConnection(connectionId: string): Promise<Connection> {
    const dto = await apiRequest<ConnectionDTO>(`/connections/${connectionId}/tests`, {
      method: 'POST',
    })
    return toConnection(dto)
  },

  /** 触发元数据探查 */
  async createProfile(
    connectionId: string,
    payload: { schema: string; table: string; sample_limit: number; role: string },
  ): Promise<DataProfile> {
    const dto = await apiRequest<ProfileDTO>(`/connections/${connectionId}/profiles`, {
      method: 'POST',
      body: JSON.stringify({
        schema_name: payload.schema,
        table_name: payload.table,
        sample_limit: payload.sample_limit,
        endpoint_role: payload.role,
      }),
    })
    return toProfile(dto)
  },

  /** 获取 Profile 详情 */
  async getProfile(profileId: string): Promise<DataProfile> {
    return toProfile(await apiRequest<ProfileDTO>(`/data-profiles/${profileId}`))
  },

  /** 列出项目下所有连接产生的 Profile 快照 */
  async listProfiles(projectId?: string): Promise<DataProfile[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<ProfileDTO[]>(`/projects/${pid}/data-profiles`)
    return rows.map(toProfile)
  },

  /** 列出项目的受管文件 */
  async listFiles(projectId?: string): Promise<FileAsset[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<FileDTO[]>(`/projects/${pid}/file-assets`)
    return rows.map(toFile)
  },

  /** 上传文件资产 */
  async uploadFile(projectId: string, file: File): Promise<FileAsset> {
    const form = new FormData()
    form.append('file', file)
    const dto = await apiRequest<FileDTO>(`/projects/${projectId}/file-assets`, {
      method: 'POST',
      body: form,
      headers: { 'Content-Type': undefined as unknown as string },
    })
    return toFile(dto)
  },

  /** 列出密钥生命周期状态（凭据轮换提醒） */
  async listSecrets(projectId?: string): Promise<SecretLifecycle[]> {
    await whenSessionReady()
    const pid = projectId ?? getProjectId()
    const rows = await apiRequest<SecretDTO[]>(`/projects/${pid}/secret-lifecycles`)
    return rows.map(toSecret)
  },
}
