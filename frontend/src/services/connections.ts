import { mockConnections, mockFiles, mockProfiles, mockSecrets } from '@/mocks/connections'

import { clone, mockDelay } from './mock'

import type { Connection, DataProfile, FileAsset, SecretLifecycle } from '@/types/models'

/** 数据连接域接口（当前返回 mock，签名即未来后端契约）。 */
export const connectionsService = {
  listConnections(): Promise<Connection[]> {
    return mockDelay(clone(mockConnections))
  },
  listProfiles(): Promise<DataProfile[]> {
    return mockDelay(clone(mockProfiles))
  },
  listFiles(): Promise<FileAsset[]> {
    return mockDelay(clone(mockFiles))
  },
  listSecrets(): Promise<SecretLifecycle[]> {
    return mockDelay(clone(mockSecrets))
  },
}
