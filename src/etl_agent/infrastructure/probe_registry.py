from etl_agent.application.profiling import MetadataProbePort
from etl_agent.domain.enums import ConnectorType
from etl_agent.domain.errors import DomainError
from etl_agent.infrastructure.mysql_probe import MySqlMetadataProbe


class ProbeRegistry:
    """按 Connector 类型返回探查 Adapter；未接入的类型失败关闭，不做隐式降级。"""

    def __init__(self) -> None:
        """初始化实例依赖和运行参数；依赖显式保存以便替换、测试和生命周期管理。"""
        mysql = MySqlMetadataProbe()
        # 第 4 章主线只有 MySQL；Doris 复用 MySQL FE 协议因此共享同一实例。
        # PostgreSQL/Oracle/SQL Server/Hive/HDFS 的 Adapter 在各自章节接入后注册到这里。
        self._probes: dict[ConnectorType, MetadataProbePort] = {
            ConnectorType.MYSQL: mysql,
            ConnectorType.DORIS: mysql,
        }

    def get(self, connector_type: ConnectorType) -> MetadataProbePort:
        """在探查器注册表中读取单个资源并校验可见性；作为业务入口，
        统一维护该步骤的输入校验、状态约束和异常语义。
        """
        probe = self._probes.get(connector_type)
        if probe is None:
            raise DomainError(f"{connector_type.value} 元数据探查器尚未接入")
        return probe
