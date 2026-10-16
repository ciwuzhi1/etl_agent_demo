ARG SEATUNNEL_VERSION=2.3.13
FROM apache/seatunnel:${SEATUNNEL_VERSION}

ARG SEATUNNEL_VERSION
ARG MYSQL_CONNECTOR_VERSION=8.4.0
ARG POSTGRESQL_CONNECTOR_VERSION=42.7.5
ARG SQLSERVER_CONNECTOR_VERSION=12.8.1.jre11
ARG ORACLE_CONNECTOR_VERSION=23.6.0.24.10
ARG HIVE_JDBC_VERSION=3.1.3

COPY plugin_config /opt/seatunnel/config/plugin_config

# 官方镜像已带 Connector 插件，但 JDBC 驱动的版本和再分发条款由数据库厂商决定。
# 这里把 V1 已验证的驱动统一装进同一个 SeaTunnel 运行镜像；数据库服务本身仍保持隔离。
# 构建时强制校验所有承诺的 Connector，避免镜像可以启动却在提交任务时才报缺插件。
RUN test -s "/opt/seatunnel/connectors/connector-jdbc-${SEATUNNEL_VERSION}.jar" \
    && test -s "/opt/seatunnel/connectors/connector-doris-${SEATUNNEL_VERSION}.jar" \
    && test -s "/opt/seatunnel/connectors/connector-file-local-${SEATUNNEL_VERSION}.jar" \
    && test -s "/opt/seatunnel/connectors/connector-file-hadoop-${SEATUNNEL_VERSION}.jar" \
    && test -s "/opt/seatunnel/connectors/connector-hive-${SEATUNNEL_VERSION}.jar" \
    && unzip -l /opt/seatunnel/lib/seatunnel-transforms-v2.jar \
       | grep -q 'org/apache/seatunnel/transform/nlpmodel/llm/LLMTransformFactory.class' \
    && test -s "/opt/seatunnel/lib/seatunnel-hadoop3-3.1.4-uber.jar" \
    && test -s "/opt/seatunnel/lib/hive-exec-3.1.3.jar" \
    && rm -f /opt/seatunnel/lib/mysql-connector-java-*.jar \
       /opt/seatunnel/lib/mysql-connector-j-*.jar \
       /opt/seatunnel/lib/postgresql-*.jar \
       /opt/seatunnel/lib/mssql-jdbc-*.jar \
       /opt/seatunnel/lib/ojdbc*.jar \
       /opt/seatunnel/lib/hive-jdbc-*.jar \
    && wget -q -O "/opt/seatunnel/lib/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
       "https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
    && wget -q -O "/opt/seatunnel/lib/postgresql-${POSTGRESQL_CONNECTOR_VERSION}.jar" \
       "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${POSTGRESQL_CONNECTOR_VERSION}/postgresql-${POSTGRESQL_CONNECTOR_VERSION}.jar" \
    && wget -q -O "/opt/seatunnel/lib/mssql-jdbc-${SQLSERVER_CONNECTOR_VERSION}.jar" \
       "https://repo.maven.apache.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${SQLSERVER_CONNECTOR_VERSION}/mssql-jdbc-${SQLSERVER_CONNECTOR_VERSION}.jar" \
    && wget -q -O "/opt/seatunnel/lib/ojdbc11-${ORACLE_CONNECTOR_VERSION}.jar" \
       "https://repo.maven.apache.org/maven2/com/oracle/database/jdbc/ojdbc11/${ORACLE_CONNECTOR_VERSION}/ojdbc11-${ORACLE_CONNECTOR_VERSION}.jar" \
    && wget -q -O "/opt/seatunnel/lib/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar" \
       "https://repo.maven.apache.org/maven2/org/apache/hive/hive-jdbc/${HIVE_JDBC_VERSION}/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar" \
    && test -s "/opt/seatunnel/lib/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar" \
    && test -s "/opt/seatunnel/lib/postgresql-${POSTGRESQL_CONNECTOR_VERSION}.jar" \
    && test -s "/opt/seatunnel/lib/mssql-jdbc-${SQLSERVER_CONNECTOR_VERSION}.jar" \
    && test -s "/opt/seatunnel/lib/ojdbc11-${ORACLE_CONNECTOR_VERSION}.jar" \
    && test -s "/opt/seatunnel/lib/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar"
