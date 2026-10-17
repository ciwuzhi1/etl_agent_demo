-- =============================================================================
-- 在共享 PostgreSQL 实例上创建本仓库需要的两个库与两个账号。
--
-- 背景：本机开发环境已收敛为「只跑 ecommerce-operation-assistant 的
-- redis + postgres 两个容器」，本仓库不再自起 postgres/redis（见 infra/compose.yaml
-- 文件头）。共享实例的初始库是 agent_checkpoints（属于另一个项目），本文件在其之上
-- 追加本仓库自己的库，两边互不干扰。
--
-- 目标实例：127.0.0.1:5433，超级用户 agent。
-- 幂等：可重复执行。
-- 用法：见 README「运行 → 基础设施」一节。
-- =============================================================================

-- 业务真相库与 Checkpoint 库分库分账号，避免 Agent 恢复数据影响业务库治理。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_agent') THEN
        CREATE ROLE etl_agent LOGIN PASSWORD 'etl_agent';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_checkpoint') THEN
        CREATE ROLE etl_checkpoint LOGIN PASSWORD 'etl_checkpoint';
    END IF;
END
$$;

-- CREATE DATABASE 不能出现在事务块或 DO 块里，用 \gexec 先判断再执行。
SELECT format('CREATE DATABASE %I OWNER %I', 'etl_agent', 'etl_agent')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'etl_agent')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'etl_checkpoint', 'etl_checkpoint')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'etl_checkpoint')
\gexec
