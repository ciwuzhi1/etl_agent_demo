-- Checkpoint 与业务状态分库分账号，避免 Agent 恢复数据影响业务库治理。
CREATE USER etl_checkpoint WITH PASSWORD 'etl_checkpoint';
CREATE DATABASE etl_checkpoint OWNER etl_checkpoint;
