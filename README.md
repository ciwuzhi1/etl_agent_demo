# SeaTunnel-native 企业级 ETL Agent

etl_agent 是面向企业 ETL 任务的智能**控制面**：理解自然语言需求、探查受管元数据、生成候选方案、
请求具名审批并跟踪执行结果；真正的数据搬运交给 Apache SeaTunnel Zeta。产品只做一件事并做到底：
**模型只负责“提建议”，从建议到真实副作用的每一道关口都由确定性代码和具名的人把守。**

## 架构总览

- **控制面**：FastAPI（API）+ Celery（Worker / Beat）三进程，模块化单体，共享一个 PostgreSQL 业务库。
- **数据面**：Apache SeaTunnel Zeta，控制面通过 REST 提交已物化制品。
- **Harness 内核**：PDP 策略裁决 / Tool Broker / Prepare→Approve→Commit / 一次性 Capability /
  事务 Outbox / Evidence Ledger 哈希链。
- **Agent 编排**：LangGraph 六节点状态机，Checkpoint 使用独立数据库。

## 目录结构

```
etl_agent_demo/
├── pyproject.toml          # uv 工程配置（依赖、入口、工具链）
├── alembic.ini             # Alembic 迁移入口
├── .env.example            # 环境变量样例（复制为 .env 使用）
├── alembic/                # 数据库迁移脚手架
│   ├── env.py
│   └── versions/
├── src/etl_agent/          # 后端包（分层：api / application / domain / infrastructure）
│   ├── agent/              # LangGraph 图与独立 Checkpoint
│   ├── api/                # FastAPI 路由与依赖装配
│   ├── application/        # 应用服务（编排领域规则与端口）
│   ├── domain/             # 纯领域：枚举 / 状态机 / 领域模型
│   └── infrastructure/     # 数据库 / 探针 / 引擎适配 / 签名
├── tests/                  # unit + simulation（L0/L1/L2）
├── infra/                  # 开发 Compose、初始化 SQL、E2E 夹具
├── benchmarks/             # 冻结基准场景
└── frontend/               # Vue 3 + TS + Vite 控制台
```

## 环境准备（uv）

前置：Python 3.12、[uv](https://docs.astral.sh/uv/)。

```bash
# 1. 创建虚拟环境并安装依赖（生成 .venv 与 uv.lock）
uv sync

# 2.（可选）验证工具链
uv run ruff check .
uv run mypy
uv run pytest -q

# 3. 复制环境变量样例
cp .env.example .env
```

> 中国大陆网络下，可先为 uv 配置国内 PyPI 镜像：
> `export UV_DEFAULT_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple`
> 若 `uv sync` 卡在可选的 `seatunnel-cli` git 依赖，可暂时移除 `pyproject.toml` 中的
> `[project.optional-dependencies].seatunnel` 与 `[tool.uv.sources]` 后再同步。

## 数据库职责

| 数据库 | 角色 | 说明 |
|---|---|---|
| PostgreSQL 17 | 控制面业务库 | 项目 / 版本 / 审批 / 执行 / 证据等全部企业事实，应用唯一存储 |
| PostgreSQL（独立库） | LangGraph Checkpoint | 图状态与中断恢复，与业务库分库分账号 |
| Redis 8 | 消息与缓存 | Celery broker/backend + 网关 Nonce 一次性消费 |
| MySQL 8.4 | 受管数据源（Source） | 被探查 / 被同步的数据，**不是**应用存储 |
| Apache Doris | 受管数据汇（Sink） | 目标库；影子表 + `REPLACE ... swap` 原子发布、回滚 |

详见 `docs/prd.md` 与 `docs/spec.md`。
