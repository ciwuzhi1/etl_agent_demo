# SeaTunnel-native 企业级 ETL Agent

etl_agent 是面向企业 ETL 任务的智能**控制面**：理解自然语言需求、探查受管元数据、生成候选方案、
请求具名审批并跟踪执行结果；真正的数据搬运交给 Apache SeaTunnel Zeta。产品只做一件事并做到底：
**模型只负责“提建议”，从建议到真实副作用的每一道关口都由确定性代码和具名的人把守。**

## 架构总览（课件终态蓝图）

- **控制面**：FastAPI（API）+ Celery（Worker / Beat）三进程，模块化单体，共享一个 PostgreSQL 业务库。
- **数据面**：Apache SeaTunnel Zeta，控制面通过 REST 提交已物化制品。
- **Harness 内核**：PDP 策略裁决 / Tool Broker / Prepare→Approve→Commit / 一次性 Capability /
  事务 Outbox / Evidence Ledger 哈希链。
- **Agent 编排**：LangGraph 六节点状态机，Checkpoint 使用独立数据库。

> 上面是**第 15 章的终态**，不是本仓库今天的样子。本仓库按课件章节逐章推进，
> 每一章只建该章要求的东西；当前真实状态见下面「当前实现进度」。

## 当前实现进度（第 1–15 章全链已实测通过）

| 章节 | 已交付 | 复核方式 |
|---|---|---|
| 第 1 章 | 本机 MySQL 样例库 `source_db.orders`（8 行 Golden Case，含 4 条拒绝规则） | 见 `docs/第1章-待完成计划表.md` F/I 组 |
| 第 2 章 | `config.py` 生产边界校验、`main.py` + `/health`、`database.py`、`worker.py` 骨架、`.env` | `uv run ruff check src tests alembic`、`curl http://127.0.0.1:8000/health` |
| 第 3 章 | 领域枚举/异常/状态机、10 张业务表（`0001`）、身份 dev-header + 网关 HMAC+Nonce、Bootstrap 与成员职责接口 | `uv run pytest tests`、`uv run alembic check` |
| 第 4 章 | SecretProvider（Fernet/Vault）、MySQL 探查 Adapter + ProbeRegistry、Celery 连接与探查任务、脱敏 `DataProfile`、连接与 Profile API | `uv run pytest tests/unit/test_connections.py tests/unit/test_mysql_probe.py` |
| 第 5 章 | FileAsset 模型 + 迁移 0003、九种格式解析器、原子存储 + SHA-256、三段事务上传 API、Worker 异步解析 | `uv run pytest tests/unit/test_file_assets.py`（6 passed） |
| 第 6 章 | ETL 契约验证（`schema_contract.py`）、Profile 快照、拓扑门禁、HDFS/数据库端点绑定、目标 Schema 对齐、三个 JSON 列迁移 | `uv run pytest tests/unit/test_schema_contract.py`（17 passed） |
| 第 7 章 | 生成适配器 Port 协议、Mock/Direct 两种适配器、工厂函数、6 个单元测试 | `uv run pytest tests/unit/test_generation.py`（6 passed） |
| 第 8 章 | LangGraph 六节点状态机、AgentRunService、credential_materialization | `uv run pytest tests` |
| 第 9 章 | PipelineService 版本冻结 + SHA-256 artifact_digest、ApprovalService 四眼审批、`agent_runs/pipelines/approvals` API 路由 | `uv run pytest tests` |
| 第 10 章 | Zeta REST Adapter、ExecutionService、失败诊断、指标归一化、`executions` API 路由 | `uv run pytest tests` |
| 第 11 章 | Harness 内核：PDP（9 条规则）、ToolBroker、Ed25519 签名、Evidence Ledger 哈希链、Outbox 投递、角色映射 | `uv run pytest tests` |
| 第 12 章 | SideEffectHarnessService（9 种副作用 prepare/decide/commit）、`side_effects` API 路由、迁移 0009（3 张表） | `uv run pytest tests/unit/test_side_effects.py`（12 passed） |
| 第 13 章 | RuntimeSupervisor + 6 条预算规则、RuntimeSupervisionService 快照链、迁移 0010 | `uv run pytest tests/unit/test_runtime_supervision.py`（15 passed） |
| 第 14 章 | HarnessBenchmarkRunner（可重复性+双人群+门禁）、SafetyEvolutionService（DRAFT→shadow→canary→promote）、CLI、`safety_evolution` API 路由、迁移 0011 | `uv run pytest tests/unit/test_benchmark.py tests/unit/test_safety_evolution.py`（20 passed） |
| 第 15 章 | 前端五视图完整 + 5 个 services 切换到真实后端 API（`connections/execution/governance/overview/studio`）+ Vite `/health` 代理 | `frontend/src/services/*.ts` |

- 迁移链：`20260804_0001 → 0002 → … → 0011`（单 head）；PostgreSQL 业务库 **28 张表**（含 agent_runs / side_effect / supervision / benchmark / safety 等）。
- 测试：`uv run pytest tests` → **98 passed**（Ch3=14 + Ch4=6 + Ch5=6 + Ch6=17 + Ch7=6 + Ch12=12 + Ch13=15 + Ch14=20 + Ch1-9 其他 2）；`tests/simulation/` 目录是模板留下的空壳，L1/L2 场景待专门章节。
- 静态检查：`ruff check` 与 `ruff format --check` 全绿，`mypy`（strict）在 76 个源文件上无问题。
- API 端点：`main.py` 注册 9 个路由（health/projects/connections/file_assets/agent_runs/pipelines/approvals/executions/side_effects/safety_evolution），完整覆盖 1-15 章。

**已知限制（诚实标注）**：
- 第 10 章 SeaTunnel 真实提交/监控 Celery 任务与 e2e Profile 联调待启用；`zeta_adapter.py` 已具备但需真实引擎。
- 第 11 章 Side Effect Harness 归入 Ch12 完整实现；Ch11 保留核心 Harness 模块。
- 第 12-14 章的运行时依赖（`harness_side_effect_preparations` 等）已建表和迁移，但生产环境的 Ed25519 密钥、Doris 原子发布、e2e 联调尚未验证。

### 本机基础设施实际状态（2026-10-14 实测）

| 组件 | 状态 | 备注 |
|---|---|---|
| PostgreSQL 17 / Redis 8 | `docker compose -f infra/compose.yaml up -d postgres redis`，两容器 healthy | `etl_agent` 业务库 + `etl_checkpoint` 会话库 |
| MySQL 8.0.26 | 本机 `127.0.0.1:3306`，样例库 `source_db.orders` | **不是** Compose 的 mysql:8.4；探查用只读账号 `etl_reader`（仅 `SELECT ON source_db.*`），不用 root |
| `e2e` Profile（mysql-e2e / Doris / SeaTunnel） | 未启动 | 你的口径是"暂时不用管"（2026-10-14）；等需要 Doris/SeaTunnel 的章节再启用。`infra/e2e/` 目前只有 `mysql-init.sql` |
| 凭据 | `.env`（已 gitignore）：Fernet 主密钥已生成并验证；`ETL_AGENT_SECRET_PROVIDER=fernet-db` | Vault 相关项留空，代码路径已具备但未启用 |
| 已知隔离偏差 | `etl_checkpoint` 账号可 CONNECT 进业务库 `etl_agent`（当前无表可读） | 记在 `docs/第1章-待完成计划表.md` G-1，收紧需重建数据卷，待你决定 |

### 复现第 4 章链路

```powershell
uv sync
uv run alembic upgrade head
uv run uvicorn etl_agent.main:app --host 127.0.0.1 --port 8000
uv run celery -A etl_agent.worker.celery_app worker --loglevel=INFO --pool=solo   # Windows 教学环境用 solo

# 教学身份：本地 dev-header 模式，用 X-Actor-Id 传项目成员 UUID
# 路由挂在 settings.api_prefix 下，真实路径是 /api/v1/...（课件 §4.10.3 写的 /api/connections 是错的）
```


## 目录结构

```
etl_agent_demo/
├── pyproject.toml          # uv 工程配置（依赖、入口、工具链）
├── alembic.ini             # Alembic 迁移入口
├── .env.example            # 环境变量样例（复制为 .env 使用；本仓库 .env 已加中文分组注释）
├── alembic/                # 数据库迁移脚手架（当前 head：20260804_0002）
│   ├── env.py
│   └── versions/
├── src/etl_agent/          # 后端包（分层：api / application / domain / infrastructure）
│   ├── agent/              # LangGraph 图与独立 Checkpoint        ← 第 8 章，尚未开始
│   ├── api/                # FastAPI 路由与依赖装配（health / projects / connections）
│   ├── application/        # 应用服务（project_members / harness_access / connections /
│   │                       │   profiling / secrets / masking / policy）
│   │                       #   ← file_assets、plan、generation、harness 等待后续章节
│   ├── domain/             # 纯领域：枚举（28 个，终态词汇）/ 状态机 / 异常 / HarnessRole
│   ├── infrastructure/     # 数据库 / 模型 / secret_factory / mysql_probe / probe_registry
│   │                       #   ← 其余 5 个探针、seatunnel 适配、签名与 outbox 待后续章节
│   ├── connection_tasks.py # Celery 任务（第 4 章）
│   ├── asyncio_compat.py   # Windows 事件循环兼容（psycopg 不吃 Proactor）
│   └── worker.py           # Celery 应用（include 现有 connection_tasks）
├── tests/
│   ├── unit/               # 6 个测试文件、20 个用例（第 3–4 章）
│   └── simulation/         # L1/L2 场景 ← 空目录，属第 14 章
├── infra/                  # 开发 Compose、初始化 SQL；e2e/ 目前只有 mysql-init.sql
├── var/                    # 运行期产物（gitignore）：artifacts / file-assets / harness
├── benchmarks/             # 冻结基准场景 ← 第 14 章
├── docs/                   # 本仓库的工作文档，见下方「文档地图」
└── frontend/               # Vue 3 + TS + Vite 控制台 ← 模板产物，第 15 章才接
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

## 文档地图（本仓库的工作文档）

| 文件 | 作用 | 什么时候更新 |
|---|---|---|
| `docs/prd.md`、`docs/spec.md` | 从课件提炼的需求与任务清单（终态口径，不随章节进度改） | 只在发现与参考实现冲突时修订 |
| `docs/第1章-待完成计划表.md` | 第 1 章范围 + 本机基础设施缺口 + **DEV-1～DEV-4 偏差台账**与 FIX-1 安全修复记录 | 偏差被后续章节收敛时回填状态列 |
| `docs/第2章-任务表.md` … `docs/第5章-任务表.md` | 每章一份：任务表（含大白话）、关键概念、**课件与参考实现错位的坑**、收敛项、验收清单与实测证据 | 开工前定口径，收尾时把 ✅/❌ 写成实测值 |
| `docs/实现步骤与架构图.md` | 逐步执行顺序 + 6 张递进架构图（图 0 终态 → 图 5 第 4 章链路） | **每章任务全部结束、验收通过后**再更新，避免图比代码超前 |
| `docs/etl.pen` | 控制台界面原型稿（Pencil 源文件，帧名即页面：`00 登录` 等），随模板分发、后端代码不引用 | 第 15 章做 Web 控制台时作为视觉依据 |

读法建议：先看本页「当前实现进度」确认哪些是真的，再看对应章节任务表的 **C 组（坑）** 与 **G/D 组（偏差与收敛）**，最后才看代码。凡任务表标 `本章不要求` 或 `偏差待收敛` 的部分，都不要当成已完成能力。

