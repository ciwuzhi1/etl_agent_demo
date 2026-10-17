# etl_agent

面向企业 ETL 任务的智能**控制面**。

`etl_agent` 理解自然语言需求、探查受管元数据、生成候选方案、请求具名审批，并跟踪执行结果；
真正的数据搬运交给 Apache SeaTunnel Zeta。产品只做一件事并做到底：

> 模型只负责「提建议」，从建议到真实副作用的每一道关口，都由确定性代码和具名的人把守。

## 功能

**受管数据接入**
- 数据连接登记与连通性测试；凭据经 SecretProvider（Fernet 加密入库 / Vault）保管，明文不落日志、不进响应
- 只读探查生成脱敏 `DataProfile`（样本返回前完成字段级脱敏，不进 LLM 提示词）
- 受管文件资产上传：临时文件 + 原子替换、SHA-256 去重、九种格式解析（CSV / JSON / JSONL / Excel / Parquet / ORC / PDF / 文本 / 定长）

**Agent 生成**
- 自然语言需求 → LangGraph 六节点状态机（需求解析 → 上下文装载 → 方案生成 → 契约校验 → 审批挂起 → 收尾）
- 生成后端可切换：`direct` 直连模型，或黑盒 CLI 适配器
- 澄清中断可恢复：`WAITING_INPUT` 状态下提交回答继续跑图，图状态存独立 Checkpoint 库

**契约与审批**
- ETL 契约校验：Schema 对齐、拓扑校验、Profile 快照比对
- Pipeline 版本冻结：制品 SHA-256 摘要，版本一经批准不可静默改写
- 四眼审批：申请人与审批人分离，同一自然人不得占用两个审批槽

**执行与观测**
- 经 REST 把已物化制品提交给 SeaTunnel Zeta；提交成功后经事务 Outbox 自续投递监控命令
- 执行指标归一化：从引擎 `/job-info` 取真实行数 / 字节数回写（源 8 行 → 汇 4 行这类差异可追溯）
- 失败诊断：确定性匹配失败类别与失败环节，落库只写脱敏文案
- 运行时监督：按记录数、字节数、超时、输出比例、拒绝率、最小质量样本量六条预算规则生成快照链

**Harness 内核**
- PDP 策略裁决、Tool Broker、Prepare→Approve→Commit、一次性 Capability、事务 Outbox、Ed25519 签名的 Evidence Ledger 哈希链
- 九类副作用的 prepare / decide / commit 全流程治理（含 Doris 影子表原子发布与回滚）

**安全进化与基准**
- 提案签名 → 影子验证 → 小流量 → 晋升，任一环节命中 P0/P1 回归或误报越线即自动回滚
- 可重复性基准：双人群跑分 + 门限判定

**Web 控制台**
- Vue 3 单页应用，五个视图：总览 / 数据连接 / Pipeline Studio / 运行中心 / 安全治理

## 技术栈

| 层 | 选型 |
|---|---|
| 语言 | Python 3.12 |
| Web 框架 | FastAPI + Uvicorn |
| 异步任务 | Celery（Worker + Beat），Redis 作 broker/backend |
| 数据库 | PostgreSQL 16（业务库 + 独立 Checkpoint 库），SQLAlchemy 2.x async，Alembic 迁移 |
| Agent 编排 | LangGraph + langgraph-checkpoint-postgres |
| 加密与签名 | cryptography（Fernet）、PyNaCl（Ed25519） |
| 可观测性 | OpenTelemetry（OTLP/HTTP，FastAPI / httpx / SQLAlchemy / Celery 自动埋点） |
| 数据面 | Apache SeaTunnel Zeta 2.3.13（REST 提交） |
| 受管数据源 / 汇 | MySQL 8.x、Apache Doris 3.x |
| 前端 | Vue 3 + TypeScript + Vite + naive-ui + Tailwind CSS 4 |
| 工具链 | uv（依赖管理）、ruff（lint + format）、mypy（strict）、pytest |

## 目录结构

```
etl_agent_demo/
├── src/etl_agent/
│   ├── api/                  # FastAPI 路由与 Pydantic 请求/响应模型
│   ├── application/          # 应用服务：连接、探查、文件资产、生成、审批、执行、副作用、监督、基准
│   ├── domain/               # 纯领域：枚举、状态机、异常、Harness 角色与策略
│   ├── infrastructure/       # SQLAlchemy 模型、SecretProvider、数据库探针、SeaTunnel 适配器、文件存储
│   ├── agent/                # LangGraph 图与 Checkpoint
│   ├── config.py             # 带生产边界校验的设置
│   ├── main.py               # FastAPI 应用装配
│   ├── worker.py             # Celery 应用与调度
│   └── *_tasks.py            # 各领域的 Worker 侧任务
├── tests/                    # 单元测试
├── alembic/versions/         # 数据库迁移（单 head）
├── frontend/                 # Vue 3 控制台
├── infra/                    # 开发用 Compose、SeaTunnel 镜像、初始化 SQL
├── scripts/e2e/              # 端到端驱动脚本
├── docs/                     # 架构、需求、设计与接口文档
└── var/                      # 运行期产物（不进版本库）
```

## 环境要求

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Node.js 20+（仅前端开发需要）
- Docker / Docker Compose（本地 PostgreSQL、Redis 复用共享容器，见「运行」；跑真实执行链路还需要 MySQL、Doris、SeaTunnel）

## 安装

```bash
git clone https://github.com/ciwuzhi1/etl_agent_demo.git
cd etl_agent_demo
uv sync
```

中国大陆网络下可先配置国内镜像：

```bash
export UV_DEFAULT_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple
```

前端依赖：

```bash
cd frontend && npm install
```

## 配置

复制 `.env.example` 为 `.env` 并按需修改。关键项：

| 变量 | 说明 |
|---|---|
| `ETL_AGENT_ENVIRONMENT` | 运行环境；生产边界校验依赖它 |
| `ETL_AGENT_DATABASE_URL` | 业务库连接串（`postgresql+asyncpg://…`） |
| `ETL_AGENT_CHECKPOINT_DATABASE_URL` | LangGraph Checkpoint 独立库 |
| `ETL_AGENT_REDIS_URL` | Celery broker / backend |
| `ETL_AGENT_SECRET_PROVIDER` | `fernet-db` 或 `vault` |
| `ETL_AGENT_SECRET_MASTER_KEY` | Fernet 主密钥（`fernet-db` 模式必填） |
| `ETL_AGENT_SEATUNNEL_API_URL` | SeaTunnel Zeta REST 地址 |
| `ETL_AGENT_GENERATION_BACKEND` | `direct` 或黑盒 CLI 适配器 |
| `ETL_AGENT_LLM_*` | 生成后端使用的模型配置 |
| `ETL_AGENT_HARNESS_PRIVATE_KEY` / `ETL_AGENT_HARNESS_PUBLIC_KEYS` | Evidence Ledger 签名密钥 |
| `ETL_AGENT_AUTH_MODE` | `dev-header`（本地）或可信网关 HMAC + Nonce |

`.env` 已被 `.gitignore` 排除，切勿提交。

本机开发下 `DATABASE_URL` / `CHECKPOINT_DATABASE_URL` 指向共享实例的 `5433`，
`REDIS_URL` 指向共享实例的 `6379` **db 1**；换环境时只需改这三项。

## 运行

**基础设施**

本仓库**不再自起 PostgreSQL / Redis**，直接复用同机 `ecommerce-operation-assistant`
的两个常驻容器（Compose 文件在
`D:\workspace\Commerce-Operations-Assistant\Commerce-Operations-Assistant\infra\compose.yaml`）：

| 复用项 | 地址 | 说明 |
|---|---|---|
| PostgreSQL | `127.0.0.1:5433` | 超级用户 `agent`，初始库 `agent_checkpoints`（属于对方项目） |
| Redis | `127.0.0.1:6379` | 无密码；本仓库走 **db 1**，对方项目占 db 0 |

先确认这两个容器在跑：

```bash
docker compose -f "D:/workspace/Commerce-Operations-Assistant/Commerce-Operations-Assistant/infra/compose.yaml" up -d
```

再在共享实例上追加本仓库自己的两库两账号（幂等，可重复执行）：

```bash
docker exec -i ecommerce-operation-assistant-agent-checkpoints \
  psql -U agent -d postgres -v ON_ERROR_STOP=1 < infra/postgres-init.sql
```

`infra/compose.yaml` 现在只保留按需启用的 `e2e` Profile（MySQL / Doris / SeaTunnel），
默认 `up -d` 不会启动任何容器。

**数据库迁移**

```bash
uv run alembic upgrade head
uv run alembic check        # 应输出 No new upgrade operations detected.
```

**三个进程**（各开一个终端）

```bash
uv run uvicorn etl_agent.main:app --host 127.0.0.1 --port 8000
uv run celery -A etl_agent.worker:celery_app worker --loglevel=INFO --pool=solo
uv run celery -A etl_agent.worker:celery_app beat --loglevel=INFO
```

> Windows 下请使用 `--pool=solo`；Linux 下可省略。

**前端控制台**

```bash
cd frontend && npm run dev        # http://localhost:5173，已配置 /api 与 /health 代理
```

**真实执行链路**（MySQL → SeaTunnel → Doris）

```bash
docker compose -f infra/compose.yaml --profile e2e up -d
uv run python scripts/e2e/pipeline_run.py
```

> 只跑共享的 PostgreSQL + Redis 两个容器时，控制面全链路（建连接 → 生成 → 校验 →
> 审批 → Prepare → Commit → 落执行记录）照常走通，但最后一步提交到 SeaTunnel Zeta
> 会以 `FAILED` 收尾 —— 数据面容器没起，这是预期结果，不是配置错误。

**健康检查**

```bash
curl http://127.0.0.1:8000/health
```

## 测试

```bash
uv run pytest tests                 # 单元测试
uv run ruff check src tests alembic scripts
uv run ruff format --check src tests alembic scripts
uv run mypy                         # strict
```

端到端脚本见 [`scripts/README.md`](scripts/README.md)。

## API 文档

服务启动后：

- Swagger UI：<http://127.0.0.1:8000/docs>
- OpenAPI JSON：<http://127.0.0.1:8000/openapi.json>

接口挂在 `ETL_AGENT_API_PREFIX`（默认 `/api/v1`）下，共 60 条路径，覆盖：

| 前缀 | 领域 |
|---|---|
| `/health` | 健康检查 |
| `/api/v1/context`、`/api/v1/projects/*` | 工作区、成员、角色授权 |
| `/api/v1/connections/*`、`/api/v1/data-profiles/*` | 数据连接、探查 |
| `/api/v1/projects/{id}/file-assets`、`/api/v1/file-assets/*` | 受管文件资产 |
| `/api/v1/projects/{id}/agent-runs`、`/api/v1/agent-runs/*` | Agent 生成任务 |
| `/api/v1/projects/{id}/pipelines`、`/api/v1/pipeline-versions/*` | Pipeline 版本与制品 |
| `/api/v1/approvals/*` | 版本审批 |
| `/api/v1/execution-preparations/*`、`/api/v1/execution-runs/*` | 执行准备与执行记录 |
| `/api/v1/side-effect-preparations/*` | 副作用治理 |
| `/api/v1/projects/{id}/safety-proposals`、`/api/v1/benchmark-runs/*` | 安全进化与基准 |
| `/api/v1/projects/{id}/overview`、`/api/v1/workers` | 控制台聚合视图 |

本地开发使用 `dev-header` 身份模式，请求头 `X-Actor-Id` 直接携带项目成员的 `users.id`；
生产环境身份来自可信网关（HMAC + Nonce），不接受请求方自述身份。

## 部署

1. 准备 PostgreSQL 16、Redis、SeaTunnel Zeta，以及受管的 MySQL / Doris
2. `ETL_AGENT_ENVIRONMENT=production`，关闭 `ETL_AGENT_BOOTSTRAP_ENABLED`
3. 配齐凭据：`ETL_AGENT_SECRET_MASTER_KEY`（或 Vault 连接）、Harness 签名密钥、网关 HMAC 密钥
4. `uv run alembic upgrade head`
5. 各进程按需水平扩展 —— API 与 Worker 无状态，共享同一业务库；Beat 必须单实例
6. 前端 `npm run build`，用任意静态服务器托管 `frontend/dist`，并把 `/api` 反向代理到 API 进程

## License

本仓库尚未附授权文件（无 `LICENSE`）。未声明许可证时默认保留全部权利，
如需开源请补充许可证文件并在此说明。

## 已知边界

- SeaTunnel CLI 黑盒生成后端未实现，默认走 `direct`
- LangGraph Checkpoint 的 PostgreSQL 后端为占位实现：依赖未安装时抛 `NotImplementedError`，调用方降级到内存 Checkpoint
- 运行时依赖 `harness_side_effect_preparations` 等表已就位，但生产环境的 Ed25519 密钥轮换与 Doris 原子发布尚未在真实环境验证
