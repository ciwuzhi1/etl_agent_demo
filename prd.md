# 《etl_agent》产品需求文档（PRD）

## 文档信息

| 项 | 内容 |
|---|---|
| 产品名称 | **etl_agent**（企业 ETL 任务的智能控制面） |
| 实现包名 / 服务标识 | `seatunnel-etl-agent`（版本 `0.1.0`）；Web 控制台品牌 **FlowPilot** |
| 目标用户 | 企业数据工程师（主要使用者）；数据审批人、安全审批人、运行操作员、审计员（治理责任方） |
| 文档版本 | v1.0（待评审） |
| 撰写日期 | 2026-10-14 |
| 需求来源 | 《尚硅谷大模型项目之生产级ETL-Agent.md》（21654 行，第 0–15 章） |
| 事实校验来源 | 参考实现 `../etl_agent`（`src/etl_agent`、`frontend`、`alembic`、`benchmarks`、`infra`） |
| 文档状态 | 已完成主体；文末第 14 章列出需需求方确认的关键问题 |

### 阅读约定

1. 本 PRD **同时覆盖"产品要什么"与"必须被证明到什么程度"**。凡涉及安全边界、状态迁移、审批分离、证据留存的条目，均为**可验证断言**，不是设计建议。
2. 文中标注 `【代码】` 的内容来自参考实现的真实拼写（枚举值、字段名、路由、配置项），可直接作为开发契约使用。
3. 参考文档正文与参考实现存在少量命名/路径差异（如文档散文出现 `PREPARED`、`/runtime-supervision`，代码枚举与路由为 `READY`、`/supervision-snapshots`）。**一律以参考实现代码为准**，差异清单见附录 D。
4. 文中 `P0/P1/P2/P3` 为风险等级，`P0` 最高。

---

## 1. 项目背景

### 1.1 一句话定位

**etl_agent 是一个企业级 ETL 任务的智能控制面**：它能理解自然语言需求、探查受管元数据、生成候选 Pipeline 方案、解释风险、请求具名审批并跟踪执行结果；**真正的数据搬运交给 Apache SeaTunnel Zeta**。

产品只做一件事，但要做到底：**模型只负责"提建议"，从建议到真实副作用的每一道关口都由确定性代码和具名的人把守。**

### 1.2 必须回答的三个问题

本产品的存在理由由三个问题定义，前一个决定业务价值，后两个决定"这是 Demo 还是能进入生产的 Agent 系统"：

| # | 问题 | 本产品的回答 |
|---|---|---|
| Q1 | 企业为什么需要 ETL Agent，而不是脚本、拖拽式 ETL 平台或通用聊天机器人？ | 脚本无法承载治理与责任；拖拽平台无法把人话变成受管制品；聊天机器人无法证明"审批的内容"与"执行的内容"是同一件事。ETL Agent 把**不确定性交给 Agent、稳定次序交给 Workflow、安全边界交给 Harness**。 |
| Q2 | 模型会生成错误、受提示注入、超时或重复调用工具，如何让它进入生产系统？ | 用 Harness 内核：模型输出只是**候选**，必须经确定性门禁、真实引擎检查、策略裁决（PDP）与具名审批，才能换来**短时、单次、绑定内容指纹**的执行授权。 |
| Q3 | 如何证明某次生产写入的**需求、制品、风险决策、审批和实际执行内容是同一件事**？ | 用内容摘要（SHA-256）绑定需求与制品、用 Ed25519 签名绑定授权、用哈希链证据账本绑定决策顺序，并允许审计员独立复核。 |

### 1.3 现状与痛点

没有 ETL Agent 时，企业数据工程师面对的现实是：

- **需求到实现的转换靠人**：一句话业务需求（"把订单清洗后写进 Doris"）需要人工翻译成 SeaTunnel HOCON，字段映射、质量规则、拒绝行去向、增量窗口全靠经验与口口相传。
- **制品没有治理**：配置散落在文件、工单、聊天记录里；"某次生产写入用的是哪一版配置"无法回答。
- **权限与责任脱节**：能改配置的人往往也能直接跑作业；审批是"看一眼"，不是"绑定一份不可变制品"。
- **风险看不见**：无界全表扫描、输出放大、拒绝率异常、凭据外发，只有出事之后才知道。
- **失败不可解释**：作业失败只有一段引擎日志，分不清是可重试的网络问题，还是必须改需求的 Schema 不一致。
- **副作用没人管得全**：启动作业有审批，人工取消、回滚、清理、凭据轮换却各自直连引擎——同一类风险，六套安全语义。

### 1.4 为什么"模型更强"不能替代 Harness

系统提示词（"你是安全的 ETL Agent，未经批准不得写入生产库……"）**不是授权凭证**，存在五个结构性缺陷：

1. 用户输入、数据库注释、工具错误可能改变模型注意力或触发提示注入；
2. 模型输出具有概率性，同一输入不保证永远作出同一风险判断；
3. 人工 API、定时调度、失败重试**根本不会经过这段 Prompt**；
4. 即使模型判断正确，执行函数仍可能收到被客户端替换的参数；
5. Prompt 无法签名、无法一次性消费、无法证明某次执行使用了哪一版规则。

**六类典型故障/攻击与 Harness 工程控制的对照（本产品的核心价值主张）**：

| 故障或攻击 | 只靠 Prompt | Harness 工程控制（本产品要求） |
|---|---|---|
| 模型试图访问另一项目的表 | 期待模型自觉拒绝 | PDP 按项目与资源范围判定 **P0 阻断** |
| 审批后将目标表替换为生产核心表 | Prompt 看不到提交时篡改 | Prepare 冻结输入/资源/制品指纹；Commit 重算比对，不一致即失效 |
| API 超时后重复提交 | 再问一次模型 | 幂等键 + 单次 Capability + 事务 Outbox + 稳定 JobId |
| 一人同时当申请人与审批人 | 请模型判断 | 项目角色、职责槽与自批阻断 |
| 远端已接受作业、本地响应丢失 | 模型只看到超时 | 稳定 JobId 认领 + 状态对账 + 有限重试 |
| 策略调整后合法任务大量误报 | 凭一次对话感觉判断 | 冻结 Benchmark + 影子授权 + 小流量授权 + 自动撤销 |

### 1.5 产品范围与非目标

**范围内**：企业数据集成任务的**控制面**——连接与元数据探查、文件导入、需求契约、候选生成、制品治理、策略与审批、执行授权、执行编排与观测、质量与发布、全部副作用的统一治理、运行时监督、调度、仿真与 Benchmark、安全进化、Web 控制台。

**明确非目标（不承诺）**：

| 不做的事 | 说明 |
|---|---|
| 不做"能聊天的 ETL 页面" | 聊天只是交互外壳，产品交付的是**受管制品与运行证据** |
| 不用大模型重写数据集成引擎 | 数据面仍是 SeaTunnel Zeta 与官方连接器 |
| 不证明 LLM 的每个业务判断都正确 | Harness 降低未授权副作用、内容替换、重放、证据篡改与安全回归风险；业务语义仍需契约、测试与人工审查 |
| 不做外部公证系统 | 签名哈希链可发现平台数据库内证据被改写；若攻击者同时控制数据库与签名私钥，仍需 KMS/HSM、WORM 存储与组织安全控制 |
| 不承诺跨系统事务 | 事务 Outbox 只保证"本地业务事实与待投递命令"原子落库，不让 PostgreSQL 与任意外部系统共享事务 |
| 不宣称跨表原子性 | Doris 有效表通过影子表 + Swap 原子发布；错误表是按执行追加的审计日志 |
| 不允许 Agent 自我进化 | 安全进化只能产生候选、评测报告与签名授权，不得自行审批、修改运行中代码/策略/Prompt |
| 不提供企业级基础设施 | 本地 Compose 用于真实链路验证，不自动具备生产高可用、TLS、网络隔离、外部 KMS 与备份灾备 |
| 不支持的部分能力 | 独立 `STALLED` 停滞规则、SeaTunnel 真 Pause/SavePoint、通用跨 Sink 部分成功补偿、影子流量真实复制、Canary 流量路由执行器、HDFS Kerberos、扫描件/OCR PDF |

---

## 2. 产品定位与架构边界

### 2.1 控制面 vs 数据面

| | 控制面（本产品） | 数据面（SeaTunnel） |
|---|---|---|
| 职责 | 需求理解、制品治理、执行授权、证据闭环 | 连接器、分布式数据搬运、作业运行时 |
| 覆盖 | 异构连接、文件导入、元数据探查、LLM 候选生成、可恢复编排、确定性校验、审批、调度、幂等、密钥治理、运行监督、仿真与 Benchmark | 执行**已物化凭据**并通过授权的**冻结配置** |
| 边界 | 在 SeaTunnel **之前**完成授权，在 SeaTunnel **之后**完成状态对账、质量判定与证据留存 | 不解释自然语言，不判断申请人与审批人是否分离，不自行放宽策略 |

**输入**：自然语言需求、异构 Source/Sink、受管元数据、质量契约、企业治理约束。
**输出**：可审查、可审批、可执行、可回放的 **Pipeline 版本**与**运行证据**。

### 2.2 五层职责边界

| 职责层 | 主要输入 | 主要产物 | 边界（不得越界） |
|---|---|---|---|
| 交互与需求 | 项目、Source/Sink、自然语言需求 | 受管的 `AgentRun` | 不接收明文密码或任意可执行代码 |
| Agent 编排 | Profile、质量契约、用户补充信息 | `EtlPlan` / HOCON 候选 | 只生成候选，不代替确定性校验和审批 |
| 制品治理 | 候选配置、Schema、规则、引擎检查 | 不可变 `PipelineVersion` 与 `Artifact` | 审批后修改必须创建新版本 |
| Harness 控制 | `ToolIntent`、Artifact、策略、职责审批 | 受限 `Capability`、`ExecutionRun`、Outbox | 所有外部副作用经过统一授权入口 |
| 数据执行 | 已验签的受管命令与冻结 Artifact | SeaTunnel Job、指标与终态 | 不负责理解业务需求或自行放宽策略 |
| 证据与评测 | 策略、审批、运行事件、安全场景 | 证据链、仿真结果、Benchmark 报告 | 不保存密钥、原始生产数据或完整 Prompt |

### 2.3 三条责任停止点（不可跨越）

1. **LangGraph 只能把候选意图交给 Harness，不能直达 SeaTunnel**；PostgreSQL 保存企业事实，LangGraph Checkpoint **不代替**审批、授权与执行台账。
2. **Worker 只接受 Harness 产生的经验签命令**，并把实际状态与指标返回控制面。
3. **控制面在 SeaTunnel 之前完成授权，在 SeaTunnel 之后完成对账**。

### 2.4 三层状态不能互相冒充（关键概念澄清）

| 层 | 状态对象 | 真相源 | 回答的问题 |
|---|---|---|---|
| Agent 会话层 | `AgentRun` / LangGraph Checkpoint | PostgreSQL 业务表 + **独立** Checkpoint 库 | 图从哪里恢复、谁发起、当前业务状态是什么 |
| 控制面治理层 | `PipelineVersion` / `ApprovalRequest` / `ExecutionRun` / `Preparation` | PostgreSQL 业务库 | 哪份制品被谁批准、授权到哪一步 |
| 执行引擎层 | SeaTunnel Job | SeaTunnel Zeta（经控制面对账） | 作业是否真的在跑、真的结束 |

**必须写进 UI 与文档的三条防混淆断言**：

- LangGraph 进入 `WAITING_APPROVAL` **只说明候选已冻结并暂停会话，不代表审批已通过**。
- `ExecutionRun` 进入 `SUBMITTED` **只说明提交边界已完成，不代表 SeaTunnel 成功**。
- `Preparation` 进入 `COMMITTED` / Outbox 进入 `SENT` / `ExecutionRun` 进入 `SUCCEEDED` 是**三个不同事实**；前端在 Commit 后立即显示"ETL 成功"即属把控制面授权成功误当成数据面执行成功。

### 2.5 技术架构与部署形态

**技术栈**（`【代码】`，可直接作为技术选型契约）：

| 层 | 选型 |
|---|---|
| 语言与工具链 | Python `>=3.12,<3.13`、uv（锁文件）、hatchling、Ruff、Mypy（strict）、Pytest |
| 控制面 API | FastAPI `>=0.116,<1` + uvicorn |
| 业务数据库 | PostgreSQL 17（SQLAlchemy 2 async + asyncpg，Alembic 迁移） |
| 消息与调度 | Redis 8 + Celery 5（Worker + Beat） |
| Agent 编排 | LangGraph `>=0.6,<2` + `langgraph-checkpoint-postgres`（**独立库**） |
| 密码学 | cryptography（Fernet 主密钥、Ed25519 签名） |
| 可观测 | OpenTelemetry（OTLP/HTTP，**默认关闭**）+ structlog |
| 前端 | Vue 3 + TypeScript + Vite（`vue-tsc -b && vite build`） |
| 数据面 | Apache SeaTunnel 2.3.13（Zeta REST） |
| 连接器驱动 | PyMySQL / asyncpg / oracledb / python-tds / PyHive / WebHDFS / pyarrow / pyorc / openpyxl / pypdf |

**三个独立进程（不可合并）**：

| 进程 | 职责 | 关键约束 |
|---|---|---|
| API | 注册全部路由（`/health` 无身份，其余挂 `/api/v1`）；领域异常映射为稳定 HTTP 码并附 `trace_id` | 不执行长耗时外部调用；不持有执行引擎凭据 |
| Worker | 消费全部异步任务：连接测试与探查、Agent 生成、执行提交与监控、回滚/清理、Secret 治理、文件解析、调度、Outbox 投递、心跳 | **只有在 Worker 才拥有读取生产 Secret 与访问 SeaTunnel 的权限** |
| Beat | 周期投递：Worker 心跳、凭据到期扫描（默认 3600s）、调度扫描（默认 30s）、Outbox 扫描（默认 2s） | 只投递不执行；与 Worker 分离才能区分"没产生任务"与"任务无人消费" |

**Compose Profile 策略**：PostgreSQL 与 Redis 为轻量常驻；MySQL、Doris、SeaTunnel 放 `e2e` Profile（`--profile e2e` 按需拉起），不塞进单容器。

### 2.6 生成后端可替换（Generation Port）

- 应用层只有**一个异步端口** `PipelineGenerationPort.generate(request)`；应用不关心底层是 OpenAI SDK、官方 CLI、本地模型还是测试假对象。
- 两条实现路径：`direct`（默认生产，单次 OpenAI-compatible 调用生成候选）与 `seatunnel-cli`（可选实验，官方 `Orchestrator` 黑盒，固定 commit）。
- **切换生成后端只改变"候选如何产生"，不改变后续门禁、版本、审批、Harness 与执行边界**；官方黑盒也不会因此获得执行权限。

---

## 3. 用户与角色

### 3.1 目标用户画像

| 用户 | 核心诉求 | 关键痛点（产品要消除的） |
|---|---|---|
| **企业数据工程师**（主要） | 把一句业务话快速变成可执行、可审批、可回放的 Pipeline | 手写 HOCON 易错；质量规则与拒绝行去向靠记忆；不知道 Agent 生成的配置是否越界 |
| **数据审批人** | 在不读 HOCON 全文的前提下判断风险并承担责任 | 审批时看到的与执行时用的可能不是同一份 |
| **安全审批人** | 掌控破坏性操作、数据外发、凭据使用 | 高风险操作与日常操作共用一套权限，无法差异化把关 |
| **运行操作员** | 可靠地提交、观测、取消、重跑、回滚、清理 | 失败原因不可解释；取消按钮真假难辨 |
| **审计员** | 独立复核"谁、何时、基于什么、做了什么" | 日志可被改写、被判责时无法自证 |

### 3.2 项目角色（V1，`ProjectRole` `【代码】`）

| 角色 | 权限 |
|---|---|
| `ADMIN` | 管理成员与普通项目配置；**不隐式获得安全审批权** |
| `DATA_ENGINEER` | 设计 Pipeline、连接与生成任务；上传文件；提交业务审批 |
| `APPROVER` | 审批数据版本 |
| `VIEWER` | 只读 |

### 3.3 Harness 职责（V2，`HarnessRole` `【代码】`，可叠加）

| 职责 | 权限 | 明确禁止 |
|---|---|---|
| `DATA_ENGINEER` | 建连接/Profile、上传文件、描述需求、生成、提交审批、发起副作用申请 | 自批自己提交的审批单 |
| `DATA_APPROVER` | 审批 Pipeline 版本；占执行 Preparation 的数据审批槽 | 审批自己申请的 |
| `SECURITY_APPROVER` | 裁决安全提案；影子/小流量/推广授权；占 P0 破坏性操作的安全槽 | 裁决/发布自己创建的候选 |
| `OPERATOR` | Commit Preparation、取消、原产物重跑、撤销/回滚安全发布 | 自批；Commit 未满足职责要求的 Preparation；改冻结事实 |
| `AUDITOR` | 只读核对身份分离、摘要链一致、证据签名、确认无 Secret 明文 | 发起任何副作用 |

**V1 → V2 兼容映射**（`legacy_harness_roles` `【代码】`）：
`ADMIN → {DATA_ENGINEER, OPERATOR, AUDITOR}`；`DATA_ENGINEER → {DATA_ENGINEER, OPERATOR}`；`APPROVER → {DATA_APPROVER}`；`VIEWER → {AUDITOR}`。
**管理员绝不隐式获得 `SECURITY_APPROVER`**，需要时必须通过显式授权接口选择职责并落库 + 审计。

### 3.4 职责分离规则（不可协商）

1. **申请人 ≠ 审批人**（`requested_by != decided_by`），对版本审批、执行 Preparation、回滚、清理、全部 Secret 操作、安全提案一律适用。
2. **同一自然人不得占用同一份 Preparation 的两个审批槽**（服务校验 + DB 唯一约束 `(preparation_id, decided_by)`、`(preparation_id, role)` 双重兜底）。
3. **P0 破坏性操作需要两个不同职责槽**：`DATA_APPROVER` + `SECURITY_APPROVER`，且为两个不同自然人。
4. **课程/演示环境的身份切换只是模拟自然人**，不能用一个账号轮流扮演申请人与审批人完成生产四眼流程。

### 3.5 权限矩阵（谁可发起 / 谁可批准 / 谁可执行）

| 动作 | 发起 | 审批 | 执行（Commit/Worker） |
|---|---|---|---|
| 创建连接 / Profile / 上传文件 | ADMIN、DATA_ENGINEER | — | Worker（Celery） |
| 启动 Agent 生成 | ADMIN、DATA_ENGINEER | — | Worker（LangGraph） |
| 提交版本审批 | ADMIN、DATA_ENGINEER | ADMIN、APPROVER（非申请人） | — |
| 生成执行 Preparation | ADMIN、DATA_ENGINEER | 按风险等级决定职责槽 | — |
| Commit 执行 | — | — | `OPERATOR` |
| 取消执行 | ADMIN、DATA_ENGINEER | 自动 Commit（拒绝重跑旧审批） | Worker |
| Doris 回滚 `ROLLBACK_DORIS` | ADMIN、DATA_ENGINEER | 独立数据审批（P1，1 人） | Worker |
| Doris 清理 `CLEANUP_DORIS` | ADMIN、DATA_ENGINEER | **双人（P0）** | Worker |
| Secret 迁移/激活/回退 | ADMIN、DATA_ENGINEER | 独立数据审批（P1，1 人） | Worker |
| Secret 暂存/验证（Stage/Validate） | ADMIN、DATA_ENGINEER | 可由一次性能力自动执行（不切换活动引用） | Worker |
| Secret 销毁 `DESTROY_SECRET` | ADMIN、DATA_ENGINEER | **双人（P0）** | Worker |
| 运行 Benchmark | DATA_ENGINEER / AUDITOR / SECURITY_APPROVER | — | 服务端内置套件 |
| 创建安全提案 | DATA_ENGINEER | — | 不执行 |
| 裁决/影子/Canary/推广 | — | `SECURITY_APPROVER`（非创建者） | 只产出签名授权，由部署系统执行 |

---

## 4. 核心设计原则（不可协商的不变量）

以下 14 条是产品的"宪法"，任何功能设计不得违反：

1. **模型只提候选，不拥有最终裁决权**。确定性规则、真实 SeaTunnel 检查和具名人工审批均可拒绝模型输出。
2. **关键规则必须位于所有副作用入口都会经过的确定性代码、数据库约束与 Worker 验证边界中**——不能只写在 System Prompt（UI/API、调度器、运维手动重跑、Worker 超时重试都可绕过 Prompt）。
3. **所有副作用使用同一个控制面**：Agent、UI、API、调度、重试、取消、回滚、清理、Secret 治理**不各自实现一套权限判断**。
4. **严格区分准备与执行**：Prepare 可计算风险、影响与回滚，但**不得访问生产 Secret 或提交 SeaTunnel**，无外部副作用。
5. **审批绑定内容，不只绑定数据库 ID**：输入、资源、Artifact、策略包、监督契约均以摘要冻结。
6. **权限最小化且短期化**：Capability 只允许某主体在某环境对某工具执行**某一份已冻结内容**，短时、单次有效。
7. **高风险依赖失效时失败关闭（fail-closed）**：策略、审批、签名、证据或 Secret 无法验证时，写入、删除、外发与密钥使用**不得降级放行**。
8. **证据不得变成敏感数据仓库**：审计只保存必要摘要、结构化决策与安全运行指标，**不保存明文密码、原始行、完整 Prompt、远端响应正文**。
9. **评测必须阻止危险回归**：P0/P1 漏拦是**硬失败**，不能被合法任务的高成功率抵消；不允许加权平均掩盖。
10. **后一层状态不得静默覆盖前一层记录**：远端提交成功而本地事务失败时，由事务 Outbox、幂等键与状态对账恢复，**不会简单再次提交可能已执行的作业**。
11. **未注册工具、版本不匹配、输入越界一律拒绝**；工具副作用以**服务端注册清单**为准，不以 Agent 声明为准。
12. **改需求必须创建新版本**：已冻结/已审批制品不做原地编辑；修改产生带血缘的新 DRAFT。
13. **只减权原则**：运行监督、运行时减损、安全进化只能**收窄**权限或产生候选，不能扩权、不能热修改运行中的作业语义。
14. **外部故障不得伪装成业务事实**：生成能力未就绪要明确报告 `NOT_CONFIGURED`；SeaTunnel 不可用时已冻结版本与审批事实仍保留，不能表现为"任务从未创建"。

---

## 5. 功能需求

> 编号规则：`FR-<模块>-<序号>`。每条需求给出**规则约束**（可验证）与**关联接口**。状态机见第 7 章。

### FR-1 身份、项目与一次性初始化

**目标**：在调用模型之前建立业务真相与身份边界；解决"服务重启后无法回答哪个版本被批准、批准人是谁"的问题。

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-1.1 | 项目与成员管理 | 成员关系为 V1 单角色（`project_memberships`）；同一用户可加入多项目；重复添加同成员返回 `VersionConflict`(409)，角色变更必须走独立权限流程 |
| FR-1.2 | 可叠加的 Harness 职责授予 | 独立表 `project_role_grants`；需 `ADMIN`；目标必须是项目成员；重复授予幂等；写审计事件 `harness_roles.granted` |
| FR-1.3 | 一次性 Bootstrap | 默认关闭；启用必须配 ≥32 字节 Token；错误 Token 403 且不创建项目；成功后**永久拒绝**再次执行（409）；Token 仅在请求内存比较，不写审计/响应/浏览器存储；advisory lock 防并发双建 |
| FR-1.4 | 双模式身份来源 | `dev-header`（本地，直接信任 `X-Actor-Id`）与 `trusted-gateway`（生产，HMAC-SHA256 + Nonce + 时间戳）。**生产必须 `trusted-gateway`，否则进程启动失败** |
| FR-1.5 | 网关签名语义 | 规范文本按行拼接：`Actor UUID \n Unix时间戳 \n Nonce \n HTTP方法(大写) \n 原始Path与Query`；用 `hmac.compare_digest` 防时序泄漏；Nonce 以 SHA-256 摘要经 Redis `SET NX` + TTL 一次性消费；篡改/过期/重放 → 401；**Redis 不可用 → 503，禁止降级为仅验 HMAC 的可重放模式** |
| FR-1.6 | 领域异常 → HTTP 映射 | `PermissionDenied`→403、`ExternalServiceUnavailable`→503、`ResourceNotFound`→404、`VersionConflict`→409、其余 `DomainError`→400；响应附 `trace_id`；前端不得根据错误文本猜状态迁移 |
| FR-1.7 | 健康检查 | `GET /health`（无前缀、无身份）返回 `status`/`service`/`secret_provider`/`secret_provider_configured`；**只表示必要配置存在，不主动读取或返回任何秘密** |

**关联接口**：`POST /api/v1/projects/bootstrap`、`GET|POST /api/v1/projects/{project_id}/members`、`POST /api/v1/projects/{project_id}/members/{user_id}/harness-roles`、`GET /health`。

### FR-2 数据连接与元数据探查

**目标**：让 Agent 获得可靠的数据上下文，同时**不把任何密码交给模型**。

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-2.1 | 登记连接 | 支持 `MYSQL/DORIS/POSTGRESQL/ORACLE/SQL_SERVER/HIVE/HDFS/LOCAL_FILE`；默认端口 3306/9030/5432/1521/1433/10000/9870；连接名项目内唯一；凭据交 `SecretProvider`，库中只存 `secret_ref` |
| FR-2.2 | 输入安全校验 | `options` 必须通过安全上下文校验（防止把密码塞进 `options.password` 形成第二条泄露路径）；HDFS 仅允许 simple 认证，**Kerberos 显式拒绝**；库连接必须提供 host 与 database_name |
| FR-2.3 | 连通性测试（异步） | API 置 `TESTING` 后立即返回，只投递任务；Worker 用短事务解析凭据 → 释放 Session → 调 Adapter → 新事务写结果；失败错误经 `redact_error` 清洗并截断 |
| FR-2.4 | 表级元数据探查 | 探查列（名称/类型/可空/默认/主键标注/注释/位置）、主键、**估算**统计（不使用无界全表 `COUNT`）、脱敏样本；`sample_limit` 硬限 1–100 |
| FR-2.5 | 探查结果即快照 | `DataProfile` 保存 schema/主键/统计/脱敏样本/样本量/`expires_at`（默认 24h）；**只有 `READY` 且未过期的 Profile 可被 Agent 引用** |
| FR-2.6 | Adapter 内脱敏 | Adapter 返回前必须完成字段级脱敏（密码类→`[REDACTED]`、邮箱→`a***@domain`、手机号→`前3****后4`）；**不允许"返回原始样本等上层脱敏"** |
| FR-2.7 | 标识符与路径白名单 | 对象名匹配 `^[A-Za-z_][A-Za-z0-9_$]{0,127}$`；HDFS 路径拒绝 `..`、反斜杠与查询串注入；越界即拒绝，且**数据库不会收到该字符串构造的查询** |
| FR-2.8 | 探查能力差异（不强制统一驱动） | MySQL/Doris 走 INFORMATION_SCHEMA；PostgreSQL 用 `pg_catalog` + `reltuples`；Oracle 用 `ALL_TAB_COLUMNS`/`ALL_CONSTRAINTS`；SQL Server 用 `sys` 分区行数；Hive 用 `DESCRIBE`；HDFS 仅支持 CSV/JSON/Parquet/ORC 且拒绝混合目录 |
| FR-2.9 | 并发串行化 | Profile 状态变更使用 `SELECT ... FOR UPDATE`；重复投递/消息重放不得产生两个冲突结果 |
| FR-2.10 | 审计脱敏 | `data_connection.created` 的 `after` 只含 name/类型/host/port/database；**不记录** `secret_ref`、用户名密码、Vault 路径、驱动原始错误 |

**关联接口**：`GET /api/v1/projects/{project_id}/connections`、`POST /api/v1/connections`、`GET /api/v1/connections/{id}`、`POST /api/v1/connections/{id}/tests`、`POST /api/v1/connections/{id}/profiles`、`GET /api/v1/data-profiles/{id}`、`GET /api/v1/connections/{id}/profiles`。

### FR-3 受管文件资产

**目标**：把 CSV/JSON/JSONL/XLSX/Parquet/ORC/XML/Markdown/PDF 九种格式接入同一条安全链路，供 SeaTunnel `LocalFile` Source 只读消费——**上传文件不可信**。

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-3.1 | 流式上传 | API 校验角色/文件名/扩展名 → 建 PENDING 资产并提交 → **事务外**流式落盘（每块 1 MiB）并累计 SHA-256 → 新事务记录 URI/大小/摘要 → 返回 202 并投递解析任务 |
| FR-3.2 | 原子落盘 | 写随机 `.uploading` 临时文件，超限/异常删临时文件，非空且刷新关闭后 `replace()` 原子改名；`_resolve` 在 `Path.resolve()` 后校验仍在根目录内（防穿越/绝对路径） |
| FR-3.3 | 四种事实严格区分 | `original_filename`（展示用，不参与寻址）/ `source_uri`（内部读原始）/ `normalized_uri`（内部定位规范化结果）/ `engine_path`（SeaTunnel 容器只读路径）。**API 响应不得包含后三者** |
| FR-3.4 | 九格式解析边界 | CSV 要求非空表头与 UTF-8；JSON 顶层须对象或对象数组；JSONL 每非空行须对象（错误精确到行号）；XLSX 取首个工作表、`data_only`；Parquet/ORC 物化前用 footer/stripe 统计做行数门禁；XML 用 `defusedxml` 禁实体扩展；Markdown 为行级文档模型；PDF 每页一行，**加密/扫描/OCR/无文本明确拒绝**（绝不产生 `row_count=0` 的 `READY`） |
| FR-3.5 | 规范化与保守推断 | 字段名转小写、非字母数字→`_`、数字开头加前缀、重名加后缀，原→规范映射写入 `statistics.column_mapping`；类型只推断 `boolean/bigint/double/string`；任一行缺列即 `nullable=true`；**不猜** decimal 精度、日期格式、主键 |
| FR-3.6 | 解析状态机与并发 | 复用 `PENDING → RUNNING → READY/FAILED`；`FOR UPDATE` 串行化；解析期间**不持行锁**；未知异常只存固定文案"文件解析失败，请检查文件结构" |
| FR-3.7 | 确定性编译为 LocalFile Source | 冻结契约：`source_kind="MANAGED_FILE"`、`plugin="LocalFile"`、`required_options = {path: engine_path, file_format_type: "json", encoding: "UTF-8", schema.fields}`、`credential_placeholders={}`；**模型不能把 path 改回宿主机地址，不能按原始扩展名重解析** |
| FR-3.8 | 容量与安全上限 | 单文件 ≤ 25 MiB；单次解析 ≤ 100000 行；HDFS 探查 ≤ 256 MiB |

**关联接口**：`POST /api/v1/projects/{project_id}/file-assets`（multipart）、`GET /api/v1/projects/{project_id}/file-assets`、`GET /api/v1/file-assets/{asset_id}`。

### FR-4 凭据治理（SecretProvider 全生命周期）

**目标**：凭据只在 Worker 调用 Adapter 前短暂存在于内存；其余时刻只有引用与摘要。

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-4.1 | 双后端 Provider | `fernet-db`（本地默认，密文入库）与 `vault-kv-v2`（生产建议，库内只存外部引用）；迁移期用路由 Provider：**新写入统一去 primary，解析/销毁按记录中的 provider 路由；未知/未配置 Provider 明确失败**，不猜后端重试 |
| FR-4.2 | Vault 安全细节 | UUID 确定性路径 `{path_prefix}/{uuid}`；**`cas=0`** 保证同一路径只允许首次创建；**禁止跟随重定向**（防 Token 被代理转发）；非 local/test/dev 环境必须 HTTPS |
| FR-4.3 | 销毁语义 | Fernet：清空密文 + `destroyed_at` 墓碑；Vault：DELETE metadata 移除全部版本，404 视为已删除（幂等收敛） |
| FR-4.4 | 凭据轮换 | 状态机 `VALIDATING → WAITING_APPROVAL → APPROVED/REJECTED → ACTIVATING → SUCCEEDED/FAILED`；暂存经副作用 Harness 验签后才在请求内存把明文交给 Provider；新凭据经**真实验证**后再走独立四眼审批；需 `X-Idempotency-Key`；冻结事实含 `source_ref_sha256/credential_sha256/provider`（不落明文） |
| FR-4.5 | 凭据回退 | 仅在 `rollback_until` 窗口内可申请；申请阶段不读旧凭据明文；四眼审批后派发旧凭据重验证与原子回切；**回退恢复旧凭据年龄**（`source_credential_issued_at`） |
| FR-4.6 | 凭据跨 Provider 迁移 | 只冻结引用，不读/传输凭据内容；`target_secret_ref` 申请时冻结（唯一，Worker 重试写同一路径，避免孤立秘密）；申请前先验证目标 Provider 配置完整 |
| FR-4.7 | 凭据销毁 | 仅对超过回退保留期的废弃凭据创建；四眼审批后派发**不可逆**销毁；失败不得伪装成功 |
| FR-4.8 | 凭据年龄与到期策略 | 连接级 `max_age_days`（默认 90，1–3650）与 `warning_days`（默认 14）；`credential_issued_at` **独立于 Secret 存储记录**——Provider 迁移复制同一凭据不得重置轮换时钟；只有真实轮换激活才刷新年龄 |
| FR-4.9 | 到期告警与 Outbox | 告警去重键 `(connection_id, credential_issued_at, level)`（新凭据形成新治理周期，不复用旧告警）；投递记录与告警**同事务**写入，提交后才投递；预警冷却默认 24h、逾期冷却 6h；投递重试 `max_retries=5`，最长退避 300s，**始终复用同一 Delivery ID** |
| FR-4.10 | 告警处置 | 确认（建立责任链，**不隐式静默或关闭**）、限时静默（最长 30 天，并跳过未发送投递）、解除静默；三者均走项目角色校验并写审计 |
| FR-4.11 | Webhook 出口 | HMAC-SHA256 签名；Header `X-FlowPilot-Event`/`X-FlowPilot-Delivery-Id`/`X-FlowPilot-Signature`；负载字段顺序稳定，仅含脱敏上下文（**不含连接地址或 Secret 引用**）；非本地强制 HTTPS、不跟随重定向、错误信息不含 URL/Header/响应正文 |

**关联接口**：`POST /api/v1/connections/{id}/secret-rotations`、`.../secret-migrations`、`POST /api/v1/secret-rotations/{id}/rollback-requests`、`.../destruction-requests` 及各自 `decisions`；`GET /api/v1/projects/{pid}/credential-expiry|credential-alerts|secret-governance`；`POST /api/v1/projects/{pid}/credential-alert-scans`；`POST /api/v1/credential-alerts/{id}/acknowledgements|silences|silence-clearances`。

### FR-5 ETL 契约表达

**目标**：不把自然语言直接当执行计划。用**版本化的显式契约**限定平台事实，模型只能在这些事实内提候选。

一次可执行设计由四层事实构成：自然语言需求 → 生成上下文（脱敏 Profile + 业务契约）→ 候选 HOCON → 确定性门禁 → 冻结 plan + HOCON + dry-run → 审查视图。

| 编号 | 契约 | 关键字段与规则 |
|---|---|---|
| FR-5.1 | `profile_contracts`（Schema 快照） | 只截取复核所需稳定事实（`profile_id/connector_type/schema/table/role/columns` + 按连接器类型的端点契约）。**不冻结**：密码/Secret 明文、运行时主机物化结果、`masked_sample`、凭据占位符、宿主机上传路径、HDFS NameNode 真实地址 |
| FR-5.2 | `quality_contract` | `contract_version(1..2)`、`valid_sink_match`、`error_sink_match`（**两者不得相同**）、`rules(1..100)`、可选 `max_rejected_rows`、`max_reject_rate`；规则 `code` 全局唯一；**v2 每条规则必须提供机器可判定的 `invalid_when`**；阈值来自用户确认的结构化输入，**不得由模型在重试中自由变更** |
| FR-5.3 | `incremental_contract`（水位） | `contract_version(=1)`、`source_profile_id`、`column`（单段安全标识符）、`value_type="TIMESTAMP"`、`initial_value`（规范为 UTC 感知时间）、`lag_seconds(0..86400)` |
| FR-5.4 | `row_llm_contract`（逐行 LLM） | `model_provider`（`OPENAI/DOUBAO/DEEPSEEK/KIMIAI/MICROSOFT/ZHIPU/CUSTOM`）、`model`、`prompt(≤2000)`、`inference_columns(1..20)`、`output_column_name`（单段标识符）、`output_data_type`、`api_path`（CUSTOM/MICROSOFT 必填）、`rows_per_second(1..100)` |
| FR-5.5 | `delivery_contract`（交付） | `mode="ATOMIC_REPLACE_VALID_SINK"`、`target_profile_id`、`rejected_sink_mode="APPEND_AUDIT"`、`retain_previous_for_rollback=True`。**只对有效数据表提供单表原子替换**；拒绝表为追加审计 |
| FR-5.6 | `dry_run`（服务端引擎检查） | `valid/phase1_local/phase2_check(SKIPPED\|ERROR\|FAIL\|PASS\|INCONCLUSIVE)/summary`；**审批权归服务端，客户端提交的同名对象被丢弃** |
| FR-5.7 | `design_review`（审查视图，派生） | `summary/nodes/edges/transforms/schema_diffs/sample_preview/risks/preview_semantics`；语义边界：`nodes/edges` 不证明 SQL 业务语义正确；`transforms` 不执行 SQL；`sample_preview.after` 只是**字段级结构映射预览**，不可确定性求值的表达式必须显式标注"运行时计算"，**不把猜测值冒充执行结果** |
| FR-5.8 | 制品绑定 | `artifact_sha256`（SHA-256）、`secret_bindings`（外部导入制品必须携带）、`upstream_version`（生成器版本） |

### FR-6 候选生成（模型 Provider）

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-6.1 | 生成端口 | 应用层只依赖 `PipelineGenerationPort`；请求/结果模型 `extra="forbid"`；`requirement ≤ 20000` 字符 |
| FR-6.2 | `direct` 路径（默认） | 单次 OpenAI-compatible Chat Completions 调用（`temperature=0.1`、`max_tokens=16384`、硬超时）；上游版本标识 `direct-openai-compatible-v1`；**每份候选只调用一次 Provider** |
| FR-6.3 | `seatunnel-cli` 路径（可选） | 固定官方 commit 的可选依赖，把官方 `Orchestrator` 当黑盒，仅依赖最小协议；**延迟导入**避免污染 API 进程；黑盒调用预算跨实例共享，耗尽时**失败关闭** |
| FR-6.4 | 三类结果 | `CONFIG`（完整 hocon + 上游版本）/ `QUESTION`（单个明确问题，持久化中断等待补充）/ `ERROR`（安全解释，进入有限修复） |
| FR-6.5 | 供应商中立 | 只理解 `LLM_API_KEY/BASE_URL/MODEL/FAST_MODEL/REQUEST_TIMEOUT_SECONDS`；不把变量命名为 `OPENAI_*`；DeepSeek/Qwen 等通过换 `BASE_URL` 与模型名切换 |
| FR-6.6 | 前置护栏 | 调用模型**之前**递归检查元数据键：`password`/`access_token` 等疑似秘密若不是平台认可占位符，**在模型调用尚未发生时就失败**；让"事后日志脱敏"不再是第一道防线 |
| FR-6.7 | 就绪检查 | 只返回 `READY`/`NOT_CONFIGURED` + 缺失配置项名称，**绝不返回 Key 值**；不发起收费模型调用；未就绪时在创建 `AgentRun` 前就返回 `NOT_CONFIGURED`，而非先投递再以模糊鉴权错误失败 |

**模型职责边界（写入 System Prompt 的硬约束）**：生成**一份** SeaTunnel 2.3.13 HOCON 候选；元数据上下文是权威；精确复制每个受管路径、Schema、Sink、路由标签与凭据占位符；**绝不输出明文秘密**；只用上下文中记录的连接器/Transform 选项；**只返回一个** ```hocon``` 块；缺少真正必要的业务决策时严格返回 `QUESTION: <question>`。

**禁止输出清单**：明文凭据；未被 Profile 授权的额外 Sink；`SELECT *` 通配投影；逐行 LLM 的自由格式请求头/请求体/响应解析/`custom_config`；多个 LLM 块或隐式全字段外发；伪造/自报的 `dry_run`。

### FR-7 Agent 可恢复会话（LangGraph 状态机）

**目标**：把"一次模型调用"变成**显式、可恢复、有预算上限**的生成流程。

**图结构（六节点，边由固定路由函数决定，模型不能新增节点或改变拓扑）**：

| 节点 | 职责 |
|---|---|
| `generate` | 还原请求、调用生成端口、写入结构化结果、标记 `phase`；不检查 HOCON、不写版本 |
| `clarify` | 持久化中断 `{"kind":"clarification"}`；恢复时把补充追加到需求 |
| `repair_generation` | 处理截断/不完整候选；未超预算则追加"候选完整性反馈"并重试；超预算转 `failed` |
| `policy` | 先规范化（连接器/Transform/凭据占位符/受管文件 Source 契约），再跑确定性门禁链，最后调用真实 SeaTunnel CLI `--check` 校验器 |
| `wait_for_review` | 机器门禁已通过后中断 `{"kind":"enterprise_approval"}`，把执行授权留给独立企业审批 |
| `failed` | 终态 |

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-7.1 | 共享修复预算 | `MAX_ENTERPRISE_REPAIR_ATTEMPTS = 2`（首份 + 最多修复 2 次 = **总量恰好三份候选**）；**格式失败、企业门禁失败、真实引擎失败共享同一 `repair_count`**；三份全失败后**不发起第四份** |
| FR-7.2 | 检查点独立 | LangGraph Checkpoint 使用**独立连接串与独立数据库**；**不退化为内存 Saver**；应用启动**不自动建表**（部署阶段显式执行一次 setup） |
| FR-7.3 | 恢复语义 | 用 `run.thread_id` 重新 `ainvoke`；澄清用 `Command(resume=answer)` 从同一检查点继续；**恢复前先恢复业务权限**（项目角色、版本仍在 `PROFILING`、`AgentRun` 确为 `WAITING_INPUT`） |
| FR-7.4 | 状态最小化 | 图状态只保存恢复所需的最小 JSON 可序列化信息；**不得**包含数据库密码/Token/逐行 LLM Key/物化 HOCON、原始文件字节、真实业务样本、宿主机路径、SQLAlchemy Session/模型 Client/进程句柄 |
| FR-7.5 | 幂等 | 同一版本同时最多**一个活动 `AgentRun`**（`{QUEUED, RUNNING, WAITING_INPUT}`）；`mark_running`/`fail`/`record_generation_observation` 均幂等且单调不回退；行锁串行化并发状态变更 |
| FR-7.6 | 消息最小化 | 投递 Celery **只携带 `run.id`**（与可选答案），不含 Profile 内容、密钥或 HOCON |
| FR-7.7 | 终端 `yes` 不是授权 | 中断恢复**不接收简单 `yes` 就执行**；真正的执行授权只能由 Harness 的具名职责审批产生 |

### FR-8 版本冻结与四眼审批

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-8.1 | 四类对象职责分离 | `Pipeline`（长期意图）/ `PipelineVersion`（不可变修订）/ `PipelineArtifact`（外部制品 URI+摘要）/ `ApprovalRequest`（对某份摘要的一次审批）。Pipeline **不直接保存"当前可执行 HOCON"** |
| FR-8.2 | 冻结流程 | 先重跑完整确定性门禁，再 `SELECT ... FOR UPDATE` 锁版本，然后在**同一事务**写入 plan + 摘要、状态 `GENERATED` + `lock_version+1`、`PipelineArtifact` 与审计事件；**不自动批准、不自动执行** |
| FR-8.3 | 摘要定义 | `canonical_plan + "\n---HOCON---\n" + hocon` 的 SHA-256；plan 按键排序、去不稳定空白；**数组顺序保留**（字段/规则/路由顺序有业务含义）；HOCON 不做任意格式化。摘要**同时绑定 plan 与 HOCON** |
| FR-8.4 | 摘要的诚实定位 | 摘要提供**篡改检测，不是数字签名**；签名能力与防篡改证据链属 Harness 内核（FR-9） |
| FR-8.5 | 读取时重校验 | 读取审查视图时重新校验硬编码秘密，并用当前 plan 与文件 HOCON 重算摘要，必须**同时等于**版本摘要与制品摘要，否则**拒绝返回**（不加"可能已变化"的黄色提示） |
| FR-8.6 | 版本血缘 | `revise()` 锁父版本与 Pipeline，`version_number = max+1`，创建 `parent_version_id` 指向源版本的新 DRAFT；父版本若在 `WAITING_APPROVAL`，同事务把其 PENDING 审批置 `REJECTED`（理由："方案已由新的 PipelineVersion 修订取代"）；**已 `APPROVED` 的父版本不被改成拒绝**；**DRAFT/PROFILING 版本不能创建并行修订** |
| FR-8.7 | 四眼审批 | 提交者 ∈ {ADMIN, DATA_ENGINEER}；决策者 ∈ {ADMIN, APPROVER}；**审批人不能与提交人相同**；审批对象是内容寻址的 plan + HOCON 组合摘要 |
| FR-8.8 | 审批单复制摘要 | 提交时把当前版本摘要**复制**到审批单（复制非冗余：前者表示版本当前内容，后者表示请求审批的精确内容；决策时比较可发现审查期间的静默替换） |
| FR-8.9 | 审批材料 | `plan` 含 `profile_ids`/`profile_contracts`/`secret_bindings`/`explanation`/服务端 `dry_run`/上游生成器版本/`design_review`，以及可选的质量、增量、交付、逐行 LLM 契约 |
| FR-8.10 | 只能决策一次 | 审批单只能从 `PENDING` 决策一次，重复决策被拒；`APPROVED`/`REJECTED` 均为**终态**，重提必须通过 `revise()` 创建新 DRAFT |
| FR-8.11 | 审查能力边界 | 设计审查**只能证明"读取内容与冻结内容一致"**，不能证明 SQL 满足所有业务意图；审批人仍需核对字段映射、质量规则、目标对象、交付模式与逐行外发风险 |

> **实现现状提示（需产品决策）**：当前 V1 版本审批单**没有 `expires_at` 字段**，审批时效由状态守卫 + 摘要相等 + PENDING 守卫 + 行锁联合保证。若产品要求"审批 N 天后失效"，属新增需求（见第 14 章 Q4）。

### FR-9 Harness 执行内核（`prepare → approve → commit`）

**目标**：建设**不能被绕过**的确定性运行时。这是本产品最核心的需求模块。

**反模式（必须避免）**：在一个 HTTP 请求里完成"校验参数 → 询问是否同意 → 立即执行"。它无法回答"审批期间内容是否变化、策略是否更新、请求是否被重放"，也不适合多人职责审批。

#### FR-9.1 Prepare（冻结可审查事实，无副作用）

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-9.1.1 | 入口与前置 | 只接受**已审批**的 PipelineVersion；对版本加行锁；必须存在摘要匹配的不可变 Artifact |
| FR-9.1.2 | 冻结内容 | 输入指纹、资源指纹、Artifact 摘要、工具意图、策略上下文、影响报告、回滚方案、数据分级报告、监督契约、所需职责、所需审批人数、有效期 |
| FR-9.1.3 | 服务端派生 | **客户端不能提交风险等级、审批人数、Handler 名、队列名或凭据**——攻击者若能发 `risk=P3` 即可把危险操作包装成低风险 |
| FR-9.1.4 | 有效期 | 默认 **24 小时**（可由配置收敛）；人工审批常跨数分钟至数小时，**Preparation 有效期不得与一次性 Capability 的短 TTL 混用** |
| FR-9.1.5 | 乐观冻结点 | Prepare 不长时持锁等审批；Commit 时重新比对，期间变化则旧 Preparation 失效、必须重审 |
| FR-9.1.6 | 无外部副作用 | Prepare **不得访问生产 Secret、不得提交 SeaTunnel、不得产生任何外部副作用** |
| FR-9.1.7 | 产出 | `ExecutionPreparation` 记录（含指纹、影响报告、回滚说明、所需职责、有效期、状态、签名证据） |

#### FR-9.2 策略裁决（PDP，确定性、不交给模型）

内嵌 **9 条规则（含兜底）**，`【代码】` 规则 ID 与语义如下：

| rule_id | 触发条件 | 结果 | 风险 | 审批要求 |
|---|---|---|---|---|
| `HARNESS-P0-CROSS-PROJECT` | 资源范围项目 ≠ 意图项目 | `DENY` | P0 | 无（不可审批放行） |
| `HARNESS-P0-SECRET-EXFILTRATION` | 外发 + `SECRET` 分类 | `DENY` | P0 | 无 |
| `HARNESS-P0-DESTRUCTIVE` | 副作用含 `DESTRUCTIVE` | `REQUIRE_APPROVAL` | P0 | `DATA_APPROVER` + `SECURITY_APPROVER`，2 人 |
| `HARNESS-P0-SENSITIVE-EGRESS` | 外发 + {CONFIDENTIAL, RESTRICTED, SECRET, UNKNOWN} | `REQUIRE_APPROVAL` | P0 | 2 人 |
| `HARNESS-P1-INTERNAL-EGRESS` | 外发 + 非敏感分类 | `REQUIRE_APPROVAL` | P1 | `DATA_APPROVER`，1 人 |
| `HARNESS-P1-UNBOUNDED` | 未声明运行预算（非 bounded） | `REQUIRE_APPROVAL` | P1 | `DATA_APPROVER`，1 人 |
| `HARNESS-P1-PRIVILEGED-MUTATION` | 工具 ∈ {`doris.rollback_release`, `secret.migrate_provider`, `secret.activate_rotation`, `secret.rollback_rotation`} | `REQUIRE_APPROVAL` | P1 | `DATA_APPROVER`，1 人 |
| `HARNESS-P1-SECRET-USE` | 含 `SECRET_USE`；`secret.stage_rotation`/`secret.validate_rotation` 恒 `ALLOW` | 生产 `REQUIRE_APPROVAL` / 非生产 `ALLOW` | P1 | 生产 1 人 |
| `HARNESS-BASELINE-ALLOW` | 未命中更高风险规则（兜底） | `ALLOW` | P3 | 无 |

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-9.2.1 | 聚合算法 | `outcome` 取最严（`ALLOW < REQUIRE_APPROVAL < DEFER < DENY`）；`risk_level` 取最高（P3<P2<P1<P0）；`required_roles` 取并集；`required_approvals = max(各命中规则)`。**执行全部命中规则而非首条返回**（审批人与审计员需要完整风险证据） |
| FR-9.2.2 | DENY 无放行路径 | 任一规则 `DENY` ⇒ 最终 `DENY`，且**清空** `required_roles` 与 `required_approvals`（避免调用方误以为补审批可覆盖 P0 拒绝） |
| FR-9.2.3 | DENY vs DEFER | `DENY` = 已有足够事实确认不允许；`DEFER` = PDP 当前无法得到可信决策（策略包不完整或外部 PDP 不可用）。**二者都不能进入执行**；`DEFER ≠ 暂时认为没问题`，而是可观测的失败关闭 |
| FR-9.2.4 | 策略包签名绑定 | 策略包清单（规则 ID、版本、引擎元数据）用 Ed25519 分离签名；**签名策略包的规则清单必须与内嵌 PDP 实现完全一致**，否则拒绝启动；请求上下文版本 ≠ 已验证包版本 ⇒ 拒绝 |
| FR-9.2.5 | 决策持久化 | 决策在同一事务落库，保存**请求指纹与规则证据**，不保存原始数据与凭据 |
| FR-9.2.6 | 工具效果由服务端决定 | 工具副作用以**服务端注册清单**为准，不以 Agent 声明为准；`intent.effects != manifest.effects` 视为意图被篡改或过期 |

#### FR-9.3 Approve（审批职责，而非点击次数）

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-9.3.1 | 五重校验（缺一不可） | ①申请人不能审批自己的 Preparation；②该职责必须属于本 Preparation 的 `required_roles`；③审批人必须**确实拥有**所声明的项目职责；④同一自然人不能填充多个职责槽位；⑤已被占用的职责槽不能被覆盖 |
| FR-9.3.2 | 审批满足条件 | 不能只判断"人数≥2"：必须同时满足**无任何 REJECT**、`len(approved) >= required_approvals`、`len(distinct people) >= required_approvals`、`required_roles ⊆ approved_roles` |
| FR-9.3.3 | 推进逻辑 | 任一 `REJECT` ⇒ `REJECTED`；全部满足 ⇒ `READY`；超过有效期 ⇒ `EXPIRED` |
| FR-9.3.4 | 审批证据 | 每条审批决定保存规范化证据文档摘要 |

#### FR-9.4 Commit（同一事务创建授权、Run 与 Outbox）

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-9.4.1 | 仅 `OPERATOR` 可提交 | 提交前复检审批是否满足 |
| FR-9.4.2 | 三重再校验 | 重新确认审批证据完整、策略包版本仍为当前版本、Artifact 摘要与版本状态未变化 |
| FR-9.4.3 | 同事务六件事 | ①记录能力声明摘要与签名（**只存 `token_sha256`，不持久化可重放的明文 Token**）；②原子消费授权（写 `consumed_at` + 持久化防重放守卫）；③创建 `ExecutionRun`；④创建 Outbox 命令；⑤Preparation 推进为 `COMMITTED`；⑥追加 `COMMITTED` 证据事件 |
| FR-9.4.4 | 事务提交后才唤醒 Celery | 若 API 在提交后/发送前崩溃，周期扫描会发现 `PENDING` Outbox 重投；**反之先发 Celery 再提交 DB 会让 Worker 收到数据库中不存在的 Run** |
| FR-9.4.5 | Commit 的成功语义 | 返回 202 + `ExecutionRun`；语义 = **授权已验证并消费 + 本地 Run 与待投递命令原子创建**，**不代表 SeaTunnel 已执行成功** |
| FR-9.4.6 | Capability 有效期 | `expires_at = min(preparation.expires_at, now + capability_ttl)`；默认 **5 分钟**，签发者硬上限 **15 分钟**；已过期则置 `EXPIRED` 并报冲突 |
| FR-9.4.7 | Commit 幂等 | Preparation 已 `COMMITTED` 时返回已有 Run（若 Run 缺失则报冲突） |

#### FR-9.5 Capability（一次性最小授权）

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-9.5.1 | 声明字段 | `token_id/project_id/subject_id/environment/policy_decision_id/policy_bundle_version/tool_name/tool_version/input_fingerprint/resource_fingerprint/artifact_sha256/supervision_contract_sha256/data_classification_report_sha256/allowed_mitigations/effects/issued_at/expires_at/single_use` |
| FR-9.5.2 | 只签最小权限 | Commit 阶段**不能只签一个 `allowed=true`**，否则令牌可能被挪到另一项目/工具/制品上使用；绑定字段必须逐项比对 |
| FR-9.5.3 | 密码学 | Ed25519；紧凑三段式 `base64url(header).base64url(claims).base64url(signature)`；header 固定 `alg=EdDSA`、`typ=ETL-HARNESS-CAPABILITY`、`kid=<key_id>`；生命周期硬上限 15 分钟；时钟偏差硬上限 60 秒 |
| FR-9.5.4 | 密钥职责分离 | **签发方持有私钥，验证方只持有公钥环**；生产缺显式私钥时**拒绝启动**，不得降级到开发密钥；非生产可在忽略目录自动生成并持久化开发密钥 |
| FR-9.5.5 | 单次使用 | `single_use` 标志只是前提；一次性由**持久化防重放守卫**原子消费 `token_id` 实现（首次成功、重复/并发失败）；进程内实现仅用于单测，不可用于生产 |
| FR-9.5.6 | 只减权的动作集 | `allowed_mitigations` 只允许"减损"动作，且**移除 `CONTINUE`**（继续运行不是额外授权） |

#### FR-9.6 Tool Broker（唯一执行出口）

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-9.6.1 | 注册即契约 | 工具只能由服务端注册（固定 Handler + Pydantic 输入/输出模型）；**同名工具禁止静默覆盖**；未知工具失败关闭，**不动态导入或猜测处理器** |
| FR-9.6.2 | 执行前五类重校验 | ①效果与注册清单一致；②输入指纹重算一致；③资源指纹重算一致；④全部冻结事实（调用 ID/项目/主体/工具名/工具版本/输入指纹/资源指纹/策略请求指纹/策略包版本/Artifact 摘要）与准备单一致；⑤有副作用或需审批的工具必须携带合法、未过期、匹配环境与策略版本的一次性 Capability |
| FR-9.6.3 | 检查顺序有意义 | 先证明工具存在且输入合 Schema → 比对指纹 → 处理策略与 Capability → **最后才调用 Handler**；**在 Handler 开始前不得读取生产密钥、建立写连接或创建远端作业** |
| FR-9.6.4 | 令牌需求判定 | `requires_token = (outcome == REQUIRE_APPROVAL) or (effects != [READ_ONLY])`；只读工具在 `ALLOW` 下可无 Token 执行，其余一律需要 Capability，缺失即失败关闭 |
| FR-9.6.5 | 只读也有风险 | 只读工具**仍**执行资源范围、数据分级与预算检查——"不写数据"不等于"没有风险"（无界全表扫描、跨项目读取、敏感样本回传模型都属风险） |
| FR-9.6.6 | 超时 | 由注册清单的 `timeout_seconds`（默认 30s，上限 3600s）控制，超时视为领域错误 |

#### FR-9.7 事务 Outbox

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-9.7.1 | 解决的问题 | 消除"DB 已提交但消息未发"与"消息已发但 DB 未提交"两个窗口 |
| FR-9.7.2 | 投递与认领 | 认领 `PENDING`/到期 `FAILED`/租约失效的 `SENDING`；`FOR UPDATE SKIP LOCKED`；按 `available_at, created_at` 排序；认领时置 `SENDING` 并记租约；**进程崩溃后的 `SENDING` 不会永久丢失** |
| FR-9.7.3 | 失败退避 | 失败置 `FAILED` 并指数退避（上限 300s），错误经脱敏并截断 |
| FR-9.7.4 | 幂等 | DB 唯一约束 `(command_type, aggregate_id)` 保证同一聚合命令不重复入队；分发**只走静态白名单**，禁止动态导入或任意任务名 |
| FR-9.7.5 | 诚实的投递语义 | Outbox 提供**"至少一次"可靠投递基础，不是"精确一次"**；下游必须用稳定标识、幂等键、能力单次消费与远端对账把重复收敛为业务上一次效果 |

#### FR-9.8 证据账本（Evidence Ledger）

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-9.8.1 | 事件与哈希链 | 事件含 `stream_id/sequence/occurred_at/actor_id/event_type/risk_level/payload/payload_sha256/previous_hash/event_hash/policy_decision_id/签名字段/trace_id`；每条事件链接上一条摘要形成哈希链；关键事件：`PREPARED`、`POLICY_DECIDED`、`APPROVAL_DECIDED`、`COMMITTED`、`OUTBOX_SENT` |
| FR-9.8.2 | 签名检查点 | 关键事件生成 Ed25519 分离签名；验证方只需公钥 |
| FR-9.8.3 | 追加式写入 + 行锁 | 对证据流头加行锁，保证序号、前序哈希与头指针原子更新 |
| FR-9.8.4 | 验证器只报告不修复 | 校验从第一条重算整条链（序号连续性、前序哈希、载荷哈希、事件哈希、签名、头指针一致）；**必须报告错误，不得自动重建或"修复"历史**，否则篡改会被平台自己覆盖 |
| FR-9.8.5 | 载荷安全白名单 | 禁止键：`api_key/ciphertext/hocon/password/passwd/private_key/prompt/raw_response/raw_rows/request_body/response_body/secret/token`；拒绝敏感值正则（`sk-*`、`bearer`、`password[:=]`）；**上限 64 KiB、嵌套深度 ≤ 20** |
| FR-9.8.6 | 诚实边界 | 哈希链可发现数据库内载荷/顺序/链头被改；若攻击者同时控制数据库与签名私钥，仍需 KMS/HSM 与独立存储域 |

### FR-10 执行、观测与失败诊断

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-10.1 | 唯一创建路径 | **旧版直接入队/直接重跑接口必须抛 `PermissionDenied`**；所有创建必须经 `prepare → approve（需要时）→ commit`；Preparation 与 Run 一对一 |
| FR-10.2 | 稳定作业标识 | 外部 JobId 由 Run UUID 低位派生为稳定正数；提交前**先查询认领**已存在的作业，确认不存在才提交——**关闭"HTTP 超时后创建第二个作业"的窗口** |
| FR-10.3 | 三种动作必须区分 | ①Celery 技术重试（同一 Run，制品不变）；②Harness 业务重跑（新 Run + `retry_of_run_id`，制品不变）；③Pipeline 修订（新版本 + 新 Run，制品改变）。**重跑必须沿用原 Artifact，不能重新生成配置冒充重试** |
| FR-10.4 | 幂等键 | `ExecutionRun.idempotency_key` 唯一；重复命中时比对版本/请求人/重跑来源/Preparation 四元组，任一不符即报冲突 |
| FR-10.5 | 提交前再校验 | 版本仍 `APPROVED`；摘要匹配的 HOCON 制品仍存在；水位版本必须带冻结窗口、批量版本不得夹带窗口；调度触发必须带调度 ID 与计划时间；当前用户仍具备执行角色 |
| FR-10.6 | 引擎 Port | 提交/查询/取消三个方法；业务层不依赖 HTTP URL、响应字段大小写或客户端生命周期；错误统一归一为引擎 API 错误 |
| FR-10.7 | 状态映射 | 引擎 `FAILED`→失败；`CANCELED`→取消终态；非 `FINISHED`→运行中；`FINISHED`→进入发布路径（监督允许 → 质量门禁 → 原子交换 → 成功） |
| FR-10.8 | 并发与限流 | 监控用自调度（默认 2s，节流时 5s）重新排队，**不长期占用 Worker 线程**；同一 Pipeline 存在活动执行时按策略跳过；副作用执行有租约，避免多 Worker 并发执行同一副作用 |
| FR-10.9 | 监控超时 | 状态查询在约 1 小时上限内按轮询重试；超过上限标记为可重试的可观测性失败，**不把"看不到"当作"已失败"或"已完成"** |
| FR-10.10 | 指标归一化 | 输出源/目标行数与字节数、表级指标；目标行数优先取"已提交"计数，缺失回退"写入"计数；**布尔值不得被当作整数**；原始指标仍保留用于排障 |
| FR-10.11 | 终态保护 | `SUCCEEDED/FAILED/CANCELLED` 为终态；**迟到的 Worker 错误或取消到达不得改写已确认的成功终态** |
| FR-10.12 | 取消语义 | 控制面先记 `CANCELLING`，**只有引擎快照确认 `CANCELED` 才进入 `CANCELLED`**；调用取消接口成功 ≠ 作业已停止 |
| FR-10.13 | 先脱敏再分类 | 持久化错误前清除密码/密钥/Token/连接串/Bearer 等模式并截断（上限 10000 字符）；**脱敏发生在分类之前**，诊断与审计都拿不到原始秘密 |
| FR-10.14 | 确定性失败分类 | 规则引擎按固定优先级分类为 `CONFIGURATION/CREDENTIALS/CONNECTIVITY/SOURCE_SCHEMA/SINK_WRITE/ENGINE/OBSERVABILITY/CANCELLATION/UNKNOWN` × 阶段 `SUBMISSION/SOURCE/TRANSFORM/SINK/MONITORING/CANCELLATION/PUBLISH/UNKNOWN`，并给出 `retryable` 与面向用户的摘要；**未命中任何规则时进入人工诊断，不允许模型猜乐观结论** |

### FR-11 质量契约与 Doris 原子发布

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-11.1 | 单次读取、双路隔离 | 质量链路**只读取一次 Source**，用**至少两个带 `WHERE` 的精确 `Sql {}` Transform** 分出有效行与拒绝行；**禁止 `Split` Transform**，**禁止用重复 Source 查询伪装双路** |
| FR-11.2 | 可追溯 | 有效 Sink 与错误 Sink 必须不同且都出现在待审批制品中；拒绝分支必须输出 `error_reason`；每条规则的错误码必须出现；v2 的 `invalid_when` 必须与 `WHEN ... THEN '<code>'` **精确绑定**（"错误码存在但谓词绑定错误"同样失败） |
| FR-11.3 | 不制造安全幻觉 | 门禁只规范空白与最外层冗余括号，**不尝试证明任意 SQL 逻辑等价** |
| FR-11.4 | 质量报告 | `status/total_rows/passed_rows/rejected_rows/reject_rate/valid_sink/error_sink/rules/diagnostics`；指标缺失（含"有效行+隔离行 ≠ 输入行"）→ `INCOMPLETE`；超过拒绝行数或拒绝率阈值 → `FAILED`；有拒绝未超阈值 → `WARNING`；零拒绝 → `PASSED` |
| FR-11.5 | 引擎成功不等于质量通过 | **质量报告不会因为 SeaTunnel 作业退出码为 0 就自动判定通过** |
| FR-11.6 | 运行级影子表 | 影子表名 = `fp_stage_` + 最终表片段 + `_` + Run 十六进制后缀（总长 ≤ 64），DDL 前先过标识符白名单；影子表在提交阶段创建并写入归属标记 |
| FR-11.7 | 原子交换 | 交换前校验影子表标记与 Schema 一致；交换后复查标记；`PublishStatus` 含 `ALREADY_PUBLISHED` 等幂等态——**交换后崩溃的重试不再交换** |
| FR-11.8 | 发布门禁 | 启用原子交付时质量状态必须 ∈ {`PASSED`, `WARNING`}；**被隔离的运行禁止发布** |
| FR-11.9 | 幂等发布事件 | `STAGING_PREPARED` 与 `ATOMIC_PUBLISHED` 按事件类型幂等；发布事件冻结库名、最终表、回滚表 |
| FR-11.10 | 制品存储安全 | 制品路径内容寻址（`pipelines/<version_id>/<sha256>.conf`）；存储层拒绝绝对路径与目录穿越 |
| FR-11.11 | 物化顺序 | ①注入本次调度水位窗口；②若启用原子交付则准备影子表并只改写唯一有效 Sink；③注入逐行 LLM 密钥；④解析数据库/HDFS 凭据占位符；⑤确认无未解析秘密变量后才提交。**物化后的 HOCON 只存在于 Worker 内存与发往引擎的请求中，不回写制品/plan/图状态** |
| FR-11.12 | 矛盾事实拒绝自动重试 | 若 Doris 标记显示已发布但引擎无对应 Job，必须**拒绝重新写入**并要求人工对账 |

### FR-12 统一副作用治理与高风险运维

**目标**：把**全部已支持副作用**收敛到同一个通用 Harness，不允许旁路。

**副作用闭集（9 类，`SideEffectOperation` `【代码】`）**：

| 操作 | 外部效果 | 效果标签 | 可逆性 | 治理要求 |
|---|---|---|---|---|
| `CANCEL_EXECUTION` | 停止 SeaTunnel 作业 | `WRITE` | 可对账 | 能力令牌 + 租约 |
| `ROLLBACK_DORIS` | 交换回上一版表 | `SECRET_USE`+`WRITE` | 向前恢复 | P1，独立数据审批 |
| `CLEANUP_DORIS` | **删除保留表** | `DESTRUCTIVE`+`SECRET_USE` | **不可逆** | P0，双人审批 |
| `MIGRATE_SECRET` | 跨 Provider 复制并切换引用 | `SECRET_USE`+`WRITE` | 保留源引用 | P1，独立数据审批 |
| `STAGE_SECRET_ROTATION` | 写入新 Secret 暂存版本 | `SECRET_USE`+`WRITE` | 可对账孤立版本 | P1，可由能力自动执行 |
| `VALIDATE_SECRET_ROTATION` | 只读验证新 Secret | `READ_ONLY`+`SECRET_USE` | — | P1，可由能力自动执行 |
| `ACTIVATE_SECRET_ROTATION` | **原子切换活动引用** | `SECRET_USE`+`WRITE` | 保留回退窗口 | P1，独立数据审批 |
| `ROLLBACK_SECRET_ROTATION` | 恢复旧引用 | `SECRET_USE`+`WRITE` | 保留两版本 | P1，独立数据审批 |
| `DESTROY_SECRET` | **删除废弃 Secret** | `DESTRUCTIVE`+`SECRET_USE` | **不可逆** | P0，双人审批 |

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-12.1 | 两层职责不可省 | **业务申请层**从数据库真相源解析并冻结操作特有事实（客户端不能自报"这是低敏项目"或塞入他项目资源 ID）；**通用 Harness 层**负责策略、职责审批、能力签发与消费、Outbox/受控同步 Handler、证据记录。只保留前者会出现旁路；只保留后者无法在执行前重建真实业务状态 |
| FR-12.2 | 统一流水线 | 业务 API → 领域申请（冻结操作特有事实）→ 通用 Preparation → PDP → 独立职责审批 → Commit（验签并消费能力）→ Outbox / 受控同步 Handler → Worker（再次验签、租约、外部调用）→ 领域结果 + 证据账本 |
| FR-12.3 | 封闭性约束 | 未在「枚举 + 规格表 + Outbox 静态路由」三处**同时注册**的操作不可执行；不能通过传入字符串"临时支持"。新增能力必须明确：工具名、效果集合、风险、命令类型、公开载荷、幂等方式、补偿语义，并补齐对抗测试 |
| FR-12.4 | 公开载荷约束 | 命令载荷只保存**可公开持久化**的内容（操作、聚合类型/ID、幂等键、冻结事实摘要、命令载荷）；敏感正文只用摘要绑定；Commit 结果不返回密码与 Provider 内部路径 |
| FR-12.5 | 同步/异步不可混用 | 有命令类型的操作走 Outbox；无命令类型（如"暂存轮换"，新口令来自本次 HTTPS 请求）走受控同步 Handler，且只保留 Secret 引用摘要与受管请求 ID，不记录明文 |
| FR-12.6 | Worker 五重检查 | Preparation 已 `COMMITTED`；Ed25519 签名通过；声明与 Preparation 全字段一致；命令载荷摘要与冻结载荷一致；租约可认领或已超时 |
| FR-12.7 | 执行租约 | 租约区间可配；未过期时抛**专用租约占用异常**并附 `retry_after_seconds`，**不得误记为领域失败** |
| FR-12.8 | 能力过期语义 | 过期后**禁止首次接触外部系统**；已开始且部分成功时，租约超时后允许同一冻结授权继续恢复（非扩权） |
| FR-12.9 | Doris 回滚前提 | 来源 Run 必须 `SUCCEEDED` 且存在 `ATOMIC_PUBLISHED` 事件；执行前再验"最终表仍带来源 Run 发布标记、回滚表存在且 Schema 一致"；已被后来发布替换则旧回滚申请失效 |
| FR-12.10 | Doris 清理前提 | 必须超过 `published_at + 保留天数`（默认 7 天），未到期拒绝并给出最早可申请时间；回滚与清理以来源 Run 行锁**互斥**；清理完成后**永久禁止回滚** |
| FR-12.11 | 安全删除双重证明 | 表名必须匹配影子表命名规则**且**表注释带来源 Run 标记，二者齐备才允许删除；删除后复查仍存在则报错 |
| FR-12.12 | Secret 迁移不销毁源 | 外部写入成功但 DB 切换失败时，目标成为可对账的孤立版本，源连接仍可用 |
| FR-12.13 | 负向测试要求 | 负向测试必须证明**外部 Adapter 未被调用**，而不只是 API 返回 4xx |

### FR-13 运行时安全监督与数据分级

**目标**：审批只能证明"制品在提交前满足规则"；作业运行后真实扫描量可能远超样本估计。必须在风险扩大时及时**只收窄权限**地介入。

**监督输入**：只读取控制面已取得的**脱敏聚合指标**（引擎状态、源/目标行数与字节数、表级指标）+ 冻结预算 + 契约摘要 + 起止时间。**不访问数据源或 Doris**。

**确定性规则（6 条 + 兜底）**：

| 命中条件 | 规则 ID | 风险 | 动作 | 阻断发布 |
|---|---|---|---|---|
| 记录数 > `max_records` | `SUPERVISOR-P0-RECORD-BUDGET` | P0 | `QUARANTINE` | 是 |
| 字节数 > `max_bytes` | `SUPERVISOR-P0-BYTE-BUDGET` | P0 | `QUARANTINE` | 是 |
| 执行时长 > `timeout_seconds` | `SUPERVISOR-P1-TIMEOUT` | P1 | `CANCEL` | 是 |
| 输出/输入行数比 > `max_output_ratio` | `SUPERVISOR-P0-OUTPUT-AMPLIFICATION` | P0 | `QUARANTINE` | 是 |
| 达最小样本后拒绝率 > `max_reject_rate` | `SUPERVISOR-P1-REJECT-RATE` | P1 | `PAUSE` | 是 |
| 接近上限（≥ `warning_ratio` 且未越限） | `SUPERVISOR-P3-BUDGET-WARNING` | P3 | `THROTTLE`（轮询 2s→5s） | 否 |
| 未命中任何规则 | `SUPERVISOR-BASELINE-CONTINUE` | P3 | `CONTINUE` | 否 |

**动作优先级（写在代码中，不交模型权衡）**：`QUARANTINE > CANCEL > PAUSE > THROTTLE > CONTINUE`；风险等级取最高。

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-13.1 | 预算七维与硬上限 | `max_records`（默认 1000 万）、`max_bytes`（默认 10 GiB）、`timeout_seconds`（默认 3600）、`max_output_ratio`（默认 2.0）、`max_reject_rate`（默认 0.5）、`minimum_quality_rows`（默认 100）、`warning_ratio`（默认 0.8）；每维有绝对平台上界（防无界整数） |
| FR-13.2 | 预算不可扩权 | Preparation 只能**收紧**平台上限；plan 缺预算时用平台上限并标记需人工审批 |
| FR-13.3 | 动作自洽校验 | `CONTINUE` 不得同时请求取消或阻断发布；非 `CONTINUE` 必须携带确定性规则证据；请求未授权的减损动作 ⇒ 拒绝执行（**不得偷偷换成更弱动作**） |
| FR-13.4 | 强动作立即生效 | 隔离（阻断发布）**立即写入运行状态，不等外部取消成功才生效**；取消走独立减损任务，以精确 JobId 调用引擎，**不修改版本或 HOCON，不转移到未审批 Sink** |
| FR-13.5 | 每次观察前重新验签 | 重新验证已消费的执行能力，比对监督契约摘要、分类报告摘要、允许减损动作清单与项目/环境/工具/制品四组事实；**令牌过期后仍允许减损（只收窄），但不允许再次提交或扩权** |
| FR-13.6 | 监督决策落库 | 快照序号递增 + 运行状态 + 执行事件 + 哈希链证据**同事务**落库；快照含 `snapshot_sha256`、`contract_sha256` 与 `mitigation_status` |
| FR-13.7 | 数据分级传播 | 分级 `PUBLIC < INTERNAL < CONFIDENTIAL < RESTRICTED = UNKNOWN < SECRET`；按字段名/类型/注释/Profile 明示标签/字段映射综合判断，**按最高等级传播**（同名字段 + 显式映射），源端 PII/Secret 不会在目标端被静默降级；**Profile 缺失或无法确认时记为 `UNKNOWN` 并失败关闭，不默认 `PUBLIC`** |
| FR-13.8 | 分类时机 | Preparation 先对冻结 Profile Schema 生成分类报告，再签运行监督契约（契约只存报告摘要，不把字段样本复制进每个快照） |
| FR-13.9 | 不得进入模型上下文的数据 | 分级为 `SECRET/RESTRICTED/CONFIDENTIAL/UNKNOWN` 的原始字段样本、Secret 明文/密码/Token/私钥、Capability 文本、原始 HOCON/SQL/错误日志/行样本/用户名、Provider 内部路径。可进入上下文/持久化的仅为：摘要、聚合指标、规则 ID、脱敏消息、引用或请求 ID |
| FR-13.10 | 面向用户的话术 | 必须支持"运行结果已隔离，禁止发布：{原因}"等可解释表达；必须明确说明"引擎 `FINISHED` 但业务运行 `FAILED` 且结果隔离"**不是状态矛盾**（引擎真相 = 计算进程结束；控制面真相 = 结果未获发布授权） |
| FR-13.11 | THROTTLE 诚实描述 | 节流降低的是**控制面观测频率**，不是引擎吞吐，**不可展示成"已降低数据读取速率"**（会给运维错误安全感） |
| FR-13.12 | 当前实现边界（不得夸大） | 缺单次指标 → 继续观察（fail-open）；最终质量指标缺失 → 原子发布失败关闭；**无独立 `STALLED` 停滞规则**；**无独立部分成功引擎状态**；**生产裁决完全由确定性规则完成，无 LLM 监督 Agent**；PAUSE 当前实为强制取消 + 阻断发布（引擎无可恢复 Pause API） |

### FR-14 调度与水位增量

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-14.1 | 调度模型 | 计划绑定唯一 Pipeline 且只能绑定**已审批**版本；`cron` 必须为有效五段表达式，时区必须为有效 IANA 名称 |
| FR-14.2 | 切换版本重置水位 | `upsert` 切换绑定版本时必须**重置水位**（新增量世代，不沿用旧游标） |
| FR-14.3 | 到期扫描 | `enabled AND next_fire_at <= now` 按序限量认领，`FOR UPDATE SKIP LOCKED` 保证多扫描器不抢同一行 |
| FR-14.4 | 跳过语义 | 超宽限期且策略为 `SKIP` → 记 `MISFIRE_SKIPPED`；存在活动执行 → `ACTIVE_EXECUTION_SKIPPED`；水位窗口为空 → `EMPTY_WATERMARK_WINDOW_SKIPPED` |
| FR-14.5 | 调度不直接执行 | 调度**只创建 Preparation**：`READY` 则同事务 Commit；`WAITING_APPROVAL` 记待审批；其他状态或领域错误则**关闭调度并记录脱敏原因**（失败关闭，等待人工修复） |
| FR-14.6 | 水位窗口语义 | `(lower, upper]`——**下界开、上界闭**，保证相邻成功窗口无重复且不漏数；上界 = 计划时刻 − `lag_seconds` |
| FR-14.7 | 制品不冻结具体时间 | 冻结 HOCON 只保存上下界 Token，**不保存某一次调度的具体时间**；Worker 创建运行时副本后才替换 Token，审批制品与数据库记录保持不变 |
| FR-14.8 | 只在成功后推进 | 只有运行最终成功**且调度仍绑定同一版本**才推进水位到本次上界；失败重试复用同一窗口；无可前进区间则跳过执行；**旧版本迟到成功不污染新调度世代** |
| FR-14.9 | 补跑语义 | `FIRE_ONCE` 允许宽限期内补跑一次；恢复调度从**当前时间**重算下次触发，**不补跑无限历史窗口** |

### FR-15 仿真、Benchmark 与受控安全进化

#### FR-15.1 三层验证体系

| 层级 | 对象 | 允许真实外部副作用 | 用途 |
|---|---|---|---|
| **L0** | 策略、HOCON 静态检查 | 否 | 攻击意图、连接器注入、明文凭据、正常制品 |
| **L1** | 运行监督、能力、适配器故障分类 | 仅内存或受控替身 | 超时、输出放大、拒绝率、重复消费、部分成功、未知状态 |
| **L2** | MySQL/文件 → SeaTunnel → Doris | 是（仅一次性环境） | 真实协议、真实提交、真实 Sink 与清理路径 |

#### FR-15.2 Benchmark 六项冻结门槛（默认值 `【代码】`）

| 指标 | 默认阈值 | 含义 |
|---|---:|---|
| `p0_p1_block_rate` | **100%** | 所有高风险非正常案例必须被拒绝或要求审批 |
| `legitimate_success_rate` | **≥95%** | 合法受限操作不能被普遍误伤 |
| `false_positive_rate` | **≤5%** | 正常案例被阻断比例受控 |
| `evidence_coverage_rate` | **100%** | 每个案例必须产出可审计规则证据 |
| `reproducibility_rate` | **100%** | 同一案例重复执行的观察结果一致 |
| `non_llm_p95_max_regression` | **≤20%** | 相对冻结基线的确定性执行延迟不明显退化 |

**统计口径**：P0/P1 阻断率只统计"高风险且非合法"案例；合法成功率与误报率只统计合法案例；证据覆盖与可复现覆盖全部案例。

#### FR-15.3 冻结场景（14 个：L0 九个、L1 五个）

`policy.legitimate_bounded_read`、`policy.cross_project_scope`、`policy.secret_exfiltration`、`policy.destructive_requires_two_people`、`policy.unbounded_scan`、`hocon.legitimate_secret_placeholder`、`hocon.inline_plaintext_secret`、`hocon.connector_injection`、`hocon.unsupported_timestamp_cast`、`supervision.normal_continue`、`supervision.timeout_cancel`、`supervision.output_amplification_quarantine`、`supervision.reject_rate_pause`、`supervision.near_budget_throttle`。

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-15.3.1 | 防作弊 | 一个套件必须**同时**包含合法案例与非合法的 P0/P1 案例——禁止"空分母得 100%"与"全拒绝策略"刷分；案例 ID 必须唯一 |
| FR-15.3.2 | 白名单执行器 | 执行器分派只按枚举白名单（`POLICY_INTENT`/`STATIC_HOCON`/`RUNTIME_SUPERVISION`）；场景文件是**仓库数据而非可执行代码**，不允许指定 Python 模块、Shell、URL 或任意函数 |
| FR-15.3.3 | 确定性重放 | 每个案例连续执行两次，**业务观察结果必须完全一致**；规则证据非空；L0/L1 案例声明零外部副作用且实际未产生副作用 |
| FR-15.3.4 | 门禁判定 | 最终通过 = 「所有案例期望匹配」**且**「所有汇总指标达标」；**Pn 高危漏拦不得被高分抵消** |
| FR-15.3.5 | 基线治理 | 基线为版本化文件，不绑定机器绝对吞吐，只固定相对回归阈值；运行前强校验套件 ID 与清单版本一致，否则报错（防拿旧阈值验证新策略或跨套件冒充证据）；**修改阈值本身属于治理变更**，必须进入基线差异审查 |
| FR-15.3.6 | 报告产物 | 一次运行原子写出三份产物：机器可读 JSON、供 CI 消费的 JUnit XML、供评审阅读的 Markdown；门禁失败返回非零退出码 |
| FR-15.3.7 | CI 分离 | 常驻工作流跑静态检查、类型检查、依赖审计、全量测试、L0、L1 与 Benchmark，报告无论成败都上传；L2 由独立工作流在一次性环境按计划或人工触发，失败上传诊断并**总是销毁环境与卷**；**历史 CI 通过不能替代当前提交的重新验证** |

#### FR-15.4 受控安全进化

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-15.4.1 | 只进化三类候选 | `SafetyProposalKind` = `POLICY` / `PROMPT` / `BENCHMARK`；候选**不是可执行代码或部署命令**；**不存在后台 Agent 自动改代码、写策略文件、切换流量或热更新 Prompt** |
| FR-15.4.2 | 候选数据泄露边界 | 递归检查候选：上限 64 KiB、嵌套深度 ≤ 20、拒绝疑似凭据键与疑似密钥正文；模板化 Prompt 允许，真实 API Key/口令/原始样本/大段日志禁止；**这是最低防线，不是完整 DLP** |
| FR-15.4.3 | 创建者不能自批 | 创建者只能提交自己的草稿，**不能裁决、启动或推广自己的候选**——即使其后来拥有 `SECURITY_APPROVER` 职责也不允许 |
| FR-15.4.4 | 冻结证据绑定 | 影子/小流量阶段必须绑定**同一套件、同一版本、同一策略包且状态为通过**的 Benchmark；候选目标版本必须等于 Benchmark 实际策略包版本，否则报冲突 |
| FR-15.4.5 | 不可补偿硬门禁 | 出现任一情况即自动回滚：冻结 Benchmark 未通过；`p0_p1_regressions != 0`；现场误报率超过冻结阈值；执行错误 ≠ 0。**没有"其他指标很好就抵消一个 P0 回归"的加权平均** |
| FR-15.4.6 | 签名发布清单 | 启动影子时生成并 Ed25519 签名的发布清单，冻结 release/proposal/candidate/benchmark/policy 版本等摘要；**不保存可被替换的报告 URL** |
| FR-15.4.7 | 小流量硬限制 | 小流量比例硬限制 **1%–10%**（服务层 + 数据库 CHECK 双重兜底） |
| FR-15.4.8 | 推广的诚实语义 | `PROMOTION_APPROVED` **只是签名推广授权**，不会热修改策略/Prompt/代码/运行中作业；治理服务证明"可以推广"，部署服务负责"如何推广"，两者之间仍需验签、幂等、回滚与环境隔离。**数据库状态为"影子运行中"不等于流量已真正复制** |
| FR-15.4.9 | 观察数据自洽 | 观察记录含样本量、P0/P1 回归数、策略误报数、执行错误数；**三个计数值不得超过样本量**（防伪造不自洽证据）；只保存脱敏聚合事实 |
| FR-15.4.10 | 报告入库与证据链 | 未通过的报告**也必须保存**（失败历史是排查规则回归的证据）；报告生成后追加证据事件（失败 P1、通过 P3，带签名检查点）；发布流程比对套件/清单/基线/策略包/报告摘要，防用另一份通过报告替换当前证据 |

### FR-16 Web 控制台

**定位**：控制台是控制面的可视化入口；**真相源始终是 PostgreSQL**，浏览器只保存当前工作区与操作者选择，不保存制品、审批结果、Capability、运行状态或 Secret；刷新后从 API 恢复事实。

**五个视图**：

| 视图 | 核心任务 | 主要角色 | 展示内容 | 可执行操作 |
|---|---|---|---|---|
| **总览** | 查看项目概况并进入目标切片 | 全部成员 | 到岸指标、就绪连接、成功/失败执行、待办 Preparation | 进入 Studio |
| **数据连接** | 连接、探查、文件与 Secret 生命周期 | 数据工程师、管理员、审批人 | 连接卡片与状态、Profile 列表与脱敏样本、受管文件列表、迁移/轮换/回退/销毁摘要 | 登记/测试连接、创建 Profile（SOURCE/SINK）、上传文件、发起并审批 Secret 生命周期操作 |
| **Pipeline Studio** | 描述需求、生成/修订、提交审批、配置调度 | 数据工程师、数据审批人 | 生成 Provider 就绪告警、Agent 控制台时间线、生成可观测性、方案审查（DAG / Schema Diff / 风险 / 脱敏 Before / 结构预览 After / 冻结 HOCON） | 填写需求与契约（质量/水位/逐行 LLM）、选择 Profile、生成、回答澄清、提交与批准审批、生成 Preparation、职责审批、一次性 Commit、创建修订、配置/暂停调度 |
| **运行中心** | Preparation、执行、指标、诊断、回滚、清理、告警、Worker | 操作员、审批人、审计员 | 凭据到期预警与告警投递、轮换治理清单、Doris 回滚保留清单、受管副作用控制面、执行台账与详情（监督快照/失败诊断/质量报告/事件时间线）、Worker 健康 | 刷新、取消执行、原产物重跑、申请回滚/清理并四眼审批、以指定职责审批 Preparation、Commit、告警确认/静默/解除静默 |
| **安全治理** | Benchmark 与安全进化 | 安全审批人、审计员 | 门禁指标卡与版本间 delta、版本化报告列表与证据、提案审查列表、签名影子/小流量卡片 | 运行 Benchmark、创建/提交/裁决提案、启动与验证影子、启动 5% 小流量、验证小流量、签署推广、撤销/回滚 |

| 编号 | 需求 | 规则约束 |
|---|---|---|
| FR-16.1 | 统一 API 边界 | 前端统一拼 `/api/v1`；JSON 自动设 `Content-Type`，multipart 让浏览器生成 boundary；非 2xx 转为可显示的领域错误（含 `trace_id`） |
| FR-16.2 | 职责分离的 UI 阻断 | 审批区必须显式提示"当前身份是审批申请人，不能自批"；执行 Commit 按钮仅在具备 `OPERATOR` 职责时可用 |
| FR-16.3 | 本地身份模拟的边界 | 前端注入身份头仅供本地演示；**生产禁止 `dev-header`**，必须由可信网关签名，且 Nonce 存储在不可用时失败关闭 |
| FR-16.4 | 不夸大状态 | 界面不得把 Commit 成功展示为"ETL 成功"；不得把节流展示为"降低读取速率"；不得把推广授权展示为"已自动更新生产" |
| FR-16.5 | 可维护性 | 当前前端集中于单个大文件（约 2086 行），**后续必须按业务域拆分页面、组合式函数与 API 模块并补组件测试**，不把"大文件能跑"当作理想架构 |

---

## 6. 端到端主流程（Golden Path）

### 6.1 一次受管执行的 11 步事实链（MySQL → Doris 主线）

1. **需求**：数据工程师在当前项目选择 MySQL Source 与 Doris Sink，提交业务需求与质量规则。
2. **探查**：Profile 服务用受限查询获取 Schema、近似统计与脱敏样本；**不执行无界全表 `COUNT`，不把密码交给模型**。
3. **生成**：缺信息时持久化中断；信息完整后调用模型生成候选（HOCON / plan）。
4. **门禁**：确定性门禁检查连接器、DAG、字段、SQL 分流、质量错误码、Secret 占位符与 Doris 投影；**真实 SeaTunnel 检查通过后才能冻结制品**。
5. **审批**：另一位项目成员审查需求、DAG、Schema Diff、脱敏前后、质量契约与原始配置，批准这个不可变版本。
6. **准备**：Harness Prepare 从服务端推导资源范围、数据分级、运行预算、制品摘要与回滚路径；PDP 决定风险与职责要求。
7. **提交**：所有必需职责完成审批后，操作员 Commit；Tool Broker 验证指纹与 Capability，**在一个 PostgreSQL 事务中创建 Run 与 Outbox**。
8. **验签**：Worker 在读取 Secret 和接触 SeaTunnel 前**再次用公钥验证能力声明**，检查已冻结制品与监督契约。
9. **执行与监督**：SeaTunnel 把有效数据写入运行级影子表、无效数据写入错误表；运行监督持续检查记录数、字节数、时长、输出放大与拒绝率。
10. **发布**：质量报告通过后，Doris 用原子交换发布有效表；旧表按受管名称保留，后续回滚或清理仍必须进入 Harness。
11. **留证**：策略决策、准备、审批、授权、投递、运行与发布的关键事件进入**不含敏感原文的证据链**，供回放、审计与 Benchmark 使用。

### 6.2 Golden Case（订单清洗）

**源表** `source_db.orders`（8 行固定教学数据，`order_id` 1001–1008）：

| 字段 | 类型 |
|---|---|
| `order_id` | BIGINT PK |
| `customer_name` | VARCHAR(100) |
| `amount` | DECIMAL(12,2) NOT NULL |
| `status` | VARCHAR(20) NOT NULL |
| `email` | VARCHAR(255) |
| `created_at` | DATETIME NOT NULL |

**清洗契约**：

| 规则 | 有效条件 | 失败错误码 |
|---|---|---|
| 订单标识 | `order_id` 非空 | `ORDER_ID_REQUIRED` |
| 客户名称 | 非空且去首尾空格后仍有内容 | `CUSTOMER_REQUIRED` |
| 订单金额 | 非空且 > 0 | `AMOUNT_POSITIVE` |
| 订单状态 | `PAID` 或 `SHIPPED` | `STATUS_ALLOWED` |
| 邮箱处理 | `@` 前本地部分替换为 `***` | 不作为拒绝条件 |

**预期结果（验收基准）**：1001–1004 进入 `analytics.clean_orders`；1005（状态不允许）、1006（金额为负）、1007/1008（客户名称无效）进入错误表；`Alice` 两侧空格被删除；`alice@example.com` → `***@example.com`。

---

## 7. 数据模型与状态机

### 7.1 数据表清单（41 张业务表 `【代码】`）

公共约定：所有表含 `created_at/updated_at`；枚举以**非原生字符串**存储（便于升级与回滚）；大制品只存 URI + 摘要。

| 分组 | 表 |
|---|---|
| 身份/项目 | `users`、`projects`、`project_memberships`、`project_role_grants` |
| 连接/密钥 | `data_connections`、`encrypted_secrets`、`secret_migration_requests`、`secret_rotation_requests`、`secret_rotation_rollback_requests`、`secret_destruction_requests`、`credential_expiry_alerts`、`credential_alert_deliveries` |
| 文件/探查 | `data_profiles`、`file_assets` |
| 契约/生成 | `pipelines`、`pipeline_versions`、`pipeline_artifacts`、`agent_runs`、`pipeline_schedules` |
| 审批/执行 | `approval_requests`、`execution_runs`、`execution_events`、`quality_reports`、`runtime_supervision_snapshots`、`rollback_requests`、`rollback_cleanup_requests`、`worker_heartbeats` |
| Harness 内核 | `harness_policy_decisions`、`execution_preparations`、`harness_approval_decisions`、`harness_capability_grants`、`harness_side_effect_preparations`、`harness_side_effect_approvals`、`harness_side_effect_capability_grants`、`harness_command_outbox`、`harness_evidence_streams`、`harness_evidence_events` |
| 评测/治理 | `audit_events`、`benchmark_runs`、`safety_improvement_proposals`、`safety_releases` |

**关键表与约束**：

| 表 | 关键字段 | 关键约束 |
|---|---|---|
| `pipeline_versions` | `version_number`、`status`、`requirement`、`parent_version_id`、`plan`、`artifact_sha256`、`lock_version` | `UNIQUE(pipeline_id, version_number)`；乐观并发 |
| `agent_runs` | `thread_id`、`status`、契约字段、生成观测字段 | `thread_id` 唯一；观测字段只存计数与安全诊断，**不存 Prompt/候选正文/凭据** |
| `execution_runs` | `harness_preparation_id`、`status`、`idempotency_key`、`seatunnel_job_id`、失败诊断、监督动作、水位窗口、尝试次数 | `harness_preparation_id` 唯一；`idempotency_key` 唯一；重试新建行并记 `retry_of_run_id` |
| `execution_preparations` | 指纹、影响报告、回滚方案、所需职责、有效期、状态 | `UNIQUE(project_id, idempotency_key)`；策略决策一对一 |
| `harness_approval_decisions` | 职责、决定、证据摘要 | `UNIQUE(preparation_id, role)`、`UNIQUE(preparation_id, decided_by)` |
| `harness_capability_grants` | `token_sha256`、`key_id`、`claims`、`consumed_at` | **只存 Token 摘要**，不存可重放明文 |
| `harness_command_outbox` | `command_type`、`aggregate_id`、`status` | `UNIQUE(command_type, aggregate_id)` |
| `harness_evidence_events` | `sequence`、`previous_hash`、`event_hash`、签名 | `UNIQUE(stream_id, sequence)`、`event_hash` 全局唯一 |
| `audit_events` | 主体、动作、资源、前后值、`trace_id` | **不保存密码/Vault 路径/Prompt/样本/外部错误正文** |

**迁移**：33 个 Alembic 版本，单一 head，无分叉；**禁止用应用启动时的自动建表**。

### 7.2 枚举字典（完整 `【代码】`）

| 枚举 | 成员 |
|---|---|
| `ProjectRole` | `ADMIN` / `DATA_ENGINEER` / `APPROVER` / `VIEWER` |
| `HarnessRole` | `DATA_ENGINEER` / `DATA_APPROVER` / `SECURITY_APPROVER` / `OPERATOR` / `AUDITOR` |
| `PipelineVersionStatus` | `DRAFT` / `PROFILING` / `GENERATED` / `VALIDATED` / `WAITING_APPROVAL` / `APPROVED` / `REJECTED` |
| `ApprovalStatus` / `ApprovalDecisionValue` | `PENDING`/`APPROVED`/`REJECTED`；`APPROVE`/`REJECT` |
| `AgentRunStatus` | `QUEUED` / `RUNNING` / `WAITING_INPUT` / `WAITING_APPROVAL` / `FAILED` |
| `GenerationKind` | `question` / `config` / `error` |
| `ConnectorType` | `MYSQL` / `DORIS` / `POSTGRESQL` / `ORACLE` / `SQL_SERVER` / `HIVE` / `HDFS` / `LOCAL_FILE` |
| `ConnectionStatus` | `UNTESTED` / `TESTING` / `READY` / `FAILED` |
| `DataProfileStatus` | `PENDING` / `RUNNING` / `READY` / `FAILED`（Profile 与文件资产共用） |
| `DataEndpointRole` | `SOURCE` / `SINK` |
| `FileAssetFormat` | `CSV` / `JSON` / `JSONL` / `XLSX` / `PARQUET` / `ORC` / `XML` / `MARKDOWN` / `PDF` |
| `SecretMigrationStatus` / `RollbackStatus` / `CleanupStatus` / `SecretRotationRollbackStatus` / `SecretDestructionStatus` | `PENDING` / `APPROVED` / `REJECTED` / `EXECUTING` / `SUCCEEDED` / `FAILED` |
| `SecretRotationStatus` | `VALIDATING` / `WAITING_APPROVAL` / `APPROVED` / `REJECTED` / `ACTIVATING` / `SUCCEEDED` / `FAILED` |
| `SecretDestructionTarget` | `ROTATION_SOURCE` / `ROLLED_BACK_STAGED` |
| `CredentialAlertLevel` / `Status` / `DeliveryStatus` | `WARNING`/`OVERDUE`；`OPEN`/`RESOLVED`；`PENDING`/`SENDING`/`SUCCEEDED`/`FAILED`/`SKIPPED` |
| `RiskLevel` | `P0` / `P1` / `P2` / `P3` |
| `PolicyOutcome` | `ALLOW` / `DENY` / `REQUIRE_APPROVAL` / `DEFER` |
| `ToolEffect` | `READ_ONLY` / `WRITE` / `DESTRUCTIVE` / `EXTERNAL_EGRESS` / `SECRET_USE` |
| `DataClassification` | `PUBLIC` / `INTERNAL` / `CONFIDENTIAL` / `RESTRICTED` / `SECRET` / `UNKNOWN` |
| `ExecutionPreparationStatus` | `DENIED` / `WAITING_APPROVAL` / `READY` / `REJECTED` / `EXPIRED` / `COMMITTED` |
| `HarnessApprovalValue` | `APPROVE` / `REJECT` |
| `HarnessOutboxStatus` | `PENDING` / `SENDING` / `SENT` / `FAILED` |
| `ExecutionStatus` | `QUEUED` / `SUBMITTED` / `RUNNING` / `SUCCEEDED` / `FAILED` / `CANCELLING` / `CANCELLED` |
| `ExecutionTrigger` | `MANUAL` / `SCHEDULED` |
| `ScheduleMode` / `ScheduleMisfirePolicy` | `BATCH`/`WATERMARK_INCREMENTAL`；`SKIP`/`FIRE_ONCE` |
| `FailureCategory` | `CONFIGURATION` / `CREDENTIALS` / `CONNECTIVITY` / `SOURCE_SCHEMA` / `SINK_WRITE` / `ENGINE` / `OBSERVABILITY` / `CANCELLATION` / `UNKNOWN` |
| `FailureStage` | `SUBMISSION` / `SOURCE` / `TRANSFORM` / `SINK` / `MONITORING` / `CANCELLATION` / `PUBLISH` / `UNKNOWN` |
| `QualityStatus` | `PASSED` / `WARNING` / `FAILED` / `INCOMPLETE` |
| `SideEffectOperation` | `CANCEL_EXECUTION` / `ROLLBACK_DORIS` / `CLEANUP_DORIS` / `MIGRATE_SECRET` / `STAGE_SECRET_ROTATION` / `VALIDATE_SECRET_ROTATION` / `ACTIVATE_SECRET_ROTATION` / `ROLLBACK_SECRET_ROTATION` / `DESTROY_SECRET` |
| `SideEffectExecutionStatus` | `NOT_COMMITTED` / `PENDING` / `RUNNING` / `SUCCEEDED` / `FAILED` |
| `RuntimeMitigation` / `Status` | `CONTINUE`/`THROTTLE`/`PAUSE`/`CANCEL`/`QUARANTINE`；`NOT_REQUIRED`/`PENDING`/`APPLYING`/`APPLIED`/`FAILED` |
| `BenchmarkScenarioKind` / `BenchmarkCaseCategory` / `BenchmarkRunStatus` | `POLICY_INTENT`/`STATIC_HOCON`/`RUNTIME_SUPERVISION`；`LEGITIMATE`/`ADVERSARIAL`/`MUTATION`/`REGRESSION`；`PASS`/`FAIL` |
| `SafetyProposalKind` / `Status` | `POLICY`/`PROMPT`/`BENCHMARK`；`DRAFT`/`IN_REVIEW`/`APPROVED`/`REJECTED`/`RELEASED` |
| `SafetyReleaseStatus` | `SHADOW_RUNNING` / `SHADOW_VERIFIED` / `CANARY_RUNNING` / `CANARY_VERIFIED` / `PROMOTION_APPROVED` / `ROLLED_BACK` |

### 7.3 状态迁移表（`【代码】`，拒绝跳步、非法回退、终态重启）

| 状态机 | 迁移 |
|---|---|
| `PIPELINE_TRANSITIONS` | `DRAFT→PROFILING→GENERATED→VALIDATED→WAITING_APPROVAL→{APPROVED, REJECTED}`；`APPROVED`、`REJECTED` 为终态 |
| `AGENT_RUN_TRANSITIONS` | `QUEUED→{RUNNING, FAILED}`；`RUNNING→{WAITING_INPUT, WAITING_APPROVAL, FAILED}`；`WAITING_INPUT→{QUEUED, FAILED}`；`WAITING_APPROVAL`、`FAILED` 为终态 |
| `EXECUTION_TRANSITIONS` | `QUEUED→{SUBMITTED, FAILED, CANCELLED}`；`SUBMITTED→{RUNNING, FAILED, CANCELLING}`；`RUNNING→{SUCCEEDED, FAILED, CANCELLING}`；`CANCELLING→{CANCELLED, FAILED, SUCCEEDED}`；三个终态不可重启 |

**必须写入验收测试的拒绝场景**：从 `GENERATED` 直接申请审批（跳过 `VALIDATED`）必须失败；`FAILED` 执行原地重启必须失败；重复决策同一审批单必须失败。

### 7.4 并发与幂等约束

| 约束 | 目的 |
|---|---|
| `pipeline_versions.lock_version` 乐观锁 | 防止最后写入者静默覆盖 |
| `(pipeline_id, version_number)`、`(project_id, user_id)`、`agent_runs.thread_id`、`execution_runs.idempotency_key` 唯一 | 防并发重复 |
| `SELECT ... FOR UPDATE`（版本冻结、审批决策、Profile/文件状态、副作用、来源 Run 互斥） | 串行化状态变更，关闭 TOCTOU 窗口 |
| Celery `task_acks_late` + 单次预取 + Worker 丢失即拒绝 | 消息重投是**正常情况**，业务必须靠幂等键收敛 |

---

## 8. 接口需求

### 8.1 设计约定

- 统一前缀 `/api/v1`；**只有 `GET /health` 无前缀、无身份**。
- 身份：`X-Actor-ID`（本地）或可信网关签名四件套（生产）；链路：`X-Trace-ID`；幂等：`Idempotency-Key`（8–128 字符，Preparation 与重跑接口必需）。
- 错误语义见 FR-1.6；**前端不得根据错误文本猜状态迁移**。

### 8.2 接口分组（完整端点见附录 E）

| 域 | 主要端点（节选） | 关键语义 |
|---|---|---|
| 项目与成员 | `POST /projects/bootstrap`、`GET|POST /projects/{pid}/members`、`POST /projects/{pid}/members/{uid}/harness-roles` | 一次性初始化；显式授予 Harness 职责 |
| 连接与探查 | `POST /connections`、`POST /connections/{id}/tests`、`POST /connections/{id}/profiles`（202）、`GET /data-profiles/{id}` | 异步化外部调用；响应永不回显秘密 |
| 文件资产 | `POST /projects/{pid}/file-assets`（202）、`GET /file-assets/{id}` | 响应不含 `source_uri`/`normalized_uri`/`engine_path` |
| Pipeline 与版本 | `POST /pipelines`、`POST /pipelines/versions/{id}/generation|validation|revisions`、`GET /pipelines/versions/{id}/design` | 生成时服务端重做真实引擎校验；读取审查视图前重算摘要 |
| Agent 运行 | `POST /pipeline-versions/{id}/agent-runs`（202）、`POST /agent-runs/{id}/answers` | 只投递 `run.id`；答案先拒绝内联凭据 |
| 版本审批 | `POST /pipeline-versions/{id}/approval-requests`（201）、`POST /approval-requests/{id}/decisions` | 冻结摘要；非申请人决策 |
| 执行准备与授权 | `POST /pipeline-versions/{id}/execution-preparations`（201）、`POST /execution-preparations/{id}/decisions`、`POST /execution-preparations/{id}/commit`（202） | Prepare 无副作用；Commit 只由 `OPERATOR` |
| 执行与观测 | `GET /projects/{pid}/execution-runs`、`GET /execution-runs/{id}`、`.../events`、`.../supervision-snapshots`、`.../quality-report`、`POST .../cancellation`（202）、`POST .../retry-preparations`（201） | 取消不伪造终态；重跑建新 Preparation |
| 回滚与清理 | `POST /execution-runs/{id}/rollback-requests|cleanup-requests`、对应 `decisions` | 四眼审批；互斥 |
| 凭据治理 | `POST /connections/{id}/secret-rotations|secret-migrations`、轮换回退与销毁申请、对应 `decisions` | 全部经统一副作用 Harness |
| 统一副作用 | `GET /projects/{pid}/side-effect-preparations`、`GET /side-effect-preparations/{id}[/approvals]` | 只读审查视图，不返回令牌/私钥 |
| 运维观测 | `GET /projects/{pid}/worker-health|rollback-artifacts|secret-governance|credential-expiry|credential-alerts`、告警确认/静默/解除静默 | 指标只含聚合事实 |
| 调度 | `GET /projects/{pid}/pipeline-schedules`、`PUT /pipelines/{id}/schedule`、`POST /pipeline-schedules/{id}/enabled` | 只能绑定已审批版本 |
| 安全进化 | `POST /projects/{pid}/benchmark-runs`、提案创建/提交/裁决、发布影子/验证/小流量/推广/回滚 | 客户端不能传路径或代码 |

---

## 9. 非功能需求（NFR）

### 9.1 安全

| 编号 | 需求 | 判定标准 |
|---|---|---|
| NFR-S1 | 生产身份必须来自可信网关 | 生产环境启用 `dev-header` 时**进程启动失败**；HMAC 密钥与 Bootstrap Token ≥32 字节 |
| NFR-S2 | 密钥职责分离 | 授权服务持私钥，Worker 只持公钥；**Worker 不挂载授权私钥**；生产缺私钥拒绝启动 |
| NFR-S3 | 秘密不落明文 | API 响应、审计、日志、Trace、证据、消息队列、Prompt、图状态、制品中均不得出现凭据或原始敏感样本 |
| NFR-S4 | 破坏性操作双重把关 | `CLEANUP_DORIS` 与 `DESTROY_SECRET` 需双职责、双自然人；不可逆操作失败不得伪装成功 |
| NFR-S5 | 失败关闭 | 策略、审批、签名、证据或 Secret 无法验证时不得降级放行；唯一的降级例外（受限只读元数据探查）必须由明确规则表达，**不能由调用方捕获异常自行放行** |
| NFR-S6 | 网络与出口控制 | 逐行 LLM 自定义端点须命中精确主机白名单、生产强制 HTTPS、URL 不含凭据/查询串/片段；WebHDFS 重定向仅允许白名单 authority；Vault 不跟随重定向 |
| NFR-S7 | 依赖安全 | CI 对生产依赖执行漏洞审计；依赖版本使用区间约束并锁定 |

### 9.2 性能与容量

| 编号 | 需求 | 目标值 |
|---|---|---|
| NFR-P1 | 静态/确定性路径延迟 | 非 LLM 路径 P95 相对冻结基线退化 ≤ 20% |
| NFR-P2 | 单文件上传 | ≤ 25 MiB；解析 ≤ 100000 行 |
| NFR-P3 | 元数据样本 | 每次探查样本 ≤ 100 行 |
| NFR-P4 | 连接超时 | 连接 1–30s；读写 30s |
| NFR-P5 | 生成调用 | 单份候选一次调用；请求超时默认 180s（上限 600s）；候选总量 ≤ 3 份 |
| NFR-P6 | 执行监控 | 轮询默认 2s（节流 5s）；监控总时长上限约 1 小时 |
| NFR-P7 | 运行预算 | 记录数 ≤ 1000 万、字节数 ≤ 10 GiB、时长 ≤ 3600s（均可收紧） |
| NFR-P8 | 逐行 LLM 速率 | 默认 10 行/秒（1–100），且不得超过环境治理上限 |

### 9.3 可用性与恢复

| 编号 | 需求 | 判定标准 |
|---|---|---|
| NFR-A1 | 至少一次投递 + 业务幂等 | 重复消息不产生第二次副作用、第二个作业或第二份秘密 |
| NFR-A2 | 崩溃可恢复 | API 在提交后崩溃可由 Outbox 扫描恢复；Worker 崩溃的租约可过期重认领 |
| NFR-A3 | 状态对账 | 远端超时且结果未知时，按稳定业务标识**先查询再决定**，不盲目创建第二作业 |
| NFR-A4 | 可重放 | 澄清中断可从同一 `thread_id` 恢复；失败重试使用同一水位窗口 |
| NFR-A5 | 心跳与离线判定 | Worker 心跳默认 10s，陈旧阈值默认 35s；离线可观测 |
| NFR-A6 | 版本回退 | 数据库迁移链单一 head，无分叉；枚举以字符串存储便于升级与回滚 |

### 9.4 可观测性

| 编号 | 需求 | 判定标准 |
|---|---|---|
| NFR-O1 | 全链路追踪 | API / Agent / Celery / 外部 HTTP / SQLAlchemy / 引擎提交可通过 Trace ID 关联；OTLP **默认关闭**，生产显式开启 |
| NFR-O2 | 脱敏采集 | 不采集请求体、SQL 绑定参数、业务样本、HOCON、Prompt、Secret、错误原文；排除健康检查与鉴权相关请求头 |
| NFR-O3 | 指标只含聚合事实 | 不把日志或行样本当标签 |
| NFR-O4 | 告警覆盖 | Worker/Beat 心跳、Outbox 积压、执行失败、凭据到期、Webhook 投递失败、回滚保留容量均有可观测状态 |
| NFR-O5 | 监督快照可审计 | 快照含动作、风险、预算与观测值、命中规则、是否阻断发布、减损状态与摘要，且**不含**原始 HOCON、行样本、Secret、令牌正文 |

### 9.5 合规与留存

| 编号 | 需求 | 默认值 |
|---|---|---|
| NFR-C1 | 凭据年龄 | 最长 90 天、提前 14 天预警（连接级可配） |
| NFR-C2 | 回滚保留 | Doris 回滚版本保留 7 天（1–3650），容量预警阈值 20 |
| NFR-C3 | Secret 回退窗口 | 7 天（1–365）内可回退 |
| NFR-C4 | Preparation / Capability | 24 小时 / 5 分钟（硬上限 15 分钟） |
| NFR-C5 | 证据最小化 | 证据只保存必要摘要与结构化决策，避免扩大敏感面与签名链体积失控 |
| NFR-C6 | 外部真相对账 | 恢复流程读取外部真相并**追加新事件，不删除或重写失败历史** |

### 9.6 兼容性与可维护性

| 编号 | 需求 | 判定标准 |
|---|---|---|
| NFR-M1 | 供应商中立 | 生成模型与逐行 LLM 通过通用环境变量切换供应商，不绑定单一前缀 |
| NFR-M2 | 生成后端可替换 | 切换 `direct` / 官方 CLI 不改变安全边界与后续流程 |
| NFR-M3 | 秘密后端可迁移 | 支持本地密文与外部 KV 双后端与迁移期路由 |
| NFR-M4 | 代码质量门禁 | 静态检查、严格类型检查、单测覆盖、依赖审计全部通过 |
| NFR-M5 | 前端可维护 | 按业务域拆分页面、组合式函数与 API 模块，补组件测试 |
| NFR-M6 | 测试可离线运行 | 单元/协议测试不消费模型额度、不要求运行 SeaTunnel 容器 |

---

## 10. 验收标准

### 10.1 第一条垂直切片（10 项，产品可交付的最小闭环）

- [ ] 项目存在数据工程师与独立审批人，**两者不能替对方完成四眼审批**。
- [ ] MySQL 与 Doris 凭据进入 SecretProvider；**API 响应与普通日志看不到明文与秘密引用**。
- [ ] Profile 保存字段、主键、近似统计与脱敏样本，**不保存原始业务行**。
- [ ] Agent 缺信息时进入等待，补充后**从同一会话恢复**。
- [ ] 每份候选经确定性门禁与真实引擎检查，**修复预算耗尽后安全终止**。
- [ ] 通过门禁的制品冻结为不可变版本，**修改需求创建新版本**。
- [ ] 独立审批后 Harness 绑定精确版本与审批事实，Worker **只消费一次性能力**。
- [ ] Doris 主表与错误表得到符合固定 Golden Case 的**记录数、字段值与错误码**。
- [ ] 运行中心能关联输入/输出行数、错误、质量报告、Trace 与执行事件。
- [ ] 重试、取消与回滚**不静默改写原执行记录**，关键动作可由审计与证据账本复核。

### 10.2 分层验证（全部必须通过）

| 层级 | 内容 | 运行条件 |
|---|---|---|
| 单元/协议 | 策略、Broker、签名、证据、执行、Harness、Secret、Benchmark、调度、质量、交付、文件、连接 | 无需外部服务（内存数据库 + 模拟传输） |
| **L0** 静态仿真 | 跨项目、秘密外发、破坏性、无界扫描、连接器注入、明文凭据 | 每次提交 |
| **L1** 确定性仿真 | 超时、输出放大、拒绝率、重复消费、部分成功、未知状态 | 每次提交 |
| **冻结 Benchmark** | 14 个冻结场景 + 六项门槛 | 每次提交，报告无论成败都上传 |
| **L2** 真实链路 | MySQL→Doris、文件→Doris 两链路行数、质量路由、准备签名证据、发布标记、清理结果 | 独立工作流，一次性环境 |

**关键验收断言（必须写成测试）**：

- 同一案例重放两次**观察结果一致**且规则证据非空；L0/L1 未产生外部副作用。
- 真实引擎检查失败 3 次后**不存在人工审批中断**（确定性失败，不把无效方案交给审批人）。
- 迟到的 Worker 错误**不能覆盖**已确认的成功终态。
- 同一 Token 二次执行必须失败；篡改声明必须验签失败。
- 审批后修改制品再 Commit 必须冲突并要求重新 Prepare。
- 遗留的"直接入队"入口必须抛拒绝。
- 清理未到期、申请人自批、仍在保留期/仍被引用的 Secret 销毁均必须被拒；**负向用例必须证明外部 Adapter 未被调用**。
- 多角色用户**不能一人双批**；只有第二名不同职责的自然人才能放行 P0。
- 权限拓扑最终边界：绕过 API 的调用者**没有外部系统凭据**。

### 10.3 端到端人工验证（可观察证据）

- Prepare 响应应含制品摘要、输入/资源指纹、风险等级、影响报告、回滚方案、所需职责与过期时间；**不含**能力明文、签名私钥、连接 Secret、原始数据样本。
- 审批后 Commit 返回 202 + 执行记录；能力已消费；Outbox 由待发转为已发；响应中无任何可重放令牌。
- 三类状态不得混淆：准备单已提交（授权已消费 + 命令原子入库）、Outbox 已发送（命令已交队列）、执行成功（真实数据作业终止且成功）。
- 关键攻击复现：①申请人用自己的身份提交审批 ⇒ 403，准备单保持待审批；②审批后修改制品再 Commit ⇒ 403/409 并要求重新 Prepare，旧审批不能自动迁移。
- **端到端对齐验收法**：不要只确认字段"存在"。选一个字段往回追溯（例如影响报告中的 Doris 目标表应来自已冻结 plan → 参与资源指纹计算 → 能力绑定同一指纹 → Worker 提交前再验证同一声明）。能完成端到端对齐，字段才不是装饰性元数据。

### 10.4 生产交付验收清单

**安全与身份**

- [ ] 生产拒绝本地身份头；可信网关签名、TLS、Nonce 与时钟偏差策略已配置。
- [ ] 初始化开关已关闭、Token 已撤销；申请人与审批人是不同自然人。
- [ ] Worker 不挂载授权私钥；密码学职责分离已在部署中体现。
- [ ] Secret 使用企业级 Provider；Token 由文件/工作负载身份提供并最小权限。
- [ ] Prompt、制品、日志、Trace、证据、消息队列均不含凭据与原始敏感样本。

**数据与执行**

- [ ] 只允许就绪、未过期、项目内的 Profile；数据分级缺失时记为 `UNKNOWN` 并失败关闭。
- [ ] 质量、增量、逐行 LLM、交付与运行预算**均随版本冻结**。
- [ ] 所有提交经 Harness；一次性能力被验签与消费；重复消息不产生第二次副作用。
- [ ] 作业标识稳定；状态未知先对账；重跑沿用原制品。
- [ ] Doris 原子发布、回滚保留、容量告警与物理清理职责已演练。

**可观测与恢复**

- [ ] 全链路 Trace 可关联；Worker/Beat 心跳、Outbox 积压、执行失败、凭据到期、Webhook 投递均有告警。
- [ ] 指标只含聚合事实；备份、迁移、对象存储、灾难恢复与密钥恢复流程已有企业方案。

**验证与发布**

- [ ] 单元测试、L0、L1、冻结 Benchmark、依赖审计、类型检查、前端构建全部通过。
- [ ] L2 在当前候选提交的一次性环境通过；失败诊断与清理制品可追溯。
- [ ] Benchmark 同时约束高风险阻断与合法成功，**不能通过"全部拒绝"作弊**。
- [ ] 安全候选经独立审批、影子、小流量、签名推广授权；硬门禁失败自动撤销授权。
- [ ] 发布系统明确知道推广授权**只是授权**，不代表平台已完成热更新。

---

## 11. 里程碑计划

| 阶段 | 交付内容 | 完成判据 |
|---|---|---|
| **M0 工程底座** | 工程初始化、配置边界、基础设施、健康检查、领域模型与迁移、身份与状态机 | 健康检查返回脱敏就绪状态；迁移单一 head；危险配置组合在启动时被拒 |
| **M1 数据上下文** | 数据连接、元数据探查、凭据隔离、受管文件九格式、Secret 生命周期 | 凭据不出现在任何响应/日志/审计；Profile 只含脱敏样本；文件可编译为无凭据的受管 Source |
| **M2 契约与生成** | 契约体系、确定性门禁、真实引擎检查、模型 Provider 双路径、就绪检查 | 契约违规候选被穷举拒绝；上游自报的 dry-run 不能成为审批证据 |
| **M3 可恢复编排与审批** | 六节点状态图、检查点、共享修复预算、制品冻结、四眼审批、审计 | 三次失败确定性终止；摘要变化使待审批失效；改需求只能建新版本 |
| **M4 执行与发布** | 执行端口、事务 Outbox、稳定作业标识、指标归一化、失败诊断、质量门禁、Doris 原子发布、调度与水位 | 迟到错误不改写成功终态；未过质量门禁不得发布；水位只在成功后推进 |
| **M5 Harness 内核** | PDP 与签名策略包、工具代理、准备/审批/提交、一次性能力、证据账本、全量避绕校验 | 任意调用者即使不走控制台、直接调 API 或重放 Worker 命令，**也不能突破同一套服务端约束** |
| **M6 治理全覆盖** | 统一副作用 Harness（9 类）、运行监督、数据分级、可解释失败、保留与合规 | 新增副作用必须同时注册三处；监督动作只能收窄；P0/P1 不可放行 |
| **M7 闭环与交付** | 仿真 L0/L1/L2、冻结 Benchmark、受控安全进化、Web 控制台、交付验收 | 六项门槛全部达标；硬门禁回归自动回滚；交付清单全勾选 |

---

## 12. 风险、依赖与开放边界

### 12.1 主要风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 模型输出不确定性 | 无效候选、越界配置 | 三层门禁（契约/确定性/真实引擎）+ 共享修复预算 + 人工审批 |
| 提示注入与内容替换 | 越权操作、目标表被换 | 服务端派生事实 + 指纹绑定 + Commit 重算比对 |
| 重放与重复投递 | 重复写入、重复副作用 | 幂等键 + 单次能力 + 事务 Outbox + 稳定作业标识 |
| 策略误伤合法任务 | 业务停摆 | 冻结 Benchmark 同时约束误报率与合法成功率；影子/小流量灰度 |
| 外部服务不可用 | 生成或执行中断 | 明确的就绪状态与失败关闭；已冻结事实保留；**不把外部故障伪装成业务事实** |
| 证据链被篡改 | 无法追责 | 哈希链 + 签名检查点；生产建议私钥入 KMS/HSM 并定期导出检查点到独立存储域 |
| 单点集中在前端源码 | 维护成本 | 明确要求按业务域拆分（NFR-M5） |
| 供应商协议差异 | 切换模型后行为不一致 | 兼容协议不代表 Token/推理字段/限流/错误格式一致，差异需在适配器与集成测试中验证 |

### 12.2 外部依赖

PostgreSQL、Redis、SeaTunnel Zeta（2.3.13）、Doris、MySQL/PostgreSQL/Oracle/SQL Server/Hive/HDFS、企业 Secret Provider（Vault 或等价物）、企业 KMS/HSM（生产建议）、对象存储（制品与受管文件）、OTLP 后端、可信身份网关、CI 环境。

### 12.3 明确的开放边界（当前版本不支持）

独立停滞规则、引擎原生暂停/保存点、通用跨 Sink 部分成功补偿、真实影子流量复制、Canary 流量路由执行器、HDFS Kerberos、扫描件/OCR/复杂版式 PDF、策略/Prompt 的自动部署系统、生产高可用与跨可用区灾备、企业 SSO/IAM/证书/网络隔离/WAF/速率限制/细粒度数据权限、Oracle/SQL Server/Hive-HDFS 在目标版本网络中的真实集成验证、前端国际化与无障碍、成本/SLO/值班与事故响应制度。

---

## 13. 附录

### 附录 A：术语表（中英对照）

| 中文 | 英文 / 标识 |
|---|---|
| 控制面 / 数据面 | Control Plane / Data Plane |
| 运行机制内核 | Harness（Execution Kernel） |
| 工具意图 / 资源范围 / 工具效果 | ToolIntent / ResourceScope / ToolEffect |
| 策略决策点 / 策略裁决 / 策略包 | PDP / PolicyDecision / PolicyBundle（签名） |
| 工具代理（唯一执行出口） | Tool Broker |
| 冻结工具调用 | PreparedToolInvocation |
| 能力声明 / 能力令牌 | CapabilityClaims / Capability Token |
| 防重放守卫 | Replay Guard |
| 准备单 / 冻结制品 | Preparation / Frozen Artifact |
| 制品摘要 / 输入指纹 / 资源指纹 | artifact_sha256 / input_fingerprint / resource_fingerprint |
| 一次 / 两次 / 三次投递语义 | at-least-once（本产品为至少一次） |
| 事务发件箱 | Transactional Outbox |
| 证据账本 / 哈希链 / 签名检查点 | Evidence Ledger / Hash Chain / Signed Checkpoint |
| 职责分离 / 四眼原则 | Separation of Duties / Four-Eyes |
| 失败关闭 / 失败开放 | Fail-closed / Fail-open |
| 只减权 | Reduce-only |
| 检查与使用之间的时间窗口 | TOCTOU |
| 版本 / 血缘 / 修订 | PipelineVersion / lineage / revision |
| 生成会话 | AgentRun |
| 执行准备单 / 执行运行 | ExecutionPreparation / ExecutionRun |
| 运行预算 / 减损动作 / 隔离 | RuntimeBudget / RuntimeMitigation / QUARANTINE |
| 数据分级 / 掩码 | DataClassification / Masking |
| 增量水位窗口 | Watermark window `(lower, upper]` |
| 原子发布 / 影子表 | Atomic Publish / staging table |
| 追加审计 | APPEND_AUDIT |
| 冻结基准 / 门禁比率 | Frozen Benchmark / gate rates |
| 影子 / 小流量 | Shadow / Canary |
| 受控安全进化 | Controlled Safety Evolution |
| 凭据轮换 / 回退 / 迁移 / 销毁 | Rotation / Rollback / Migration / Destruction |
| 到期告警与投递 | Credential Alert / Delivery |
| 脱敏样本 | masked_sample |
| 受管文件 / 引擎只读路径 | FileAsset / engine_path |
| 确定性门禁 | deterministic gate |
| 澄清 / 企业审批中断 | clarification / enterprise approval interrupt |
| 幂等键 | Idempotency Key |
| 追踪号 | trace_id |

### 附录 B：Golden Case 验收基准

见 6.2。验收时核对：`analytics.clean_orders` 的 4 行记录数、字段值（`Alice` 去空格、邮箱 `***@example.com`）与错误表的 4 行记录数及四个错误码。

### 附录 C：关键配置项（`ETL_AGENT_` 前缀 `【代码】`）

| 分组 | 配置项与默认值 |
|---|---|
| 应用/身份 | `ENVIRONMENT=local`、`API_PREFIX=/api/v1`、`AUTH_MODE=dev-header`（生产必须 `trusted-gateway`）、`GATEWAY_HMAC_SECRET`（≥32 字节）、`GATEWAY_MAX_CLOCK_SKEW_SECONDS=60(5–300)`、`BOOTSTRAP_ENABLED=false`、`BOOTSTRAP_TOKEN`（≥32 字节） |
| 基础设施 | `DATABASE_URL`（PostgreSQL）、`CHECKPOINT_DATABASE_URL`（LangGraph 独立库）、`REDIS_URL` |
| 引擎/生成 | `SEATUNNEL_API_URL`、`SEATUNNEL_TIMEZONE=UTC`、`SEATUNNEL_LLM_PROVIDER=openai-compatible`、`GENERATION_BACKEND=direct`、`LLM_API_KEY/BASE_URL/MODEL/FAST_MODEL`、`LLM_REQUEST_TIMEOUT_SECONDS=180(≤600)` |
| 逐行 LLM | `ROW_LLM_TRANSFORM_ENABLED=false`、`ROW_LLM_API_KEY`、`ROW_LLM_ALLOWED_API_HOSTS`、`ROW_LLM_MAX_ROWS_PER_SECOND=10(1–100)` |
| 密钥 | `SECRET_PROVIDER=fernet-db`（或 `vault-kv-v2`）、`SECRET_MASTER_KEY`、`VAULT_ADDRESS/TOKEN/TOKEN_FILE/NAMESPACE/KV_MOUNT=secret/PATH_PREFIX=etl-agent/connections/TIMEOUT_SECONDS=10/VERIFY_TLS=true`、`SECRET_ROTATION_ROLLBACK_DAYS=7(1–365)` |
| 凭据告警 | `CREDENTIAL_ALERT_WEBHOOK_URL`、`CREDENTIAL_ALERT_WEBHOOK_SIGNING_SECRET`、`CREDENTIAL_ALERT_WEBHOOK_TIMEOUT_SECONDS=10`、`CREDENTIAL_ALERT_SCAN_INTERVAL_SECONDS=3600`、`CREDENTIAL_ALERT_WARNING_COOLDOWN_HOURS=24`、`CREDENTIAL_ALERT_OVERDUE_COOLDOWN_HOURS=6` |
| 调度 | `SCHEDULE_SCAN_INTERVAL_SECONDS=30(5–3600)`、`SCHEDULE_MISFIRE_GRACE_SECONDS=120` |
| Harness | `HARNESS_POLICY_BUNDLE_VERSION=harness-v2-20260808`、`HARNESS_SIGNING_KEY_ID=local-harness-v1`、`HARNESS_PRIVATE_KEY`（生产必填）、`HARNESS_PUBLIC_KEYS`（生产必填）、`HARNESS_PREPARATION_TTL_SECONDS=86400(300–604800)`、`HARNESS_CAPABILITY_TTL_SECONDS=300(30–900)`、`HARNESS_OUTBOX_SCAN_INTERVAL_SECONDS=2` |
| 运行预算硬上限 | `HARNESS_RUNTIME_MAX_RECORDS=10000000`、`HARNESS_RUNTIME_MAX_BYTES=10GiB`、`HARNESS_RUNTIME_TIMEOUT_SECONDS=3600`、`HARNESS_RUNTIME_MAX_OUTPUT_RATIO=2.0`、`HARNESS_RUNTIME_MAX_REJECT_RATE=0.5`、`HARNESS_RUNTIME_MINIMUM_QUALITY_ROWS=100`、`HARNESS_RUNTIME_WARNING_RATIO=0.8` |
| 存储/文件 | `ARTIFACT_ROOT=var/artifacts`、`FILE_ASSET_ROOT=var/file-assets`、`FILE_ASSET_ENGINE_ROOT=/data/file-assets`、`FILE_ASSET_MAX_BYTES=25MiB`、`FILE_ASSET_MAX_ROWS=100000` |
| Worker/Doris | `WORKER_HEARTBEAT_INTERVAL_SECONDS=10`、`WORKER_HEARTBEAT_STALE_SECONDS=35`、`DORIS_ROLLBACK_RETENTION_DAYS=7`、`DORIS_ROLLBACK_CAPACITY_WARNING_COUNT=20` |
| 可观测 | `OTEL_ENABLED=false`、`OTEL_SERVICE_NAME=seatunnel-etl-agent`、`OTEL_TRACES_ENDPOINT`、`OTEL_EXPORT_TIMEOUT_SECONDS=10`、`OTEL_TRACE_SAMPLE_RATIO=1.0` |

**启动期强校验（不满足即进程启动失败，不打印警告继续）**：生产必须 `trusted-gateway`；`trusted-gateway` 必配 ≥32 字节 HMAC；启用 Bootstrap 必配 ≥32 字节 Token；启用逐行 LLM 必配独立 Key；生产必须配置公钥环。

### 附录 D：参考文档与实现的不一致处（以代码为准）

| 项 | 文档表述 | 实现事实 | 采纳 |
|---|---|---|---|
| API 前缀 | 章节示例写 `/api/...` | 实际 `/api/v1` | 采用 `/api/v1` |
| 准备单状态 | 状态图出现 `PREPARING` / 散文出现 `PREPARED` | 枚举为 `DENIED/WAITING_APPROVAL/READY/REJECTED/EXPIRED/COMMITTED` | 采用枚举 |
| 监督查询路径 | `/execution-runs/{id}/runtime-supervision` | 实现为 `/execution-runs/{id}/supervision-snapshots` | 采用实现路径 |
| 凭据轮换/迁移/销毁/告警 | 第 4–5 章正文未展开 | 代码已完整实现 | 按代码补全（FR-4） |
| 文件资产状态 | — | 复用 `DataProfileStatus`，无独立枚举 | 采用复用方案 |
| `pass@k` 类概率指标 | 无 | 文档与实现均未使用；明确要求"不能把概率结果混入 100% 确定性指标" | 不引入 |
| 审批有效期 | 无 | 版本审批单**无** `expires_at` | 视为新增需求（见第 14 章 Q4） |

### 附录 E：参考实现追溯索引

| 关注点 | 参考实现位置 |
|---|---|
| 领域枚举与状态机 | `src/etl_agent/domain/enums.py`、`domain/state_machine.py` |
| Harness 领域与策略 | `domain/harness.py`、`application/harness_policy.py` |
| 工具代理与执行内核 | `application/tool_broker.py`、`application/execution_harness.py` |
| 签名与令牌 | `infrastructure/harness_signing.py`、`infrastructure/harness_runtime.py` |
| 证据账本 | `application/evidence_ledger.py` |
| 事务 Outbox | `application/harness_outbox.py`、`harness_outbox_tasks.py` |
| 副作用治理与监督 | `domain/side_effects.py`、`domain/supervision.py`、`application/side_effect_harness.py`、`application/runtime_supervision.py` |
| 契约与门禁 | `application/schema_contract.py`、`quality.py`、`incremental.py`、`row_llm.py`、`delivery.py`、`design_review.py`、`pipelines.py` |
| 生成端口与适配器 | `application/generation.py`、`infrastructure/direct_generation_adapter.py`、`infrastructure/seatunnel/*` |
| Agent 图与检查点 | `agent/graph.py`、`agent/checkpoint.py`、`agent_tasks.py` |
| 执行与发布 | `application/execution.py`、`execution_metrics.py`、`execution_diagnostics.py`、`infrastructure/doris_publish.py` |
| 连接与探查 | `application/profiling.py`、`infrastructure/*_probe.py`、`probe_registry.py`、`application/masking.py` |
| 凭据治理 | `application/secrets.py`、`secret_*.py`、`credential_*.py`、`infrastructure/secret_factory.py` |
| 文件资产 | `application/file_assets.py`、`infrastructure/file_asset_store.py` |
| Benchmark 与安全进化 | `application/benchmark.py`、`safety_evolution.py`、`benchmark_cli.py`、`benchmarks/harness-v2/v1/*` |
| 数据模型与迁移 | `infrastructure/models.py`、`alembic/versions/*` |
| API 路由 | `api/routes/*.py`、`api/dependencies.py`、`main.py` |
| 控制台 | `frontend/src/App.vue`、`api.ts`、`types.ts` |
| 基础设施与 CI | `infra/compose.yaml`、`infra/e2e/*`、`.github/workflows/*` |

---

## 14. 待确认的关键问题

请需求方就以下问题给出结论，它们会直接影响范围、优先级与对外承诺：

1. **目标客户与规模画像**：本产品第一版要服务的是单团队（数名数据工程师）还是多团队/多租户？是否需要**跨项目**的资源隔离与配额（当前模型为单层项目 + 项目内角色）？
2. **数据源优先级**：首批必须完整支持并做真实集成验证的连接器是哪几个？当前 MySQL/Doris 有真实链路验证，PostgreSQL/Oracle/SQL Server/Hive/HDFS 尚未完成目标版本与网络下的真实验证——是否需要在第一版就承诺？
3. **SeaTunnel 版本绑定策略**：是否接受长期锁定 2.3.13 与固定官方 CLI commit？升级窗口与回归责任如何界定？
4. **审批时效**：是否需要"审批 N 天后自动失效"？当前版本审批单没有有效期字段，审批有效性仅由"状态守卫 + 摘要相等 + 待处理守卫 + 行锁"保证（准备单有 24 小时有效期，能力令牌有 5 分钟有效期）。若需要，属新增字段与新增需求。
5. **P0 审批人配置**：破坏性操作要求"数据审批人 + 安全审批人且为两个不同自然人"。在小型团队中，是否允许通过**配置**降级为"同一人不同职责"？还是坚持组织层面的强制分离（当前实现强制分离）？
6. **数据出境与逐行 LLM 的合规口径**：逐行 LLM 默认关闭。企业是否会启用？允许的模型供应商白名单、允许外发的字段分级上限、是否需要在契约层强制限制（当前仅由主机白名单 + 速率 + 开关治理）？
7. **运行监督的收敛策略**：当前无独立"停滞"规则，也无引擎级暂停/保存点。业务是否能接受"PAUSE 即强制取消 + 阻断发布"？是否需要投入实现真正的可恢复暂停与停滞检测？
8. **部分成功的业务语义**：跨多个非原子 Sink 的"部分成功"当前未实现通用补偿。目标场景是否会同时写多个非原子目标？如果需要，期望的补偿语义是什么？
9. **发布与推广的落地方式**：安全进化当前只产出"签名推广授权"，不负责部署。企业侧的部署系统是什么？是否需要本产品提供对接契约（如 webhook、制品仓库推送）？
10. **证据与签名密钥的托管**：生产是否具备 KMS/HSM 与 WORM 存储？证据检查点的导出频率与保留期要求是什么？
11. **保留与合规**：Doris 回滚保留 7 天、Secret 回退窗口 7 天是否符合企业数据留存与审计要求？是否需要按数据类型/项目差异化配置？
12. **性能与规模目标**：单次执行的目标吞吐与并发上限（当前单次预算为 1000 万行 / 10 GiB / 1 小时）、同时在线项目数与并发执行数是多少？是否需要多 Worker 队列隔离与优先级？
13. **错误预算与运维口径**：平台可用性目标、失败率与恢复时间目标（RTO/RPO）、值班与升级机制由谁承担？当前 Compose 形态不构成生产高可用方案。
14. **前端交付边界**：控制台是否作为产品正式交付件？若需要多语言、无障碍与大规模列表性能，应作为独立里程碑排期。
15. **演示与教学的边界**：本仓库同时承担教学用途（本地身份模拟、角色切换演示）。正式交付时，**哪些"教学便利"（本地身份头、自动生成开发密钥、一次性初始化）必须被封禁或替换**？

---

*文档结束。本 PRD 的所有安全边界、状态迁移与验收条目均可映射到参考实现中的具体模块与测试，追溯索引见附录 E。*
