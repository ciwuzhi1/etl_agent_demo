# scripts

运维与验证脚本。默认经 Vite 开发代理访问控制面，与浏览器走同一条链路。

## 前置

1. 数据库迁移到位：`uv run alembic upgrade head`
2. 三个进程都起来：
   ```bash
   uv run uvicorn etl_agent.main:app --host 127.0.0.1 --port 8000
   uv run celery -A etl_agent.worker:celery_app worker --loglevel=INFO --pool=solo
   uv run celery -A etl_agent.worker:celery_app beat --loglevel=INFO
   ```
3. 前端开发服务器：`cd frontend && npm run dev`
4. 若要走真实引擎（MySQL → SeaTunnel → Doris）：`docker compose -f infra/compose.yaml --profile e2e up -d`

## 环境变量

脚本不写死工作区与成员 UUID，缺省时从 `/context` 自动发现：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `ETL_AGENT_BASE_URL` | `http://localhost:5173/api/v1` | 也可指向 `http://127.0.0.1:8000/api/v1` 绕过代理 |
| `ETL_AGENT_PROJECT_ID` | `/context` 的首个工作区 | |
| `ETL_AGENT_ADMIN_ID` | 持有 `ADMIN` 的成员 | 提交侧身份 |
| `ETL_AGENT_APPROVER_ID` | 持有 `DATA_APPROVER` 的成员 | 审批侧身份 |
| `ETL_AGENT_SOURCE_CONNECTION` | `mysql-e2e-orders` | `pipeline_run.py` 使用的源连接名 |
| `ETL_AGENT_SINK_CONNECTION` | `doris-analytics` | `pipeline_run.py` 使用的汇连接名 |

## e2e

| 脚本 | 覆盖范围 | 是否写数据 |
|---|---|---|
| `e2e/console_probe.py` | 控制台五个视图的数据接口巡检 | 否（全 GET） |
| `e2e/control_plane_flow.py` | 控制面逐步驱动：连接→探查→上传→生成→Prepare→审批→Commit→取消→回滚 | 是 |
| `e2e/pipeline_run.py` | 全链路：建版本→记制品→校验→审批→Prepare→Commit→等终态 | 是 |
| `e2e/engine_smoke.py` | 执行链路冒烟，复用最近一个 APPROVED 版本 | 是 |
| `e2e/side_effects.py` | 副作用治理：Doris 回滚申请、四眼审批、一次性能力提交 | 是 |

```bash
uv run python scripts/e2e/console_probe.py        # 先跑这个确认后端在喂数据
uv run python scripts/e2e/pipeline_run.py         # 再跑全链路
uv run python scripts/e2e/engine_smoke.py
uv run python scripts/e2e/side_effects.py
uv run python scripts/e2e/control_plane_flow.py
```

每个脚本都会打印逐步 `[PASS]/[FAIL]` 与合计，全部通过时退出码为 0。
