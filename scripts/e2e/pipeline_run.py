"""全链路验证：建版本 → 记录制品 → 校验 → 审批 → Prepare → Commit → 等终态。

这是「网页点 Commit 就出数据」那条链路的脚本化版本，覆盖模型只出建议、真实副作用
由确定性代码与具名的人把守的每一道关口。制品正文同时写入 `var/artifacts/`，因为
执行任务会从 `artifact_root` 读取该文件。

前置：控制面（uvicorn + celery worker + beat）已启动，且项目内已登记 MySQL 与 Doris
连接；跑真实引擎时还需要 e2e Profile（mysql-e2e / doris-fe / seatunnel）。

用法：uv run python scripts/e2e/pipeline_run.py
"""

import os
import time
from pathlib import Path

from _client import (
    TERMINAL_STATUSES,
    admin_id,
    approver_id,
    call,
    connection_named,
    project_id,
    report,
    step,
)

ARTIFACT_NAME = "mysql-to-doris.conf"

HOCON = """env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    plugin_output = "raw_orders"
    url = "${MYSQL_JDBC_URL}"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "${MYSQL_USERNAME}"
    password = "${MYSQL_PASSWORD}"
    query = "SELECT order_id, customer_name, amount, status, email, created_at FROM orders"
  }
}

transform {
  Sql {
    plugin_input = "raw_orders"
    plugin_output = "valid_orders"
    query = \"\"\"SELECT order_id, TRIM(customer_name) AS customer_name, amount, status, REGEXP_REPLACE(email, '^[^@]+', '***') AS email, created_at, 'mysql' AS source_system FROM raw_orders WHERE customer_name IS NOT NULL AND TRIM(customer_name) <> '' AND amount > 0 AND status IN ('PAID','SHIPPED')\"\"\"
  }
}

sink {
  Doris {
    plugin_input = "valid_orders"
    fenodes = "${DORIS_FENODES}"
    username = "${DORIS_USERNAME}"
    password = "${DORIS_PASSWORD}"
    database = "analytics"
    table = "clean_orders"
    sink.label-prefix = "platform-e2e"
    sink.enable-2pc = true
    schema_save_mode = "IGNORE"
    data_save_mode = "DROP_DATA"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
"""

# 制品里只放平台占位符，真实凭据在提交前由 SecretProvider 物化。
PROFILE_CONTRACTS = [
    {"connector_type": "MYSQL", "role": "source"},
    {
        "connector_type": "DORIS",
        "role": "sink",
        "schema": "analytics",
        "table": "clean_orders",
        "columns": [
            {"name": "order_id"},
            {"name": "customer_name"},
            {"name": "amount"},
            {"name": "status"},
            {"name": "email"},
            {"name": "created_at"},
            {"name": "source_system"},
        ],
    },
]


def main() -> int:
    """跑完整条链路，返回进程退出码。"""
    artifact_path = Path("var/artifacts") / ARTIFACT_NAME
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(HOCON, encoding="utf-8")
    print(f"制品已写入 {artifact_path}（{len(HOCON)} 字符）")

    project = project_id()
    admin = admin_id()
    approver = approver_id()

    # 凭据物化只认 plan 里声明过的 connection_id，所以这里先解析出真实连接。
    contracts = [
        {**contract, "connection_id": _connection_id_for(contract["role"])}
        for contract in PROFILE_CONTRACTS
    ]

    print("\n=== 1 · 建 Pipeline（冻结 v1 DRAFT）===")
    status, version = call(
        "POST",
        f"/projects/{project}/pipelines",
        admin,
        {"name": "mysql-to-doris", "requirement": "MySQL 订单同步到 Doris，邮箱脱敏"},
    )
    step("POST /projects/{id}/pipelines", status, _brief(version), "2xx")
    if not isinstance(version, dict) or not version.get("id"):
        return report()
    version_id = version["id"]

    print("\n=== 2 · 记录生成制品（真实 HOCON）===")
    status, _ = call(
        "POST",
        f"/pipeline-versions/{version_id}/record-generation",
        admin,
        {
            "plan": {"profile_contracts": contracts},
            "hocon": HOCON,
            "artifact_uri": ARTIFACT_NAME,
            "upstream_version": "e2e-run",
        },
    )
    step("POST /pipeline-versions/{id}/record-generation", status, "", "2xx")
    if not 200 <= status < 300:
        return report()

    print("\n=== 2b · 确定性校验（推进 VALIDATED）===")
    status, validated = call("POST", f"/pipeline-versions/{version_id}/validate", admin)
    detail = validated.get("status") if isinstance(validated, dict) else str(validated)
    step("POST /pipeline-versions/{id}/validate", status, f"status={detail}", "2xx")
    if not 200 <= status < 300:
        return report()

    print("\n=== 3 · 提交审批并批准 ===")
    status, approval = call(
        "POST", f"/pipeline-versions/{version_id}/approvals", admin, {"reason": "e2e 验证"}
    )
    step("POST /pipeline-versions/{id}/approvals", status, _brief(approval), "2xx")
    if not isinstance(approval, dict) or not approval.get("id"):
        return report()
    status, decision = call(
        "POST",
        f"/approvals/{approval['id']}/decisions",
        approver,
        {"decision": "APPROVE", "reason": "e2e 验证通过"},
    )
    detail = decision.get("status") if isinstance(decision, dict) else str(decision)
    step("POST /approvals/{id}/decisions", status, f"status={detail}", "2xx")

    print("\n=== 4 · Prepare + 任务审批 ===")
    status, preparation = call(
        "POST", f"/pipeline-versions/{version_id}/execution-preparations", admin, {"intent": None}
    )
    step("POST /execution-preparations", status, _brief(preparation), "2xx")
    if not isinstance(preparation, dict) or not preparation.get("id"):
        return report()
    status, decision = call(
        "POST",
        f"/execution-preparations/{preparation['id']}/decisions",
        approver,
        {"role": "DATA_APPROVER", "decision": "APPROVE", "reason": "e2e 验证"},
    )
    detail = decision.get("status") if isinstance(decision, dict) else str(decision)
    step("POST /execution-preparations/{id}/decisions", status, f"status={detail}", "2xx")

    print("\n=== 5 · Commit ===")
    status, run = call("POST", f"/execution-preparations/{preparation['id']}/commit", admin)
    step("POST /execution-preparations/{id}/commit", status, _brief(run), "2xx")
    if not isinstance(run, dict) or not run.get("execution_run_id"):
        return report()
    run_id = run["execution_run_id"]

    print("\n=== 6 · 轮询执行终态 ===")
    payload = None
    for index in range(40):
        _, payload = call("GET", f"/execution-runs/{run_id}", admin)
        state = payload.get("status") if isinstance(payload, dict) else None
        print(f"  [{index:02d}] {state}")
        if state in TERMINAL_STATUSES:
            break
        time.sleep(5)
    if isinstance(payload, dict):
        for key in ("status", "engine_status", "source_rows", "sink_rows", "failure_category"):
            print(f"    {key} = {payload.get(key)}")
        step(
            "执行到达终态",
            200 if payload.get("status") in TERMINAL_STATUSES else 500,
            f"status={payload.get('status')} 读={payload.get('source_rows')} 写={payload.get('sink_rows')}",
            "2xx",
        )
    return report()


def _connection_id_for(role: str) -> str:
    """按契约角色取出对应连接的 ID：source 用 MySQL，sink 用 Doris。

    连接名可用 `ETL_AGENT_SOURCE_CONNECTION` / `ETL_AGENT_SINK_CONNECTION` 覆盖。
    """
    if role == "source":
        name = os.environ.get("ETL_AGENT_SOURCE_CONNECTION", "mysql-e2e-orders")
    else:
        name = os.environ.get("ETL_AGENT_SINK_CONNECTION", "doris-analytics")
    return str(connection_named(name)["id"])


def _brief(payload: object) -> str:
    """把响应压成一行短摘要。"""
    if not isinstance(payload, dict):
        return str(payload)[:80]
    keys = ("id", "status", "execution_run_id")
    return " ".join(f"{key}={payload[key]}" for key in keys if key in payload)


if __name__ == "__main__":
    raise SystemExit(main())
