"""控制面逐步端到端驱动：按控制台五个视图的真实按钮调用顺序走一遍。

与浏览器完全同一条链路（经 Vite 代理）。每一步打印 HTTP 状态与关键字段，
既验证成功路径，也验证失败关闭（越权审批、终态改写、重复提交）。

用法：uv run python scripts/e2e/control_plane_flow.py
"""

import time

from _client import (
    admin_id,
    approved_version,
    approver_id,
    call,
    poll,
    post_multipart,
    project_id,
    ready_connection,
    report,
    step,
)


def main() -> int:
    """按视图顺序跑完全部步骤，返回进程退出码。"""
    project = project_id()
    admin = admin_id()
    approver = approver_id()

    print("=== 步骤 1 · 会话上下文（登录后 Vue 首屏）===")
    status, context = call("GET", "/context")
    name = context.get("project_name") if isinstance(context, dict) else None
    step("GET /context", status, f"project={name}", "2xx")

    connection_id = str(ready_connection()["id"])
    version_id = approved_version()

    print("\n=== 步骤 2 · 数据连接视图：测试连接 ===")
    status, connection = call("POST", f"/connections/{connection_id}/tests", admin)
    detail = connection.get("status") if isinstance(connection, dict) else str(connection)
    step("POST /connections/{id}/tests", status, f"status={detail}", "2xx")
    connection = poll(f"/connections/{connection_id}", admin, "status", {"READY", "FAILED"})
    step("轮询 GET /connections/{id}", 200, f"最终 status={connection.get('status')}", "2xx")

    print("\n=== 步骤 3 · 数据连接视图：发起探查 ===")
    status, profile = call(
        "POST",
        f"/connections/{connection_id}/profiles",
        admin,
        {"schema_name": "source_db", "table_name": "orders", "sample_limit": 5},
    )
    detail = profile.get("status") if isinstance(profile, dict) else str(profile)
    step("POST /connections/{id}/profiles", status, f"status={detail}", "2xx")
    if isinstance(profile, dict) and profile.get("id"):
        profiled = poll(f"/data-profiles/{profile['id']}", admin, "status", {"READY", "FAILED"})
        step("轮询 GET /data-profiles/{id}", 200, f"最终 status={profiled.get('status')}", "2xx")

    print("\n=== 步骤 4 · 数据连接视图：上传受管文件 ===")
    status, asset = post_multipart(
        f"/projects/{project}/file-assets",
        admin,
        filename="e2e-orders.csv",
        content=b"order_id,email\n1,a@example.com\n",
    )
    detail = asset.get("status") if isinstance(asset, dict) else str(asset)
    step("POST /projects/{id}/file-assets", status, f"status={detail}", "2xx")
    if isinstance(asset, dict) and asset.get("id"):
        parsed = poll(f"/file-assets/{asset['id']}", admin, "status", {"READY", "FAILED"})
        step("轮询 GET /file-assets/{id}", 200, f"最终 status={parsed.get('status')}", "2xx")

    print("\n=== 步骤 5 · Studio：「生成候选」按钮（真实 LLM 调用）===")
    status, run = call(
        "POST",
        f"/projects/{project}/agent-runs",
        admin,
        {"requirement": "把 MySQL 的订单同步到 Doris，邮箱脱敏", "profile_ids": []},
    )
    detail = run.get("status") if isinstance(run, dict) else str(run)
    step("POST /projects/{id}/agent-runs", status, f"status={detail}", "2xx")
    if isinstance(run, dict) and run.get("id"):
        agent_run_id = run["id"]
        run = poll(
            f"/agent-runs/{agent_run_id}",
            admin,
            "status",
            {"WAITING_APPROVAL", "WAITING_INPUT", "FAILED"},
            limit=30,
        )
        _, steps = call("GET", f"/agent-runs/{agent_run_id}/steps", admin)
        count = len(steps) if isinstance(steps, list) else 0
        step(
            "轮询 GET /agent-runs/{id}",
            200,
            f"最终 status={run.get('status')} 节点步骤={count}",
            "2xx",
        )

        if run.get("status") == "WAITING_INPUT":
            print("\n=== 步骤 5b · Studio：「提交回答并继续」===")
            status, answered = call(
                "POST",
                f"/agent-runs/{agent_run_id}/answers",
                admin,
                {"answer": "按 created_at 增量，水位窗口左开右闭，错误数据单独保存。"},
            )
            detail = answered.get("status") if isinstance(answered, dict) else str(answered)
            step("POST /agent-runs/{id}/answers", status, f"status={detail}", "2xx")
            resumed = poll(
                f"/agent-runs/{agent_run_id}",
                admin,
                "status",
                {"WAITING_APPROVAL", "WAITING_INPUT", "FAILED"},
                limit=30,
            )
            step("轮询恢复后的 run", 200, f"最终 status={resumed.get('status')}", "2xx")

    print("\n=== 步骤 6 · Studio：Prepare（「提交 Commit」按钮第一步）===")
    status, preparation = call(
        "POST", f"/pipeline-versions/{version_id}/execution-preparations", admin, {"intent": None}
    )
    preparation_id = preparation.get("id") if isinstance(preparation, dict) else None
    detail = (
        f"status={preparation.get('status')} 需审批={preparation.get('required_roles')}"
        if isinstance(preparation, dict)
        else str(preparation)
    )
    step("POST /pipeline-versions/{id}/execution-preparations", status, detail, "2xx")
    if not preparation_id:
        return report()

    print("\n=== 步骤 7 · Studio：「通过」按钮 ===")
    status, decision = call(
        "POST",
        f"/execution-preparations/{preparation_id}/decisions",
        approver,
        {"role": "DATA_APPROVER", "decision": "APPROVE", "reason": "控制台审批通过"},
    )
    detail = decision.get("status") if isinstance(decision, dict) else str(decision)
    step("POST /execution-preparations/{id}/decisions (APPROVER)", status, detail, "2xx")

    print("\n=== 步骤 7b · 失败关闭：无该职责的人审批必须被拒 ===")
    _, second = call(
        "POST", f"/pipeline-versions/{version_id}/execution-preparations", admin, {"intent": None}
    )
    status, denied = call(
        "POST",
        f"/execution-preparations/{second.get('id')}/decisions",
        admin,
        {"role": "DATA_APPROVER", "decision": "APPROVE", "reason": "越权尝试"},
    )
    detail = str(denied.get("detail") if isinstance(denied, dict) else denied)[:60]
    step("无 DATA_APPROVER 者审批（应被拒）", status, f"detail={detail}", "rejected")

    print("\n=== 步骤 8 · Studio：Commit ===")
    status, committed = call("POST", f"/execution-preparations/{preparation_id}/commit", admin)
    run_id = committed.get("execution_run_id") if isinstance(committed, dict) else None
    step("POST /execution-preparations/{id}/commit", status, f"run={run_id}", "2xx")

    print("\n=== 步骤 8b · 幂等：重复 Commit 必须返回同一执行编号 ===")
    status, replay = call("POST", f"/execution-preparations/{preparation_id}/commit", admin)
    same = replay.get("execution_run_id") if isinstance(replay, dict) else None
    step("重复 Commit", status, f"run={same} 与首次一致={same == run_id}", "2xx")

    if run_id:
        print("\n=== 步骤 9 · 运行中心：取消执行 ===")
        status, cancelled = call("POST", f"/execution-runs/{run_id}/cancellation", admin)
        detail = cancelled.get("status") if isinstance(cancelled, dict) else str(cancelled)
        step("POST /execution-runs/{id}/cancellation", status, detail, "2xx")

        print("\n=== 步骤 9b · 终态不可改写：再次取消必须被拒 ===")
        time.sleep(10)
        status, again = call("POST", f"/execution-runs/{run_id}/cancellation", admin)
        detail = str(again.get("detail") if isinstance(again, dict) else again)[:60]
        step("终态后重复取消（应被拒）", status, f"detail={detail}", "409")

        print("\n=== 步骤 10 · 运行中心：观测端点 ===")
        for path, label in (
            (f"/execution-runs/{run_id}", "GET /execution-runs/{id}"),
            (f"/execution-runs/{run_id}/events", "GET …/events"),
            (f"/execution-runs/{run_id}/quality-report", "GET …/quality-report"),
            (f"/execution-runs/{run_id}/supervision", "GET …/supervision"),
        ):
            status, payload = call("GET", path, admin)
            size = len(payload) if isinstance(payload, list) else "object"
            step(label, status, f"payload={size}", "2xx")

    print("\n=== 步骤 11 · 运行中心：申请回滚（走副作用治理）===")
    status, rollback = call(
        "POST",
        f"/projects/{project}/side-effect-preparations",
        admin,
        {"operation": "ROLLBACK_DORIS", "aggregate_id": run_id, "context": {"run_id": run_id}},
    )
    detail = (
        f"status={rollback.get('status')} 需审批={rollback.get('required_roles')}"
        if isinstance(rollback, dict)
        else str(rollback)
    )
    step("POST /projects/{id}/side-effect-preparations", status, detail, "2xx")

    print("\n=== 步骤 12 · 各视图列表端点 ===")
    for path, label in (
        (f"/projects/{project}/execution-preparations", "准备单列表"),
        (f"/projects/{project}/execution-runs", "执行列表"),
        (f"/projects/{project}/side-effect-preparations", "副作用准备单列表"),
        ("/workers", "Worker 列表"),
    ):
        status, payload = call("GET", path, admin)
        count = len(payload) if isinstance(payload, list) else "?"
        step(label, status, f"count={count}", "2xx")

    return report()


if __name__ == "__main__":
    raise SystemExit(main())
