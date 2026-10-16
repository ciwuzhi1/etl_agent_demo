"""执行链路冒烟：Prepare → 审批 → Commit → 等终态 → 读事件与质量报告。

复用项目里最近一个 APPROVED 版本，不新建版本，因此可以在 `pipeline_run.py`
之后反复执行，专门验证「提交到引擎 → 状态回写 → 指标与质量报告落库」这一段。

用法：uv run python scripts/e2e/engine_smoke.py
"""

import time

from _client import (
    TERMINAL_STATUSES,
    admin_id,
    approved_version,
    approver_id,
    call,
    poll,
    report,
    step,
)


def main() -> int:
    """跑一次执行冒烟，返回进程退出码。"""
    admin = admin_id()
    approver = approver_id()
    version_id = approved_version()
    print(f"使用已批准版本 {version_id}")

    print("\n=== 1 · Prepare（提交执行准备）===")
    status, preparation = call(
        "POST", f"/pipeline-versions/{version_id}/execution-preparations", admin, {"intent": None}
    )
    detail = preparation.get("status") if isinstance(preparation, dict) else str(preparation)
    step("POST /pipeline-versions/{id}/execution-preparations", status, f"status={detail}", "2xx")
    if not isinstance(preparation, dict) or not preparation.get("id"):
        return report()
    preparation_id = preparation["id"]

    print("\n=== 2 · 审批通过（四眼之一：DATA_APPROVER）===")
    status, decision = call(
        "POST",
        f"/execution-preparations/{preparation_id}/decisions",
        approver,
        {"role": "DATA_APPROVER", "decision": "APPROVE", "reason": "冒烟验证"},
    )
    detail = decision.get("status") if isinstance(decision, dict) else str(decision)
    step("POST /execution-preparations/{id}/decisions", status, f"status={detail}", "2xx")

    print("\n=== 3 · Commit（提交执行）===")
    status, run = call("POST", f"/execution-preparations/{preparation_id}/commit", admin)
    detail = str(run)[:120] if isinstance(run, dict) else str(run)
    step("POST /execution-preparations/{id}/commit", status, detail, "2xx")
    if not isinstance(run, dict):
        return report()
    run_id = run.get("execution_run_id") or run.get("run_id") or run.get("id")
    if not run_id:
        return report()

    print("\n=== 4 · 轮询执行到终态 ===")
    payload = None
    for index in range(60):
        _, payload = call("GET", f"/execution-runs/{run_id}", admin)
        state = payload.get("status") if isinstance(payload, dict) else None
        print(f"  [{index:02d}] status={state}")
        if state in TERMINAL_STATUSES:
            break
        time.sleep(5)
    if isinstance(payload, dict):
        print("\n最终字段:")
        for key, value in payload.items():
            print(f"  {key} = {str(value)[:200]}")
        step(
            "执行到达终态",
            200 if payload.get("status") in TERMINAL_STATUSES else 500,
            f"status={payload.get('status')} 读={payload.get('source_rows')} 写={payload.get('sink_rows')}",
            "2xx",
        )

    print("\n=== 5 · 执行事件与质量报告 ===")
    status, events = call("GET", f"/execution-runs/{run_id}/events", admin)
    count = len(events) if isinstance(events, list) else events
    step("GET /execution-runs/{id}/events", status, f"事件数={count}", "2xx")
    status, quality = call("GET", f"/execution-runs/{run_id}/quality-report", admin)
    step("GET /execution-runs/{id}/quality-report", status, str(quality)[:120], "2xx")

    # 观测端点收敛是异步的，等一次再读，避免把"还没写回"误判成故障。
    poll(f"/execution-runs/{run_id}", admin, "status", TERMINAL_STATUSES, limit=1)
    return report()


if __name__ == "__main__":
    raise SystemExit(main())
