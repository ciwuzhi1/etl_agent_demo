"""副作用治理全流程：对一条已成功的执行申请 Doris 回滚。

链路：发起 → DATA_APPROVER 占槽 → 申请人自批被拒 → DATA_ENGINEER 占槽 → 同一人不得
再占第二槽 → 提交执行（签发一次性能力 + 投递 Outbox）→ Worker 消费 → 终态。

前置：名册里需要有第三人持有 DATA_ENGINEER —— 后端禁止同一自然人占两个副作用审批槽，
而 ROLLBACK_DORIS 要求 DATA_ENGINEER + DATA_APPROVER 双人。脚本会先按需用管理员接口
补齐这名成员（幂等：已存在则复用）。

用法：uv run python scripts/e2e/side_effects.py
"""

import time

from _client import (
    admin_id,
    approver_id,
    call,
    latest_run_id,
    member_by_subject,
    project_id,
    report,
    step,
)

# 副作用治理要求第三人持有 DATA_ENGINEER，且不能与申请人、审批人重合。
THIRD_SUBJECT = "course.operator"
THIRD_NAME = "运行操作员"
TERMINAL_EXECUTION = {"SUCCEEDED", "FAILED"}


def ensure_third_member() -> str | None:
    """确保名册里存在持有 DATA_ENGINEER 的第三人，返回其 user_id。"""
    existing = member_by_subject(THIRD_SUBJECT)
    if existing is not None:
        print(f"  已存在成员 {THIRD_SUBJECT} → {existing['user_id']}")
        return str(existing["user_id"])

    project = project_id()
    admin = admin_id()
    status, created = call(
        "POST",
        f"/projects/{project}/members",
        admin,
        {"subject": THIRD_SUBJECT, "display_name": THIRD_NAME, "role": "DATA_ENGINEER"},
    )
    if status not in (200, 201) or not isinstance(created, dict):
        print(f"  建成员失败：{status} {created}")
        return None
    user_id = created["user_id"]
    grant_status, _ = call(
        "POST",
        f"/projects/{project}/members/{user_id}/harness-roles",
        admin,
        {"roles": ["DATA_ENGINEER"]},
    )
    print(f"  新建成员 {THIRD_SUBJECT} → {user_id}（授予 DATA_ENGINEER：{grant_status}）")
    return str(user_id)


def main() -> int:
    """跑完副作用治理全流程，返回进程退出码。"""
    project = project_id()
    admin = admin_id()
    approver = approver_id()

    print("=== 步骤 0 · 名册准备（第三人 DATA_ENGINEER）===")
    third = ensure_third_member()
    step("确保第三人存在", 200 if third else 500, f"user_id={third}", "2xx")
    if not third:
        return report()

    # 回滚以一条真实执行记录为聚合根；取最近的运行，避免依赖某条历史数据。
    aggregate_id = latest_run_id()
    print(f"  回滚聚合根 run_id={aggregate_id}")

    print("\n=== 步骤 1 · 发起 Doris 回滚（运行中心「申请回滚」）===")
    status, preparation = call(
        "POST",
        f"/projects/{project}/side-effect-preparations",
        admin,
        {
            "operation": "ROLLBACK_DORIS",
            "aggregate_id": aggregate_id,
            "context": {"run_id": aggregate_id, "reason": "行数异常，回滚到上一版本"},
        },
    )
    preparation_id = preparation.get("id") if isinstance(preparation, dict) else None
    detail = (
        f"status={preparation.get('status')} 职责={preparation.get('required_roles')} "
        f"槽位={preparation.get('required_approvals')}"
        if isinstance(preparation, dict)
        else str(preparation)
    )
    step("POST /projects/{id}/side-effect-preparations", status, detail, "2xx")
    if not preparation_id:
        return report()

    print("\n=== 步骤 2 · 安全治理：「通过」按钮 — DATA_APPROVER 槽 ===")
    status, decided = call(
        "POST",
        f"/side-effect-preparations/{preparation_id}/decisions",
        approver,
        {"role": "DATA_APPROVER", "decision": "APPROVE", "reason": "控制台审批通过"},
    )
    detail = decided.get("status") if isinstance(decided, dict) else str(decided)
    step("DATA_APPROVER 占槽", status, detail, "2xx")

    print("\n=== 步骤 2b · 四眼守卫：申请人自批必须被拒 ===")
    status, denied = call(
        "POST",
        f"/side-effect-preparations/{preparation_id}/decisions",
        admin,
        {"role": "DATA_ENGINEER", "decision": "APPROVE", "reason": "自批尝试"},
    )
    detail = str(denied.get("detail") if isinstance(denied, dict) else denied)[:50]
    step("申请人自批（应被拒）", status, f"detail={detail}", "rejected")

    print("\n=== 步骤 3 · 安全治理：「通过」按钮 — DATA_ENGINEER 槽 ===")
    status, decided = call(
        "POST",
        f"/side-effect-preparations/{preparation_id}/decisions",
        third,
        {"role": "DATA_ENGINEER", "decision": "APPROVE", "reason": "控制台审批通过"},
    )
    detail = decided.get("status") if isinstance(decided, dict) else str(decided)
    step("DATA_ENGINEER 占槽 → READY", status, detail, "2xx")

    print("\n=== 步骤 3b · 一人不得占两槽 ===")
    status, duplicate = call(
        "POST",
        f"/side-effect-preparations/{preparation_id}/decisions",
        third,
        {"role": "DATA_APPROVER", "decision": "APPROVE", "reason": "想再占一槽"},
    )
    detail = str(duplicate.get("detail") if isinstance(duplicate, dict) else duplicate)[:50]
    step("同一人占第二槽（应被拒）", status, f"detail={detail}", "rejected")

    print("\n=== 步骤 4 · 提交执行（OPERATOR）→ 签发一次性能力 + 投递 Outbox ===")
    status, committed = call("POST", f"/side-effect-preparations/{preparation_id}/commit", admin)
    detail = (
        f"execution_id={str(committed.get('execution_id', ''))[:8]} "
        f"令牌={str(committed.get('token_id', ''))[:8]}"
        if isinstance(committed, dict)
        else str(committed)
    )
    step("POST /side-effect-preparations/{id}/commit", status, detail, "2xx")

    print("\n=== 步骤 5 · 轮询执行状态（Worker 消费 Outbox）===")
    final: object = {}
    for _ in range(15):
        _, final = call("GET", f"/side-effect-preparations/{preparation_id}", admin)
        if isinstance(final, dict) and final.get("execution_status") in TERMINAL_EXECUTION:
            break
        time.sleep(2)
    execution_status = final.get("execution_status") if isinstance(final, dict) else final
    step("轮询执行状态", 200, f"execution_status={execution_status}", "2xx")

    print("\n=== 步骤 6 · 列表与详情端点 ===")
    status, rows = call("GET", f"/projects/{project}/side-effect-preparations", admin)
    count = len(rows) if isinstance(rows, list) else "?"
    step("副作用准备单列表", status, f"count={count}", "2xx")
    status, one = call("GET", f"/side-effect-preparations/{preparation_id}", admin)
    detail = one.get("status") if isinstance(one, dict) else str(one)
    step("副作用准备单详情", status, f"status={detail}", "2xx")

    return report()


if __name__ == "__main__":
    raise SystemExit(main())
