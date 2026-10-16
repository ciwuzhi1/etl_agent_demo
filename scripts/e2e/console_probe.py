"""控制台五个视图的数据接口巡检（只读）。

所有请求都是 GET，不产生副作用，适合作为前端联调前的「后端是否喂得出数据」自检。

用法：uv run python scripts/e2e/console_probe.py
"""

from _client import (
    BASE_URL,
    admin_id,
    approved_version,
    call,
    latest_run_id,
    project_id,
    report,
    step,
)


def summarize(payload: object) -> str:
    """把响应压成一行摘要，便于人工核对非空与非错误态。"""
    if isinstance(payload, str):
        return payload.replace("\n", " ")[:100]
    if isinstance(payload, list):
        return f"list[{len(payload)}]"
    if isinstance(payload, dict):
        return f"object({','.join(list(payload)[:5])})"
    return repr(payload)


def main() -> int:
    """按视图顺序打一遍接口，返回进程退出码。"""
    project = project_id()
    admin = admin_id()
    version_id = approved_version()
    run_id = latest_run_id()

    views: list[tuple[str, list[str]]] = [
        (
            "① 总览 Overview",
            [
                "/context",
                f"/projects/{project}/overview",
                f"/projects/{project}/connections",
                "/workers",
            ],
        ),
        (
            "② 数据连接 Connections",
            [
                f"/projects/{project}/connections",
                f"/projects/{project}/data-profiles",
                f"/projects/{project}/file-assets",
            ],
        ),
        (
            "③ Pipeline Studio",
            [
                # POST /projects/{id}/pipelines 会写数据，只读巡检不调用。
                f"/projects/{project}/pipeline-versions",
                f"/projects/{project}/agent-runs",
                f"/pipeline-versions/{version_id}",
                f"/pipeline-versions/{version_id}/artifact",
            ],
        ),
        (
            "④ 运行中心 Operations",
            [
                f"/projects/{project}/execution-runs",
                f"/projects/{project}/execution-preparations",
                f"/execution-runs/{run_id}",
                f"/execution-runs/{run_id}/events",
                f"/execution-runs/{run_id}/quality-report",
                f"/execution-runs/{run_id}/supervision",
            ],
        ),
        (
            "⑤ 安全治理 Governance",
            [
                f"/projects/{project}/safety-proposals",
                f"/projects/{project}/secret-lifecycles",
                "/benchmark-runs/latest",
            ],
        ),
    ]

    print(f"控制面 {BASE_URL}\n")
    for title, paths in views:
        print(title)
        for path in paths:
            status, payload = call("GET", path, admin, timeout=15.0)
            step(path, status, summarize(payload), "2xx")
        print()

    return report()


if __name__ == "__main__":
    raise SystemExit(main())
