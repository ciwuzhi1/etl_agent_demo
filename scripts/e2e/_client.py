"""端到端驱动脚本共用的 HTTP 客户端与实体发现。

所有脚本默认经 Vite 开发代理访问控制面，与浏览器走**完全同一条链路**
（Vite 代理再把 /api 转发到 uvicorn）。可用环境变量覆盖：

    ETL_AGENT_BASE_URL     默认 http://localhost:5173/api/v1
    ETL_AGENT_PROJECT_ID   缺省时取 /context 返回的首个工作区
    ETL_AGENT_ADMIN_ID     缺省时取 /context 中持有 ADMIN 的成员
    ETL_AGENT_APPROVER_ID  缺省时取 /context 中持有 DATA_APPROVER 的成员

不写死工作区与成员的 UUID，是为了让脚本能在任意一份种子数据上直接跑。
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

# Windows 控制台默认按本地代码页（cp936）编码标准输出，中文报告会变成乱码。
# 显式切到 UTF-8，让脚本在 PowerShell / Git Bash / CI 里输出一致。
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_URL = os.environ.get("ETL_AGENT_BASE_URL", "http://localhost:5173/api/v1")

# 运行终态：出现这些状态就停止轮询。
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"})

_workspace: dict[str, Any] | None = None
_results: list[tuple[str, str]] = []


def call(
    method: str,
    path: str,
    actor: str | None = None,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 120.0,
) -> tuple[int, Any]:
    """发一次 JSON 请求，返回 (状态码, 解析后的响应)。

    连接层失败不抛异常：驱动脚本要把「连不上」也当成一步结果展示出来。
    """
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Accept": "application/json"}
    if actor:
        headers["X-Actor-Id"] = actor
    if data is not None:
        headers["Content-Type"] = "application/json"
    return _send(method, path, data, headers, timeout)


def post_multipart(
    path: str,
    actor: str,
    *,
    filename: str,
    content: bytes,
    content_type: str = "text/csv",
    timeout: float = 60.0,
) -> tuple[int, Any]:
    """用 multipart/form-data 上传一个内存文件，模拟浏览器「上传文件」。"""
    boundary = f"----e2e{uuid.uuid4().hex}"
    body = b"".join(
        [
            f"--{boundary}\r\n".encode(),
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode(),
            f"Content-Type: {content_type}\r\n\r\n".encode(),
            content,
            f"\r\n--{boundary}--\r\n".encode(),
        ]
    )
    headers = {
        "X-Actor-Id": actor,
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }
    return _send("POST", path, body, headers, timeout)


def _send(
    method: str,
    path: str,
    data: bytes | None,
    headers: dict[str, str],
    timeout: float,
) -> tuple[int, Any]:
    request = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return response.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw[:300]
    except Exception as exc:
        return 0, f"连接失败: {exc}"


def workspace(*, refresh: bool = False) -> dict[str, Any]:
    """读取并缓存 /context（工作区与成员名册）。"""
    global _workspace
    if _workspace is None or refresh:
        status, payload = call("GET", "/context")
        if status != 200 or not isinstance(payload, dict):
            raise SystemExit(f"读取 /context 失败（HTTP {status}）：{payload}")
        _workspace = payload
    return _workspace


def project_id() -> str:
    """当前工作区 UUID，优先取环境变量。"""
    return os.environ.get("ETL_AGENT_PROJECT_ID") or str(workspace()["project_id"])


def member_with_role(role: str) -> str:
    """按 Harness 职责挑一名成员，返回其 user_id。"""
    for member in workspace()["members"]:
        if role in member.get("harness_roles", []):
            return str(member["user_id"])
    raise SystemExit(f"名册中没有持有 {role} 的成员，请先在项目中授权该职责")


def member_by_subject(subject: str) -> dict[str, Any] | None:
    """按 subject 找成员，找不到返回 None。"""
    for member in workspace()["members"]:
        if member.get("subject") == subject:
            return member
    return None


def member_with_project_role(role: str) -> str:
    """按**项目角色**（ADMIN / DATA_ENGINEER / APPROVER / VIEWER）挑一名成员。

    项目角色与 Harness 职责是两套词汇：管理员的项目角色是 ADMIN，但 Harness 职责
    里并没有 ADMIN 这一项，所以不能拿 `member_with_role("ADMIN")` 来找提交人。
    """
    for member in workspace()["members"]:
        if member.get("role") == role:
            return str(member["user_id"])
    raise SystemExit(f"名册中没有项目角色为 {role} 的成员")


def admin_id() -> str:
    """提交侧身份：默认取项目角色为 ADMIN 的成员。"""
    return os.environ.get("ETL_AGENT_ADMIN_ID") or member_with_project_role("ADMIN")


def approver_id() -> str:
    """审批侧身份：默认取持有 DATA_APPROVER 的成员。"""
    return os.environ.get("ETL_AGENT_APPROVER_ID") or member_with_role("DATA_APPROVER")


def connection_named(name: str) -> dict[str, Any]:
    """按名字取项目内已登记的数据连接。"""
    status, connections = call("GET", f"/projects/{project_id()}/connections", admin_id())
    if not isinstance(connections, list):
        raise SystemExit(f"读取连接列表失败（HTTP {status}）：{connections}")
    for connection in connections:
        if connection.get("name") == name:
            return connection  # type: ignore[no-any-return]
    raise SystemExit(f"未找到连接 {name}，请先登记该连接")


def ready_connection() -> dict[str, Any]:
    """取第一条已 READY 的连接，供探查与审批链路使用。"""
    status, connections = call("GET", f"/projects/{project_id()}/connections", admin_id())
    if not isinstance(connections, list):
        raise SystemExit(f"读取连接列表失败（HTTP {status}）：{connections}")
    for connection in connections:
        if connection.get("status") == "READY":
            return connection  # type: ignore[no-any-return]
    raise SystemExit("项目内没有 READY 连接，请先测试连接使其就绪")


def approved_version() -> str:
    """取最近一个已批准（含制品）的 Pipeline 版本，用于执行与观测链路。"""
    status, versions = call("GET", f"/projects/{project_id()}/pipeline-versions", admin_id())
    if not isinstance(versions, list):
        raise SystemExit(f"读取版本列表失败（HTTP {status}）：{versions}")
    for version in versions:
        if version.get("status") == "APPROVED":
            return str(version["id"])
    raise SystemExit("项目内没有 APPROVED 的 Pipeline 版本，请先跑 pipeline_run.py 生成一个")


def latest_run_id() -> str:
    """取最近一条执行记录，供回滚与观测端点使用。"""
    status, runs = call("GET", f"/projects/{project_id()}/execution-runs", admin_id())
    if not isinstance(runs, list) or not runs:
        raise SystemExit(f"项目内没有执行记录（HTTP {status}）")
    return str(runs[0]["id"])


def poll(
    path: str,
    actor: str,
    field: str,
    terminal: set[str] | frozenset[str],
    *,
    limit: int = 20,
    gap: float = 2.0,
) -> Any:
    """轮询到终态，返回最后一次响应。"""
    payload: Any = None
    for _ in range(limit):
        _, payload = call("GET", path, actor)
        if isinstance(payload, dict) and payload.get(field) in terminal:
            return payload
        time.sleep(gap)
    return payload


def step(label: str, status: int, detail: str, expect: str) -> None:
    """记录并打印一步的判定。

    expect 取 "2xx"、"rejected"（任意 4xx 都算失败关闭成功）或具体状态码字符串。
    """
    if expect == "2xx":
        ok = 200 <= status < 300
    elif expect == "rejected":
        ok = 400 <= status < 500
    else:
        ok = status == int(expect)
    mark = "PASS" if ok else "FAIL"
    _results.append((label, mark))
    print(f"  [{mark}] {label:<46} HTTP {status}  {detail}")


def report() -> int:
    """打印合计并按是否全绿返回进程退出码。"""
    passed = sum(1 for _, mark in _results if mark == "PASS")
    print(f"\n合计：{passed}/{len(_results)} 步 PASS")
    return 0 if passed == len(_results) else 1
