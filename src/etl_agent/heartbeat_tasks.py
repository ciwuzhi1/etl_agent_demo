"""Worker 心跳任务：Beat 周期上报节点存活，供控制台 `/workers` 判定在线状态。

设计要点（对应第 15 章控制面运行时闭环）：

- `worker_id` 直接用 Celery 当前节点名（形如 ``celery@host``）：它天然唯一，
  且与运维在 `celery inspect` 里看到的节点一一对上，不需要另造一套命名。
- 心跳是 upsert：同一节点重复上报只更新一行，`worker_id` 唯一约束兜底并发。
- 心跳自身绝不允许把 Worker 带崩：`active_tasks` 只做 best-effort 统计，任何
  异常都退化为 0；在线与否留给读侧按 `worker_heartbeat_stale_seconds` 现算。
"""

import socket
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert

from etl_agent.asyncio_compat import run_async
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.models import WorkerHeartbeat
from etl_agent.worker import celery_app


def _active_task_count() -> int:
    """用 Celery inspect 统计本节点当前活跃任务数；拿不到就返回 0。

    `inspect()` 会向 broker 广播并等待应答，本机 broker 或网络异常时可能失败或
    超时；心跳是"尽力而为"信号，因此任何异常都吞掉并退化为 0，绝不让心跳任务
    因为统计失败而失败。
    """
    try:
        inspector = celery_app.control.inspect(timeout=1.0)
        active: dict[str, list[dict[str, Any]]] | None = inspector.active()
        if not active:
            return 0
        return sum(len(tasks) for tasks in active.values())
    except Exception:
        return 0


async def _upsert_heartbeat(worker_id: str, hostname: str, active_tasks: int) -> None:
    """按 worker_id upsert 一行心跳，`last_seen_at` 与 `active_tasks` 每次都刷新。"""
    now = datetime.now(UTC)
    async with SessionFactory() as session:
        async with session.begin():
            statement = (
                pg_insert(WorkerHeartbeat)
                .values(
                    id=uuid.uuid4(),
                    worker_id=worker_id,
                    hostname=hostname,
                    last_seen_at=now,
                    active_tasks=active_tasks,
                    status="ONLINE",
                )
                .on_conflict_do_update(
                    index_elements=[WorkerHeartbeat.worker_id],
                    set_={
                        "hostname": hostname,
                        "last_seen_at": now,
                        "active_tasks": active_tasks,
                        "status": "ONLINE",
                        "updated_at": now,
                    },
                )
            )
            await session.execute(statement)


@celery_app.task(bind=True, name="etl_agent.worker_beat")  # type: ignore[untyped-decorator]
def worker_beat(self: Any) -> dict[str, object]:
    """Beat 周期上报：把当前 Celery 节点名的存活事实写进 worker_heartbeats。"""
    hostname = socket.gethostname()
    # bind=True 时可拿到执行本次任务的节点名（形如 celery@host）；缺失则退化为主机名。
    worker_id = str(getattr(self.request, "hostname", "") or hostname)
    active_tasks = _active_task_count()
    run_async(_upsert_heartbeat(worker_id, hostname, active_tasks))
    return {"worker_id": worker_id, "hostname": hostname, "active_tasks": active_tasks}
