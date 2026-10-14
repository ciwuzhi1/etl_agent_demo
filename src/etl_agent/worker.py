from celery import Celery

from etl_agent.config import get_settings

settings = get_settings()
celery_app = Celery(
    "etl_agent",
    broker=settings.redis_url,
    backend=settings.redis_url,
    # 任务模块按章节追加：connection_tasks 已在第 4 章接入；后续还有 tasks、agent_tasks、
    # operations_tasks、rollback_tasks、cleanup_tasks、secret_* 、credential_alert_tasks、
    # file_asset_tasks、schedule_tasks、harness_outbox_tasks。
    include=["etl_agent.connection_tasks"],
)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    # 心跳、凭据告警、Pipeline 调度与 Harness Outbox 四条 Beat 周期任务，
    # 依赖后续章节的任务实现；任务模块就位后在此登记，未登记前不要启动 Beat。
    beat_schedule={},
)
