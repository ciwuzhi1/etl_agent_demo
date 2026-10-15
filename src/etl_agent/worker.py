from celery import Celery

from etl_agent.config import get_settings

settings = get_settings()
celery_app = Celery(
    "etl_agent",
    broker=settings.redis_url,
    backend=settings.redis_url,
    # 任务模块按章节追加：connection_tasks（第 4 章）、file_asset_tasks（第 5 章）、
    # heartbeat/agent/execution/schedule（第 8/10/15/16 章）、harness_outbox_tasks
    # （第 11 章）与 side_effect_tasks（第 12 章）。模块必须显式登记，否则 Worker
    # 收到消息会因"未注册任务"直接丢弃。
    include=[
        "etl_agent.connection_tasks",
        "etl_agent.file_asset_tasks",
        "etl_agent.heartbeat_tasks",
        "etl_agent.agent_tasks",
        "etl_agent.execution_tasks",
        "etl_agent.schedule_tasks",
        "etl_agent.harness_outbox_tasks",
        "etl_agent.side_effect_tasks",
    ],
)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    # 四条 Beat 周期任务，全部用浮点秒数（celery.schedules.crontab 不支持秒级，
    # 直接写秒数最可靠）：
    #   心跳 10s / 凭据到期巡检 3600s / 调度扫描 30s / Outbox 扫描 2s。
    beat_schedule={
        "worker-heartbeat": {
            "task": "etl_agent.worker_beat",
            "schedule": float(settings.worker_heartbeat_interval_seconds),
        },
        "credential-expiry-scan": {
            "task": "etl_agent.scan_credential_expiry",
            "schedule": float(settings.credential_alert_scan_interval_seconds),
        },
        "schedule-scan": {
            "task": "etl_agent.scan_schedules",
            "schedule": float(settings.schedule_scan_interval_seconds),
        },
        "harness-outbox-scan": {
            "task": "harness.scan-outbox",
            "schedule": float(settings.harness_outbox_scan_interval_seconds),
        },
    },
)
