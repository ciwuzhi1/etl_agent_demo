"""create worker_heartbeats table

Revision ID: 20260804_0012
Revises: 4597eb861c2a
Create Date: 2026-08-04

第 15 章 Worker 心跳落库结构：Beat 周期任务按 Celery 节点名 upsert 一行，
控制台 `/workers` 据此现算在线状态。`worker_id` 唯一——同一节点只保留最新一次
心跳，不写历史（进程崩溃时没有机会把行改成 OFFLINE，在线与否只能由读侧按
`worker_heartbeat_stale_seconds` 判定）。
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0012"
down_revision: str | None = "4597eb861c2a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def timestamps() -> list[sa.Column]:
    """返回统一时间戳列定义，保持非空约束和数据库默认值一致。"""
    return [
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    ]


def upgrade() -> None:
    """创建 worker_heartbeats 表及 worker_id 唯一约束。"""
    op.create_table(
        "worker_heartbeats",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("worker_id", sa.String(128), nullable=False),
        sa.Column("hostname", sa.String(255), nullable=False),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("active_tasks", sa.Integer(), server_default="0", nullable=False),
        sa.Column("status", sa.String(32), server_default="ONLINE", nullable=False),
        sa.UniqueConstraint("worker_id", name="uq_worker_heartbeats_worker_id"),
        *timestamps(),
    )


def downgrade() -> None:
    """移除 worker_heartbeats 表。"""
    op.drop_table("worker_heartbeats")
