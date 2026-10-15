"""create runtime_supervision_snapshots and supervision columns on execution_runs

Revision ID: 20260804_0010
Revises: 20260804_0008
Create Date: 2026-10-15

第 13 章（Runtime Supervision）：为运行监督快照建表，并在 execution_runs 上补齐
监督动作 / 契约摘要 / 隔离事实三类列。链尾从 0009（第 12 章副作用治理表）接续。
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0010"
down_revision: str | None = "20260804_0009"
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
    """建监督快照表，并给 execution_runs 增加运行监督与隔离列。"""
    op.create_table(
        "runtime_supervision_snapshots",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "run_id",
            sa.Uuid(),
            sa.ForeignKey("execution_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("engine_status", sa.String(32), nullable=False),
        sa.Column("action", sa.String(32), nullable=False),
        sa.Column("risk_level", sa.String(8), nullable=False),
        sa.Column("cancel_required", sa.Boolean(), nullable=False),
        sa.Column("block_publish", sa.Boolean(), nullable=False),
        sa.Column("next_poll_seconds", sa.Integer(), nullable=False),
        sa.Column("observed", sa.JSON(), nullable=False),
        sa.Column("budget", sa.JSON(), nullable=False),
        sa.Column("rule_matches", sa.JSON(), nullable=False),
        sa.Column("contract_sha256", sa.String(64), nullable=False),
        sa.Column("snapshot_sha256", sa.String(64), nullable=False),
        sa.Column("mitigation_status", sa.String(32), nullable=False),
        sa.Column("mitigation_error", sa.Text()),
        sa.Column("applied_at", sa.DateTime(timezone=True)),
        *timestamps(),
        sa.UniqueConstraint("run_id", "sequence", name="uq_runtime_supervision_run_sequence"),
    )
    op.create_index(
        "ix_runtime_supervision_run_observed",
        "runtime_supervision_snapshots",
        ["run_id", "observed_at"],
    )
    op.create_index(
        "ix_runtime_supervision_mitigation_status",
        "runtime_supervision_snapshots",
        ["mitigation_status"],
    )

    op.add_column("execution_runs", sa.Column("supervision_action", sa.String(32)))
    op.add_column("execution_runs", sa.Column("supervision_contract_sha256", sa.String(64)))
    op.add_column("execution_runs", sa.Column("quarantined_at", sa.DateTime(timezone=True)))
    op.add_column("execution_runs", sa.Column("quarantine_reason", sa.Text()))
    op.add_column("execution_runs", sa.Column("cancel_requested_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    """移除监督快照列与表，回退第 13 章的 schema 变更。"""
    op.drop_column("execution_runs", "cancel_requested_at")
    op.drop_column("execution_runs", "quarantine_reason")
    op.drop_column("execution_runs", "quarantined_at")
    op.drop_column("execution_runs", "supervision_contract_sha256")
    op.drop_column("execution_runs", "supervision_action")

    op.drop_index(
        "ix_runtime_supervision_mitigation_status",
        table_name="runtime_supervision_snapshots",
    )
    op.drop_index(
        "ix_runtime_supervision_run_observed",
        table_name="runtime_supervision_snapshots",
    )
    op.drop_table("runtime_supervision_snapshots")
