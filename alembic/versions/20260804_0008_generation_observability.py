"""add generation tracking columns to agent_runs

Revision ID: 20260804_0008
Revises: 20260804_0007
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0008"
down_revision: str | None = "20260804_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """为 agent_runs 增加生成追踪与可观测性字段。"""
    op.add_column("agent_runs", sa.Column("generation_backend", sa.String(32)))
    op.add_column("agent_runs", sa.Column("generation_model", sa.String(255)))
    op.add_column("agent_runs", sa.Column("generation_upstream_version", sa.String(255)))
    op.add_column(
        "agent_runs",
        sa.Column("generation_provider_calls", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "agent_runs",
        sa.Column("generation_candidates", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "agent_runs",
        sa.Column("generation_repairs", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "agent_runs",
        sa.Column("generation_elapsed_ms", sa.BigInteger(), server_default="0", nullable=False),
    )
    op.add_column("agent_runs", sa.Column("generation_phase", sa.String(64)))
    op.add_column("agent_runs", sa.Column("generation_diagnostics", sa.JSON()))
    op.add_column("agent_runs", sa.Column("generation_started_at", sa.DateTime(timezone=True)))
    op.add_column("agent_runs", sa.Column("generation_finished_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    """移除生成追踪扩展字段，不影响已有执行记录。"""
    op.drop_column("agent_runs", "generation_finished_at")
    op.drop_column("agent_runs", "generation_started_at")
    op.drop_column("agent_runs", "generation_diagnostics")
    op.drop_column("agent_runs", "generation_phase")
    op.drop_column("agent_runs", "generation_elapsed_ms")
    op.drop_column("agent_runs", "generation_repairs")
    op.drop_column("agent_runs", "generation_candidates")
    op.drop_column("agent_runs", "generation_provider_calls")
    op.drop_column("agent_runs", "generation_upstream_version")
    op.drop_column("agent_runs", "generation_model")
    op.drop_column("agent_runs", "generation_backend")
