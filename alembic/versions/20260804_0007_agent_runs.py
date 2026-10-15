"""create agent_runs table for tracking Agent execution runs

Revision ID: 20260804_0007
Revises: 20260804_0006
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0007"
down_revision: str | None = "20260804_0006"
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
    """创建 agent_runs 表用于跟踪 Agent 执行记录。"""
    op.create_table(
        "agent_runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "pipeline_version_id",
            sa.Uuid(),
            sa.ForeignKey("pipeline_versions.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("thread_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("requirement", sa.Text(), nullable=False),
        sa.Column("metadata_context", sa.JSON(), nullable=False),
        sa.Column("candidate_hocon", sa.Text()),
        sa.Column("approved_hocon", sa.Text()),
        sa.Column("error_message", sa.Text()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("failed_at", sa.DateTime(timezone=True)),
        sa.Column("created_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        *timestamps(),
    )
    op.create_index(
        "ix_agent_runs_project_status",
        "agent_runs",
        ["project_id", "status"],
    )


def downgrade() -> None:
    """移除 agent_runs 表及其索引。"""
    op.drop_index("ix_agent_runs_project_status", "agent_runs")
    op.drop_table("agent_runs")
