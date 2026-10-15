"""freeze watermark contracts and successful positions

Revision ID: 20260804_0005
Revises: 20260804_0004
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0005"
down_revision: str | None = "20260804_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """保存 Agent 输入契约以及调度成功后才能推进的当前水位。"""
    op.add_column("agent_runs", sa.Column("incremental_contract", sa.JSON()))
    op.add_column("pipeline_schedules", sa.Column("watermark_value", sa.String(128)))
    op.add_column(
        "pipeline_schedules",
        sa.Column("watermark_updated_at", sa.DateTime(timezone=True)),
    )


def downgrade() -> None:
    """移除水位契约扩展字段，不改变既有 Pipeline 与执行记录。"""
    op.drop_column("pipeline_schedules", "watermark_updated_at")
    op.drop_column("pipeline_schedules", "watermark_value")
    op.drop_column("agent_runs", "incremental_contract")
