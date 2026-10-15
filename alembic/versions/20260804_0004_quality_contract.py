"""add agent run quality contract

Revision ID: 20260804_0004
Revises: 20260804_0003
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0004"
down_revision: str | None = "20260804_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """向前应用本版本数据库结构变更；DDL 必须与当前 ORM 元数据保持一致。"""
    op.add_column("agent_runs", sa.Column("quality_contract", sa.JSON(), nullable=True))


def downgrade() -> None:
    """按升级的反向依赖顺序撤销结构变更，避免残留表、索引或约束。"""
    op.drop_column("agent_runs", "quality_contract")
