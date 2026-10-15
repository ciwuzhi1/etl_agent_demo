"""增加默认关闭的逐行 LLM Transform 冻结契约。

Revision ID: 20260804_0006
Revises: 20260804_0005
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0006"
down_revision: str | None = "20260804_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """为 AgentRun 增加 JSON 契约；历史记录为空即明确表示未授权逐行外发。"""
    op.add_column("agent_runs", sa.Column("row_llm_contract", sa.JSON(), nullable=True))


def downgrade() -> None:
    """移除逐行 LLM 契约字段，不影响已经冻结的历史 Artifact 文件。"""
    op.drop_column("agent_runs", "row_llm_contract")
