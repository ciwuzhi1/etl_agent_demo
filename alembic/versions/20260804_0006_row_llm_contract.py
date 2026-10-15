"""record row-level LLM contract slot (no dedicated column)

Revision ID: 20260804_0006
Revises: 20260804_0005
Create Date: 2026-10-15

同 0004/0005：`agent_runs` 在 0007 才创建，且逐行 LLM 契约在本仓库的落点是
`pipeline_versions.plan["row_llm_contract"]`（由 `agent/graph.py` 组装），
ORM 未声明独立列。默认关闭语义由 `ETL_AGENT_ROW_LLM_TRANSFORM_ENABLED=false` 保证。
故本版本记录为无操作。
"""

from collections.abc import Sequence

revision: str = "20260804_0006"
down_revision: str | None = "20260804_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """逐行 LLM 契约走 plan JSON 承载，本版本不产生 DDL。"""


def downgrade() -> None:
    """无 DDL 可回滚。"""
