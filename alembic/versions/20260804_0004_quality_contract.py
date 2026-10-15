"""record agent run quality contract slot (no dedicated column)

Revision ID: 20260804_0004
Revises: 20260804_0003
Create Date: 2026-10-15

课件第 6 章把 quality/incremental/row_llm 三个契约快照写进制品，
但本仓库的落点是 `agent_runs.metadata_context` 与 `pipeline_versions.plan`
这两个 JSON 列（见 `agent/graph.py`），而不是 agent_runs 上的独立列。
原先这里写的是 `ALTER TABLE agent_runs ADD COLUMN quality_contract`，
而 agent_runs 表要到 0007 才创建，导致 `alembic upgrade head` 在空库上直接失败；
且 ORM（`infrastructure/agent_run_model.py`）从未声明该列，属于 DDL 与元数据漂移。
因此本版本记录为无操作，保留版本号以维持既有迁移链与提交历史。
"""

from collections.abc import Sequence

revision: str = "20260804_0004"
down_revision: str | None = "20260804_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """契约快照走 JSON 上下文承载，本版本不产生 DDL。"""


def downgrade() -> None:
    """无 DDL 可回滚。"""
