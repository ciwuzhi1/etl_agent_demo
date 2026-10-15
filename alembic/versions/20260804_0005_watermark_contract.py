"""record incremental watermark contract slot (no dedicated column)

Revision ID: 20260804_0005
Revises: 20260804_0004
Create Date: 2026-10-15

与 0004 同源的问题：原 DDL 想给 `agent_runs` 加 incremental_contract、
给 `pipeline_schedules` 加 watermark_value / watermark_updated_at，
但 agent_runs 到 0007 才创建，而 pipeline_schedules 在本仓库既无 ORM 模型、
也没有任何代码引用（调度水位属第 10 章 e2e 范畴）。
增量契约的实际落点是 `pipeline_versions.plan["incremental_contract"]`。
故本版本记录为无操作。
"""

from collections.abc import Sequence

revision: str = "20260804_0005"
down_revision: str | None = "20260804_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """增量契约走 plan JSON 承载；pipeline_schedules 属后续章节，本版本无 DDL。"""


def downgrade() -> None:
    """无 DDL 可回滚。"""
