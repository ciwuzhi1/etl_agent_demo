"""create benchmark & safe evolution tables

Revision ID: 20260804_0011
Revises: 20260804_0008
Create Date: 2026-08-04

第 14 章建立冻结 Benchmark 与受控安全进化的落库结构：
- safety_proposals：不可越级状态机的改进提案
- safety_releases：影子/小流量/推广/回滚的签名发布候选
- benchmark_runs：一次冻结运行的内容寻址报告证据
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0011"
down_revision: str | None = "20260804_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


safety_proposal_kind = sa.Enum(
    "POLICY", "PROMPT", "BENCHMARK", name="safetyproposalkind", native_enum=False
)
safety_proposal_status = sa.Enum(
    "DRAFT",
    "IN_REVIEW",
    "APPROVED",
    "REJECTED",
    "RELEASED",
    name="safetyproposalstatus",
    native_enum=False,
)
safety_release_status = sa.Enum(
    "SHADOW_RUNNING",
    "SHADOW_VERIFIED",
    "CANARY_RUNNING",
    "CANARY_VERIFIED",
    "PROMOTION_APPROVED",
    "ROLLED_BACK",
    name="safetyreleasestatus",
    native_enum=False,
)
safety_release_stage = sa.Enum(
    "SHADOW", "CANARY", "PROMOTED", "ROLLED_BACK", name="safetyreleasestage", native_enum=False
)


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
    """创建安全提案、签名发布候选与冻结 Benchmark 运行三张表。"""
    op.create_table(
        "safety_proposals",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("kind", safety_proposal_kind, nullable=False),
        sa.Column("status", safety_proposal_status, nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("title", sa.String(255)),
        sa.Column("rationale", sa.Text()),
        sa.Column(
            "created_by",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("reviewed_by", sa.Uuid(), sa.ForeignKey("users.id", ondelete="SET NULL")),
        sa.Column("decision_reason", sa.Text()),
        sa.Column("submitted_at", sa.DateTime(timezone=True)),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
        sa.Column("lock_version", sa.Integer(), server_default="1", nullable=False),
        *timestamps(),
    )
    op.create_index(
        "ix_safety_proposals_status_created", "safety_proposals", ["status", "created_at"]
    )

    op.create_table(
        "safety_releases",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "proposal_id",
            sa.Uuid(),
            sa.ForeignKey("safety_proposals.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("status", safety_release_status, nullable=False),
        sa.Column("stage", safety_release_stage, nullable=False),
        sa.Column("canary_traffic_percent", sa.Integer()),
        sa.Column("manifest", sa.JSON()),
        sa.Column("manifest_sha256", sa.String(64)),
        sa.Column("signature_algorithm", sa.String(32)),
        sa.Column("signature_key_id", sa.String(128)),
        sa.Column("signature_value", sa.Text()),
        sa.Column("observation", sa.JSON()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("verified_at", sa.DateTime(timezone=True)),
        sa.Column("rolled_back_at", sa.DateTime(timezone=True)),
        sa.Column("rollback_reason", sa.Text()),
        sa.Column("created_by", sa.Uuid(), sa.ForeignKey("users.id", ondelete="SET NULL")),
        *timestamps(),
    )
    op.create_index("ix_safety_releases_status", "safety_releases", ["status"])

    op.create_table(
        "benchmark_runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("suite_id", sa.String(255), nullable=False),
        sa.Column("version", sa.String(128), nullable=False),
        sa.Column("policy_bundle_version", sa.String(128), nullable=False),
        sa.Column("manifest_sha256", sa.String(64), nullable=False),
        sa.Column("report", sa.JSON(), nullable=False),
        sa.Column("report_sha256", sa.String(64), nullable=False),
        sa.Column("gates_passed", sa.Boolean(), nullable=False),
        sa.Column("gate_failures", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_by",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        *timestamps(),
    )
    op.create_index(
        "ix_benchmark_runs_manifest_started", "benchmark_runs", ["manifest_sha256", "started_at"]
    )


def downgrade() -> None:
    """按依赖逆序移除三张第 14 章表及其索引。"""
    op.drop_index("ix_benchmark_runs_manifest_started", "benchmark_runs")
    op.drop_table("benchmark_runs")
    op.drop_index("ix_safety_releases_status", "safety_releases")
    op.drop_table("safety_releases")
    op.drop_index("ix_safety_proposals_status_created", "safety_proposals")
    op.drop_table("safety_proposals")
