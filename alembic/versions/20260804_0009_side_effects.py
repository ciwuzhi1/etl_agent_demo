"""create side effect governance tables (chapter 12)

Revision ID: 20260804_0009
Revises: 20260804_0008
Create Date: 2026-10-15

第 12 章（Side Effect Governance）：取消执行、回滚 Doris、密钥轮换与销毁等
9 种副作用统一走 prepare → approve → commit 门禁。本迁移建 3 张表：
harness_side_effect_preparations / harness_side_effect_approvals /
harness_side_effect_capability_grants。
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0009"
down_revision: str | None = "20260804_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

side_effect_operation = sa.Enum(
    "CANCEL_EXECUTION",
    "ROLLBACK_DORIS",
    "CLEANUP_DORIS",
    "MIGRATE_SECRET",
    "STAGE_SECRET_ROTATION",
    "VALIDATE_SECRET_ROTATION",
    "ACTIVATE_SECRET_ROTATION",
    "ROLLBACK_SECRET_ROTATION",
    "DESTROY_SECRET",
    name="sideeffectoperation",
    native_enum=False,
)
side_effect_preparation_status = sa.Enum(
    "WAITING_APPROVAL",
    "READY",
    "COMMITTED",
    "REJECTED",
    "EXPIRED",
    "DENIED",
    name="sideeffectpreparationstatus",
    native_enum=False,
)
side_effect_execution_status = sa.Enum(
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    name="sideeffectexecutionstatus",
    native_enum=False,
)
harness_role = sa.Enum(
    "DATA_ENGINEER",
    "DATA_APPROVER",
    "SECURITY_APPROVER",
    "OPERATOR",
    "AUDITOR",
    name="harnessrole",
    native_enum=False,
)
harness_approval_value = sa.Enum(
    "APPROVE",
    "REJECT",
    name="harnessapprovalvalue",
    native_enum=False,
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
    """建副作用 Preparation、职责审批与一次性能力授权三张表。"""
    op.create_table(
        "harness_side_effect_preparations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("operation", side_effect_operation, nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column(
            "status",
            side_effect_preparation_status,
            nullable=False,
        ),
        sa.Column(
            "execution_status",
            side_effect_execution_status,
            nullable=False,
        ),
        sa.Column("risk_level", sa.String(8), nullable=False),
        sa.Column("facts_sha256", sa.String(64), nullable=False),
        sa.Column("frozen_facts", sa.JSON(), nullable=False),
        sa.Column("command_payload", sa.JSON(), nullable=False),
        sa.Column("required_roles", sa.JSON(), nullable=False),
        sa.Column("required_approvals", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column(
            "requested_by",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("committed_at", sa.DateTime(timezone=True)),
        sa.Column("lease_owner", sa.String(128)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("executed_at", sa.DateTime(timezone=True)),
        sa.Column("execution_error", sa.Text()),
        sa.Column("result_sha256", sa.String(64)),
        sa.UniqueConstraint(
            "project_id",
            "idempotency_key",
            name="uq_side_effect_preparations_project_idempotency",
        ),
        *timestamps(),
    )
    op.create_index(
        "ix_side_effect_preparations_project_created",
        "harness_side_effect_preparations",
        ["project_id", "created_at"],
    )
    op.create_index(
        "ix_side_effect_preparations_operation_aggregate",
        "harness_side_effect_preparations",
        ["operation", "aggregate_id"],
    )
    op.create_index(
        "ix_side_effect_preparations_status_execution",
        "harness_side_effect_preparations",
        ["status", "execution_status"],
    )

    op.create_table(
        "harness_side_effect_approvals",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "preparation_id",
            sa.Uuid(),
            sa.ForeignKey("harness_side_effect_preparations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", harness_role, nullable=False),
        sa.Column("decision", harness_approval_value, nullable=False),
        sa.Column(
            "decided_by",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("evidence_sha256", sa.String(64), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "preparation_id",
            "role",
            name="uq_side_effect_approvals_role",
        ),
        sa.UniqueConstraint(
            "preparation_id",
            "decided_by",
            name="uq_side_effect_approvals_person",
        ),
    )
    op.create_index(
        "ix_side_effect_approvals_preparation",
        "harness_side_effect_approvals",
        ["preparation_id", "decided_at"],
    )

    op.create_table(
        "harness_side_effect_capability_grants",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "preparation_id",
            sa.Uuid(),
            sa.ForeignKey("harness_side_effect_preparations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_sha256", sa.String(64), nullable=False, unique=True),
        sa.Column("claims_json", sa.JSON(), nullable=False),
        sa.Column(
            "subject_id",
            sa.Uuid(),
            sa.ForeignKey("users.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        # 指向 harness_command_outbox.id；Outbox 表由第 11 章引入，本迁移链尚未
        # 建表，因此这里保持为不带数据库外键的引用列，由应用层保证一致性。
        sa.Column("execution_command_id", sa.Uuid()),
        sa.UniqueConstraint(
            "preparation_id",
            name="uq_side_effect_grants_preparation",
        ),
    )
    op.create_index(
        "ix_side_effect_grants_execution_command",
        "harness_side_effect_capability_grants",
        ["execution_command_id"],
    )


def downgrade() -> None:
    """按依赖顺序移除副作用治理三张表及其索引。"""
    op.drop_index(
        "ix_side_effect_grants_execution_command",
        table_name="harness_side_effect_capability_grants",
    )
    op.drop_table("harness_side_effect_capability_grants")
    op.drop_index(
        "ix_side_effect_approvals_preparation",
        table_name="harness_side_effect_approvals",
    )
    op.drop_table("harness_side_effect_approvals")
    op.drop_index(
        "ix_side_effect_preparations_status_execution",
        table_name="harness_side_effect_preparations",
    )
    op.drop_index(
        "ix_side_effect_preparations_operation_aggregate",
        table_name="harness_side_effect_preparations",
    )
    op.drop_index(
        "ix_side_effect_preparations_project_created",
        table_name="harness_side_effect_preparations",
    )
    op.drop_table("harness_side_effect_preparations")
