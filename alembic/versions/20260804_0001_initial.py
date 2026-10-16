"""initial enterprise control-plane schema

Revision ID: 20260804_0001
Revises:
Create Date: 2026-08-04
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

project_role = sa.Enum(
    "ADMIN", "DATA_ENGINEER", "APPROVER", "VIEWER", name="projectrole", native_enum=False
)
pipeline_status = sa.Enum(
    "DRAFT",
    "PROFILING",
    "GENERATED",
    "VALIDATED",
    "WAITING_APPROVAL",
    "APPROVED",
    "REJECTED",
    name="pipelineversionstatus",
    native_enum=False,
)
approval_status = sa.Enum(
    "PENDING", "APPROVED", "REJECTED", name="approvalstatus", native_enum=False
)
# Harness 职责表在此一并建立，使成员服务与职责授予可用；
# 后续补迁移时必须跳过本表，避免重复建表。
harness_role = sa.Enum(
    "DATA_ENGINEER",
    "DATA_APPROVER",
    "SECURITY_APPROVER",
    "OPERATOR",
    "AUDITOR",
    name="harnessrole",
    native_enum=False,
)
execution_status = sa.Enum(
    "QUEUED",
    "SUBMITTED",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLING",
    "CANCELLED",
    name="executionstatus",
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
    """向前应用本版本数据库结构变更；DDL 必须与当前 ORM 元数据保持一致。"""
    op.create_table(
        "users",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("subject", sa.String(255), nullable=False, unique=True),
        sa.Column("display_name", sa.String(255), nullable=False),
        *timestamps(),
    )
    op.create_table(
        "projects",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False, unique=True),
        *timestamps(),
    )
    op.create_table(
        "project_memberships",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id", sa.Uuid(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("role", project_role, nullable=False),
        *timestamps(),
        sa.UniqueConstraint("project_id", "user_id"),
    )
    op.create_table(
        "project_role_grants",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id", sa.Uuid(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("role", harness_role, nullable=False),
        sa.Column("granted_by", sa.Uuid(), sa.ForeignKey("users.id", ondelete="SET NULL")),
        *timestamps(),
        sa.UniqueConstraint(
            "project_id",
            "user_id",
            "role",
            name="uq_project_role_grants_identity",
        ),
    )
    op.create_index(
        "ix_project_role_grants_project_user",
        "project_role_grants",
        ["project_id", "user_id"],
    )
    op.create_table(
        "pipelines",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(255), nullable=False),
        *timestamps(),
    )
    op.create_table(
        "pipeline_versions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "pipeline_id",
            sa.Uuid(),
            sa.ForeignKey("pipelines.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("status", pipeline_status, nullable=False),
        sa.Column("requirement", sa.Text(), nullable=False),
        sa.Column("plan", sa.JSON(), nullable=False),
        sa.Column("artifact_sha256", sa.String(64)),
        sa.Column("lock_version", sa.Integer(), nullable=False),
        sa.Column("created_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        *timestamps(),
        sa.UniqueConstraint("pipeline_id", "version_number"),
    )
    op.create_table(
        "pipeline_artifacts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "pipeline_version_id",
            sa.Uuid(),
            sa.ForeignKey("pipeline_versions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(64), nullable=False),
        sa.Column("uri", sa.String(1024), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        *timestamps(),
    )
    op.create_table(
        "approval_requests",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "pipeline_version_id",
            sa.Uuid(),
            sa.ForeignKey("pipeline_versions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("status", approval_status, nullable=False),
        sa.Column("artifact_sha256", sa.String(64), nullable=False),
        sa.Column("requested_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("decided_by", sa.Uuid(), sa.ForeignKey("users.id")),
        sa.Column("reason", sa.Text()),
        sa.Column("decided_at", sa.DateTime(timezone=True)),
        *timestamps(),
    )
    op.create_table(
        "execution_runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "pipeline_version_id", sa.Uuid(), sa.ForeignKey("pipeline_versions.id"), nullable=False
        ),
        sa.Column("status", execution_status, nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False, unique=True),
        sa.Column("seatunnel_job_id", sa.String(255)),
        sa.Column("requested_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("error", sa.Text()),
        *timestamps(),
    )
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "occurred_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("actor_id", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("project_id", sa.Uuid(), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("action", sa.String(128), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("resource_id", sa.Uuid(), nullable=False),
        sa.Column("before", sa.JSON()),
        sa.Column("after", sa.JSON()),
        sa.Column("reason", sa.Text()),
        sa.Column("trace_id", sa.String(64)),
    )


def downgrade() -> None:
    """按升级的反向依赖顺序撤销结构变更，避免残留表、索引或约束。"""
    for table in (
        "audit_events",
        "execution_runs",
        "approval_requests",
        "pipeline_artifacts",
        "pipeline_versions",
        "pipelines",
        "project_role_grants",
        "project_memberships",
        "projects",
        "users",
    ):
        op.drop_table(table)
