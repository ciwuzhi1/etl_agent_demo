"""add encrypted secrets, data connections and data profiles

Revision ID: 20260804_0002
Revises: 20260804_0001
Create Date: 2026-08-04
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260804_0002"
down_revision: str | None = "20260804_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

connector_type = sa.Enum(
    "MYSQL",
    "DORIS",
    "POSTGRESQL",
    "ORACLE",
    "SQL_SERVER",
    "HIVE",
    "HDFS",
    "LOCAL_FILE",
    name="connectortype",
    native_enum=False,
)
connection_status = sa.Enum(
    "UNTESTED", "TESTING", "READY", "FAILED", name="connectionstatus", native_enum=False
)
profile_status = sa.Enum(
    "PENDING", "RUNNING", "READY", "FAILED", name="dataprofilestatus", native_enum=False
)
endpoint_role = sa.Enum("SOURCE", "SINK", name="dataendpointrole", native_enum=False)


def timestamps() -> list[sa.Column]:
    """返回统一时间戳列定义，保持非空约束和数据库默认值一致。"""
    return [
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    ]


def upgrade() -> None:
    """向前应用本版本数据库结构变更；DDL 必须与当前 ORM 元数据保持一致。

    本章的 §4.4 SecretProvider 代码同时要求 `provider`/`external_ref`/`destroyed_at`
    （参考实现把它们放在 0013、0017）以及 §4.8 响应模型要求的凭据年龄列
    （参考实现放在 0018）。为了让第 4 章的代码可以在本章真实跑通，这里一并建立；
    后续章节补迁移时必须跳过这些列。
    """
    op.create_table(
        "encrypted_secrets",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("provider", sa.String(32), server_default="fernet-db", nullable=False),
        sa.Column("ciphertext", sa.LargeBinary()),
        sa.Column("external_ref", sa.String(512)),
        sa.Column("key_version", sa.String(64), server_default="local-v1", nullable=False),
        sa.Column("destroyed_at", sa.DateTime(timezone=True), nullable=True),
        *timestamps(),
        sa.CheckConstraint(
            "(destroyed_at IS NULL AND provider = 'fernet-db' "
            "AND ciphertext IS NOT NULL AND external_ref IS NULL) "
            "OR (destroyed_at IS NULL AND provider <> 'fernet-db' "
            "AND ciphertext IS NULL AND external_ref IS NOT NULL) "
            "OR (destroyed_at IS NOT NULL AND ciphertext IS NULL)",
            name="ck_encrypted_secrets_storage_shape",
        ),
        sa.UniqueConstraint("provider", "external_ref"),
    )
    op.create_table(
        "data_connections",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("connector_type", connector_type, nullable=False),
        sa.Column("host", sa.String(255)),
        sa.Column("port", sa.Integer()),
        sa.Column("database_name", sa.String(255)),
        sa.Column("options", sa.JSON(), nullable=False),
        sa.Column("secret_ref", sa.Uuid(), sa.ForeignKey("encrypted_secrets.id"), nullable=False),
        sa.Column("status", connection_status, nullable=False),
        sa.Column("test_error", sa.Text()),
        sa.Column("last_tested_at", sa.DateTime(timezone=True)),
        sa.Column(
            "credential_issued_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "credential_rotation_enabled",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
        sa.Column("credential_max_age_days", sa.Integer(), server_default="90", nullable=False),
        sa.Column("credential_warning_days", sa.Integer(), server_default="14", nullable=False),
        sa.Column("created_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        *timestamps(),
        sa.UniqueConstraint("project_id", "name"),
        sa.CheckConstraint(
            "credential_max_age_days >= 1 AND credential_max_age_days <= 3650",
            name="ck_data_connections_credential_max_age",
        ),
        sa.CheckConstraint(
            "credential_warning_days >= 0 AND credential_warning_days < credential_max_age_days",
            name="ck_data_connections_credential_warning",
        ),
    )
    op.create_table(
        "data_profiles",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "connection_id",
            sa.Uuid(),
            sa.ForeignKey("data_connections.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("schema_name", sa.String(255), nullable=False),
        sa.Column("table_name", sa.String(255), nullable=False),
        sa.Column("endpoint_role", endpoint_role, nullable=False),
        sa.Column("status", profile_status, nullable=False),
        sa.Column("schema_json", sa.JSON(), nullable=False),
        sa.Column("primary_keys", sa.JSON(), nullable=False),
        sa.Column("statistics", sa.JSON(), nullable=False),
        sa.Column("masked_sample", sa.JSON(), nullable=False),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("error", sa.Text()),
        sa.Column("requested_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        *timestamps(),
    )
    op.create_index(
        "ix_data_profiles_connection_table",
        "data_profiles",
        ["connection_id", "schema_name", "table_name"],
    )


def downgrade() -> None:
    """按升级的反向依赖顺序撤销结构变更，避免残留表、索引或约束。"""
    op.drop_index("ix_data_profiles_connection_table", table_name="data_profiles")
    op.drop_table("data_profiles")
    op.drop_table("data_connections")
    op.drop_table("encrypted_secrets")
