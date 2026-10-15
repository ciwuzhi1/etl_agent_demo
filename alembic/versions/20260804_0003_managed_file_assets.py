"""managed file assets: file_assets table + index + checks

课件 §5.3（第 5 章）要求把异构文件接进受管链路，故建 file_assets 表。
参考实现放在 20260807_0022（down_revision=20260807_0021），我们链头是 20260804_0002，
故自写 0003 承接；后续复刻参考迁移时必须跳过其中的 file_assets 建表段与索引段。
"""

import sqlalchemy as sa

from alembic import op

revision = "20260804_0003"
down_revision = "20260804_0002"


def upgrade() -> None:
    op.create_table(
        "file_assets",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("project_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("original_filename", sa.String(255), nullable=False),
        sa.Column("file_format", sa.String(32), nullable=False),
        sa.Column("media_type", sa.String(255)),
        sa.Column("byte_size", sa.BigInteger),
        sa.Column("sha256", sa.String(64)),
        sa.Column("source_uri", sa.String(1024)),
        sa.Column("normalized_uri", sa.String(1024)),
        sa.Column("engine_path", sa.String(1024)),
        sa.Column("status", sa.String(32), nullable=False, server_default="PENDING"),
        sa.Column("schema_json", sa.JSON, nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column("statistics", sa.JSON, nullable=False, server_default=sa.text("'{}'::json")),
        sa.Column("masked_sample", sa.JSON, nullable=False, server_default=sa.text("'[]'::json")),
        sa.Column("row_count", sa.BigInteger),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("error", sa.Text),
        sa.Column("created_by", sa.Uuid(as_uuid=True)),
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
            onupdate=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.CheckConstraint("byte_size >= 0", name="ck_file_assets_byte_size"),
        sa.CheckConstraint("row_count >= 0", name="ck_file_assets_row_count"),
    )
    op.create_index("ix_file_assets_project_created", "file_assets", ["project_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_file_assets_project_created", "file_assets")
    op.drop_table("file_assets")
