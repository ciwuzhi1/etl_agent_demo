"""AgentRun 模型：记录每次生成任务的执行状态和结果。

列定义必须与迁移 `20260804_0007`（建表）+ `20260804_0008`（generation_* 观测列）
逐一对齐，否则 `alembic check` 会把已存在的观测列判为漂移并试图删除。
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from etl_agent.domain.enums import AgentRunStatus
from etl_agent.infrastructure.database import Base
from etl_agent.infrastructure.models import TimestampMixin


class AgentRun(Base, TimestampMixin):
    __tablename__ = "agent_runs"
    __table_args__ = (Index("ix_agent_runs_project_status", "project_id", "status"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    pipeline_version_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipeline_versions.id", ondelete="CASCADE"), nullable=True
    )
    thread_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), unique=True, default=uuid.uuid4, nullable=False
    )
    status: Mapped[AgentRunStatus] = mapped_column(
        String(32), default=AgentRunStatus.QUEUED, nullable=False
    )
    requirement: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_context: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    candidate_hocon: Mapped[str | None] = mapped_column(Text)
    approved_hocon: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_by: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False
    )

    # 第 8 章生成观测：只记次数/耗时/非敏感诊断，不保存 Prompt、模型正文或候选 HOCON。
    generation_backend: Mapped[str | None] = mapped_column(String(32))
    generation_model: Mapped[str | None] = mapped_column(String(255))
    generation_upstream_version: Mapped[str | None] = mapped_column(String(255))
    generation_provider_calls: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    generation_candidates: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    generation_repairs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    generation_elapsed_ms: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    generation_phase: Mapped[str | None] = mapped_column(String(64))
    generation_diagnostics: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    generation_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    generation_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
