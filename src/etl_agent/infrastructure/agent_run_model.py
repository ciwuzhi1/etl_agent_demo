"""AgentRun 模型：记录每次生成任务的执行状态和结果。"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from etl_agent.domain.enums import AgentRunStatus
from etl_agent.infrastructure.database import Base, TimestampMixin


class AgentRun(Base, TimestampMixin):
    __tablename__ = "agent_runs"
    __table_args__ = (Index("ix_agent_runs_project_status", "project_id", "status"),)

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False
    )
    pipeline_version_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipeline_versions.id", ondelete="SET NULL"), nullable=True
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
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
