import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.application.execution_diagnostics import redact_execution_error
from etl_agent.domain.harness import HarnessOutboxStatus
from etl_agent.infrastructure.models import HarnessCommandOutbox


class HarnessOutboxService:
    """以行锁和可恢复状态管理 Harness 命令的至少一次可靠投递。"""

    async def claim_pending(
        self,
        session: AsyncSession,
        *,
        now: datetime | None = None,
        limit: int = 50,
        lease_seconds: int = 60,
    ) -> list[HarnessCommandOutbox]:
        """认领到期或租约失效命令；进程崩溃后的 SENDING 不会永久丢失。"""
        if not 5 <= lease_seconds <= 3600:
            raise ValueError("Outbox 租约必须位于 5 到 3600 秒")
        current = now or datetime.now(UTC)
        stale_before = current - timedelta(seconds=lease_seconds)
        rows = list(
            (
                await session.scalars(
                    select(HarnessCommandOutbox)
                    .where(
                        or_(
                            and_(
                                HarnessCommandOutbox.status.in_(
                                    [HarnessOutboxStatus.PENDING, HarnessOutboxStatus.FAILED]
                                ),
                                HarnessCommandOutbox.available_at <= current,
                            ),
                            and_(
                                HarnessCommandOutbox.status == HarnessOutboxStatus.SENDING,
                                or_(
                                    HarnessCommandOutbox.locked_at.is_(None),
                                    HarnessCommandOutbox.locked_at <= stale_before,
                                ),
                            ),
                        ),
                    )
                    .order_by(HarnessCommandOutbox.available_at, HarnessCommandOutbox.created_at)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).all()
        )
        for row in rows:
            row.status = HarnessOutboxStatus.SENDING
            row.locked_at = current
            row.attempts += 1
        await session.flush()
        return rows

    async def mark_sent(
        self,
        session: AsyncSession,
        *,
        outbox_id: uuid.UUID,
    ) -> None:
        """在消息代理确认接收后标记完成；重复调用保持幂等。"""
        row = await session.scalar(
            select(HarnessCommandOutbox)
            .where(HarnessCommandOutbox.id == outbox_id)
            .with_for_update()
        )
        if row is None or row.status is HarnessOutboxStatus.SENT:
            return
        row.status = HarnessOutboxStatus.SENT
        row.sent_at = datetime.now(UTC)
        row.locked_at = None
        row.last_error = None
        await session.flush()

    async def mark_failed(
        self,
        session: AsyncSession,
        *,
        outbox_id: uuid.UUID,
        error: str,
    ) -> None:
        """记录脱敏失败摘要并指数退避，保留命令等待下次安全重投。"""
        row = await session.scalar(
            select(HarnessCommandOutbox)
            .where(HarnessCommandOutbox.id == outbox_id)
            .with_for_update()
        )
        if row is None or row.status is HarnessOutboxStatus.SENT:
            return
        delay = min(300, 2 ** min(row.attempts, 8))
        row.status = HarnessOutboxStatus.FAILED
        row.available_at = datetime.now(UTC) + timedelta(seconds=delay)
        row.locked_at = None
        row.last_error = redact_execution_error(error)[:2_000]
        await session.flush()
