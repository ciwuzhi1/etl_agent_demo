"""调度扫描任务：按 cron 找出到期的 Pipeline 调度并推进游标，不直接执行。

第 16 章水位与周期任务的前置闭环。两条边界：

- **只准备、不执行**：扫描到期的 `pipeline_schedules` 后只推进调度游标（记录
  "本轮已到期"这一事实），真正的执行提交必须经 Harness 的 Prepare→Approve→Commit，
  绝不能在这里 `submit_execution`。
- **缺表要安全**：`pipeline_schedules` 属于后续章节的表，本迁移链尚未建出；表不存在
  或行为空时直接返回 0，而不是抛错把 Beat 打崩。cron 求值统一用 `croniter`。

附带实现凭据到期扫描（`scan_credential_expiry`）：它是 1 小时级的只读巡检，
放在本模块与调度扫描共用 Beat 入口，不引入新的写入路径。
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from croniter import croniter  # type: ignore[import-untyped]
from sqlalchemy import MetaData, Table, select
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.asyncio_compat import run_async
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.models import DataConnection
from etl_agent.worker import celery_app

_SCHEDULE_TABLE = "pipeline_schedules"
_SCAN_LIMIT = 200


async def _table_exists(session: AsyncSession, name: str) -> bool:
    """用数据库自身的内省判断表是否存在，避免在缺表时抛 ProgrammingError。"""
    connection = await session.connection()
    return await connection.run_sync(lambda sync_conn: sa_inspect(sync_conn).has_table(name))


def _first_present(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    """返回候选列名中第一个实际存在的列；都不存在时返回 None。"""
    for name in candidates:
        if name in columns:
            return name
    return None


def _as_utc(moment: datetime) -> datetime:
    """把可能 naive 的时间补成 UTC，避免 aware/naive 混比。"""
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


async def _scan_schedules() -> int:
    """扫描到期调度并推进 `next_run_at`；缺表/空表安全返回 0。"""
    async with SessionFactory() as session:
        if not await _table_exists(session, _SCHEDULE_TABLE):
            return 0
        metadata = MetaData()
        connection = await session.connection()
        table = await connection.run_sync(
            lambda sync_conn: Table(_SCHEDULE_TABLE, metadata, autoload_with=sync_conn)
        )
        columns = set(table.columns.keys())
        cron_column = _first_present(
            columns, ("cron_expression", "cron", "schedule_cron", "cron_expr")
        )
        next_column = _first_present(columns, ("next_run_at", "next_fire_at", "next_trigger_at"))
        enabled_column = _first_present(columns, ("enabled", "is_active", "active"))
        if cron_column is None or next_column is None:
            # 表结构并非本任务预期的调度表，宁可不猜也不乱写。
            return 0

        now = datetime.now(UTC)
        statement = select(table).where(table.c[next_column] <= now).limit(_SCAN_LIMIT)
        if enabled_column is not None:
            statement = statement.where(table.c[enabled_column].is_(True))
        rows: list[Any] = list((await session.scalars(statement)).all())

        advanced = 0
        for row in rows:
            mapping: dict[str, Any] = dict(row._mapping)
            expression = str(mapping.get(cron_column) or "").strip()
            base = _as_utc(mapping.get(next_column) or now)
            try:
                next_fire = croniter(expression, base).get_next(datetime)
            except (KeyError, ValueError):
                # 非法 cron 表达式不推进，留给人工修正；不让单条脏数据阻断整轮扫描。
                continue
            primary_keys = {
                column.name: mapping.get(column.name) for column in table.primary_key.columns
            }
            if not primary_keys:
                continue
            await session.execute(
                table.update()
                .where(*[table.c[name] == value for name, value in primary_keys.items()])
                .values({next_column: next_fire})
            )
            advanced += 1
        await session.commit()
        return advanced


async def _scan_credential_expiry() -> int:
    """只读巡检即将到期/已逾期的连接凭据，返回命中数量；不做任何写操作。

    真实告警需要 Webhook 投递与告警表（后续章节），本任务先提供"有多少条需要关注"
    的确定性信号，避免在没有出口配置时产生半成品副作用。
    """
    now = datetime.now(UTC)
    async with SessionFactory() as session:
        connections = (
            await session.scalars(
                select(DataConnection).where(DataConnection.credential_rotation_enabled.is_(True))
            )
        ).all()
    expiring = 0
    for connection in connections:
        issued_at = _as_utc(connection.credential_issued_at)
        expires_at = issued_at + timedelta(days=connection.credential_max_age_days)
        warning_at = expires_at - timedelta(days=max(connection.credential_warning_days, 0))
        if now >= warning_at:
            expiring += 1
    return expiring


@celery_app.task(name="etl_agent.scan_schedules")  # type: ignore[untyped-decorator]
def scan_schedules() -> int:
    """Beat 周期任务：扫描到期 Pipeline 调度并推进游标，返回本轮推进数量。"""
    return run_async(_scan_schedules())


@celery_app.task(name="etl_agent.scan_credential_expiry")  # type: ignore[untyped-decorator]
def scan_credential_expiry() -> int:
    """Beat 周期任务：巡检凭据到期窗口，返回需要关注的连接数量。"""
    return run_async(_scan_credential_expiry())
