"""文件资产异步解析任务：读取受管原文件、规范化为 JSONL，并原子提交脱敏 Profile。

Worker 侧任务只接 UUID，读原文件→解析→写 JSONL→推进状态；
异常只存固定文案，不把主机路径或文件内容写进库。
"""

import uuid

from etl_agent.application.file_assets import (
    FileAssetService,
    engine_path_for,
    normalized_relative_path,
    parse_file_content,
)
from etl_agent.asyncio_compat import run_async
from etl_agent.config import get_settings
from etl_agent.domain.errors import DomainError
from etl_agent.infrastructure.database import SessionFactory
from etl_agent.infrastructure.file_asset_store import LocalFileAssetStore
from etl_agent.worker import celery_app


async def _profile_file_asset(asset_id: uuid.UUID) -> dict[str, object]:
    """读取受管原文件、规范化为 JSONL，并原子提交脱敏 Profile。"""
    settings = get_settings()
    service = FileAssetService()
    store = LocalFileAssetStore(settings.file_asset_root)
    async with SessionFactory() as session:
        async with session.begin():
            asset = await service.mark_running(session, asset_id)
            source_uri = asset.source_uri
            file_format = asset.file_format
    if source_uri is None:
        raise DomainError("文件资产缺少受管源文件")

    try:
        content = await store.read_bytes(source_uri)
        profile = parse_file_content(
            content,
            file_format=file_format,
            max_rows=settings.file_asset_max_rows,
        )
        normalized = await store.write_json_lines(
            normalized_relative_path(asset_id),
            profile.rows,
        )
        engine_path = engine_path_for(asset_id, settings.file_asset_engine_root)
    except Exception as exc:
        # 解析器领域错误已经是可展示的脱敏文案；未知异常不把路径或内容写入数据库。
        error = str(exc) if isinstance(exc, DomainError) else "文件解析失败，请检查文件结构"
        async with SessionFactory() as session:
            async with session.begin():
                await service.fail(session, asset_id=asset_id, error=error)
        return {"asset_id": str(asset_id), "status": "FAILED"}

    async with SessionFactory() as session:
        async with session.begin():
            completed = await service.complete(
                session,
                asset_id=asset_id,
                normalized_uri=normalized.uri,
                engine_path=engine_path,
                profile=profile,
            )
    return {
        "asset_id": str(asset_id),
        "status": completed.status.value,
        "row_count": completed.row_count or 0,
    }


@celery_app.task  # type: ignore[untyped-decorator]
def profile_file_asset(asset_id: str) -> dict[str, object]:
    """由 Celery 执行受管文件解析，任务参数只携带文件资产 UUID。"""
    return run_async(_profile_file_asset(uuid.UUID(asset_id)))
