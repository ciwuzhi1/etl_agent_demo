"""文件资产 API：上传、列表与查询。

课件 §5.5（第 5 章）要求三个端点：
POST /projects/{id}/file-assets（202 + PENDING，三段事务边界）、
GET /projects/{id}/file-assets、GET /file-assets/{asset_id}。
参考实现 111 行，与课件 5807–5917 清单逐字一致。
"""

import uuid
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, File, Header, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import FileAssetResponse
from etl_agent.application.file_assets import (
    source_relative_path,
)
from etl_agent.config import get_settings
from etl_agent.domain.errors import DomainError
from etl_agent.file_asset_tasks import profile_file_asset
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.file_asset_store import LocalFileAssetStore

router = APIRouter(tags=["file-assets"])


@router.post(
    "/projects/{project_id}/file-assets",
    response_model=FileAssetResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def upload_file_asset(
    project_id: uuid.UUID,
    file: Annotated[UploadFile, File(description="待导入并探查的受管文件")],
    actor: uuid.UUID = Depends(actor_id),
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    session: AsyncSession = Depends(get_session),
) -> FileAssetResponse:
    """流式接收受支持文件，原子落盘后投递异步解析任务。"""
    from etl_agent.application.file_assets import FileAssetService

    settings = get_settings()
    filename = file.filename or ""
    service = FileAssetService()
    async with session.begin():
        asset = await service.request(
            session,
            project_id=project_id,
            actor_id=actor,
            filename=filename,
            media_type=file.content_type,
            trace_id=trace_id,
        )

    store = LocalFileAssetStore(settings.file_asset_root)
    try:
        stored = await store.write_upload(
            source_relative_path(asset.id, asset.file_format),
            _upload_chunks(file),
            max_bytes=settings.file_asset_max_bytes,
        )
    except Exception as exc:
        error = str(exc) if isinstance(exc, DomainError) else "文件上传失败"
        async with session.begin():
            await service.fail(session, asset_id=asset.id, error=error)
        if isinstance(exc, DomainError):
            raise
        raise DomainError(error) from exc
    finally:
        await file.close()

    async with session.begin():
        asset = await service.record_stored(
            session,
            asset_id=asset.id,
            source_uri=stored.uri,
            byte_size=stored.byte_size,
            sha256=stored.sha256,
        )
        response = FileAssetResponse.model_validate(asset)
    profile_file_asset.delay(str(asset.id))
    return response


@router.get(
    "/projects/{project_id}/file-assets",
    response_model=list[FileAssetResponse],
)
async def list_file_assets(
    project_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> list[FileAssetResponse]:
    """列出项目最近一百个文件资产及解析状态。"""
    from etl_agent.application.file_assets import FileAssetService

    service = FileAssetService()
    assets = await service.list_for_project(
        session,
        project_id=project_id,
        actor_id=actor,
    )
    return [FileAssetResponse.model_validate(asset) for asset in assets]


@router.get("/file-assets/{asset_id}", response_model=FileAssetResponse)
async def get_file_asset(
    asset_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> FileAssetResponse:
    """读取单个文件资产，供前端有界轮询异步解析结果。"""
    from etl_agent.application.file_assets import FileAssetService

    service = FileAssetService()
    asset = await service.get_visible(session, asset_id=asset_id, actor_id=actor)
    return FileAssetResponse.model_validate(asset)


async def _upload_chunks(upload: UploadFile, chunk_size: int = 1024 * 1024) -> AsyncIterator[bytes]:
    """按固定块读取上传流，避免 API 进程把整个文件一次性载入内存。"""
    while chunk := await upload.read(chunk_size):
        yield chunk
