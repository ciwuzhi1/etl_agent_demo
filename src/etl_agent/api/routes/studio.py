"""Studio 制品 API：读取 Pipeline 版本的 HOCON 制品正文（第 15 章 Web Console 接入）。

端点：
- GET /pipeline-versions/{version_id}/artifact   版本最新一条 PipelineArtifact 的正文

设计口径：
* 摘要才是证据：``sha256`` 一律取库内记录，``uri`` 只是取件位置；正文按需从
  ``uri`` 指向的本地文件读取，文件缺失或不可读时返回空正文，而不是虚构内容；
* ``uri`` 支持 ``file://`` URI 与相对/绝对路径；其它协议（s3://、hdfs:// 等）不在
  本端点读取，返回空正文由前端展示空态；
* 读权限沿用 ``require_project_role``，VIEWER 也可读取，便于 Studio 展示。
"""

import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.dependencies import actor_id
from etl_agent.api.schemas import PipelineArtifactResponse
from etl_agent.application.profiling import require_project_role
from etl_agent.config import get_settings
from etl_agent.domain.enums import ProjectRole
from etl_agent.domain.errors import ResourceNotFound
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.models import Pipeline, PipelineArtifact, PipelineVersion

router = APIRouter(tags=["studio"])

_VIEWER_ROLES = {
    ProjectRole.ADMIN,
    ProjectRole.DATA_ENGINEER,
    ProjectRole.APPROVER,
    ProjectRole.VIEWER,
}

_FILE_URI_PREFIX = "file://"


@router.get(
    "/pipeline-versions/{version_id}/artifact",
    response_model=PipelineArtifactResponse,
)
async def get_pipeline_version_artifact(
    version_id: uuid.UUID,
    actor: uuid.UUID = Depends(actor_id),
    session: AsyncSession = Depends(get_session),
) -> PipelineArtifactResponse:
    """返回版本最新制品的 HOCON 正文；版本无制品返回 404，文件缺失则正文为空。"""
    project_id = await _project_id_of_version(session, version_id)
    await require_project_role(session, project_id, actor, _VIEWER_ROLES)
    artifact = await session.scalar(
        select(PipelineArtifact)
        .where(PipelineArtifact.pipeline_version_id == version_id)
        .order_by(PipelineArtifact.created_at.desc())
        .limit(1)
    )
    if artifact is None:
        raise ResourceNotFound("Pipeline 版本尚未生成制品")
    return PipelineArtifactResponse(
        kind=artifact.kind,
        uri=artifact.uri,
        sha256=artifact.sha256,
        hocon=_read_artifact_text(artifact.uri),
    )


async def _project_id_of_version(session: AsyncSession, version_id: uuid.UUID) -> uuid.UUID:
    """经 PipelineVersion -> Pipeline 回溯版本所属项目，缺失即 404。"""
    project_id = await session.scalar(
        select(Pipeline.project_id)
        .join(PipelineVersion, PipelineVersion.pipeline_id == Pipeline.id)
        .where(PipelineVersion.id == version_id)
    )
    if project_id is None:
        raise ResourceNotFound("Pipeline 版本不存在")
    return project_id


def _read_artifact_text(uri: str) -> str:
    """按 ``uri`` 读取制品正文；不是本地文件或读取失败时返回空正文。"""
    for candidate in _local_candidates(uri):
        try:
            return candidate.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
    return ""


def _local_candidates(uri: str) -> list[Path]:
    """枚举 ``uri`` 可能对应的本地路径候选，非本地协议返回空列表。

    相对路径先按进程工作目录解析，再退化到 ``artifact_root``，兼容“相对制品根”
    与“相对项目根”两种写入习惯。
    """
    candidate = uri.strip()
    if not candidate:
        return []
    if candidate.startswith(_FILE_URI_PREFIX):
        return [_file_uri_path(candidate)]
    if "://" in candidate:
        return []
    path = Path(candidate)
    if path.is_absolute():
        return [path]
    return [path, get_settings().artifact_root / candidate]


def _file_uri_path(uri: str) -> Path:
    """把 ``file://`` URI 解析为本地路径，处理百分号编码、UNC 与 Windows 盘符。"""
    parsed = urlparse(uri)
    raw = unquote(parsed.path)
    if parsed.netloc and parsed.netloc not in {"", "localhost"}:
        raw = f"//{parsed.netloc}{raw}"
    # file:///C:/x 解析后是 /C:/x，去掉前导斜杠还原成 Windows 盘符路径。
    if len(raw) >= 3 and raw[0] == "/" and raw[2] == ":":
        raw = raw[1:]
    return Path(raw)
