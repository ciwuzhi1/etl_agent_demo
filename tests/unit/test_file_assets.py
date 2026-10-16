"""第 5 章单元测试：九种格式解析、受管存储原子性、上传 API 响应脱敏。

参考实现 348 行共 7 个测试；其中 `test_ready_file_asset_enters_agent_as_localfile_source`
需要 `AgentRunService`（第 8 章）、`credential_bindings_for_artifact`（全文无清单）与
`WorkerHeartbeat` 模型（无清单，只在参考迁移 0007/0010 建表），本章跑不了。
故本章只落 6 个测试（4 个解析 + 存储 + 上传 API）。
"""

import io
import json
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.orc as arrow_orc
import pyarrow.parquet as arrow_parquet
import pytest
from httpx import ASGITransport, AsyncClient
from openpyxl import Workbook
from pypdf import PdfWriter
from sqlalchemy.ext.asyncio import AsyncSession

from etl_agent.api.routes import file_assets as file_asset_routes
from etl_agent.application.file_assets import parse_file_content
from etl_agent.domain.enums import DataProfileStatus, FileAssetFormat, ProjectRole
from etl_agent.domain.errors import DomainError, ResourceNotFound
from etl_agent.infrastructure.database import get_session
from etl_agent.infrastructure.file_asset_store import LocalFileAssetStore
from etl_agent.infrastructure.models import (
    Project,
    ProjectMembership,
    User,
)
from etl_agent.main import app


def test_tabular_and_document_formats_normalize_to_masked_rows() -> None:
    """验证 CSV、JSONL 与 Markdown 都进入统一字段模型，敏感样本不会保留明文。"""
    csv_profile = parse_file_content(
        "订单号,手机号,金额\nA-1,13800138000,12.5\n".encode(),
        file_format=FileAssetFormat.CSV,
        max_rows=100,
    )
    jsonl_profile = parse_file_content(
        b'{"Order ID":1,"enabled":true}\n{"Order ID":2,"enabled":false}\n',
        file_format=FileAssetFormat.JSONL,
        max_rows=100,
    )
    markdown_profile = parse_file_content(
        "# 使用说明\n\n- 第一步\n正文内容".encode(),
        file_format=FileAssetFormat.MARKDOWN,
        max_rows=100,
    )

    assert csv_profile.rows[0]["field_3"] == 12.5
    assert csv_profile.rows[0]["field_2"] == "13800138000"
    assert csv_profile.masked_sample[0]["field_2"] == "138****8000"
    assert jsonl_profile.schema_definition["columns"] == [
        {"name": "order_id", "type": "bigint", "nullable": False},
        {"name": "enabled", "type": "boolean", "nullable": False},
    ]
    assert [row["element_type"] for row in markdown_profile.rows] == [
        "heading_1",
        "list_item",
        "paragraph",
    ]
    with pytest.raises(DomainError, match="行上限"):
        parse_file_content(
            b"id\n1\n2\n",
            file_format=FileAssetFormat.CSV,
            max_rows=1,
        )


def test_xlsx_and_pdf_capability_boundaries() -> None:
    """验证 XLSX 首表可探查，而无文本 PDF 会明确失败且不伪装 OCR。"""
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append(["id", "name"])
    worksheet.append([1, "Alice"])
    xlsx_buffer = io.BytesIO()
    workbook.save(xlsx_buffer)
    workbook.close()
    xlsx_profile = parse_file_content(
        xlsx_buffer.getvalue(),
        file_format=FileAssetFormat.XLSX,
        max_rows=100,
    )

    pdf_writer = PdfWriter()
    pdf_writer.add_blank_page(width=100, height=100)
    pdf_buffer = io.BytesIO()
    pdf_writer.write(pdf_buffer)

    assert xlsx_profile.statistics["sheet_name"] == "Sheet"
    assert xlsx_profile.rows[0]["id"] == 1
    with pytest.raises(DomainError, match="OCR 暂不支持"):
        parse_file_content(
            pdf_buffer.getvalue(),
            file_format=FileAssetFormat.PDF,
            max_rows=100,
        )


def test_parquet_orc_and_xml_normalize_to_unified_rows() -> None:
    """验证 Parquet、ORC 与 XML 都进入统一 JSON 行模型，并保留格式级诊断信息。"""
    table = pa.table(
        {
            "order_id": [1, 2],
            "email": ["alice@example.com", "bob@example.com"],
            "amount": [12.5, 8.0],
        }
    )
    parquet_buffer = io.BytesIO()
    arrow_parquet.write_table(table, parquet_buffer)
    orc_buffer = io.BytesIO()
    arrow_orc.write_table(table, orc_buffer)

    parquet_profile = parse_file_content(
        parquet_buffer.getvalue(),
        file_format=FileAssetFormat.PARQUET,
        max_rows=100,
    )
    orc_profile = parse_file_content(
        orc_buffer.getvalue(),
        file_format=FileAssetFormat.ORC,
        max_rows=100,
    )
    xml_profile = parse_file_content(
        b"<orders><order source='erp'><id>1</id><amount>12.5</amount>"
        b"<tag>new</tag><tag>paid</tag></order></orders>",
        file_format=FileAssetFormat.XML,
        max_rows=100,
    )

    assert parquet_profile.rows[0]["order_id"] == 1
    assert parquet_profile.masked_sample[0]["email"] == "a***@example.com"
    assert parquet_profile.statistics["row_group_count"] == 1
    assert orc_profile.rows[1]["amount"] == 8.0
    assert orc_profile.statistics["stripe_count"] == 1
    assert xml_profile.rows == [
        {"attr_source": "erp", "id": 1, "amount": 12.5, "tag": '["new","paid"]'}
    ]
    assert xml_profile.statistics["root_element"] == "orders"


def test_columnar_formats_reject_row_count_over_limit() -> None:
    """验证压缩列式文件在读取全部记录前执行行数门禁，防止小文件造成内存放大。"""
    table = pa.table({"id": [1, 2]})
    parquet_buffer = io.BytesIO()
    arrow_parquet.write_table(table, parquet_buffer)

    with pytest.raises(DomainError, match="行上限"):
        parse_file_content(
            parquet_buffer.getvalue(),
            file_format=FileAssetFormat.PARQUET,
            max_rows=1,
        )


@pytest.mark.asyncio
async def test_local_store_is_atomic_bounded_and_path_safe(tmp_path: Path) -> None:
    """验证受管存储流式限额、完整性摘要与目录穿越防护。"""
    store = LocalFileAssetStore(tmp_path)

    async def chunks() -> AsyncIterator[bytes]:
        """提供两个上传块，模拟 multipart 流式读取。"""
        yield b"hello"
        yield b" world"

    stored = await store.write_upload("asset/source.csv", chunks(), max_bytes=20)
    assert stored.byte_size == 11
    assert await store.read_bytes(stored.uri) == b"hello world"
    assert len(stored.sha256) == 64

    async def oversized() -> AsyncIterator[bytes]:
        """提供超过限制的上传块，验证失败时不会留下最终文件。"""
        yield b"0123456789"

    with pytest.raises(DomainError, match="上限"):
        await store.write_upload("asset/large.csv", oversized(), max_bytes=5)
    with pytest.raises(ResourceNotFound, match="越界"):
        store.path_for_uri("file-asset://../outside")


@pytest.mark.asyncio
async def test_failed_upload_leaves_no_empty_asset_directory(tmp_path: Path) -> None:
    """校验失败的上传必须回收本次新建的资产目录，不留空壳。

    临时文件与目标同目录才能原子替换，所以目录在校验之前就建好了；失败时若只删
    临时文件，受管目录里就会积累空目录，让"哪些资产真的落过盘"不再可信。
    """
    store = LocalFileAssetStore(tmp_path)

    async def empty() -> AsyncIterator[bytes]:
        """不产出任何数据块，模拟空文件上传。"""
        for chunk in ():
            yield chunk

    with pytest.raises(DomainError, match="空文件"):
        await store.write_upload("asset-empty/source.csv", empty(), max_bytes=1024)
    assert not (tmp_path / "asset-empty").exists()

    async def oversized() -> AsyncIterator[bytes]:
        """提供超过限制的上传块，模拟超限失败。"""
        yield b"0123456789"

    with pytest.raises(DomainError, match="上限"):
        await store.write_upload("asset-large/source.csv", oversized(), max_bytes=5)
    assert not (tmp_path / "asset-large").exists()


@pytest.mark.asyncio
async def test_failed_upload_keeps_directory_that_already_held_assets(tmp_path: Path) -> None:
    """目录里已有内容时，失败清理只回收空目录，绝不牵连既有资产。"""
    store = LocalFileAssetStore(tmp_path)

    async def kept() -> AsyncIterator[bytes]:
        """写入一份应当被保留的资产。"""
        yield b"kept"

    await store.write_upload("asset-shared/source.csv", kept(), max_bytes=1024)

    async def oversized() -> AsyncIterator[bytes]:
        """提供超过限制的上传块，模拟同目录下的后续失败。"""
        yield b"0123456789"

    with pytest.raises(DomainError, match="上限"):
        await store.write_upload("asset-shared/normalized.jsonl", oversized(), max_bytes=5)
    assert (tmp_path / "asset-shared" / "source.csv").read_bytes() == b"kept"


@pytest.mark.asyncio
async def test_file_upload_api_is_streamed_queued_and_redacted(
    session: AsyncSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 multipart 上传只排队 UUID，响应不泄露存储 URI 或引擎挂载路径。"""
    project = Project(name=f"upload-project-{uuid.uuid4()}")
    engineer = User(subject=f"upload-engineer-{uuid.uuid4()}", display_name="上传工程师")
    session.add_all([project, engineer])
    await session.flush()
    session.add(
        ProjectMembership(
            project_id=project.id,
            user_id=engineer.id,
            role=ProjectRole.DATA_ENGINEER,
        )
    )
    await session.commit()
    dispatches: list[str] = []

    async def override_session() -> AsyncIterator[AsyncSession]:
        """让 ASGI 请求复用当前 SQLite 会话。"""
        yield session

    def record_dispatch(asset_id: str) -> None:
        """记录 Celery 参数，确认任务不携带文件内容或路径。"""
        dispatches.append(asset_id)

    monkeypatch.setattr(
        file_asset_routes,
        "get_settings",
        lambda: SimpleNamespace(file_asset_root=tmp_path, file_asset_max_bytes=1024),
    )
    monkeypatch.setattr(file_asset_routes.profile_file_asset, "delay", record_dispatch)
    app.dependency_overrides[get_session] = override_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                f"/api/v1/projects/{project.id}/file-assets",
                headers={"X-Actor-ID": str(engineer.id)},
                files={"file": ("orders.csv", b"id,phone\n1,13800138000\n", "text/csv")},
            )
            await session.commit()
            listing = await client.get(
                f"/api/v1/projects/{project.id}/file-assets",
                headers={"X-Actor-ID": str(engineer.id)},
            )
    finally:
        app.dependency_overrides.pop(get_session, None)

    assert response.status_code == 202
    assert listing.status_code == 200
    body = json.dumps(response.json())
    assert response.json()["status"] == DataProfileStatus.PENDING.value
    assert dispatches == [response.json()["id"]]
    assert "source_uri" not in body
    assert "normalized_uri" not in body
    assert "engine_path" not in body
    assert "file-asset://" not in body
