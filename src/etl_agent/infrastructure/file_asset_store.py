"""本地受管文件存储：原子写入 + 流式 SHA-256 + 目录穿越防护。

课件 §5.4.2（第 5 章）要求上传用临时文件+原子替换、读时校验 file-asset:// 协议和目录穿越。
参考实现 124 行，`_resolve()` 用 `is_relative_to` 挡 `..`；
`file_asset_root=var/file-assets` 已在 `.env:246`。
"""

import asyncio
import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from etl_agent.domain.errors import DomainError, ResourceNotFound


@dataclass(frozen=True)
class StoredFile:
    """描述一次原子文件写入的非敏感定位信息与完整性摘要。"""

    uri: str
    byte_size: int
    sha256: str


class LocalFileAssetStore:
    """在本地受管目录内原子保存文件；生产部署可替换为对象存储实现。"""

    scheme = "file-asset://"

    def __init__(self, root: Path) -> None:
        """冻结存储根目录，后续所有路径都必须通过越界检查。"""
        self._root = root.resolve()

    async def write_upload(
        self,
        relative_path: str,
        chunks: AsyncIterator[bytes],
        *,
        max_bytes: int,
    ) -> StoredFile:
        """流式写入上传内容并计算 SHA-256，超限或异常时删除临时文件。

        临时文件必须与目标同目录才能做原子替换，所以资产目录在校验内容之前就建好了。
        一旦校验失败（空文件、超限），只删临时文件会在受管目录里留下空壳，让
        "哪些资产真的落过盘"变得不可信；因此失败时还要回收本次新建的空目录。
        """
        destination = self._resolve(relative_path)
        parent_existed = destination.parent.is_dir()
        await asyncio.to_thread(destination.parent.mkdir, parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.uploading")
        digest = hashlib.sha256()
        size = 0
        try:
            handle = await asyncio.to_thread(temporary.open, "xb")
            try:
                async for chunk in chunks:
                    if not chunk:
                        continue
                    size += len(chunk)
                    if size > max_bytes:
                        raise DomainError(f"文件超过允许的 {max_bytes // 1024 // 1024} MiB 上限")
                    digest.update(chunk)
                    await asyncio.to_thread(handle.write, chunk)
                await asyncio.to_thread(handle.flush)
            finally:
                await asyncio.to_thread(handle.close)
            if size == 0:
                raise DomainError("不能上传空文件")
            await asyncio.to_thread(temporary.replace, destination)
        except Exception:
            await asyncio.to_thread(temporary.unlink, missing_ok=True)
            if not parent_existed:
                await asyncio.to_thread(_remove_empty_directory, destination.parent)
            raise
        return StoredFile(
            uri=self.scheme + PurePosixPath(relative_path).as_posix(),
            byte_size=size,
            sha256=digest.hexdigest(),
        )

    async def read_bytes(self, uri: str) -> bytes:
        """读取受管文件字节，并拒绝未知协议、路径穿越和不存在的文件。"""
        path = self.path_for_uri(uri)
        return await asyncio.to_thread(path.read_bytes)

    async def write_json_lines(
        self,
        relative_path: str,
        rows: Iterable[dict[str, Any]],
    ) -> StoredFile:
        """把规范化行原子写成 UTF-8 JSONL，供 SeaTunnel LocalFile 只读消费。"""
        destination = self._resolve(relative_path)
        parent_existed = destination.parent.is_dir()
        await asyncio.to_thread(destination.parent.mkdir, parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.normalizing")

        def write() -> StoredFile:
            """在线程中完成阻塞文件写入，避免占用 API/Worker 的事件循环。"""
            digest = hashlib.sha256()
            size = 0
            try:
                with temporary.open("xb") as handle:
                    for row in rows:
                        payload = (
                            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
                        ).encode("utf-8")
                        handle.write(payload)
                        digest.update(payload)
                        size += len(payload)
                temporary.replace(destination)
            except Exception:
                temporary.unlink(missing_ok=True)
                if not parent_existed:
                    _remove_empty_directory(destination.parent)
                raise
            return StoredFile(
                uri=self.scheme + PurePosixPath(relative_path).as_posix(),
                byte_size=size,
                sha256=digest.hexdigest(),
            )

        return await asyncio.to_thread(write)

    def path_for_uri(self, uri: str) -> Path:
        """把内部 URI 转回受管绝对路径，不向 API 响应暴露该结果。"""
        if not uri.startswith(self.scheme):
            raise ResourceNotFound("不支持的文件资产 URI")
        path = self._resolve(uri.removeprefix(self.scheme))
        if not path.is_file():
            raise ResourceNotFound("文件资产不存在")
        return path

    def _resolve(self, relative_path: str) -> Path:
        """把相对路径限制在根目录，阻止绝对路径和 `..` 目录穿越。"""
        path = (self._root / relative_path).resolve()
        if not path.is_relative_to(self._root):
            raise ResourceNotFound("文件资产路径越界")
        return path


def _remove_empty_directory(path: Path) -> None:
    """尽力回收一个空目录；非空或已被并发删除时静默跳过。

    只用 ``rmdir``（不递归），所以目录里一旦有并发上传写入的内容就必然失败并保留，
    不存在误删他人数据的可能。这两种失败都是本函数的正常语义，不是被吞掉的错误。
    """
    try:
        path.rmdir()
    except OSError:
        return
