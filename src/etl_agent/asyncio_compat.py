import asyncio
import sys
import threading
from collections.abc import Coroutine
from typing import TypeVar

T = TypeVar("T")
_windows_loop: asyncio.AbstractEventLoop | None = None
_windows_loop_lock = threading.Lock()


def run_async(coro: Coroutine[object, object, T]) -> T:
    """在 Windows 上使用兼容 psycopg 的事件循环执行协程，并同步返回结果。"""
    if sys.platform == "win32":
        # psycopg 不支持 Proactor；Celery solo 也必须复用同一 loop，
        # 否则 SQLAlchemy/asyncpg 连接池会把连接带到新 loop 中。
        global _windows_loop
        with _windows_loop_lock:
            if _windows_loop is None or _windows_loop.is_closed():
                _windows_loop = asyncio.SelectorEventLoop()
            return _windows_loop.run_until_complete(coro)
    return asyncio.run(coro)
