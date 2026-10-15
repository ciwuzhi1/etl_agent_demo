"""LangGraph Checkpoint 持久化配置。

第 8 章要求使用 PostgreSQL 作为 Checkpoint 后端，让 AgentRun 能在中断后恢复。
本章先建骨架，等 LangGraph 包安装后再补完整实现。
"""

from typing import Any


def create_checkpoint_saver(database_url: str) -> Any:
    """创建 PostgreSQL Checkpoint Saver。

    Args:
        database_url: PostgreSQL 连接字符串

    Returns:
        LangGraph PostgresSaver 实例（待实现）

    Note:
        需要安装 langgraph-checkpoint-postgres 包
    """
    # TODO: 实现 PostgreSQL Checkpoint 后端
    # from langgraph.checkpoint.postgres import PostgresSaver
    # return PostgresSaver.from_conn_string(database_url)
    raise NotImplementedError(
        "LangGraph Checkpoint 后端尚未实现，需先安装 langgraph-checkpoint-postgres"
    )
