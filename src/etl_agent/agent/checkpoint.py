"""LangGraph Checkpoint 持久化配置。

Checkpoint 让 AgentRun 在中断（澄清追问、人工审批）后能从同一状态继续。
PostgreSQL 后端依赖 langgraph-checkpoint-postgres，该包当前未安装，因此这里
先以 NotImplementedError 占位，由调用方降级到内存 Checkpoint。
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
    raise NotImplementedError(
        "LangGraph Checkpoint 后端尚未实现：需先安装 langgraph-checkpoint-postgres"
    )
