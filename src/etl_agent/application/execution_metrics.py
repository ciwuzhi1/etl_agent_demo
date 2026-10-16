"""执行指标归一化：将 SeaTunnel 引擎原始指标转换为平台统一结构。

对外提供：
- TableMetrics: 单表级别的输入/输出行数与字节数
- ExecutionMetrics: 整个执行任务的聚合指标及分表明细
- normalize_seatunnel_metrics: 从 SeaTunnel 响应中提取并归一化指标

注意：本文件使用 dataclass 而非 Pydantic，以保持轻量级依赖，
适合在 Worker 侧高频调用。
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TableMetrics:
    """单表级别的执行指标。"""

    table_name: str
    source_rows: int = 0
    sink_rows: int = 0
    source_bytes: int = 0
    sink_bytes: int = 0


@dataclass
class ExecutionMetrics:
    """一次执行任务的聚合指标。"""

    source_rows: int = 0
    sink_rows: int = 0
    source_bytes: int = 0
    sink_bytes: int = 0
    tables: list[TableMetrics] = field(default_factory=list)


def _to_int(value: Any) -> int | None:
    """安全地将任意值转换为整数，失败返回 None。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except (ValueError, TypeError):
            return None
    return None


def normalize_seatunnel_metrics(raw_metrics: dict[str, Any]) -> ExecutionMetrics:
    """从 SeaTunnel 引擎响应中提取并归一化指标。

    SeaTunnel 可能返回多种字段名（不同版本/配置），本函数尝试多个候选键，
    取第一个有效的值作为最终指标。

    Args:
        raw_metrics: SeaTunnel 引擎返回的原始指标字典，可能包含：
            - SourceReceivedCount / SourceReadCount
            - SinkWriteCount / SinkCommittedCount
            - SourceReceivedBytes / SourceReadBytes
            - SinkWriteBytes / SinkCommittedBytes
            - TableSourceReceivedCount / TableSinkWriteCount 等分表指标

    Returns:
        ExecutionMetrics 包含聚合指标和分表明细。
    """
    # 提取聚合指标：尝试多个候选键名
    source_rows = _first_int(
        raw_metrics, "SourceReceivedCount", "SourceReadCount", "source_received_count"
    )
    sink_rows = _first_int(raw_metrics, "SinkCommittedCount", "SinkWriteCount", "sink_write_count")
    source_bytes = _first_int(
        raw_metrics, "SourceReceivedBytes", "SourceReadBytes", "source_received_bytes"
    )
    sink_bytes = _first_int(raw_metrics, "SinkCommittedBytes", "SinkWriteBytes", "sink_write_bytes")

    # 提取分表指标
    tables = _extract_table_metrics(raw_metrics)

    return ExecutionMetrics(
        source_rows=source_rows or 0,
        sink_rows=sink_rows or 0,
        source_bytes=source_bytes or 0,
        sink_bytes=sink_bytes or 0,
        tables=tables,
    )


def _first_int(metrics: dict[str, Any], *keys: str) -> int | None:
    """按优先级尝试多个键名，返回第一个有效的整数值。"""
    for key in keys:
        value = _to_int(metrics.get(key))
        if value is not None:
            return value
    return None


def _extract_table_metrics(raw_metrics: dict[str, Any]) -> list[TableMetrics]:
    """从原始指标中提取分表级别的指标。

    SeaTunnel 可能以以下形式返回分表数据：
    - TableSourceReceivedCount: {"table1": 100, "table2": 200}
    - TableSinkWriteCount: {"table1": 95, "table2": 198}
    """
    table_source_rows = _to_dict(raw_metrics.get("TableSourceReceivedCount"))
    table_sink_rows = _first_dict(raw_metrics, "TableSinkCommittedCount", "TableSinkWriteCount")
    table_source_bytes = _to_dict(raw_metrics.get("TableSourceReceivedBytes"))
    table_sink_bytes = _first_dict(raw_metrics, "TableSinkCommittedBytes", "TableSinkWriteBytes")

    # 收集所有出现过的表名
    all_tables: set[str] = set()
    all_tables.update(table_source_rows.keys())
    all_tables.update(table_sink_rows.keys())
    all_tables.update(table_source_bytes.keys())
    all_tables.update(table_sink_bytes.keys())

    tables = []
    for table_name in sorted(all_tables):
        metrics = TableMetrics(
            table_name=table_name,
            source_rows=table_source_rows.get(table_name, 0),
            sink_rows=table_sink_rows.get(table_name, 0),
            source_bytes=table_source_bytes.get(table_name, 0),
            sink_bytes=table_sink_bytes.get(table_name, 0),
        )
        tables.append(metrics)

    return tables


def _to_dict(value: Any) -> dict[str, int]:
    """安全地将值转换为 {str: int} 字典。"""
    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for key, val in value.items():
        if isinstance(key, str):
            int_val = _to_int(val)
            if int_val is not None:
                result[key] = int_val
    return result


def _first_dict(metrics: dict[str, Any], *keys: str) -> dict[str, int]:
    """按优先级尝试多个键名，返回第一个有效的字典值。"""
    for key in keys:
        value = _to_dict(metrics.get(key))
        if value:
            return value
    return {}
