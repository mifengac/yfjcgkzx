from __future__ import annotations

from typing import Any, Mapping, Optional


def first_of(row: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    """
    从行数据中按优先级取第一个存在且非空的字段。
    """
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return default


def first_key(row: Mapping[str, Any], *keys: str) -> Optional[str]:
    """
    返回第一个存在的 key（不要求非空）。
    """
    for k in keys:
        if k in row:
            return k
    return None

