from __future__ import annotations

from typing import Any, Dict, List


REGION_CODE_NAME_MAP: Dict[str, str] = {
    "445300": "市局",
    "445302": "云城",
    "445303": "云安",
    "445321": "新兴",
    "445322": "郁南",
    "445381": "罗定",
}

REGION_NAME_ORDER: List[str] = ["云城", "云安", "罗定", "新兴", "郁南", "市局", "全市"]


def map_region_name(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return "其他"
    if s in REGION_CODE_NAME_MAP:
        return REGION_CODE_NAME_MAP[s]
    if len(s) >= 6 and s[:6] in REGION_CODE_NAME_MAP:
        return REGION_CODE_NAME_MAP[s[:6]]
    if s in REGION_CODE_NAME_MAP.values():
        return s
    return "其他"
