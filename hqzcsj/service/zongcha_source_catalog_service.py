from __future__ import annotations

from typing import Any, Dict, List

from hqzcsj.service.tqws_service import get_tqws_source_catalog
from hqzcsj.service.zongcha_service import get_fetch_sources


VALID_SCOPES = {"fetch", "tqws", "all"}


def get_source_catalog(*, scope: str = "all") -> List[Dict[str, Any]]:
    use_scope = str(scope or "all").strip().lower()
    if use_scope not in VALID_SCOPES:
        raise ValueError(f"非法 scope: {scope}")

    out: List[Dict[str, Any]] = []
    if use_scope in ("fetch", "all"):
        for item in get_fetch_sources():
            out.append(
                {
                    "scope": "fetch",
                    "key": str(item.get("key") or "").strip(),
                    "name": str(item.get("name") or "").strip(),
                    "table": str(item.get("table") or "").strip(),
                    "pk_fields": list(item.get("pk_fields") or []),
                    "requires": str(item.get("requires") or "cookie+authorization"),
                    "time_mode": str(item.get("time_mode") or "range"),
                }
            )
    if use_scope in ("tqws", "all"):
        for item in get_tqws_source_catalog():
            out.append(
                {
                    "scope": "tqws",
                    "key": str(item.get("key") or "").strip(),
                    "name": str(item.get("name") or "").strip(),
                    "table": str(item.get("table") or "").strip(),
                    "pk_fields": list(item.get("pk_fields") or []),
                    "requires": str(item.get("requires") or "access_token"),
                    "time_mode": str(item.get("time_mode") or "none"),
                }
            )
    return out
