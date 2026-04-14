from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List


def normalize_option_rows(
    raw_rows: Iterable[Dict[str, Any]] | None,
    *,
    value_key: str = "value",
    label_key: str = "label",
) -> List[Dict[str, str]]:
    seen = set()
    normalized: List[Dict[str, str]] = []
    for row in raw_rows or []:
        value = str(row.get(value_key) or "").strip()
        label = str(row.get(label_key) or "").strip()
        if not value or not label or value in seen:
            continue
        seen.add(value)
        normalized.append({"value": value, "label": label})
    return normalized


def normalize_text_list(values: Iterable[Any] | None) -> List[str]:
    return [str(value).strip() for value in (values or []) if str(value).strip()]


def sanitize_filename_text(text: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "-", (text or "").strip())