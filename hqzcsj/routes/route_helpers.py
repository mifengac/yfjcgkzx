from __future__ import annotations

from typing import Any, Callable, List

from flask import request


def user_has_module_access(
    get_connection: Callable[[], Any],
    *,
    username: str,
    module_name: str = "获取综查数据",
) -> bool:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (username, module_name),
            )
            return bool(cur.fetchone())
    finally:
        try:
            conn.close()
        except Exception:
            pass


def parse_list_arg(name: str) -> List[str]:
    values = request.args.getlist(name)
    return [text for text in (str(value or "").strip() for value in values) if text]


def parse_bool_arg(name: str) -> bool:
    value = str(request.args.get(name) or "").strip().lower()
    return value in ("1", "true", "yes", "on")