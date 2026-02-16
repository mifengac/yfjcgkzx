from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence, Tuple

import psycopg2.extras

from gonggong.config.database import get_database_connection


TABLE_NAME = '"ywdata"."b_dxpt_mdjfyj"'


class SmsManageDAO:
    """短信发送管理数据访问层。"""

    @staticmethod
    def list_rows(*, keyword: str, page: int, page_size: int) -> Tuple[List[Dict[str, Any]], int]:
        page = max(1, int(page or 1))
        page_size = max(1, min(200, int(page_size or 20)))
        offset = (page - 1) * page_size

        kw = (keyword or "").strip()
        where_sql = ""
        params: List[Any] = []
        if kw:
            where_sql = (
                " WHERE xq ILIKE %s OR xqdm ILIKE %s OR sspcs ILIKE %s OR "
                "sspcsdm ILIKE %s OR xm ILIKE %s OR zw ILIKE %s OR lxdh ILIKE %s OR bz ILIKE %s"
            )
            like = f"%{kw}%"
            params.extend([like] * 8)

        sql_count = f"SELECT COUNT(*) AS total FROM {TABLE_NAME}{where_sql}"
        sql_data = (
            f"SELECT xh, xq, xqdm, sspcs, sspcsdm, xm, zw, lxdh, bz, lrsj "
            f"FROM {TABLE_NAME}{where_sql} "
            "ORDER BY lrsj DESC NULLS LAST, sspcsdm ASC, xm ASC "
            "LIMIT %s OFFSET %s"
        )

        conn = get_database_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql_count, tuple(params))
                total = int(cur.fetchone()["total"])
                cur.execute(sql_data, tuple(params + [page_size, offset]))
                rows = [dict(r) for r in cur.fetchall()]
                return rows, total
        finally:
            conn.close()

    @staticmethod
    def upsert_rows(items: Iterable[Dict[str, Any]]) -> int:
        data = list(items)
        if not data:
            return 0

        sql = (
            f"INSERT INTO {TABLE_NAME} (xh, xq, xqdm, sspcs, sspcsdm, xm, zw, lxdh, bz, lrsj) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()) "
            "ON CONFLICT (sspcsdm, xm) DO UPDATE SET "
            "xh = EXCLUDED.xh, "
            "xq = EXCLUDED.xq, "
            "xqdm = EXCLUDED.xqdm, "
            "sspcs = EXCLUDED.sspcs, "
            "zw = EXCLUDED.zw, "
            "lxdh = EXCLUDED.lxdh, "
            "bz = EXCLUDED.bz, "
            "lrsj = NOW()"
        )

        params: List[Tuple[Any, ...]] = []
        for item in data:
            params.append(
                (
                    item.get("xh"),
                    item.get("xq"),
                    item.get("xqdm"),
                    item.get("sspcs"),
                    item.get("sspcsdm"),
                    item.get("xm"),
                    item.get("zw"),
                    item.get("lxdh"),
                    item.get("bz"),
                )
            )

        conn = get_database_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, sql, params, page_size=200)
            return len(params)
        finally:
            conn.close()

    @staticmethod
    def delete_by_keys(keys: Sequence[Tuple[str, str]]) -> int:
        clean_keys = [(str(k1).strip(), str(k2).strip()) for k1, k2 in keys if str(k1).strip() and str(k2).strip()]
        if not clean_keys:
            return 0

        conn = get_database_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    deleted = 0
                    for sspcsdm, xm in clean_keys:
                        cur.execute(
                            f"DELETE FROM {TABLE_NAME} WHERE sspcsdm = %s AND xm = %s",
                            (sspcsdm, xm),
                        )
                        deleted += int(cur.rowcount or 0)
                    return deleted
        finally:
            conn.close()
