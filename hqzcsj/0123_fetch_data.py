#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
从 68.26.7.188:8088 拉取 3 类“政法办案”数据并导入人大金仓（KingbaseES）。

数据来源与请求参数：
- SQL/fetch_zfba_data.txt（包含请求网址/请求头/3 组请求参数样例）

你通常只需要修改：
- HTTP_COOKIE（或环境变量 ZFBA_COOKIE）
- HTTP_AUTHORIZATION（或环境变量 ZFBA_AUTHORIZATION）
- DB_CONFIG（或环境变量 KINGBASE_*）

写入目标（固定 schema=ywdata）：
- ywdata.zq_zfba_xjs (PK: xjs_id)
- ywdata.zq_zfba_jtjyzdtzs (PK: jqjhjyzljsjtjyzdtzs_id)
- ywdata.zq_zfba_zlwcnrzstdxwgftzs (PK: zltzs_id)

建表规则：
- 字段来自响应 JSON 的 context.result.result（即“第二个 result”）里对象的 key
- 默认 TEXT
- 若值形如 YYYY-MM-DD HH:MM:SS，则按 TIMESTAMP 建列
- 显式创建主键约束
- 表备注（COMMENT）写表名（不含 schema）
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None


FETCH_FILE = os.getenv("ZFBA_FETCH_FILE", "SQL/fetch_zfba_data.txt")

# 内置默认请求参数（不依赖 --fetch-file）；如需从文件读取，可传 --fetch-file SQL/fetch_zfba_data.txt
DEFAULT_URL = "http://68.26.7.188:8088/api/search/v3/fusionQuery"

# Cookie / Authorization 由 HTTP_COOKIE / HTTP_AUTHORIZATION 覆盖（默认留空）
DEFAULT_BASE_HEADERS: Dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded",
    "Module": "/comprehensive-query",
    "Origin": "http://68.26.7.188:8088",
    "Referer": "http://68.26.7.188:8088/",
    "Screen": "1920x1080",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36",
}

DEFAULT_REQUEST_FORMS: Dict[str, Dict[str, str]] = {
    "训诫书": {
        "json": "{\"paramArray\":[{\"conditions\":[{\"tabId\":\"1782354546966700043\",\"tabCode\":\"xjs\",\"fieldCode\":\"xjs_cbdw_bh\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"7\",\"values\":[\"445300000000\"],\"isIncludeChilds\":true,\"dicCode\":\"06\"},{\"tabId\":\"1782354546966700043\",\"tabCode\":\"xjs\",\"fieldCode\":\"xjs_tfsj\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"10\",\"values\":[\"2026-01-01 00:00:00\",\"2026-01-23 23:59:59\"],\"excludeDays\":[],\"rangeIncludeType\":\"0\"},{\"tabId\":\"1782354546966700043\",\"tabCode\":\"xjs\",\"fieldCode\":\"xjs_wszt\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"7\",\"values\":[\"03\"],\"isIncludeChilds\":false,\"dicCode\":\"ZD_CASE_WSZT\"}],\"tabId\":\"1782354546966700043\",\"tabCode\":\"xjs\",\"domainId\":\"11\"}]}",
        "domainId": "11",
        "resultTabId": "1782354546966700043",
        "resultTabCode": "xjs",
        "resultTableName": "训诫书（未成年人）",
        "tabId": "1782354546966700043",
        "pageSize": "100",
        "pageNumber": "1",
        "sortColumns": "",
    },
    "加强监督教育/责令接受家庭教育指导通知书": {
        "json": "{\"paramArray\":[{\"conditions\":[{\"tabId\":\"1782350085472952324\",\"tabCode\":\"jqjhjyzljsjtjyzdtzs\",\"fieldCode\":\"jqjhjyzljsjtjyzdtzs_cbdw_bh\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"7\",\"values\":[\"445300000000\"],\"isIncludeChilds\":true,\"dicCode\":\"06\"},{\"tabId\":\"1782350085472952324\",\"tabCode\":\"jqjhjyzljsjtjyzdtzs\",\"fieldCode\":\"jqjhjyzljsjtjyzdtzs_tfsj\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"10\",\"values\":[\"2026-01-01 00:00:00\",\"2026-01-23 23:59:59\"],\"excludeDays\":[],\"rangeIncludeType\":\"0\"},{\"tabId\":\"1782350085472952324\",\"tabCode\":\"jqjhjyzljsjtjyzdtzs\",\"fieldCode\":\"jqjhjyzljsjtjyzdtzs_wszt\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"7\",\"values\":[\"03\"],\"isIncludeChilds\":false,\"dicCode\":\"ZD_CASE_WSZT\"}],\"tabId\":\"1782350085472952324\",\"tabCode\":\"jqjhjyzljsjtjyzdtzs\",\"domainId\":\"11\"}]}",
        "domainId": "11",
        "resultTabId": "1782350085472952324",
        "resultTabCode": "jqjhjyzljsjtjyzdtzs",
        "resultTableName": "加强监督教育/责令接受家庭教育指导通知书",
        "tabId": "1782350085472952324",
        "pageSize": "100",
        "pageNumber": "1",
        "sortColumns": "",
    },
    "责令未成年人遵守特定行为规范通知书": {
        "json": "{\"paramArray\":[{\"conditions\":[{\"tabId\":\"1782315945839075392\",\"tabCode\":\"zltzs\",\"fieldCode\":\"zltzs_cbdw_bh\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"7\",\"values\":[\"445300000000\"],\"isIncludeChilds\":true,\"dicCode\":\"06\"},{\"tabId\":\"1782315945839075392\",\"tabCode\":\"zltzs\",\"fieldCode\":\"zltzs_tfsj\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"10\",\"values\":[\"2026-01-01 00:00:00\",\"2026-01-23 23:59:59\"],\"excludeDays\":[],\"rangeIncludeType\":\"0\"},{\"tabId\":\"1782315945839075392\",\"tabCode\":\"zltzs\",\"fieldCode\":\"zltzs_wszt\",\"tabType\":\"2\",\"isPub\":false,\"operateSign\":\"7\",\"values\":[\"03\"],\"isIncludeChilds\":false,\"dicCode\":\"ZD_CASE_WSZT\"}],\"tabId\":\"1782315945839075392\",\"tabCode\":\"zltzs\",\"domainId\":\"11\"}]}",
        "domainId": "11",
        "resultTabId": "1782315945839075392",
        "resultTabCode": "zltzs",
        "resultTableName": "责令未成年人遵守特定行为规范通知书",
        "tabId": "1782315945839075392",
        "pageSize": "100",
        "pageNumber": "1",
        "sortColumns": "",
    },
}

# 只要改这里（或用环境变量覆盖）
HTTP_COOKIE = os.getenv("ZFBA_COOKIE", "").strip()  # e.g. "projectToken=..."
HTTP_AUTHORIZATION = os.getenv("ZFBA_AUTHORIZATION", "").strip()  # e.g. "Bearer xxx"


DB_CONFIG = {
    # 你来填（也可用环境变量 KINGBASE_* 覆盖）
    "host": os.getenv("KINGBASE_HOST", ""),
    "port": os.getenv("KINGBASE_PORT", ""),
    "user": os.getenv("KINGBASE_USER", ""),
    "password": os.getenv("KINGBASE_PASSWORD", ""),
    "database": os.getenv("KINGBASE_DB", ""),
    # 固定要求
    "schema": "ywdata",
}


_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


@dataclass(frozen=True)
class JobSpec:
    name: str
    request_label: str
    table: str
    pk: str


JOBS: Sequence[JobSpec] = (
    JobSpec(
        name="训诫书",
        request_label="训诫书",
        table="zq_zfba_xjs",
        pk="xjs_id",
    ),
    JobSpec(
        name="家庭教育指导通知书",
        request_label="加强监督教育/责令接受家庭教育指导通知书",
        table="zq_zfba_jtjyzdtzs",
        pk="jqjhjyzljsjtjyzdtzs_id",
    ),
    JobSpec(
        name="责令未成年人遵守特定行为规范通知书",
        request_label="责令未成年人遵守特定行为规范通知书",
        table="zq_zfba_zlwcnrzstdxwgftzs",
        pk="zltzs_id",
    ),
)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_fetch_file(path: str) -> Tuple[str, Dict[str, str], Dict[str, Dict[str, str]]]:
    """
    返回：(url, headers, request_params_by_label)
    request_params_by_label[label] 是 data(form) dict，其中包含 "json" 字段。
    """
    text = Path(path).read_text(encoding="utf-8").lstrip("\ufeff")
    lines = text.splitlines()

    url = ""
    headers: Dict[str, str] = {}
    reqs: Dict[str, Dict[str, str]] = {}

    def strip_line(s: str) -> str:
        return (s or "").strip().strip("\u200b").strip()

    i = 0
    while i < len(lines):
        line = strip_line(lines[i])
        if line == "请求网址:" and i + 1 < len(lines):
            url = strip_line(lines[i + 1])
            i += 2
            continue

        if line == "请求标头:":
            i += 1
            while i < len(lines):
                k = strip_line(lines[i])
                if not k:
                    break
                if k.endswith(":") and i + 1 < len(lines):
                    v = strip_line(lines[i + 1])
                    headers[k[:-1].strip()] = v
                    i += 2
                else:
                    i += 1
            continue

        if line.endswith(":请求参数:") or line.endswith("请求参数:"):
            # 兼容两种写法：
            # - 训诫书:请求参数:
            # - 责令未成年人遵守特定行为规范通知书请求参数:
            if line.endswith(":请求参数:"):
                label = strip_line(line[: -len(":请求参数:")])
            else:
                label = strip_line(line[: -len("请求参数:")])
                if label.endswith(":"):
                    label = strip_line(label[:-1])
            i += 1
            params: Dict[str, str] = {}
            while i < len(lines):
                cur = strip_line(lines[i])
                if not cur:
                    break
                # 避免把“响应数据”段落误当成请求参数
                if cur.endswith("响应数据:") or cur.startswith("{"):
                    break
                if cur.startswith("json:"):
                    params["json"] = strip_line(cur[len("json:") :])
                elif ":" in cur:
                    k, v = cur.split(":", 1)
                    params[strip_line(k)] = strip_line(v)
                i += 1
            reqs[label] = params
            continue

        i += 1

    if not url:
        raise SystemExit(f"无法从 {path} 解析出 请求网址。")
    if not reqs:
        raise SystemExit(f"无法从 {path} 解析出 3 组请求参数。")
    return url, headers, reqs


def _apply_dynamic_end_time(json_obj: Any, *, now_str: str) -> Any:
    """
    把 conditions 中 *_tfsj 的 values[1] 替换为当前时间。
    """
    try:
        for item in json_obj.get("paramArray", []) or []:
            for cond in item.get("conditions", []) or []:
                field_code = str(cond.get("fieldCode") or "")
                values = cond.get("values")
                if field_code.endswith("_tfsj") and isinstance(values, list) and len(values) >= 2:
                    values[1] = now_str
    except Exception:
        return json_obj
    return json_obj


def _http_headers(base_headers: Dict[str, str]) -> Dict[str, str]:
    headers = dict(base_headers or {})
    # requests/urllib3 会自行处理这些；保留可能导致不一致
    headers.pop("Content-Length", None)
    headers.pop("Host", None)
    if HTTP_COOKIE:
        headers["Cookie"] = HTTP_COOKIE
    if HTTP_AUTHORIZATION:
        headers["Authorization"] = HTTP_AUTHORIZATION
    # requests 会自动处理 Content-Length；这里确保表单
    headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
    return headers


def _fetch_all_pages(
    *,
    url: str,
    headers: Dict[str, str],
    base_form: Dict[str, str],
    now_str: str,
    max_pages: int = 2000,
    timeout_s: int = 60,
) -> List[Dict[str, Any]]:
    if requests is None:
        raise SystemExit("缺少依赖 requests。请先执行: pip install requests")
    if "json" not in base_form or not base_form["json"]:
        raise SystemExit("请求参数缺少 json 字段。")

    try:
        json_obj = json.loads(base_form["json"])
    except Exception as e:
        raise SystemExit(f"无法解析请求参数 json: {e}") from e

    json_obj = _apply_dynamic_end_time(json_obj, now_str=now_str)

    out: List[Dict[str, Any]] = []
    page = int(base_form.get("pageNumber") or "1")
    page_size = int(base_form.get("pageSize") or "100")

    for _ in range(max_pages):
        form = dict(base_form)
        form["pageNumber"] = str(page)
        form["pageSize"] = str(page_size)
        form["json"] = json.dumps(json_obj, ensure_ascii=False)

        resp = requests.post(url, headers=headers, data=form, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json()

        rows = (
            (data or {}).get("context", {}).get("result", {}).get("result")
            if isinstance(data, dict)
            else None
        )
        if rows is None:
            raise SystemExit(f"响应缺少 context.result.result，无法解析：{type(data)}")
        if not isinstance(rows, list):
            raise SystemExit(f"响应 context.result.result 不是数组：{type(rows)}")

        if not rows:
            break
        for r in rows:
            if isinstance(r, dict):
                out.append(r)
        page += 1

    return out


def _infer_col_type_from_values(values: Iterable[Any]) -> str:
    """
    返回 SQL 类型：TEXT / TIMESTAMP
    """
    for v in values:
        if v is None:
            continue
        if isinstance(v, str):
            s = v.strip()
            if _TIME_RE.match(s):
                return "TIMESTAMP"
    return "TEXT"


def _parse_value_for_type(v: Any, col_type: str) -> Any:
    if v is None:
        return None
    if col_type.upper().startswith("TIMESTAMP"):
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            s = v.strip()
            if _TIME_RE.match(s):
                try:
                    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    return None
        return None
    return str(v) if not isinstance(v, str) else v


def _import_psycopg2():
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
        from psycopg2 import sql  # type: ignore

        return psycopg2, psycopg2.extras, sql
    except Exception as e:
        raise SystemExit("缺少依赖 psycopg2，无法连接人大金仓。请先执行: pip install psycopg2") from e


def _connect_db(cfg: Dict[str, str]):
    psycopg2, _, _ = _import_psycopg2()
    connect_kwargs: Dict[str, Any] = {
        "host": cfg.get("host", ""),
        "port": cfg.get("port", ""),
        "user": cfg.get("user", ""),
        "password": cfg.get("password", ""),
        "dbname": cfg.get("database", ""),
    }
    connect_kwargs = {k: v for k, v in connect_kwargs.items() if v not in ("", None)}
    return psycopg2.connect(**connect_kwargs)


def _ensure_schema(conn, schema: str) -> None:
    psycopg2, _, sql = _import_psycopg2()
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
    conn.commit()


def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def _get_existing_columns(conn, schema: str, table: str) -> Dict[str, str]:
    cols: Dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        for name, data_type in cur.fetchall() or []:
            if name:
                cols[str(name)] = str(data_type or "")
    return cols


def _ensure_table_and_columns(
    *,
    conn,
    schema: str,
    table: str,
    pk: str,
    table_comment: str,
    inferred_types: Dict[str, str],
) -> Dict[str, str]:
    psycopg2, _, sql = _import_psycopg2()

    exists = _table_exists(conn, schema, table)

    if not exists:
        cols_sql = []
        # 至少保证 PK 列存在
        inferred_types = dict(inferred_types)
        inferred_types.setdefault(pk, "TEXT")
        for col_name, col_type in sorted(inferred_types.items(), key=lambda x: x[0]):
            cols_sql.append(
                sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type))
            )

        create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({}, PRIMARY KEY ({}))").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(cols_sql) if cols_sql else sql.SQL("{} TEXT").format(sql.Identifier(pk)),
            sql.Identifier(pk),
        )
        with conn.cursor() as cur:
            cur.execute(create_sql)
            cur.execute(
                sql.SQL("COMMENT ON TABLE {}.{} IS %s").format(
                    sql.Identifier(schema), sql.Identifier(table)
                ),
                (table_comment,),
            )
        conn.commit()

    # 确保主键约束存在（若表是历史创建且无 PK）
    constraint_name = f"pk_{table}"
    with conn.cursor() as cur:
        try:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(constraint_name),
                    sql.Identifier(pk),
                )
            )
        except psycopg2.Error:
            conn.rollback()

    # 补齐新增字段
    existing_cols = _get_existing_columns(conn, schema, table)
    missing = [c for c in inferred_types.keys() if c not in existing_cols]
    if missing:
        with conn.cursor() as cur:
            for col in sorted(missing):
                col_type = inferred_types[col]
                try:
                    cur.execute(
                        sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                            sql.Identifier(schema),
                            sql.Identifier(table),
                            sql.Identifier(col),
                            sql.SQL(col_type),
                        )
                    )
                except psycopg2.Error:
                    conn.rollback()
        conn.commit()

    # 返回最终已知列类型：优先 DB 中已有类型，其次推断
    merged: Dict[str, str] = {}
    existing_cols = _get_existing_columns(conn, schema, table)
    for col, dtype in existing_cols.items():
        merged[col] = "TIMESTAMP" if "timestamp" in dtype.lower() else "TEXT"
    for col, ctype in inferred_types.items():
        merged.setdefault(col, ctype)
    return merged


def _upsert_rows(
    *,
    conn,
    schema: str,
    table: str,
    pk: str,
    col_types: Dict[str, str],
    rows: Sequence[Dict[str, Any]],
    batch_size: int = 500,
) -> Tuple[int, int]:
    _, extras, sql = _import_psycopg2()

    if not rows:
        return 0, 0

    columns = sorted({k for r in rows for k in (r or {}).keys() if k})
    if pk not in columns:
        columns.insert(0, pk)

    insert_cols = [sql.Identifier(c) for c in columns]
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)

    update_cols = [c for c in columns if c != pk]
    update_sql = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in update_cols
    )

    stmt = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES ({}) "
        "ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(insert_cols),
        placeholders,
        sql.Identifier(pk),
        update_sql if update_cols else sql.SQL(""),
    )

    data_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        tup: List[Any] = []
        for c in columns:
            ctype = col_types.get(c, "TEXT")
            tup.append(_parse_value_for_type(r.get(c), ctype))
        data_rows.append(tuple(tup))

    inserted_or_updated = 0
    with conn.cursor() as cur:
        for i in range(0, len(data_rows), batch_size):
            batch = data_rows[i : i + batch_size]
            extras.execute_batch(cur, stmt, batch, page_size=min(batch_size, len(batch)))
            inserted_or_updated += len(batch)
    conn.commit()
    # ON CONFLICT 下无法区分 insert/update；这里返回总处理数，update 数置 0（可按需扩展）
    return inserted_or_updated, 0


def main() -> int:
    ap = argparse.ArgumentParser(description="拉取政法办案数据并导入人大金仓（schema=ywdata）")
    ap.add_argument(
        "--fetch-file",
        default="",
        help="从文件读取请求参数（覆盖脚本内置默认）。例如: SQL/fetch_zfba_data.txt",
    )
    ap.add_argument("--max-pages", type=int, default=2000, help="每个接口最多翻页数")
    ap.add_argument("--dry-run", action="store_true", help="只拉取不入库（打印条数）")
    args = ap.parse_args()

    if args.fetch_file:
        url, base_headers, reqs = _parse_fetch_file(args.fetch_file)
        print(f"[INFO] 使用请求参数文件: {args.fetch_file}")
    else:
        url, base_headers, reqs = DEFAULT_URL, DEFAULT_BASE_HEADERS, DEFAULT_REQUEST_FORMS
        print("[INFO] 使用脚本内置请求参数（如需从文件读取，传 --fetch-file）")

    headers = _http_headers(base_headers)
    now_str = _now_str()

    # 先拉数据（用于推断列），再建表入库
    all_results: Dict[str, List[Dict[str, Any]]] = {}
    for job in JOBS:
        if job.request_label not in reqs:
            raise SystemExit(f"缺少请求参数块：{job.request_label}")
        base_form = reqs[job.request_label]
        rows = _fetch_all_pages(
            url=url,
            headers=headers,
            base_form=base_form,
            now_str=now_str,
            max_pages=args.max_pages,
        )
        all_results[job.table] = rows
        print(f"[{job.name}] 拉取 {len(rows)} 条")

    if args.dry_run:
        return 0

    conn = _connect_db(DB_CONFIG)
    try:
        schema = DB_CONFIG["schema"]
        _ensure_schema(conn, schema)

        for job in JOBS:
            rows = all_results.get(job.table, [])

            # 推断列类型（优先按当前数据）
            inferred: Dict[str, str] = {}
            if rows:
                # key -> list(values)
                bucket: Dict[str, List[Any]] = {}
                for r in rows:
                    for k, v in (r or {}).items():
                        bucket.setdefault(k, []).append(v)
                for k, vs in bucket.items():
                    inferred[k] = _infer_col_type_from_values(vs)
            else:
                inferred[job.pk] = "TEXT"

            col_types = _ensure_table_and_columns(
                conn=conn,
                schema=schema,
                table=job.table,
                pk=job.pk,
                table_comment=job.table,
                inferred_types=inferred,
            )

            processed, _updated = _upsert_rows(
                conn=conn,
                schema=schema,
                table=job.table,
                pk=job.pk,
                col_types=col_types,
                rows=rows,
            )
            print(f"[{job.name}] 入库处理 {processed} 条 -> {schema}.{job.table}")

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
