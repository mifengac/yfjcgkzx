from __future__ import annotations

import json
import os
import threading
import time
import uuid
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from psycopg2 import sql

from gonggong.config.database import DB_CONFIG, get_database_connection
from hqzcsj.dao.zongcha_dao import ensure_schema, ensure_table_and_columns, infer_col_types, upsert_rows


DEFAULT_URL = "http://68.29.177.247/jg/queryInfo/queryPersonInfo"

DEFAULT_HEADERS: Dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "http://68.29.177.247",
    "Referer": "http://68.29.177.247/jg/auth/page?path=/pages/cxtj/xxcx/ryxxcx&unitType=1,2,3,4",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36",
}


@dataclass(frozen=True)
class JsxxSourceDef:
    name: str
    table: str
    dwlx: str
    show_field: str
    extra_fields: Sequence[str]


KSS_FIELDS = [
    "map['RYLX']",
    "map['ZP']",
    "map['ZW']",
    "map['XM']",
    "map['XM_TYPE']",
    "map['ZJHM']",
    "map['MZ']",
    "map['BM']",
    "map['BM_TYPE']",
    "map['DABH']",
    "map['TABH']",
    "map['CSRQ_BEGIN']",
    "map['CSRQ_END']",
    "map['JSH']",
    "map['SFBHTARY']",
    "map['HJD']",
    "map['HJD_TYPE']",
    "map['JZD']",
    "map['SFBHWGR']",
    "map['XB']",
    "map['GJ']",
    "map['BADWLX']",
    "map['BAHJ']",
    "map['SXZM']",
    "map['XQ']",
    "map['XQ_BEGIN']",
    "map['XQ_END']",
    "map['JL_BEGIN']",
    "map['JL_END']",
    "map['DB_BEGIN']",
    "map['DB_END']",
    "map['SFEJG']",
    "map['RSSJ_BEGIN']",
    "map['RSSJ_END']",
    "map['RSYY']",
    "map['SSDW']",
    "map['CSSJ_BEGIN']",
    "map['CSSJ_END']",
    "map['CSYY']",
    "map['LSYY']",
    "map['SHRQ_BEGIN']",
    "map['SHRQ_END']",
    "map['JYSJ']",
]

JLS_FIELDS = [
    "map['RYLX']",
    "map['ZP']",
    "map['ZW']",
    "map['XM']",
    "map['XM_TYPE']",
    "map['ZJHM']",
    "map['MZ']",
    "map['BM']",
    "map['BM_TYPE']",
    "map['DABH']",
    "map['JSH']",
    "map['CSRQ_BEGIN']",
    "map['CSRQ_END']",
    "map['HJD']",
    "map['HJD_TYPE']",
    "map['JZD']",
    "map['XB']",
    "map['GJ']",
    "map['AJLB']",
    "map['JLJZRQ_BEGIN']",
    "map['JLJZRQ_END']",
    "map['JLQX']",
    "map['RSSJ_BEGIN']",
    "map['RSSJ_END']",
    "map['RSYY']",
    "map['SSDW']",
    "map['CSSJ_BEGIN']",
    "map['CSSJ_END']",
    "map['CSYY']",
    "map['SHRQ_BEGIN']",
    "map['SHRQ_END']",
    "map['JYSJ']",
]


SOURCE_DEFS: List[JsxxSourceDef] = [
    JsxxSourceDef(
        name="看守所入所信息",
        table="zq_jsxx_kss",
        dwlx="1",
        show_field="BADW,TABH,RYZT,JSH,DABH,SXZM,XM,GLLB,FXDJ,ZXF,ZA,JYAQ,WHCD,CYLX,WFFZJL,ZY,XZZ,GZDW,JG,ZJHM,HJDXZ,HYZK,XB,RSSJ,SSHJ,GYQX,HJD,DWDM",
        extra_fields=KSS_FIELDS,
    ),
    JsxxSourceDef(
        name="拘留所入所信息",
        table="zq_jsxx_jls",
        dwlx="2",
        show_field="RYZT,JSH,XM,XB,WFDD,WFSJ,MZ,JLQX,RSYY,JDJG,CSSJ,LXFS,WFFZJL,DWDM,JLJZRQ,AJBH,JLQSRQ,RSSJ,JYAQ,AJLB,WHCD,GZDW,ZY,XZZXZ,XZZ,HJDXZ,HJD,DABH,JG,HYZK,RYBH,ZJHM",
        extra_fields=JLS_FIELDS,
    ),
]


_STATUS_LOCK = threading.Lock()
_JOB_STATUS: Dict[Tuple[str, str], Dict[str, Any]] = {}


def start_jsxx_job(
    *,
    username: str,
    session_cookie: str,
    start_date: str,
    end_date: str,
    sources: Optional[Sequence[str]] = None,
    page_size: Optional[int] = None,
) -> str:
    real_page_size = _normalize_page_size(page_size)
    job_id = uuid.uuid4().hex
    key = (username or "", job_id)
    with _STATUS_LOCK:
        _JOB_STATUS[key] = {
            "job_id": job_id,
            "username": username,
            "created_at": time.time(),
            "state": "queued",
            "message": "",
            "start_date": start_date,
            "end_date": end_date,
            "sources": list(sources or []),
            "page_size": real_page_size,
            "progress": {"current": 0, "total": 0},
            "results": [],
        }

    t = threading.Thread(
        target=_run_job,
        kwargs={
            "username": username,
            "job_id": job_id,
            "session_cookie": session_cookie,
            "start_date": start_date,
            "end_date": end_date,
            "sources": list(sources or []),
            "page_size": real_page_size,
        },
        daemon=True,
    )
    t.start()
    return job_id


def get_jsxx_job_status(*, username: str, job_id: str) -> Optional[Dict[str, Any]]:
    key = (username or "", job_id)
    with _STATUS_LOCK:
        status = _JOB_STATUS.get(key)
        if not status:
            return None
        return dict(status)


def get_jsxx_sources() -> List[Dict[str, str]]:
    return [{"name": s.name, "table": s.table} for s in SOURCE_DEFS]


def _run_job(
    *,
    username: str,
    job_id: str,
    session_cookie: str,
    start_date: str,
    end_date: str,
    sources: Sequence[str],
    page_size: int,
) -> None:
    key = (username or "", job_id)
    try:
        _update_status(key, state="running", message="任务执行中...")
        selected = _filter_sources(sources=sources)
        _update_status(key, progress={"current": 0, "total": len(selected)})

        conn = get_database_connection()
        schema = DB_CONFIG.get("schema") or "ywdata"
        try:
            ensure_schema(conn, schema)
            results: List[Dict[str, Any]] = []
            for idx, source in enumerate(selected, start=1):
                _update_status(
                    key,
                    progress={"current": idx - 1, "total": len(selected)},
                    message=f"拉取：{source.name}",
                )

                _full_refresh_table(conn=conn, schema=schema, table=source.table)

                fetched, processed = _fetch_and_import_source(
                    conn=conn,
                    schema=schema,
                    source=source,
                    session_cookie=session_cookie,
                    start_date=start_date,
                    end_date=end_date,
                    page_size=page_size,
                )

                results.append(
                    {
                        "name": source.name,
                        "schema": schema,
                        "table": source.table,
                        "fetched": fetched,
                        "processed": processed,
                    }
                )
                _update_status(
                    key,
                    results=results,
                    progress={"current": idx, "total": len(selected)},
                    message=f"完成：{source.name}",
                )
            _update_status(key, state="success", message="全部完成", results=results)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        _update_status(key, state="failed", message=str(exc))


def _fetch_and_import_source(
    *,
    conn,
    schema: str,
    source: JsxxSourceDef,
    session_cookie: str,
    start_date: str,
    end_date: str,
    page_size: int,
) -> Tuple[int, int]:
    fetched_total = 0
    processed_total = 0
    inferred: Dict[str, str] = {}
    max_pages = _max_pages()
    total_record: Optional[int] = None

    for page in range(1, max_pages + 1):
        payload = _post_one_page(
            source=source,
            session_cookie=session_cookie,
            start_date=start_date,
            end_date=end_date,
            page_number=page,
            page_size=page_size,
        )
        rows, total = _extract_rows_total(payload)
        if total_record is None and total is not None:
            total_record = total
        if not rows:
            break

        fetched_total += len(rows)
        inferred = _merge_inferred_types(inferred, infer_col_types(rows))
        col_types = ensure_table_and_columns(
            conn=conn,
            schema=schema,
            table=source.table,
            pk_fields=["ID"],
            inferred_types={**inferred, "ID": "TEXT"},
            table_comment=source.table,
        )
        processed_total += upsert_rows(
            conn=conn,
            schema=schema,
            table=source.table,
            pk_fields=["ID"],
            rows=rows,
            col_types=col_types,
        )

        if len(rows) < page_size:
            break
        if total_record is not None and page * page_size >= total_record:
            break
    else:
        raise RuntimeError(f"{source.name} 分页超出上限 {max_pages}，请缩小时间范围")

    if not inferred:
        ensure_table_and_columns(
            conn=conn,
            schema=schema,
            table=source.table,
            pk_fields=["ID"],
            inferred_types={"ID": "TEXT"},
            table_comment=source.table,
        )
    return fetched_total, processed_total


def _post_one_page(
    *,
    source: JsxxSourceDef,
    session_cookie: str,
    start_date: str,
    end_date: str,
    page_number: int,
    page_size: int,
) -> Dict[str, Any]:
    url = _build_url_with_millis()
    headers = _build_headers(session_cookie=session_cookie)
    form = _build_form(
        source=source,
        start_date=start_date,
        end_date=end_date,
        page_number=page_number,
        page_size=page_size,
    )
    retries = _retries()
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            resp = requests.post(url, headers=headers, data=form, timeout=60)
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception:
                return json.loads(resp.text or "{}")
        except Exception as exc:
            last_err = exc
            if i + 1 < retries:
                time.sleep(0.6 * (i + 1))
    raise RuntimeError(f"{source.name} 第{page_number}页请求失败: {last_err}")


def _build_form(
    *,
    source: JsxxSourceDef,
    start_date: str,
    end_date: str,
    page_number: int,
    page_size: int,
) -> Dict[str, str]:
    form: Dict[str, str] = {k: "" for k in source.extra_fields}
    form["map['RYLX']"] = "0"
    form["map['XM_TYPE']"] = "2"
    form["map['BM_TYPE']"] = "1"
    form["map['HJD_TYPE']"] = "1"
    form["map['RSSJ_BEGIN']"] = start_date
    form["map['RSSJ_END']"] = end_date
    form["map['DWDM']"] = "4453"
    form["map['DWLX']"] = source.dwlx
    form["map['QUERY_TYPE']"] = "1"
    form["map['YWLX']"] = "人员信息查询"
    form["map['orderBy']"] = ""
    form["map['pageNumber']"] = str(page_number)
    form["map['pageSize']"] = str(page_size)
    form["map['showField']"] = source.show_field
    return form


def _build_headers(*, session_cookie: str) -> Dict[str, str]:
    cookie_value = (session_cookie or "").strip()
    if "=" not in cookie_value:
        cookie_value = f"SESSION={cookie_value}"
    headers = dict(DEFAULT_HEADERS)
    headers["Cookie"] = cookie_value
    return headers


def _build_url_with_millis() -> str:
    base_url = (os.getenv("JSXX_BASE_URL", DEFAULT_URL) or DEFAULT_URL).strip()
    millis = str(int(time.time() * 1000))
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}millis={millis}"


def _extract_rows_total(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    if not isinstance(payload, dict):
        return [], None
    rows = payload.get("record")
    if not isinstance(rows, list):
        rows = []
    clean_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean_rows.append(_normalize_row_with_id(row))
    total = _parse_int(payload.get("totalRecord"))
    return clean_rows, total


def _normalize_row_with_id(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row or {})
    rid = (out.get("ID") or "").strip() if isinstance(out.get("ID"), str) else out.get("ID")
    if rid:
        return out
    for key in ("RYBH", "DABH"):
        v = out.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out["ID"] = s
            return out
    canonical = json.dumps(out, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    out["ID"] = hashlib.md5(canonical.encode("utf-8")).hexdigest()
    return out


def _parse_int(v: Any) -> Optional[int]:
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _filter_sources(*, sources: Sequence[str]) -> List[JsxxSourceDef]:
    if not sources:
        return list(SOURCE_DEFS)
    want = {str(s).strip() for s in sources if str(s).strip()}
    if not want:
        return list(SOURCE_DEFS)
    return [s for s in SOURCE_DEFS if s.name in want]


def _full_refresh_table(*, conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE IF EXISTS {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
    conn.commit()


def _normalize_page_size(page_size: Optional[int]) -> int:
    default_size = _parse_int(os.getenv("JSXX_PAGE_SIZE_DEFAULT")) or 2000
    max_size = _parse_int(os.getenv("JSXX_PAGE_SIZE_MAX")) or 5000
    if max_size < 100:
        max_size = 5000
    v = page_size if isinstance(page_size, int) else default_size
    if v < 100:
        v = 100
    if v > max_size:
        v = max_size
    return v


def _max_pages() -> int:
    return _parse_int(os.getenv("JSXX_MAX_PAGES")) or 2000


def _retries() -> int:
    v = _parse_int(os.getenv("JSXX_RETRIES")) or 3
    if v < 1:
        v = 1
    if v > 8:
        v = 8
    return v


def _merge_inferred_types(base: Dict[str, str], inc: Dict[str, str]) -> Dict[str, str]:
    out = dict(base or {})
    for k, v in (inc or {}).items():
        if not k:
            continue
        cur = out.get(k)
        if cur == "TEXT":
            continue
        if v == "TEXT":
            out[k] = "TEXT"
            continue
        if cur is None:
            out[k] = v
    return out


def _update_status(key: Tuple[str, str], **patch: Any) -> None:
    with _STATUS_LOCK:
        cur = _JOB_STATUS.get(key)
        if not cur:
            return
        for k, v in patch.items():
            cur[k] = v
