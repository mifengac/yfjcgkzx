from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests

from gonggong.config.database import DB_CONFIG, get_database_connection
from hqzcsj.dao.zongcha_dao import ensure_schema, ensure_table_and_columns, infer_col_types, upsert_rows


DEFAULT_URL = "http://68.26.7.47:1999/com/api/v1/com/model/getQueryPageData"

DEFAULT_HEADERS: Dict[str, str] = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "http://68.26.7.148:999",
    "Referer": "http://68.26.7.148:999/com/datagrid/yshdws",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36",
}


_STATUS_LOCK = threading.Lock()
_JOB_STATUS: Dict[Tuple[str, str], Dict[str, Any]] = {}


def start_tqws_job(
    *,
    username: str,
    access_token: str,
    url: str,
    params: Dict[str, Any],
) -> str:
    job_id = uuid.uuid4().hex
    key = (username or "", job_id)
    with _STATUS_LOCK:
        _JOB_STATUS[key] = {
            "job_id": job_id,
            "username": username,
            "created_at": time.time(),
            "state": "queued",  # queued/running/success/failed
            "message": "",
            "progress": {"current": 0, "total": 0},
            "results": [],
        }

    t = threading.Thread(
        target=_run_job,
        kwargs={
            "username": username,
            "job_id": job_id,
            "access_token": access_token,
            "url": url,
            "params": dict(params or {}),
        },
        daemon=True,
    )
    t.start()
    return job_id


def get_tqws_job_status(*, username: str, job_id: str) -> Optional[Dict[str, Any]]:
    key = (username or "", job_id)
    with _STATUS_LOCK:
        status = _JOB_STATUS.get(key)
        if not status:
            return None
        return dict(status)


def _update_status(key: Tuple[str, str], **patch: Any) -> None:
    with _STATUS_LOCK:
        cur = _JOB_STATUS.get(key)
        if not cur:
            return
        for k, v in patch.items():
            cur[k] = v


def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = (payload or {}).get("Rows")
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    return []


def _extract_total(payload: Dict[str, Any]) -> int:
    total = (payload or {}).get("Total")
    try:
        return int(total or 0)
    except Exception:
        return 0


def _post_page(*, url: str, headers: Dict[str, str], form: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    resp = requests.post(url, headers=headers, data=form, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError("接口响应不是 JSON 对象")
    if payload.get("success") is False:
        raise RuntimeError(payload.get("msg") or payload.get("message") or "接口返回失败")
    return payload


def _run_job(*, username: str, job_id: str, access_token: str, url: str, params: Dict[str, Any]) -> None:
    key = (username or "", job_id)
    try:
        _update_status(key, state="running", message="任务执行中...")

        token = (access_token or "").strip()
        if not token:
            raise RuntimeError("access_token 不能为空")
        use_url = (url or "").strip() or DEFAULT_URL

        headers = dict(DEFAULT_HEADERS)
        headers["Authorization"] = f"Bearer {token}"

        # 固定：不允许前端传入 Authorization；统一以 access_token 生成
        base_form: Dict[str, Any] = dict(params or {})
        base_form["access_token"] = token

        try:
            page_size = int(base_form.get("pagesize") or 0)
        except Exception:
            page_size = 0
        if page_size <= 0:
            page_size = 1000
        base_form["pagesize"] = str(page_size)

        schema = DB_CONFIG.get("schema") or "ywdata"
        # 固定写入表（按需求）：zq_zfba_tqzmjy
        table = "zq_zfba_tqzmjy"
        pk_name = str(base_form.get("pkName") or "ID").strip() or "ID"

        conn = get_database_connection()
        try:
            ensure_schema(conn, schema)

            fetched_total = 0
            processed_total = 0
            inferred: Dict[str, str] = {}
            col_types: Optional[Dict[str, str]] = None

            # 第 1 页探测 Total
            cur_page = int(base_form.get("page") or 1)
            if cur_page < 1:
                cur_page = 1
            max_pages = int(base_form.get("max_pages") or 5000)
            if max_pages < 1:
                max_pages = 1

            first_form = dict(base_form)
            first_form["page"] = str(cur_page)
            first_payload = _post_page(url=use_url, headers=headers, form=first_form)
            total = _extract_total(first_payload)
            first_rows = _extract_rows(first_payload)
            # total 不可靠时用“未知总数”模式：progress.total=0
            _update_status(key, progress={"current": 0, "total": total or 0}, message="拉取：第 1 页")

            def ingest(rows: List[Dict[str, Any]]) -> None:
                nonlocal fetched_total, processed_total, inferred, col_types
                if not rows:
                    return
                fetched_total += len(rows)
                inferred = {**inferred, **infer_col_types(rows)}
                col_types = ensure_table_and_columns(
                    conn=conn,
                    schema=schema,
                    table=table,
                    pk_fields=[pk_name],
                    inferred_types=inferred,
                    table_comment=table,
                )
                processed_total += upsert_rows(
                    conn=conn,
                    schema=schema,
                    table=table,
                    pk_fields=[pk_name],
                    rows=rows,
                    col_types=col_types,
                )

            ingest(first_rows)

            # 后续页
            page_idx = 1
            while True:
                if total and fetched_total >= total:
                    break
                page_idx += 1
                if page_idx > max_pages:
                    raise RuntimeError(f"超过最大页数限制：max_pages={max_pages}")
                next_page = cur_page + (page_idx - 1)
                form = dict(base_form)
                form["page"] = str(next_page)
                _update_status(
                    key,
                    progress={"current": min(fetched_total, total) if total else fetched_total, "total": total or 0},
                    message=f"拉取：第 {next_page} 页",
                )
                payload = _post_page(url=use_url, headers=headers, form=form)
                rows = _extract_rows(payload)
                if not rows:
                    break
                ingest(rows)

            # 若全程无数据，也确保表存在（仅 PK 列）
            if not inferred:
                ensure_table_and_columns(
                    conn=conn,
                    schema=schema,
                    table=table,
                    pk_fields=[pk_name],
                    inferred_types={pk_name: "TEXT"},
                    table_comment=table,
                )

            results = [
                {
                    "name": "提请文书",
                    "schema": schema,
                    "table": table,
                    "fetched": fetched_total,
                    "processed": processed_total,
                }
            ]
            _update_status(key, state="success", message="全部完成", results=results, progress={"current": total or fetched_total, "total": total or 0})
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        _update_status(key, state="failed", message=str(exc))
