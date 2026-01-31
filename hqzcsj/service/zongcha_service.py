from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from gonggong.config.database import DB_CONFIG, get_database_connection
from hqzcsj.dao.zongcha_dao import (
    ensure_schema,
    ensure_table_and_columns,
    ensure_table_jsonb,
    infer_col_types,
    upsert_rows,
    upsert_rows_jsonb,
)


DEFAULT_URL = "http://68.26.7.188:8088/api/search/v3/fusionQuery"

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

def _page_size() -> int:
    try:
        v = int(os.getenv("ZFBA_PAGE_SIZE", "1000") or "1000")
    except Exception:
        v = 1000
    if v < 10:
        v = 10
    if v > 5000:
        v = 5000
    return v


@dataclass(frozen=True)
class ZongchaJobDef:
    name: str
    table: str
    pk_fields: Sequence[str]
    base_form: Dict[str, str]
    time_field_codes: Sequence[str]


_STATUS_LOCK = threading.Lock()
_JOB_STATUS: Dict[Tuple[str, str], Dict[str, Any]] = {}


def start_zongcha_job(
    *, username: str, cookie: str, authorization: str, start_time: str, end_time: str, sources: Optional[Sequence[str]] = None
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
            "start_time": start_time,
            "end_time": end_time,
            "sources": list(sources or []),
            "progress": {"current": 0, "total": 0},
            "results": [],
        }

    t = threading.Thread(
        target=_run_job,
        kwargs={
            "username": username,
            "job_id": job_id,
            "cookie": cookie,
            "authorization": authorization,
            "start_time": start_time,
            "end_time": end_time,
            "sources": list(sources or []),
        },
        daemon=True,
    )
    t.start()
    return job_id


def get_zongcha_job_status(*, username: str, job_id: str) -> Optional[Dict[str, Any]]:
    key = (username or "", job_id)
    with _STATUS_LOCK:
        status = _JOB_STATUS.get(key)
        if not status:
            return None
        return dict(status)


def _run_job(
    *, username: str, job_id: str, cookie: str, authorization: str, start_time: str, end_time: str, sources: Sequence[str]
) -> None:
    key = (username or "", job_id)
    try:
        _update_status(key, state="running", message="任务执行中...")

        url, base_headers, base_forms = _load_base_request_forms()
        headers = _build_headers(base_headers, cookie=cookie, authorization=authorization)
        org_codes = _org_codes()

        job_defs = _build_job_defs(
            base_forms=base_forms,
            start_time=start_time,
            end_time=end_time,
            org_codes=org_codes,
        )
        selected = _filter_jobs(job_defs, sources=sources)
        _update_status(key, progress={"current": 0, "total": len(selected)})

        conn = get_database_connection()
        schema = DB_CONFIG.get("schema") or "ywdata"
        try:
            ensure_schema(conn, schema)
            results: List[Dict[str, Any]] = []
            for idx, job in enumerate(selected, start=1):
                _update_status(key, progress={"current": idx - 1, "total": len(selected)}, message=f"拉取：{job.name}")
                fetched_total = 0
                processed_total = 0
                inferred: Dict[str, str] = {}

                windows = _split_time_windows_if_needed(
                    job=job,
                    start_time=start_time,
                    end_time=end_time,
                    url=url,
                    headers=headers,
                    base_form=job.base_form,
                )
                fast_mode = _fast_mode_for_table(job.table)
                if fast_mode:
                    ensure_table_jsonb(
                        conn=conn,
                        schema=schema,
                        table=job.table,
                        pk_fields=job.pk_fields,
                        table_comment=job.table,
                    )
                for w_start, w_end in windows:
                    window_rows = _fetch_all_pages_for_window(
                        url=url,
                        headers=headers,
                        base_form=job.base_form,
                        time_field_codes=job.time_field_codes,
                        window_start=w_start,
                        window_end=w_end,
                        max_pages=2000,
                    )
                    if not window_rows:
                        continue

                    fetched_total += len(window_rows)
                    if job.table in {"zq_zfba_wcnr_xyr", "zq_zfba_xyrxx"}:
                        window_rows = _expand_rows_by_ajxx_ajbhs(window_rows)
                    if fast_mode:
                        processed_total += upsert_rows_jsonb(
                            conn=conn,
                            schema=schema,
                            table=job.table,
                            pk_fields=job.pk_fields,
                            rows=window_rows,
                        )
                    else:
                        inferred = _merge_inferred_types(inferred, infer_col_types(window_rows))
                        col_types = ensure_table_and_columns(
                            conn=conn,
                            schema=schema,
                            table=job.table,
                            pk_fields=job.pk_fields,
                            inferred_types=inferred,
                            table_comment=job.table,
                        )
                        processed_total += upsert_rows(
                            conn=conn,
                            schema=schema,
                            table=job.table,
                            pk_fields=job.pk_fields,
                            rows=window_rows,
                            col_types=col_types,
                        )

                # 若整个时间段无数据，也确保表存在（仅 PK 列）
                if not fast_mode and not inferred:
                    ensure_table_and_columns(
                        conn=conn,
                        schema=schema,
                        table=job.table,
                        pk_fields=job.pk_fields,
                        inferred_types={pk: "TEXT" for pk in job.pk_fields},
                        table_comment=job.table,
                    )
                results.append(
                    {
                        "name": job.name,
                        "schema": schema,
                        "table": job.table,
                        "fetched": fetched_total,
                        "processed": processed_total,
                    }
                )
                _update_status(key, results=results, progress={"current": idx, "total": len(selected)}, message=f"完成：{job.name}")

            _update_status(key, state="success", message="全部完成", results=results)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    except Exception as exc:
        _update_status(key, state="failed", message=str(exc))


def _update_status(key: Tuple[str, str], **patch: Any) -> None:
    with _STATUS_LOCK:
        cur = _JOB_STATUS.get(key)
        if not cur:
            return
        for k, v in patch.items():
            cur[k] = v


def _org_codes() -> List[str]:
    raw = os.getenv("ZFBA_ORG_CODES", "").strip()
    if not raw:
        raw = os.getenv("ZFBA_ORG_CODE", "").strip()
    if not raw:
        return ["445300000000"]
    parts = [p.strip() for p in raw.replace("，", ",").split(",") if p.strip()]
    return parts or ["445300000000"]

def _fast_mode_for_table(table: str) -> bool:
    """
    快速入库模式：
    - ZFBA_FAST_MODE=1：全表启用（适合大数据量表）
    - ZFBA_FAST_TABLES=表1,表2：仅对指定表启用
    """
    if (os.getenv("ZFBA_FAST_MODE", "").strip() or "").lower() in ("1", "true", "yes", "y"):
        return True
    raw = os.getenv("ZFBA_FAST_TABLES", "").strip()
    if not raw:
        return False
    want = {p.strip() for p in raw.replace("，", ",").split(",") if p.strip()}
    return (table or "") in want


def _build_headers(base_headers: Dict[str, str], *, cookie: str, authorization: str) -> Dict[str, str]:
    headers = dict(base_headers or {})
    headers["Cookie"] = cookie
    headers["Authorization"] = authorization
    return headers


def _load_base_request_forms() -> Tuple[str, Dict[str, str], Dict[str, Dict[str, str]]]:
    """
    复用 hqzcsj/0123_fetch_data.py 中的请求参数（3 个接口）。
    文件名以数字开头不可直接 import；这里用 importlib 动态加载。
    """
    import importlib.util
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    py_path = root / "hqzcsj" / "0123_fetch_data.py"

    spec = importlib.util.spec_from_file_location("hqzcsj_0123_fetch_data", str(py_path))
    if not spec or not spec.loader:
        raise RuntimeError("无法加载 0123_fetch_data.py")
    mod = importlib.util.module_from_spec(spec)
    # dataclasses/typing 在解析注解时会通过 sys.modules 找到模块命名空间；
    # 这里手动注册，避免出现 "'NoneType' object has no attribute '__dict__'"。
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[call-arg]

    url = getattr(mod, "DEFAULT_URL", DEFAULT_URL)  # noqa: B009
    base_headers = getattr(mod, "DEFAULT_BASE_HEADERS", DEFAULT_BASE_HEADERS)  # noqa: B009
    base_forms = getattr(mod, "DEFAULT_REQUEST_FORMS", {})  # noqa: B009
    return url, base_headers, base_forms


def _set_time_range_in_form_json(form_json_str: str, *, field_codes: Sequence[str], start_time: str, end_time: str) -> str:
    obj = json.loads(form_json_str)
    want = set(field_codes or [])
    for p in obj.get("paramArray", []) or []:
        for cond in p.get("conditions", []) or []:
            if cond.get("fieldCode") in want and cond.get("operateSign") == "10":
                cond["values"] = [start_time, end_time]
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _parse_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _fetch_all_pages_for_window(
    *,
    url: str,
    headers: Dict[str, str],
    base_form: Dict[str, str],
    time_field_codes: Sequence[str],
    window_start: datetime,
    window_end: datetime,
    max_pages: int,
) -> List[Dict[str, Any]]:
    form = dict(base_form)
    form_json = form.get("json") or ""
    if form_json and time_field_codes:
        form["json"] = _set_time_range_in_form_json(
            form_json,
            field_codes=time_field_codes,
            start_time=_fmt_dt(window_start),
            end_time=_fmt_dt(window_end),
        )
    rows, _capped = _fetch_all_pages(url=url, headers=headers, base_form=form, max_pages=max_pages, stop_after_rows=None)
    return rows


def _split_time_windows_if_needed(
    *,
    job: ZongchaJobDef,
    start_time: str,
    end_time: str,
    url: str,
    headers: Dict[str, str],
    base_form: Dict[str, str],
) -> List[Tuple[datetime, datetime]]:
    """
    规避接口“最多返回 5000 条”的限制：当一次查询疑似命中 5000 条上限时，自动二分时间范围继续拉取。
    """
    start_dt = _parse_dt(start_time)
    end_dt = _parse_dt(end_time)
    if end_dt <= start_dt:
        return [(start_dt, end_dt)]

    if not job.time_field_codes:
        return [(start_dt, end_dt)]

    cap = int(os.getenv("ZFBA_RESULT_CAP", "5000") or "5000")
    page_size = int(base_form.get("pageSize") or "100")
    cap_pages = max(1, cap // max(1, page_size))
    min_seconds = int(os.getenv("ZFBA_SPLIT_MIN_SECONDS", "600") or "600")
    min_window = timedelta(seconds=max(1, min_seconds))

    def will_cap(window_start: datetime, window_end: datetime) -> bool:
        form = dict(base_form)
        form_json = form.get("json") or ""
        if form_json:
            form["json"] = _set_time_range_in_form_json(
                form_json,
                field_codes=job.time_field_codes,
                start_time=_fmt_dt(window_start),
                end_time=_fmt_dt(window_end),
            )
        # 探测到 cap_pages+1，避免单次探测太慢
        rows, capped = _fetch_all_pages(
            url=url,
            headers=headers,
            base_form=form,
            max_pages=cap_pages + 1,
            stop_after_rows=cap,
        )
        return capped or (len(rows) >= cap)

    def split(window_start: datetime, window_end: datetime) -> List[Tuple[datetime, datetime]]:
        if window_end - window_start <= min_window:
            return [(window_start, window_end)]
        if not will_cap(window_start, window_end):
            return [(window_start, window_end)]

        mid = window_start + (window_end - window_start) / 2
        mid = mid.replace(microsecond=0)
        if mid <= window_start:
            mid = window_start + timedelta(seconds=1)
        if mid >= window_end:
            return [(window_start, window_end)]

        # 第二段从 mid+1s 开始，避免时间范围重叠导致同一主键重复入库
        second_start = mid + timedelta(seconds=1)
        if second_start >= window_end:
            return [(window_start, window_end)]
        return split(window_start, mid) + split(second_start, window_end)

    return split(start_dt, end_dt)


def _set_org_codes_in_form_json(form_json_str: str, *, field_codes: Sequence[str], org_codes: Sequence[str]) -> str:
    obj = json.loads(form_json_str)
    want = set(field_codes or [])
    values = list(org_codes or [])
    for p in obj.get("paramArray", []) or []:
        for cond in p.get("conditions", []) or []:
            if cond.get("fieldCode") in want and cond.get("operateSign") == "7":
                cond["values"] = values
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _build_job_defs(*, base_forms: Dict[str, Dict[str, str]], start_time: str, end_time: str, org_codes: Sequence[str]) -> List[ZongchaJobDef]:
    jobs: List[ZongchaJobDef] = []

    # 0123_fetch_data.py 的 3 类数据（保持原表名与主键）
    script_jobs = [
        ("训诫书", "zq_zfba_xjs", ["xjs_id"], ["xjs_tfsj"], ["xjs_cbdw_bh"]),
        ("加强监督教育/责令接受家庭教育指导通知书", "zq_zfba_jtjyzdtzs", ["jqjhjyzljsjtjyzdtzs_id"], ["jqjhjyzljsjtjyzdtzs_tfsj"], ["jqjhjyzljsjtjyzdtzs_cbdw_bh"]),
        ("责令未成年人遵守特定行为规范通知书", "zq_zfba_zlwcnrzstdxwgftzs", ["zltzs_id"], ["zltzs_tfsj"], ["zltzs_cbdw_bh"]),
    ]
    for label, table, pk_fields, time_field_codes, org_field_codes in script_jobs:
        base_form = dict(base_forms.get(label) or {})
        if not base_form:
            raise RuntimeError(f"缺少请求参数块：{label}")
        base_form["pageSize"] = str(_page_size())
        base_form["pageNumber"] = "1"
        base_form["json"] = _set_time_range_in_form_json(
            base_form["json"], field_codes=time_field_codes, start_time=start_time, end_time=end_time
        )
        base_form["json"] = _set_org_codes_in_form_json(
            base_form["json"], field_codes=org_field_codes, org_codes=org_codes
        )
        jobs.append(
            ZongchaJobDef(
                name=label,
                table=table,
                pk_fields=pk_fields,
                base_form=base_form,
                time_field_codes=time_field_codes,
            )
        )

    # 新增 8 类综查数据
    jobs.extend(
        [
            ZongchaJobDef(
                name="未成年人(嫌疑人)",
                table="zq_zfba_wcnr_xyr",
                pk_fields=["ajxx_ajbhs", "xyrxx_sfzh"],
                base_form=_make_form_wcnr_xyr(start_time=start_time, end_time=end_time),
                time_field_codes=["xyrxx_lrsj"],
            ),
            ZongchaJobDef(
                name="未成年人(受害人)案件",
                table="zq_zfba_wcnr_shr_ajxx",
                pk_fields=["ajxx_ajbh"],
                base_form=_make_form_wcnr_shr_ajxx(start_time=start_time, end_time=end_time),
                time_field_codes=["ajxx_lasj"],
            ),
            ZongchaJobDef(
                name="未成年人案件",
                table="zq_zfba_wcnr_ajxx",
                pk_fields=["ajxx_ajbh"],
                base_form=_make_form_wcnr_ajxx(start_time=start_time, end_time=end_time),
                time_field_codes=["ajxx_lasj"],
            ),
            ZongchaJobDef(
                name="案件信息",
                table="zq_zfba_ajxx",
                pk_fields=["ajxx_ajbh"],
                base_form=_make_form_ajxx(start_time=start_time, end_time=end_time, org_codes=org_codes),
                time_field_codes=["ajxx_lasj"],
            ),
            ZongchaJobDef(
                name="行政处罚决定书",
                table="zq_zfba_xzcfjds",
                pk_fields=["xzcfjds_id"],
                base_form=_make_form_xzcfjds(start_time=start_time, end_time=end_time, org_codes=org_codes),
                time_field_codes=["xzcfjds_spsj"],
            ),
            ZongchaJobDef(
                name="拘留证",
                table="zq_zfba_jlz",
                pk_fields=["jlz_id"],
                base_form=_make_form_jlz(start_time=start_time, end_time=end_time, org_codes=org_codes),
                time_field_codes=["jlz_pzsj"],
            ),
            ZongchaJobDef(
                name="逮捕证",
                table="zq_zfba_dbz",
                pk_fields=["dbz_id"],
                base_form=_make_form_dbz(start_time=start_time, end_time=end_time, org_codes=org_codes),
                time_field_codes=["dbz_pzdbsj"],
            ),
            ZongchaJobDef(
                name="移送案件通知书",
                table="zq_zfba_ysajtzs",
                pk_fields=["ysajtzs_id"],
                base_form=_make_form_ysajtzs(start_time=start_time, end_time=end_time, org_codes=org_codes),
                time_field_codes=["ysajtzs_pzsj"],
            ),
            ZongchaJobDef(
                name="起诉人员信息",
                table="zq_zfba_qsryxx",
                pk_fields=["qsryxx_id"],
                base_form=_make_form_qsryxx(start_time=start_time, end_time=end_time, org_codes=org_codes),
                time_field_codes=["qsryxx_tfsj"],
            ),
            ZongchaJobDef(
                name="嫌疑人信息",
                table="zq_zfba_xyrxx",
                pk_fields=["ajxx_ajbhs", "xyrxx_sfzh"],
                base_form=_make_form_xyrxx(start_time=start_time, end_time=end_time),
                time_field_codes=["xyrxx_lrsj"],
            ),
        ]
    )

    return jobs


def _filter_jobs(job_defs: Sequence[ZongchaJobDef], *, sources: Sequence[str]) -> List[ZongchaJobDef]:
    if not sources:
        return list(job_defs)
    want = set([str(s).strip() for s in sources if str(s).strip()])
    if not want:
        return list(job_defs)
    return [j for j in job_defs if j.name in want]


def _merge_inferred_types(base: Dict[str, str], inc: Dict[str, str]) -> Dict[str, str]:
    """
    合并列类型推断：
    - TEXT 优先级最高（一旦某列出现非时间值，应固定为 TEXT）
    - 其次 TIMESTAMP
    """
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


def _expand_rows_by_ajxx_ajbhs(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    若 ajxx_ajbhs 含空格（多个案件编号），按空格拆分为多条记录，其它字段不变。

    例：{"ajxx_ajbhs": "A1 A2", ...} -> [{"ajxx_ajbhs": "A1", ...}, {"ajxx_ajbhs": "A2", ...}]
    """
    expanded: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        raw = row.get("ajxx_ajbhs")
        if not isinstance(raw, str):
            expanded.append(row)
            continue
        cleaned = raw.strip()
        parts = [p for p in cleaned.split() if p]
        if len(parts) <= 1:
            if cleaned != raw:
                new_row = dict(row)
                new_row["ajxx_ajbhs"] = cleaned
                expanded.append(new_row)
            else:
                expanded.append(row)
            continue
        for part in parts:
            new_row = dict(row)
            new_row["ajxx_ajbhs"] = part
            expanded.append(new_row)
    return expanded


def _fetch_all_pages(
    *, url: str, headers: Dict[str, str], base_form: Dict[str, str], max_pages: int, stop_after_rows: Optional[int]
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    返回 (rows, capped)。
    capped=True 表示疑似命中接口返回上限（常见为最多 5000 条）。
    """
    all_rows: List[Dict[str, Any]] = []
    page_size = int(base_form.get("pageSize") or "100")
    last_nonempty_len = 0
    capped = False
    cap = int(os.getenv("ZFBA_RESULT_CAP", "5000") or "5000")
    for page in range(1, max_pages + 1):
        form = dict(base_form)
        form["pageNumber"] = str(page)

        resp = requests.post(url, headers=headers, data=form, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        rows = _extract_result_rows(payload)
        if not rows:
            break
        last_nonempty_len = len(rows)
        all_rows.extend(rows)
        if stop_after_rows is not None and stop_after_rows > 0 and len(all_rows) >= stop_after_rows:
            # 探测模式：达到上限阈值后可提前停止
            capped = True
            break
    # 若刚好凑齐 cap 且最后一页是满页，基本可以判定存在上限/截断风险
    if cap > 0 and len(all_rows) >= cap and page_size > 0 and last_nonempty_len == page_size:
        capped = True
    return all_rows, capped


def _extract_result_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    ctx = (payload or {}).get("context") or {}
    res = ctx.get("result") or {}
    inner = res.get("result") or []
    if isinstance(inner, list):
        return [r for r in inner if isinstance(r, dict)]
    return []


def _make_form_base(*, json_obj: Dict[str, Any], domain_id: str, result_tab_id: str, result_tab_code: str, result_table_name: str, tab_id: str, tab_code: str) -> Dict[str, str]:
    return {
        "json": json.dumps(json_obj, ensure_ascii=False, separators=(",", ":")),
        "domainId": domain_id,
        "resultTabId": result_tab_id,
        "resultTabCode": result_tab_code,
        "resultTableName": result_table_name,
        "tabId": tab_id,
        "pageSize": str(_page_size()),
        "pageNumber": "1",
        "sortColumns": "",
    }


def _make_form_wcnr_xyr(*, start_time: str, end_time: str) -> Dict[str, str]:
    conds = [
        {
            "tabId": "1764455104469049399",
            "tabCode": "ragl",
            "fieldCode": "ragl_fasnl",
            "tabType": "2",
            "isPub": False,
            "operateSign": "10",
            "values": [1, 18],
            "rangeIncludeType": "2",
        },
        {
            "tabId": "22",
            "tabCode": "xyr",
            "fieldCode": "xyrxx_nl",
            "tabType": "1",
            "isPub": False,
            "operateSign": "10",
            "values": [1, 18],
            "rangeIncludeType": "2",
        },
        {
            "tabId": "22",
            "tabCode": "xyr",
            "fieldCode": "xyrxx_ryzt",
            "tabType": "1",
            "isPub": False,
            "operateSign": "7",
            "values": ["01", "04"],
            "isIncludeChilds": False,
            "dicCode": "ZD_CASE_RYZT_BH",
        },
        {
            "tabId": "22",
            "tabCode": "xyr",
            "fieldCode": "xyrxx_lrsj",
            "tabType": "1",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": "22", "tabCode": "xyr", "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id="22",
        result_tab_code="xyr",
        result_table_name="嫌疑人信息",
        tab_id="22",
        tab_code="xyr",
    )


def _make_form_wcnr_ajxx(*, start_time: str, end_time: str) -> Dict[str, str]:
    conds = [
        {
            "tabId": "16",
            "tabCode": "ajxx_join",
            "fieldCode": "ajxx_lasj",
            "tabType": "1",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
        {
            "tabId": "1764455104469049399",
            "tabCode": "ragl",
            "fieldCode": "ragl_fasnl",
            "tabType": "2",
            "isPub": False,
            "operateSign": "10",
            "values": [1, 18],
            "rangeIncludeType": "2",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": "16", "tabCode": "ajxx_join", "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id="16",
        result_tab_code="ajxx_join",
        result_table_name="案件信息",
        tab_id="16",
        tab_code="ajxx_join",
    )


def _make_form_wcnr_shr_ajxx(*, start_time: str, end_time: str) -> Dict[str, str]:
    conds = [
        {
            "tabId": "16",
            "tabCode": "ajxx_join",
            "fieldCode": "ajxx_lasj",
            "tabType": "1",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
        {
            "tabId": "51",
            "tabCode": "saryxx",
            "fieldCode": "saryxx_rylx",
            "tabType": "1",
            "isPub": False,
            "operateSign": "7",
            "values": ["0502"],
            "isIncludeChilds": False,
            "dicCode": "ZD_CASE_RYLX",
        },
        {
            "tabId": "51",
            "tabCode": "saryxx",
            "fieldCode": "saryxx_nl",
            "tabType": "1",
            "isPub": False,
            "operateSign": "10",
            "values": [1, 17],
            "rangeIncludeType": "2",
        },
        {
            "tabId": "16",
            "tabCode": "ajxx_join",
            "fieldCode": "ajxx_isdel",
            "tabType": "1",
            "isPub": False,
            "operateSign": "7",
            "values": ["0"],
            "isIncludeChilds": False,
            "dicCode": "00",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": "16", "tabCode": "ajxx_join", "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id="16",
        result_tab_code="ajxx_join",
        result_table_name="案件信息",
        tab_id="16",
        tab_code="ajxx_join",
    )


def _make_form_ajxx(*, start_time: str, end_time: str, org_codes: Sequence[str]) -> Dict[str, str]:
    conds = [
        {
            "tabId": "16",
            "tabCode": "ajxx_join",
            "fieldCode": "ajxx_sldw_bh",
            "tabType": "1",
            "isPub": False,
            "operateSign": "7",
            "values": list(org_codes or []),
            "isIncludeChilds": True,
            "dicCode": "06",
        },
        {
            "tabId": "16",
            "tabCode": "ajxx_join",
            "fieldCode": "ajxx_isdel",
            "tabType": "1",
            "isPub": False,
            "operateSign": "7",
            "values": ["0"],
            "isIncludeChilds": False,
            "dicCode": "00",
        },
        {
            "tabId": "16",
            "tabCode": "ajxx_join",
            "fieldCode": "ajxx_lasj",
            "tabType": "1",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": "16", "tabCode": "ajxx_join", "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id="16",
        result_tab_code="ajxx_join",
        result_table_name="案件信息",
        tab_id="16",
        tab_code="ajxx_join",
    )


def _make_form_xzcfjds(*, start_time: str, end_time: str, org_codes: Sequence[str]) -> Dict[str, str]:
    conds = [
        {
            "tabId": "52",
            "tabCode": "xzcfjds",
            "fieldCode": "xzcfjds_cbdw_bh",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": list(org_codes or []),
            "isIncludeChilds": True,
            "dicCode": "06",
        },
        {
            "tabId": "52",
            "tabCode": "xzcfjds",
            "fieldCode": "xzcfjds_wszt",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["03"],
            "isIncludeChilds": False,
            "dicCode": "ZD_CASE_WSZT",
        },
        {
            "tabId": "52",
            "tabCode": "xzcfjds",
            "fieldCode": "xzcfjds_isdel",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["0"],
            "isIncludeChilds": False,
            "dicCode": "00",
        },
        {
            "tabId": "52",
            "tabCode": "xzcfjds",
            "fieldCode": "xzcfjds_spsj",
            "tabType": "2",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": "52", "tabCode": "xzcfjds", "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id="52",
        result_tab_code="xzcfjds",
        result_table_name="行政-行政处罚决定书",
        tab_id="52",
        tab_code="xzcfjds",
    )


def _make_form_jlz(*, start_time: str, end_time: str, org_codes: Sequence[str]) -> Dict[str, str]:
    conds = [
        {
            "tabId": "32",
            "tabCode": "jlz",
            "fieldCode": "jlz_cbdw_bh",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": list(org_codes or []),
            "isIncludeChilds": True,
            "dicCode": "06",
        },
        {
            "tabId": "32",
            "tabCode": "jlz",
            "fieldCode": "jlz_wszt",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["03"],
            "isIncludeChilds": False,
            "dicCode": "ZD_CASE_WSZT",
        },
        {
            "tabId": "32",
            "tabCode": "jlz",
            "fieldCode": "jlz_isdel",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["0"],
            "isIncludeChilds": False,
            "dicCode": "00",
        },
        {
            "tabId": "32",
            "tabCode": "jlz",
            "fieldCode": "jlz_pzsj",
            "tabType": "2",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": "32", "tabCode": "jlz", "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id="32",
        result_tab_code="jlz",
        result_table_name="刑事-拘留证信息",
        tab_id="32",
        tab_code="jlz",
    )


def _make_form_dbz(*, start_time: str, end_time: str, org_codes: Sequence[str]) -> Dict[str, str]:
    conds = [
        {
            "tabId": "37",
            "tabCode": "dbz",
            "fieldCode": "dbz_pzdbsj",
            "tabType": "2",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
        {
            "tabId": "37",
            "tabCode": "dbz",
            "fieldCode": "dbz_cbdw_bh",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": list(org_codes or []),
            "isIncludeChilds": True,
            "dicCode": "06",
        },
        {
            "tabId": "37",
            "tabCode": "dbz",
            "fieldCode": "dbz_isdel",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["0"],
            "isIncludeChilds": False,
            "dicCode": "00",
        },
        {
            "tabId": "37",
            "tabCode": "dbz",
            "fieldCode": "dbz_wszt",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["03"],
            "isIncludeChilds": False,
            "dicCode": "ZD_CASE_WSZT",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": "37", "tabCode": "dbz", "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id="37",
        result_tab_code="dbz",
        result_table_name="刑事-逮捕证信息",
        tab_id="37",
        tab_code="dbz",
    )


def _make_form_ysajtzs(*, start_time: str, end_time: str, org_codes: Sequence[str]) -> Dict[str, str]:
    tab_id = "1306056082585960501"
    tab_code = "ysajtzs"
    conds = [
        {
            "tabId": tab_id,
            "tabCode": tab_code,
            "fieldCode": "ysajtzs_isdel",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["0"],
            "isIncludeChilds": False,
            "dicCode": "00",
        },
        {
            "tabId": tab_id,
            "tabCode": tab_code,
            "fieldCode": "ysajtzs_cbdw_bh",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": list(org_codes or []),
            "isIncludeChilds": True,
            "dicCode": "06",
        },
        {
            "tabId": tab_id,
            "tabCode": tab_code,
            "fieldCode": "ysajtzs_pzsj",
            "tabType": "2",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
        {
            "tabId": tab_id,
            "tabCode": tab_code,
            "fieldCode": "ysajtzs_wszt",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["03"],
            "isIncludeChilds": False,
            "dicCode": "ZD_CASE_WSZT",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": tab_id, "tabCode": tab_code, "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id=tab_id,
        result_tab_code=tab_code,
        result_table_name="刑事-移送案件通知书",
        tab_id=tab_id,
        tab_code=tab_code,
    )


def _make_form_qsryxx(*, start_time: str, end_time: str, org_codes: Sequence[str]) -> Dict[str, str]:
    tab_id = "1523543873106780162"
    tab_code = "qsryxx"
    conds = [
        {
            "tabId": tab_id,
            "tabCode": tab_code,
            "fieldCode": "qsryxx_cbdw_bh",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": list(org_codes or []),
            "isIncludeChilds": True,
            "dicCode": "06",
        },
        {
            "tabId": tab_id,
            "tabCode": tab_code,
            "fieldCode": "qsryxx_wszt",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["03"],
            "isIncludeChilds": False,
            "dicCode": "ZD_CASE_WSZT",
        },
        {
            "tabId": tab_id,
            "tabCode": tab_code,
            "fieldCode": "qsryxx_isdel",
            "tabType": "2",
            "isPub": False,
            "operateSign": "7",
            "values": ["0"],
            "isIncludeChilds": False,
            "dicCode": "00",
        },
        {
            "tabId": tab_id,
            "tabCode": tab_code,
            "fieldCode": "qsryxx_tfsj",
            "tabType": "2",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": tab_id, "tabCode": tab_code, "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id=tab_id,
        result_tab_code=tab_code,
        result_table_name="起诉人员信息",
        tab_id=tab_id,
        tab_code=tab_code,
    )


def _make_form_xyrxx(*, start_time: str, end_time: str) -> Dict[str, str]:
    """
    嫌疑人信息：过滤 isdel=0，并按录入时间范围筛选。
    主键：ajxx_ajbhs + xyrxx_sfzh
    """
    conds = [
        {
            "tabId": "22",
            "tabCode": "xyr",
            "fieldCode": "xyrxx_lrsj",
            "tabType": "1",
            "isPub": False,
            "operateSign": "10",
            "values": [start_time, end_time],
            "excludeDays": [],
            "rangeIncludeType": "0",
        },
        {
            "tabId": "22",
            "tabCode": "xyr",
            "fieldCode": "xyrxx_isdel",
            "tabType": "1",
            "isPub": False,
            "operateSign": "7",
            "values": ["0"],
            "isIncludeChilds": False,
            "dicCode": "00",
        },
    ]
    json_obj = {"paramArray": [{"conditions": conds, "tabId": "22", "tabCode": "xyr", "domainId": "11"}]}
    return _make_form_base(
        json_obj=json_obj,
        domain_id="11",
        result_tab_id="22",
        result_tab_code="xyr",
        result_table_name="嫌疑人信息",
        tab_id="22",
        tab_code="xyr",
    )
