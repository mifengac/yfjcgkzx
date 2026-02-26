from __future__ import annotations

import json
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
    sources: List[str],
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
            "sources": list(sources or []),
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


def _resolve_source(source: str) -> Tuple[str, str]:
    """
    返回 (source_name, table)。
    """
    s = (source or "").strip()
    key = _normalize_source_key(s)
    cfg = TQWS_SOURCE_REGISTRY.get(key)
    if not cfg:
        raise RuntimeError(f"未知数据源: {s}")
    return str(cfg["name"]), str(cfg["table"])


def _normalize_source_key(source: str) -> str:
    s = (source or "").strip()
    mapping = {
        # 提请文书
        "tqws": "tqws",
        "提请文书": "tqws",
        "提请": "tqws",
        # 训诫书（未成年人）
        "xjs2": "xjs2",
        "训诫书（未成年人）": "xjs2",
        "训诫书(未成年人)": "xjs2",
        "训诫书2": "xjs2",
        "训诫书": "xjs2",
        # 加强监督教育/责令接受家庭教育指导通知书（新）
        "jtjyzdtzs2": "jtjyzdtzs2",
        "加强监督教育/责令接受家庭教育指导通知书(新)": "jtjyzdtzs2",
        "加强监督教育/责令接受家庭教育指导通知书（新）": "jtjyzdtzs2",
        "加强监督教育/责令接受家庭教育指导通知书2": "jtjyzdtzs2",
    }
    return mapping.get(s, s)


COMMON_OTHER_PARAMS: Dict[str, Any] = {
    "quickFilter": "C75050E4832B4F5C882B7AE04B11FAC3",
    "modelId": "3014B3FB4791461998A87D794ED94077",
    "mark": "yshdws",
    "modelName": "已审核的文书SZ",
    "pkName": "ID",
    "modelMark": "yshdws",
    "resType": "02",
    "funcMark": "yshdws",
    "funcId": "6CDD88E5222140A1B66E608814697B84",
    "resId": "17CF516331DE4DF6800761D9452BAAEF",
    "sortname": "KJSJ,LRSJ,SPSJ,DYSJ",
    "sortorder": "desc,desc,asc,asc",
}


TQWS_SOURCE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "tqws": {
        "key": "tqws",
        "name": "提请文书",
        "table": "zq_zfba_tqzmjy",
        "where": {
            "rules": [
                {"field": "AJBH", "op": "like", "value": "", "type": "string", "format": ""},
                {
                    "field": "WS_ID",
                    "op": "like",
                    "value": "E232953F755A49DF90F73295347FEECA,A00A6FF90C9E423C960D7FE4224970CD",
                    "type": "string",
                    "format": "",
                    "linkOp": "or",
                },
                {"field": "WSZH", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "XGRY_XM", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "DYCS", "op": "between", "value": "0|999", "type": "number", "format": ""},
                {"field": "DYSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd HH:mm:ss"},
                {"field": "CBDW_MC", "op": "like", "value": "", "type": "string", "format": "", "linkOp": "or"},
                {"field": "CBDW_BH_1", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "CBR_XM", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "KJSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd HH:mm:ss"},
                {"field": "SPSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd HH:mm:ss"},
                {"field": "WSZT", "op": "like", "value": "03", "type": "string", "format": "", "linkOp": "or"},
            ],
            "op": "and",
        },
    },
    "xjs2": {
        "key": "xjs2",
        "name": "训诫书（未成年人）",
        "table": "zq_zfba_xjs2",
        "where": {
            "rules": [
                {"field": "AJBH", "op": "like", "value": "", "type": "string", "format": ""},
                {
                    "field": "WS_ID",
                    "op": "like",
                    "value": "16D4504F3D143E95E06307E21D44D3E7,3D87947C9AAC4533B62C0905806D78F8,F9C5ECF4DC7A42D699749F81140E1E9B",
                    "type": "string",
                    "format": "",
                    "linkOp": "or",
                },
                {"field": "WSZH", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "XGRY_XM", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "DYCS", "op": "between", "value": "0|999", "type": "number", "format": ""},
                {"field": "DYSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd+HH:mm:ss"},
                {"field": "CBDW_MC", "op": "like", "value": "", "type": "string", "format": "", "linkOp": "or"},
                {"field": "CBDW_BH_1", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "CBR_XM", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "KJSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd+HH:mm:ss"},
                {"field": "SPSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd+HH:mm:ss"},
                {"field": "WSZT", "op": "like", "value": "03", "type": "string", "format": "", "linkOp": "or"},
            ],
            "op": "and",
        },
    },
    "jtjyzdtzs2": {
        "key": "jtjyzdtzs2",
        "name": "加强监督教育/责令接受家庭教育指导通知书(新)",
        "table": "zq_zfba_jtjyzdtzs2",
        "where": {
            "rules": [
                {"field": "AJBH", "op": "like", "value": "", "type": "string", "format": ""},
                {
                    "field": "WS_ID",
                    "op": "like",
                    "value": "16D4504F3D133E95E06307E21D44D3E7,2355D72E6AA448329C46D56C8A57B8B4",
                    "type": "string",
                    "format": "",
                    "linkOp": "or",
                },
                {"field": "WSZH", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "XGRY_XM", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "DYCS", "op": "between", "value": "0|999", "type": "number", "format": ""},
                {"field": "DYSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd HH:mm:ss"},
                {"field": "CBDW_MC", "op": "like", "value": "", "type": "string", "format": "", "linkOp": "or"},
                {"field": "CBDW_BH_1", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "CBR_XM", "op": "like", "value": "", "type": "string", "format": ""},
                {"field": "KJSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd HH:mm:ss"},
                {"field": "SPSJ", "op": "between", "value": "|", "type": "date", "format": "yyyy/MM/dd HH:mm:ss"},
                {"field": "WSZT", "op": "like", "value": "03", "type": "string", "format": "", "linkOp": "or"},
            ],
            "op": "and",
        },
        "extra_form": {
            "new-password": "",
        },
    },
}


def get_tqws_sources() -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for k in ("tqws", "xjs2", "jtjyzdtzs2"):
        cfg = TQWS_SOURCE_REGISTRY.get(k)
        if not cfg:
            continue
        out.append({"key": str(cfg["key"]), "name": str(cfg["name"]), "table": str(cfg["table"])})
    # 兜底：避免 registry 顺序变化导致前端无数据
    for k, cfg in sorted(TQWS_SOURCE_REGISTRY.items()):
        if any(x["key"] == k for x in out):
            continue
        out.append({"key": str(cfg["key"]), "name": str(cfg["name"]), "table": str(cfg["table"])})
    return out


def get_tqws_source_catalog() -> List[Dict[str, Any]]:
    pk_name = str(COMMON_OTHER_PARAMS.get("pkName") or "ID").strip() or "ID"
    out: List[Dict[str, Any]] = []
    for item in get_tqws_sources():
        out.append(
            {
                "key": str(item.get("key") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "table": str(item.get("table") or "").strip(),
                "pk_fields": [pk_name],
                "requires": "access_token",
                "time_mode": "none",
            }
        )
    return out


def _make_form(
    *,
    token: str,
    where_obj: Dict[str, Any],
    page: int,
    extra_form: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    form: Dict[str, Any] = {
        **COMMON_OTHER_PARAMS,
        "access_token": token,
        "where": json.dumps(where_obj, ensure_ascii=False, separators=(",", ":")),
        "page": str(page),
        "pagesize": "1000",
    }
    if isinstance(extra_form, dict):
        for k, v in extra_form.items():
            form[str(k)] = v
    return form


def _run_job(*, username: str, job_id: str, access_token: str, sources: List[str]) -> None:
    key = (username or "", job_id)
    try:
        _update_status(key, state="running", message="任务执行中...")

        token = (access_token or "").strip()
        if not token:
            raise RuntimeError("access_token 不能为空")
        srcs = [str(s).strip() for s in (sources or []) if str(s).strip()]
        if not srcs:
            raise RuntimeError("请至少选择 1 个数据源")
        use_url = DEFAULT_URL

        headers = dict(DEFAULT_HEADERS)
        headers["Authorization"] = f"Bearer {token}"

        schema = DB_CONFIG.get("schema") or "ywdata"
        pk_name = str(COMMON_OTHER_PARAMS.get("pkName") or "ID").strip() or "ID"

        conn = get_database_connection()
        try:
            ensure_schema(conn, schema)

            results: List[Dict[str, Any]] = []
            overall_fetched = 0
            overall_processed = 0

            for idx, raw_source in enumerate(srcs, start=1):
                source_key = _normalize_source_key(raw_source)
                cfg = TQWS_SOURCE_REGISTRY.get(source_key)
                if not cfg:
                    raise RuntimeError(f"未知数据源: {raw_source}")
                source_name = str(cfg["name"])
                table = str(cfg["table"])
                where_obj = cfg["where"]
                if not isinstance(where_obj, dict):
                    raise RuntimeError(f"数据源 where 配置非法: {raw_source}")
                extra_form = cfg.get("extra_form")
                if extra_form is not None and not isinstance(extra_form, dict):
                    raise RuntimeError(f"数据源 extra_form 配置非法: {raw_source}")

                fetched_total = 0
                processed_total = 0
                inferred: Dict[str, str] = {}
                col_types: Optional[Dict[str, str]] = None

                cur_page = 1
                max_pages = 5000

                first_form = _make_form(token=token, where_obj=where_obj, page=cur_page, extra_form=extra_form)
                _update_status(
                    key,
                    progress={"current": overall_fetched, "total": 0},
                    message=f"[{idx}/{len(srcs)}] {source_name}：拉取第 1 页",
                    results=list(results),
                )
                first_payload = _post_page(url=use_url, headers=headers, form=first_form)
                total = _extract_total(first_payload)
                first_rows = _extract_rows(first_payload)

                def ingest(rows: List[Dict[str, Any]]) -> None:
                    nonlocal fetched_total, processed_total, inferred, col_types, overall_fetched, overall_processed
                    if not rows:
                        return
                    fetched_total += len(rows)
                    overall_fetched += len(rows)
                    inferred = {**inferred, **infer_col_types(rows)}
                    col_types = ensure_table_and_columns(
                        conn=conn,
                        schema=schema,
                        table=table,
                        pk_fields=[pk_name],
                        inferred_types=inferred,
                        table_comment=table,
                    )
                    n = upsert_rows(
                        conn=conn,
                        schema=schema,
                        table=table,
                        pk_fields=[pk_name],
                        rows=rows,
                        col_types=col_types,
                    )
                    processed_total += n
                    overall_processed += n

                ingest(first_rows)

                page_idx = 1
                while True:
                    if total and fetched_total >= total:
                        break
                    page_idx += 1
                    if page_idx > max_pages:
                        raise RuntimeError(f"{source_name}：超过最大页数限制：max_pages={max_pages}")
                    next_page = cur_page + (page_idx - 1)
                    form = _make_form(token=token, where_obj=where_obj, page=next_page, extra_form=extra_form)
                    _update_status(
                        key,
                        progress={"current": overall_fetched, "total": 0},
                        message=f"[{idx}/{len(srcs)}] {source_name}：拉取第 {next_page} 页",
                        results=list(results),
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

                results.append(
                    {
                        "name": source_name,
                        "schema": schema,
                        "table": table,
                        "fetched": fetched_total,
                        "processed": processed_total,
                    }
                )
                _update_status(
                    key,
                    progress={"current": overall_fetched, "total": 0},
                    message=f"[{idx}/{len(srcs)}] {source_name}：完成（Total={total or 0}）",
                    results=list(results),
                )

            _update_status(
                key,
                state="success",
                message="全部完成",
                results=list(results),
                progress={"current": overall_fetched, "total": 0},
            )
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        _update_status(key, state="failed", message=str(exc))
