from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
from openpyxl import Workbook

from gzrzdd.dao.gzrygzrz_dao import normalize_datetime_text, query_gzrygzrz


DISPLAY_COLUMNS = [
    "姓名",
    "证件号码",
    "两会是否有进京风险",
    "联系电话",
    "分局名称",
    "所属派出所",
    "数据登记时间",
    "工作日志系统登记时间",
    "工作日志-工作开展时间",
    "工作日志-工作方式",
    "工作日志-工作开展情况",
    "是否存在进京指向",
    "目前人员所在位置",
]


RENAME_MAP = {
    "工作日志_工作开展时间": "工作日志-工作开展时间",
    "工作日志_工作方式": "工作日志-工作方式",
    "工作日志_工作开展情况": "工作日志-工作开展情况",
}


def default_time_range() -> Tuple[str, str]:
    now = datetime.now()
    end_dt = datetime(now.year, now.month, now.day, 0, 0, 0)
    start_dt = end_dt - timedelta(days=7)
    return (
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def _format_cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, pd.Timestamp):
        if pd.isna(v):
            return ""
        return v.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return v


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=DISPLAY_COLUMNS)

    out = df.rename(columns=RENAME_MAP).copy()
    for col in DISPLAY_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[DISPLAY_COLUMNS]
    for col in out.columns:
        out[col] = out[col].map(_format_cell)
    return out


def _unique_branch_options(df: pd.DataFrame) -> List[str]:
    if df is None or df.empty or "分局名称" not in df.columns:
        return []
    vals = []
    seen = set()
    for v in df["分局名称"].tolist():
        s = "" if v is None else str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        vals.append(s)
    return vals


def query_gzrygzrz_records(
    *,
    start_time: str,
    end_time: str,
    sfczjjzx: str = "",
    branches: Iterable[str] | None = None,
) -> Dict[str, Any]:
    start_text = normalize_datetime_text(start_time)
    end_text = normalize_datetime_text(end_time)
    branch_list = [x.strip() for x in (branches or []) if x and x.strip()]

    # 分局选项不受“分局筛选”本身影响，仅受时间与进京指向影响
    base_df = query_gzrygzrz(
        start_time=start_text,
        end_time=end_text,
        sfczjjzx=sfczjjzx,
        branches=[],
    )
    branch_options = _unique_branch_options(_prepare_dataframe(base_df))

    data_df = query_gzrygzrz(
        start_time=start_text,
        end_time=end_text,
        sfczjjzx=sfczjjzx,
        branches=branch_list,
    )
    show_df = _prepare_dataframe(data_df)
    records = show_df.to_dict(orient="records")
    return {
        "success": True,
        "records": records,
        "count": len(records),
        "branch_options": branch_options,
        "filters": {
            "start_time": start_text,
            "end_time": end_text,
            "sfczjjzx": (sfczjjzx or "").strip(),
            "branches": branch_list,
        },
    }


def _sanitize_filename_text(text: str) -> str:
    t = (text or "").strip()
    return re.sub(r'[\\/:*?"<>|]', "-", t)


def _to_csv_bytes(records: List[Dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    if records:
        cols = list(records[0].keys())
        writer.writerow(cols)
        for row in records:
            writer.writerow([row.get(c, "") for c in cols])
    else:
        writer.writerow(DISPLAY_COLUMNS)
    return ("\ufeff" + buf.getvalue()).encode("utf-8")


def _to_xlsx_bytes(records: List[Dict[str, Any]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "关注人员工作日志"
    if records:
        cols = list(records[0].keys())
        ws.append(cols)
        for row in records:
            ws.append([row.get(c, "") for c in cols])
    else:
        ws.append(DISPLAY_COLUMNS)
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def export_gzrygzrz_records(
    *,
    fmt: str,
    start_time: str,
    end_time: str,
    sfczjjzx: str = "",
    branches: Iterable[str] | None = None,
) -> Tuple[bytes, str, str]:
    payload = query_gzrygzrz_records(
        start_time=start_time,
        end_time=end_time,
        sfczjjzx=sfczjjzx,
        branches=branches,
    )
    records = payload["records"]
    start_text = payload["filters"]["start_time"]
    end_text = payload["filters"]["end_time"]

    fmt_text = (fmt or "xlsx").lower().strip()
    if fmt_text not in ("xlsx", "csv"):
        raise ValueError("导出格式仅支持 xlsx/csv")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = (
        f"{_sanitize_filename_text(start_text)}至{_sanitize_filename_text(end_text)}"
        f"_关注人员工作日志_{timestamp}.{fmt_text}"
    )
    if fmt_text == "csv":
        return _to_csv_bytes(records), "text/csv; charset=utf-8", filename
    return (
        _to_xlsx_bytes(records),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename,
    )
