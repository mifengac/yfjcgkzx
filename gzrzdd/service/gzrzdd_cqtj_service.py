from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from openpyxl import Workbook

from gzrzdd.dao.gzrzdd_cqtj_dao import load_zdrygzrzs
from gzrzdd.dao.gzrzdd_dao import find_col


COL_NAME = "姓名"
COL_ID = "证件号码"
COL_RISK = "风险等级"
COL_BRANCH = "分局名称"
COL_STATION = "所属派出所"
COL_SORT = "列管时间"
COL_WORK_TIME = "工作日志开展工作时间"


RISK_RULES: Dict[str, Dict[str, int]] = {
    "高风险": {"warn": 7, "remind": 6},
    "中风险": {"warn": 15, "remind": 13},
    "低风险": {"warn": 30, "remind": 20},
}


@dataclass
class CqtjQuery:
    mode: str  # detail | branch
    level: str  # remind | warn
    risk_types: List[str]
    branches: List[str]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


def _norm(s: Any) -> str:
    return ("" if s is None else str(s)).strip()


def _parse_datetime_filter(value: Any, field_name: str, *, end_of_day: bool = False) -> Optional[datetime]:
    text = _norm(value).replace("T", " ")
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            if fmt == "%Y-%m-%d" and end_of_day:
                dt = dt.replace(hour=23, minute=59, second=59)
            return dt
        except ValueError:
            pass
    raise ValueError(f"{field_name}格式错误，应为 YYYY-MM-DD HH:MM:SS")


def _parse_work_time_range(start_time: Any = None, end_time: Any = None) -> Tuple[Optional[datetime], Optional[datetime]]:
    start_dt = _parse_datetime_filter(start_time, "工作日志开始时间")
    end_dt = _parse_datetime_filter(end_time, "工作日志结束时间", end_of_day=True)
    if start_dt and end_dt and start_dt > end_dt:
        raise ValueError("工作日志开始时间不能大于结束时间")
    return start_dt, end_dt


def _required_columns(df: pd.DataFrame) -> Dict[str, str]:
    c_name = find_col(df, COL_NAME)
    c_id = find_col(df, COL_ID)
    c_risk = find_col(df, COL_RISK)
    c_branch = find_col(df, COL_BRANCH)
    c_station = find_col(df, COL_STATION)
    c_sort = find_col(df, COL_SORT)
    c_work = find_col(df, COL_WORK_TIME)
    missing = [
        n
        for n, c in [
            (COL_NAME, c_name),
            (COL_ID, c_id),
            (COL_RISK, c_risk),
            (COL_BRANCH, c_branch),
            (COL_STATION, c_station),
            (COL_SORT, c_sort),
            (COL_WORK_TIME, c_work),
        ]
        if not c
    ]
    if missing:
        raise ValueError(f"SQL 结果缺少必要字段：{', '.join(missing)}（请在 SQL 中使用 AS 设置中文别名）")
    return {
        "name": c_name or COL_NAME,
        "id": c_id or COL_ID,
        "risk": c_risk or COL_RISK,
        "branch": c_branch or COL_BRANCH,
        "station": c_station or COL_STATION,
        "sort": c_sort or COL_SORT,
        "work": c_work or COL_WORK_TIME,
    }


def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, (pd.Timestamp,)):
        if pd.isna(v):
            return None
        try:
            return v.to_pydatetime()
        except Exception:
            return None
    s = _norm(v)
    if not s:
        return None
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    try:
        return dt.to_pydatetime()
    except Exception:
        return None


def _days_since(dt: Optional[datetime], now: datetime) -> Optional[int]:
    if dt is None:
        return None
    delta = now - dt
    return int(delta.total_seconds() // 86400)


def _status_by_risk(risk: str, days: Optional[int]) -> Tuple[str, str]:
    """
    Returns: (level, color)
    level: warn | remind | ok
    color: red | yellow | normal
    """
    rules = RISK_RULES.get(risk)
    if days is None:
        return "warn", "red"
    if not rules:
        return "remind", "yellow" if days >= 1 else "ok", "normal"
    if days > int(rules["warn"]):
        return "warn", "red"
    if days > int(rules["remind"]):
        return "remind", "yellow"
    return "ok", "normal"


def _filter_by_query(df: pd.DataFrame, cols: Dict[str, str], q: CqtjQuery, now: datetime) -> pd.DataFrame:
    work = df.copy()
    c_work = cols["work"]
    work["__work_dt"] = pd.to_datetime(work[c_work], errors="coerce")
    if q.start_time:
        work = work[work["__work_dt"] >= q.start_time].copy()
    if q.end_time:
        work = work[work["__work_dt"] <= q.end_time].copy()

    # 分组取最近一条（按开展工作时间倒序）
    group_keys = [cols["name"], cols["id"], cols["risk"], cols["branch"], cols["station"], cols["sort"]]
    work = work.sort_values(by=group_keys + ["__work_dt"], ascending=[True] * len(group_keys) + [False], kind="mergesort")
    latest = work.groupby(group_keys, dropna=False, sort=False).head(1).copy()

    # 风险类型过滤
    want_risks = [x for x in (q.risk_types or []) if x]
    if want_risks:
        latest = latest[latest[cols["risk"]].astype(str).isin(want_risks)].copy()

    # 分局过滤（控件值 -> 数据库分局名称值映射）
    branch_map = {
        "云城": "云城分局",
        "云安": "云安分局",
        "罗定": "罗定市公安局",
        "新兴": "新兴县公安局",
        "郁南": "郁南县公安局",
    }
    want_branches_ui = [x for x in (q.branches or []) if x]
    if want_branches_ui:
        want_values = set()
        for b in want_branches_ui:
            want_values.add(branch_map.get(b, b))
            want_values.add(b)
        latest = latest[latest[cols["branch"]].astype(str).isin(list(want_values))].copy()

    # 计算天数/状态
    now_dt = now
    latest["__days"] = [
        _days_since(_parse_dt(v), now_dt) for v in latest[c_work].tolist()
    ]
    levels = []
    colors = []
    for r, d in zip(latest[cols["risk"]].astype(str).tolist(), latest["__days"].tolist()):
        lv, color = _status_by_risk(r, d)
        levels.append(lv)
        colors.append(color)
    latest["__level"] = levels
    latest["__color"] = colors

    # level 过滤：默认 remind = remind+warn；warn = warn
    if q.level == "warn":
        latest = latest[latest["__level"] == "warn"].copy()
    else:
        latest = latest[latest["__level"].isin(["remind", "warn"])].copy()

    # 输出排序：警告优先，其次提醒；同级按天数降序
    order = {"warn": 2, "remind": 1, "ok": 0}
    latest["__lv_order"] = latest["__level"].map(lambda x: order.get(str(x), 0))
    latest = latest.sort_values(by=["__lv_order", "__days"], ascending=[False, False], kind="mergesort")
    return latest


def query_cqtj(
    *,
    mode: str = "detail",
    level: str = "remind",
    risk_types: Optional[List[str]] = None,
    branches: Optional[List[str]] = None,
    start_time: Any = None,
    end_time: Any = None,
) -> Tuple[datetime, List[Dict[str, Any]]]:
    start_dt, end_dt = _parse_work_time_range(start_time, end_time)
    q = CqtjQuery(
        mode=(mode or "detail").strip(),
        level=(level or "remind").strip(),
        risk_types=risk_types or [],
        branches=branches or [],
        start_time=start_dt,
        end_time=end_dt,
    )
    now = datetime.now()
    df = load_zdrygzrzs(start_time=start_dt, end_time=end_dt)
    if df is None or df.empty:
        return now, []
    cols = _required_columns(df)
    latest = _filter_by_query(df, cols, q, now)

    if q.mode == "branch":
        c_branch = cols["branch"]
        grp = latest.groupby(latest[c_branch].astype(str), dropna=False)
        out: List[Dict[str, Any]] = []
        for branch, g in grp:
            warn_cnt = int((g["__level"] == "warn").sum())
            remind_cnt = int((g["__level"] == "remind").sum())
            out.append(
                {
                    "分局名称": branch,
                    "提醒数": remind_cnt,
                    "警告数": warn_cnt,
                    "合计": remind_cnt + warn_cnt,
                }
            )
        out.sort(key=lambda r: (int(r.get("警告数") or 0), int(r.get("提醒数") or 0)), reverse=True)
        return now, out

    # detail
    c_work = cols["work"]
    rename = {c_work: "最近工作日志时间"}
    show_cols = [
        cols["name"],
        cols["id"],
        cols["risk"],
        cols["branch"],
        cols["station"],
        cols["sort"],
        c_work,
    ]
    sub = latest[show_cols + ["__days", "__level", "__color"]].rename(columns=rename).copy()
    out = []
    for r in sub.to_dict(orient="records"):
        days = r.pop("__days", None)
        level_v = r.pop("__level", "")
        color = r.pop("__color", "normal")
        r["间隔天数"] = "" if days is None else int(days)
        r["状态"] = "警告" if level_v == "warn" else ("提醒" if level_v == "remind" else "正常")
        r["__row_color"] = color
        out.append(_format_record(r))
    return now, out


def _format_record(r: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in r.items():
        if k == "__row_color":
            out[k] = v
            continue
        if isinstance(v, (pd.Timestamp, datetime)):
            out[k] = "" if pd.isna(v) else v.strftime("%Y-%m-%d %H:%M:%S")
        else:
            out[k] = "" if (isinstance(v, float) and pd.isna(v)) else v
    return out


def export_cqtj(
    *,
    fmt: str,
    mode: str,
    level: str,
    risk_types: Optional[List[str]] = None,
    branches: Optional[List[str]] = None,
    start_time: Any = None,
    end_time: Any = None,
) -> Tuple[bytes, str, str]:
    now, records = query_cqtj(
        mode=mode,
        level=level,
        risk_types=risk_types,
        branches=branches,
        start_time=start_time,
        end_time=end_time,
    )
    ts = now.strftime("%Y%m%d_%H%M%S")
    fmt = (fmt or "xlsx").lower()
    filename = f"矛盾纠纷风险人员工作日志超期统计_{ts}.{fmt}"

    # 导出不需要前端行颜色字段
    clean_records = [{k: v for k, v in r.items() if k != "__row_color"} for r in records]
    if fmt == "csv":
        return _records_to_csv_bytes(clean_records), "text/csv; charset=utf-8", filename
    return _records_to_xlsx_bytes(clean_records, sheet="超期统计"), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename


def _records_to_csv_bytes(records: List[Dict[str, Any]]) -> bytes:
    if not records:
        content = ""
    else:
        cols = list(records[0].keys())
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(cols)
        for r in records:
            w.writerow([_norm(r.get(c)) for c in cols])
        content = buf.getvalue()
    return ("\ufeff" + content).encode("utf-8")


def _records_to_xlsx_bytes(records: List[Dict[str, Any]], *, sheet: str) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet
    if not records:
        ws.append([])
    else:
        cols = list(records[0].keys())
        ws.append(cols)
        for r in records:
            ws.append([r.get(c, "") for c in cols])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()
