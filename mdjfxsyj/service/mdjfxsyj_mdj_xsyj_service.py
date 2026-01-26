"""
矛盾纠纷线索移交：业务逻辑层

- 默认时间：
  - 结束时间：昨日 00:00:00
  - 开始时间：结束时间 - 7 日 00:00:00
- 支持分页查询与导出（csv/xlsx）
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, time
from io import BytesIO, StringIO
from typing import Any, Dict, List, Optional, Tuple

from flask import Response, send_file
from openpyxl import Workbook

from mdjfxsyj.dao.mdjfxsyj_mdj_xsyj_dao import get_all_mdj_xsyj_data, query_mdj_xsyj_data


def _parse_dt(val: str) -> datetime:
    val = (val or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    raise ValueError("时间格式错误，请使用 YYYY-MM-DD HH:MM:SS")


def default_range() -> Tuple[datetime, datetime]:
    today = datetime.now().date()
    end_dt = datetime.combine(today - timedelta(days=1), time(0, 0, 0))
    start_dt = end_dt - timedelta(days=7)
    return start_dt, end_dt


def normalize_range(start_time: Optional[str], end_time: Optional[str]) -> Tuple[datetime, datetime, str, str]:
    if start_time and end_time:
        s = _parse_dt(start_time)
        e = _parse_dt(end_time)
    else:
        s, e = default_range()

    if s > e:
        raise ValueError("开始时间不能晚于结束时间")

    s_str = s.strftime("%Y-%m-%d %H:%M:%S")
    e_str = e.strftime("%Y-%m-%d %H:%M:%S")
    return s, e, s_str, e_str


def get_mdj_xsyj_data(
    *,
    start_time: Optional[str],
    end_time: Optional[str],
    jfmc: Optional[str],
    fenju_list: Optional[List[str]],
    page: int,
    page_size: int,
) -> Tuple[List[Dict[str, Any]], int, str, str]:
    s, e, s_str, e_str = normalize_range(start_time, end_time)
    rows, total = query_mdj_xsyj_data(
        start_time=s,
        end_time=e,
        jfmc=(jfmc or "").strip() or None,
        fenju_list=fenju_list or None,
        page=page,
        page_size=page_size,
    )
    return rows, total, s_str, e_str


def export_to_csv(
    *,
    start_time: Optional[str],
    end_time: Optional[str],
    jfmc: Optional[str],
    fenju_list: Optional[List[str]],
) -> Response:
    s, e, _, _ = normalize_range(start_time, end_time)
    rows = get_all_mdj_xsyj_data(
        start_time=s,
        end_time=e,
        jfmc=(jfmc or "").strip() or None,
        fenju_list=fenju_list or None,
    )

    output = StringIO()
    if rows:
        headers = list(rows[0].keys())
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: (row.get(key) or "") for key in headers})
    else:
        output.write("无数据\n")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"矛盾纠纷线索移交{timestamp}.csv"
    buffer = BytesIO(output.getvalue().encode("utf-8-sig"))
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name=filename, mimetype="text/csv; charset=utf-8")


def export_to_xlsx(
    *,
    start_time: Optional[str],
    end_time: Optional[str],
    jfmc: Optional[str],
    fenju_list: Optional[List[str]],
) -> Response:
    s, e, _, _ = normalize_range(start_time, end_time)
    rows = get_all_mdj_xsyj_data(
        start_time=s,
        end_time=e,
        jfmc=(jfmc or "").strip() or None,
        fenju_list=fenju_list or None,
    )

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "矛盾纠纷线索移交"

    if rows:
        headers = list(rows[0].keys())
        sheet.append(headers)
        for row in rows:
            cleaned_row = []
            for key in headers:
                value = row.get(key)
                if value is None:
                    value = ""
                elif isinstance(value, (list, dict)):
                    import json

                    value = json.dumps(value, ensure_ascii=False)
                cleaned_row.append(value)
            sheet.append(cleaned_row)
    else:
        sheet.append(["无数据"])

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"矛盾纠纷线索移交{timestamp}.xlsx"

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

