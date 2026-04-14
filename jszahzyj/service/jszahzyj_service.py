"""
精神障碍患者预警业务逻辑层
处理查询和导出功能
"""
from typing import Dict, Any, List, Tuple
from datetime import datetime
from io import BytesIO, StringIO
import csv
import json
import logging
from openpyxl import Workbook
from flask import Response, send_file

from jszahzyj.dao.jszahzyj_dao import query_jszahzyj_data, get_all_jszahzyj_data

logger = logging.getLogger(__name__)


def _build_export_filename(extension: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"精神障碍患者预警{timestamp}.{extension}"


def _fetch_export_rows(
    *,
    liguan_start: str = None,
    liguan_end: str = None,
    maodun_start: str = None,
    maodun_end: str = None,
    fenju_list: List[str] = None,
) -> List[Dict[str, Any]]:
    return get_all_jszahzyj_data(
        liguan_start=liguan_start,
        liguan_end=liguan_end,
        maodun_start=maodun_start,
        maodun_end=maodun_end,
        fenju_list=fenju_list,
    )


def _serialize_xlsx_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def get_jszahzyj_data(
    liguan_start: str = None,
    liguan_end: str = None,
    maodun_start: str = None,
    maodun_end: str = None,
    fenju_list: List[str] = None,
    page: int = 1,
    page_size: int = 20
) -> Tuple[List[Dict[str, Any]], int]:
    """
    获取精神障碍患者预警数据（分页）

    参数:
        liguan_start: 列管时间开始，可选
        liguan_end: 列管时间结束，可选
        maodun_start: 矛盾纠纷录入时间开始，可选
        maodun_end: 矛盾纠纷录入时间结束，可选
        fenju_list: 分局列表，可选
        page: 页码
        page_size: 每页记录数

    返回:
        (数据列表, 总记录数)
    """
    try:
        rows, total = query_jszahzyj_data(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list,
            page=page,
            page_size=page_size
        )
        return rows, total
    except Exception as e:
        logger.error("获取数据失败: %s", e)
        raise


def export_to_csv(
    liguan_start: str = None,
    liguan_end: str = None,
    maodun_start: str = None,
    maodun_end: str = None,
    fenju_list: List[str] = None
) -> Response:
    """
    导出为CSV格式

    参数:
        liguan_start: 列管时间开始
        liguan_end: 列管时间结束
        maodun_start: 矛盾纠纷录入时间开始
        maodun_end: 矛盾纠纷录入时间结束
        fenju_list: 分局列表

    返回:
        Flask Response对象（CSV文件）
    """
    try:
        rows = _fetch_export_rows(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list,
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

        filename = _build_export_filename("csv")

        buffer = BytesIO(output.getvalue().encode("utf-8-sig"))
        buffer.seek(0)

        logger.info("导出CSV成功，文件名: %s，记录数: %s", filename, len(rows))

        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="text/csv; charset=utf-8"
        )

    except Exception as e:
        logger.error("导出CSV失败: %s", e)
        raise


def export_to_xlsx(
    liguan_start: str = None,
    liguan_end: str = None,
    maodun_start: str = None,
    maodun_end: str = None,
    fenju_list: List[str] = None
) -> Response:
    """
    导出为Excel格式

    参数:
        liguan_start: 列管时间开始，可选
        liguan_end: 列管时间结束，可选
        maodun_start: 矛盾纠纷录入时间开始，可选
        maodun_end: 矛盾纠纷录入时间结束，可选
        fenju_list: 分局列表，可选

    返回:
        Flask Response对象（Excel文件）
    """
    try:
        rows = _fetch_export_rows(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list,
        )

        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "精神障碍患者预警"

        if rows:
            headers = list(rows[0].keys())
            sheet.append(headers)

            for row in rows:
                sheet.append([_serialize_xlsx_cell(row.get(key)) for key in headers])
        else:
            sheet.append(["无数据"])

        filename = _build_export_filename("xlsx")

        buffer = BytesIO()
        workbook.save(buffer)
        buffer.seek(0)

        logger.info("导出Excel成功，文件名: %s，记录数: %s", filename, len(rows))

        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        logger.error("导出Excel失败: %s", e)
        raise
