"""
精神障碍患者预警业务逻辑层
处理查询和导出功能
"""
from typing import Dict, Any, List, Tuple
from datetime import datetime
from io import BytesIO, StringIO
import csv
import logging
from openpyxl import Workbook
from flask import Response, send_file

from jszahzyj.dao.jszahzyj_dao import query_jszahzyj_data, get_all_jszahzyj_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


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
        logging.error(f"获取数据失败: {e}")
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
        # 获取所有数据（不分页）
        rows = get_all_jszahzyj_data(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list
        )

        # 创建CSV内容
        output = StringIO()
        if rows:
            headers = list(rows[0].keys())
            writer = csv.DictWriter(output, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: (row.get(key) or "") for key in headers})
        else:
            output.write("无数据\n")

        # 生成文件名（带时间戳）
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"精神障碍患者预警{timestamp}.csv"

        # 转换为字节流（使用UTF-8-BOM编码，确保Excel正确显示中文）
        buffer = BytesIO(output.getvalue().encode("utf-8-sig"))
        buffer.seek(0)

        logging.info(f"导出CSV成功，文件名: {filename}，记录数: {len(rows)}")

        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="text/csv; charset=utf-8"
        )

    except Exception as e:
        logging.error(f"导出CSV失败: {e}")
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
        # 获取所有数据（不分页）
        rows = get_all_jszahzyj_data(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list
        )

        # 创建Excel工作簿
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "精神障碍患者预警"

        if rows:
            # 写入表头
            headers = list(rows[0].keys())
            sheet.append(headers)

            # 写入数据行
            for row in rows:
                cleaned_row = []
                for key in headers:
                    value = row.get(key)
                    # 处理None值
                    if value is None:
                        value = ""
                    # 处理列表和字典类型（转为字符串）
                    elif isinstance(value, (list, dict)):
                        import json
                        value = json.dumps(value, ensure_ascii=False)
                    cleaned_row.append(value)
                sheet.append(cleaned_row)
        else:
            sheet.append(["无数据"])

        # 生成文件名（带时间戳）
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"精神障碍患者预警{timestamp}.xlsx"

        # 保存到字节流
        buffer = BytesIO()
        workbook.save(buffer)
        buffer.seek(0)

        logging.info(f"导出Excel成功，文件名: {filename}，记录数: {len(rows)}")

        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        logging.error(f"导出Excel失败: {e}")
        raise
