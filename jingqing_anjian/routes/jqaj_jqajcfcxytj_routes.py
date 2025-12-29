# -*- coding: utf-8 -*-
"""
警情案件处罚查询与统计 - 路由层
Routes (蓝图中转层)
"""

import io
import logging
from datetime import datetime

from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
    send_file,
)

from jingqing_anjian.service.jqaj_jqajcfcxytj_service import JqajcfcxytjService


# 创建蓝图
jqajcfcxytj_bp = Blueprint(
    "jqajcfcxytj",
    __name__,
    template_folder="../templates"
)

# 服务实例
service = JqajcfcxytjService()


def _normalize_export_value(value):
    """标准化导出值"""
    import json

    if value is None:
        return ""

    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)

    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") or stripped.startswith("{"):
            try:
                parsed = json.loads(stripped)
                return json.dumps(parsed, ensure_ascii=False)
            except json.JSONDecodeError:
                return value

    return value


def _build_csv_content(columns, rows):
    """构建CSV内容"""
    import csv

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([_normalize_export_value(row.get(col, "")) for col in columns])
    return buffer.getvalue()


def _build_excel_content(columns, rows):
    """构建Excel内容"""
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.append(columns)
    for row in rows:
        ws.append([_normalize_export_value(row.get(col, "")) for col in columns])
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def _calculate_totals(columns, rows):
    """计算合计行"""
    totals = {}
    for col in columns:
        if col == "地区":
            totals[col] = "合计"
        else:
            # 对数字列求和
            total = 0
            for row in rows:
                val = row.get(col, 0)
                if isinstance(val, (int, float)):
                    total += val
                elif isinstance(val, str):
                    try:
                        total += int(val) if val else 0
                    except ValueError:
                        pass
            totals[col] = total
    return totals


def _build_timestamp():
    """构建时间戳"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


@jqajcfcxytj_bp.route("/jqajcfcxytj")
def jqajcfcxytj_page():
    """警情案件处罚查询与统计主页面"""
    return render_template("jqajcfcxytj.html")


@jqajcfcxytj_bp.route("/jqajcfcxytj/detail")
def jqajcfcxytj_detail_page():
    """警情案件处罚查询与统计明细页面"""
    return render_template("jqajcfcxytj_detail.html")


@jqajcfcxytj_bp.route("/api/jqajcfcxytj/types", methods=["GET"])
def get_case_types():
    """获取警情类型列表"""
    try:
        data = service.get_case_types()
        return jsonify({
            "success": True,
            "data": data
        })
    except Exception as exc:
        logging.error("获取警情类型失败: %s", exc)
        return jsonify({
            "success": False,
            "message": f"获取警情类型失败: {exc}"
        }), 500


@jqajcfcxytj_bp.route("/api/jqajcfcxytj/summary", methods=["POST"])
def get_summary():
    """获取汇总统计数据"""
    try:
        params = request.get_json() or {}
        leixing = params.get("leixing", [])
        kssj = params.get("kssj", "")
        jssj = params.get("jssj", "")

        # 不再强制要求选择类型，允许查询全部

        if not kssj or not jssj:
            return jsonify({
                "success": False,
                "message": "请选择开始时间和结束时间"
            }), 400

        data = service.process_summary_stats(kssj, jssj, leixing)

        return jsonify({
            "success": True,
            "columns": service.SUMMARY_COLUMNS,
            "data": data,
            "count": len(data)
        })
    except ValueError as exc:
        logging.error("汇总统计参数错误: %s", exc)
        return jsonify({
            "success": False,
            "message": str(exc)
        }), 400
    except Exception as exc:
        logging.error("汇总统计失败: %s", exc)
        return jsonify({
            "success": False,
            "message": f"汇总统计失败: {exc}"
        }), 500


@jqajcfcxytj_bp.route("/api/jqajcfcxytj/detail", methods=["POST"])
def get_detail():
    """获取明细数据"""
    try:
        params = request.get_json() or {}
        leixing = params.get("leixing", [])
        kssj = params.get("kssj", "")
        jssj = params.get("jssj", "")
        click_field = params.get("click_field", "")
        region_code = params.get("region", "")

        # 不再强制要求选择类型，允许查询全部

        if not kssj or not jssj:
            return jsonify({
                "success": False,
                "message": "请选择开始时间和结束时间"
            }), 400

        if not click_field:
            return jsonify({
                "success": False,
                "message": "缺少点击字段参数"
            }), 400

        if not region_code:
            return jsonify({
                "success": False,
                "message": "缺少地区参数"
            }), 400

        columns, data = service.get_detail_data(kssj, jssj, leixing, click_field, region_code)

        return jsonify({
            "success": True,
            "columns": columns,
            "data": data,
            "count": len(data)
        })
    except ValueError as exc:
        logging.error("明细查询参数错误: %s", exc)
        return jsonify({
            "success": False,
            "message": str(exc)
        }), 400
    except Exception as exc:
        logging.error("明细查询失败: %s", exc)
        return jsonify({
            "success": False,
            "message": f"明细查询失败: {exc}"
        }), 500


@jqajcfcxytj_bp.route("/api/jqajcfcxytj/export", methods=["POST"])
def export_data():
    """导出数据"""
    try:
        params = request.get_json() or {}
        leixing = params.get("leixing", [])
        kssj = params.get("kssj", "")
        jssj = params.get("jssj", "")
        export_format = params.get("format", "csv").lower()
        data_type = params.get("data_type", "summary")

        # 不再强制要求选择类型，允许导出全部

        if not kssj or not jssj:
            return jsonify({
                "success": False,
                "message": "请选择开始时间和结束时间"
            }), 400

        timestamp = _build_timestamp()

        if data_type == "summary":
            # 导出汇总数据
            data = service.process_summary_stats(kssj, jssj, leixing)
            columns = service.SUMMARY_COLUMNS
            filename = f"警情案件处罚统计_{timestamp}"
        else:
            # 导出明细数据
            click_field = params.get("click_field", "")
            region_code = params.get("region", "")

            if not click_field or not region_code:
                return jsonify({
                    "success": False,
                    "message": "导出明细缺少必要参数"
                }), 400

            columns, data = service.get_detail_data(kssj, jssj, leixing, click_field, region_code)
            filename = f"警情案件处罚统计明细_{timestamp}"

        if export_format == "excel":
            excel_io = _build_excel_content(columns, data)
            return send_file(
                excel_io,
                as_attachment=True,
                download_name=f"{filename}.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            csv_content = _build_csv_content(columns, data)
            csv_bytes = ("\ufeff" + csv_content).encode("utf-8")
            return send_file(
                io.BytesIO(csv_bytes),
                as_attachment=True,
                download_name=f"{filename}.csv",
                mimetype="text/csv; charset=utf-8",
            )
    except ValueError as exc:
        logging.error("导出参数错误: %s", exc)
        return jsonify({
            "success": False,
            "message": str(exc)
        }), 400
    except Exception as exc:
        logging.error("导出失败: %s", exc)
        return jsonify({
            "success": False,
            "message": f"导出失败: {exc}"
        }), 500
