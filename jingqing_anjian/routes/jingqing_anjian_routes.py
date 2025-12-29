from __future__ import annotations

"""
jingqing_anjian 模块路由
处理警情案件管理相关的页面与接口：
- 案件统计分析
- 案件详情
- 人员详情
- 数据同步
- 警情研判报告导出
"""

from pathlib import Path
import io
import logging

from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
    send_file,
    current_app,
)
from urllib.parse import quote

from jingqing_anjian.service.case_service import CaseService
from jingqing_anjian.service.jingqing_yanpan_service import generate_yanpan_report
from jingqing_anjian.service.biaochezhajie_service import generate_biaochezhajie_report
from jingqing_anjian.service.jqajzl_service import JqajzlService
from gonggong.config.database import execute_query, get_database_connection


jingqing_anjian_bp = Blueprint(
    "jingqing_anjian", __name__, template_folder="../templates"
)

case_service = CaseService()
jqajzl_service = JqajzlService()


@jingqing_anjian_bp.route("/")
def jingqing_anjian() -> str:
    """警情案件管理主页面。"""
    return render_template("jingqing_anjian.html")


@jingqing_anjian_bp.route("/jqajzl")
def jqajzl_page() -> str:
    """警情案件总览页面。"""
    return render_template("jqajzl.html")


@jingqing_anjian_bp.route("/jqajzl_detail")
def jqajzl_detail_page() -> str:
    """警情案件总览明细弹窗页面。"""
    return render_template("jqajzl_detail.html")


@jingqing_anjian_bp.route("/api/case_stats", methods=["GET"])
def get_case_stats():
    """获取案件统计分析数据。"""
    case_type = request.args.get("case_type")
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    case_stats = case_service.get_case_stats_by_type(
        case_type, start_time, end_time
    )

    return jsonify(
        {
            "success": True,
            "data": case_stats,
            "count": len(case_stats),
        }
    )


@jingqing_anjian_bp.route("/api/case_types", methods=["GET"])
def get_case_types():
    """获取案件类型列表。"""
    try:
        query = 'SELECT leixing FROM "ywdata"."case_type_config" ctc'
        results = execute_query(query)

        logging.info("获取到 %d 种案件类型", len(results))

        return jsonify({"success": True, "data": results})
    except Exception as exc:
        logging.error("查询案件类型时发生错误: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": "查询案件类型失败",
                }
            ),
            500,
        )


@jingqing_anjian_bp.route("/api/case_details", methods=["GET"])
def get_case_details():
    """获取案件详情数据。"""
    case_type = request.args.get("case_type")
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    result = case_service.get_ordered_case_details(
        case_type, start_time, end_time
    )

    return jsonify(
        {
            "success": True,
            "field_config": result["field_config"],
            "data": result["data"],
            "count": len(result["data"]),
        }
    )


@jingqing_anjian_bp.route("/api/case_ry_details", methods=["GET"])
def get_case_ry_details():
    """获取人员详情数据（列顺序与数据库函数返回保持一致）。"""
    case_type = request.args.get("case_type")
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    result = case_service.get_case_ry_data(case_type, start_time, end_time)

    return jsonify(
        {
            "success": True,
            "columns": result.get("columns", []),
            "data": result.get("data", []),
            "count": len(result.get("data", [])),
        }
    )


@jingqing_anjian_bp.route("/api/jqajzl/summary", methods=["GET"])
def get_jqajzl_summary():
    case_types = request.args.getlist("case_types")
    if not case_types:
        case_types_param = (request.args.get("case_types") or "").strip()
        if case_types_param:
            case_types = [item for item in case_types_param.split(",") if item]
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    result = jqajzl_service.get_summary(case_types, start_time, end_time)

    return jsonify(
        {
            "success": True,
            "columns": result["columns"],
            "data": result["data"],
            "count": len(result["data"]),
        }
    )


@jingqing_anjian_bp.route("/api/jqajzl/detail", methods=["GET"])
def get_jqajzl_detail():
    case_types = request.args.getlist("case_types")
    if not case_types:
        case_types_param = (request.args.get("case_types") or "").strip()
        if case_types_param:
            case_types = [item for item in case_types_param.split(",") if item]
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")
    region = (request.args.get("region") or "").strip() or None
    status_name = (request.args.get("status_name") or "").strip() or None
    metric = (request.args.get("metric") or "").strip()

    require_case = False
    if metric == "案件数":
        require_case = True

    result = jqajzl_service.get_detail(
        case_types,
        start_time,
        end_time,
        region=region,
        status_name=status_name,
        require_case=require_case,
    )

    return jsonify(
        {
            "success": True,
            "columns": result["columns"],
            "data": result["data"],
            "count": len(result["data"]),
        }
    )


def _normalize_export_value(value):
    import json

    def _join_doc_names(items):
        names = []
        for item in items:
            if isinstance(item, dict):
                name = item.get("name")
                if name:
                    names.append(str(name))
        if names:
            return ";".join(names)
        return None

    if isinstance(value, list):
        doc_names = _join_doc_names(value)
        if doc_names is not None:
            return doc_names
        return json.dumps(value, ensure_ascii=False)

    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)

    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") or stripped.startswith("{"):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return value
            if isinstance(parsed, list):
                doc_names = _join_doc_names(parsed)
                if doc_names is not None:
                    return doc_names
            return json.dumps(parsed, ensure_ascii=False)
    return value


def _build_csv_content(columns, rows):
    import csv
    import io

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(
            [_normalize_export_value(row.get(col, "")) for col in columns]
        )
    return buffer.getvalue()


def _build_excel_content(columns, rows):
    from openpyxl import Workbook
    import io

    wb = Workbook()
    ws = wb.active
    ws.append(columns)
    for row in rows:
        ws.append(
            [_normalize_export_value(row.get(col, "")) for col in columns]
        )
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def _build_timestamp():
    from datetime import datetime

    return datetime.now().strftime("%Y%m%d_%H%M%S")


@jingqing_anjian_bp.route("/api/jqajzl/export", methods=["GET"])
def export_jqajzl_summary():
    export_format = (request.args.get("format") or "csv").lower()
    case_types = request.args.getlist("case_types")
    if not case_types:
        case_types_param = (request.args.get("case_types") or "").strip()
        if case_types_param:
            case_types = [item for item in case_types_param.split(",") if item]
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    result = jqajzl_service.get_summary(case_types, start_time, end_time)
    columns = result["columns"]
    rows = result["data"]

    timestamp = _build_timestamp()
    if export_format == "excel":
        excel_io = _build_excel_content(columns, rows)
        filename = f"警情案件总览_{timestamp}.xlsx"
        return send_file(
            excel_io,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    csv_content = _build_csv_content(columns, rows)
    csv_bytes = ("\ufeff" + csv_content).encode("utf-8")
    filename = f"警情案件总览_{timestamp}.csv"
    return send_file(
        io.BytesIO(csv_bytes),
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv; charset=utf-8",
    )


@jingqing_anjian_bp.route("/api/jqajzl/detail_export", methods=["GET"])
def export_jqajzl_detail():
    export_format = (request.args.get("format") or "csv").lower()
    case_types = request.args.getlist("case_types")
    if not case_types:
        case_types_param = (request.args.get("case_types") or "").strip()
        if case_types_param:
            case_types = [item for item in case_types_param.split(",") if item]
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")
    region = (request.args.get("region") or "").strip() or None
    status_name = (request.args.get("status_name") or "").strip() or None
    metric = (request.args.get("metric") or "").strip()

    require_case = False
    if metric == "案件数":
        require_case = True

    result = jqajzl_service.get_detail(
        case_types,
        start_time,
        end_time,
        region=region,
        status_name=status_name,
        require_case=require_case,
    )
    columns = result["columns"]
    rows = result["data"]

    timestamp = _build_timestamp()
    if export_format == "excel":
        excel_io = _build_excel_content(columns, rows)
        filename = f"警情案件总览明细_{timestamp}.xlsx"
        return send_file(
            excel_io,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    csv_content = _build_csv_content(columns, rows)
    csv_bytes = ("\ufeff" + csv_content).encode("utf-8")
    filename = f"警情案件总览明细_{timestamp}.csv"
    return send_file(
        io.BytesIO(csv_bytes),
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv; charset=utf-8",
    )


@jingqing_anjian_bp.route("/sync_data", methods=["POST"])
def sync_data():
    """同步数据库物化视图。"""
    materialized_views = [
        "mv_zfba_all_ajxx",
        "mv_zfba_jlzxx",
        "mv_zfba_xzcfjdsxx",
        "mv_minor_person",
        "mv_zfba_wenshu",
    ]

    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        refreshed_views = []

        for view_name in materialized_views:
            try:
                refresh_sql = f"REFRESH MATERIALIZED VIEW ywdata.{view_name};"
                cursor.execute(refresh_sql)
                refreshed_views.append(view_name)
                logging.info("成功刷新物化视图: ywdata.%s", view_name)
            except Exception as exc:
                logging.error("刷新物化视图 ywdata.%s 失败: %s", view_name, exc)

        connection.commit()
        cursor.close()
        connection.close()

        logging.info("数据同步完成，刷新了 %d 个物化视图", len(refreshed_views))

        return jsonify(
            {
                "success": True,
                "message": "数据同步完成",
                "refreshed_views": refreshed_views,
                "count": len(refreshed_views),
            }
        )
    except Exception as exc:
        logging.error("同步数据时发生错误: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": f"同步数据失败: {exc}",
                }
            ),
            500,
        )


@jingqing_anjian_bp.route("/export_yanpan_report", methods=["GET"])
def export_yanpan_report():
    """
    导出涉黄、赌、打架斗殴类警情研判分析报告（docx）。
    使用项目公共数据库连接和 jingqing_anjian 模块下的模板。
    """
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()

    if not start_time or not end_time:
        return (
            jsonify(
                {
                    "success": False,
                    "message": "开始时间和结束时间不能为空",
                }
            ),
            400,
        )
    try:
        template_path = (
            Path(current_app.root_path)
            / "jingqing_anjian"
            / "templates"
            / "template.docx"
        )
        result = generate_yanpan_report(start_time, end_time, template_path)

        buffer = io.BytesIO(result.content)
        buffer.seek(0)

        return send_file(
            buffer,
            as_attachment=True,
            download_name=result.filename,
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
    except RuntimeError as exc:
        # 大模型服务不可用等情况
        logging.warning("导出警情研判报告的大模型服务错误: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": "请启动大模型服务！",
                }
            ),
            400,
        )
    except FileNotFoundError as exc:
        logging.error("导出警情研判报告失败，模板缺失: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": "模板文件不存在，请联系管理员配置 template.docx",
                }
            ),
            500,
        )
    except ValueError as exc:
        logging.warning("导出警情研判报告参数或数据错误: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": str(exc),
                }
            ),
            400,
        )
    except Exception as exc:  # pragma: no cover - 兜底
        logging.exception("导出警情研判报告时发生异常: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": f"生成警情研判报告失败: {exc}",
                }
            ),
            500,
        )

@jingqing_anjian_bp.route("/export_biaochezhajie_report", methods=["POST"])
def export_biaochezhajie_report():
    """
    导出飙车炸街日报（docx）。

    前端上传 xlsx 附件后：
    - 校验必须为 xlsx
    - 读取第一个 sheet 的单元格作为模板变量（如 {{B7}}）
    - 对 C/K/L/N/R/U/V 列进行“上升/下降/持平”清洗
    - 使用 biaochezhajie_ribao.docx 渲染并下载
    """
    upload = request.files.get("file")
    if upload is None:
        return jsonify({"success": False, "message": "请先上传xlsx文件"}), 400

    filename = (upload.filename or "").strip()
    if not filename.lower().endswith(".xlsx"):
        return jsonify({"success": False, "message": "请上传xlsx文件"}), 400

    try:
        template_path = (
            Path(current_app.root_path)
            / "jingqing_anjian"
            / "templates"
            / "biaochezhajie_ribao.docx"
        )
        xlsx_bytes = upload.read()
        result = generate_biaochezhajie_report(xlsx_bytes, template_path)

        buffer = io.BytesIO(result.content)
        buffer.seek(0)

        response = send_file(
            buffer,
            as_attachment=True,
            download_name=result.filename,
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        ascii_fallback = "biaochezhajie_report.docx"
        response.headers["Content-Disposition"] = (
            f'attachment; filename="{ascii_fallback}"; '
            f"filename*=UTF-8''{quote(result.filename)}"
        )
        return response
    except FileNotFoundError as exc:
        logging.error("导出飙车炸街日报失败，模板缺失: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": "模板文件不存在，请联系管理员配置 biaochezhajie_ribao.docx",
                }
            ),
            500,
        )
    except Exception as exc:
        logging.exception("导出飙车炸街日报时发生异常: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": f"生成飙车炸街日报失败: {exc}",
                }
            ),
            500,
        )
    
