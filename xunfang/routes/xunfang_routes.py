"""
巡防统计模块路由。

提供以下能力：
1. 模块首页展示。
2. 巡防批量计算、在线率导出、象限图导出、警力表导出。
3. 代理调用外部接口、解析 JSON 数据，并支持结果下载。
4. 所有请求统一进行 IP 访问控制。
"""

from __future__ import annotations

import csv
import json
import zipfile
from datetime import datetime
from io import BytesIO, StringIO
from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qs, unquote_plus, urlparse

import requests
from flask import (
    Blueprint,
    Response,
    abort,
    jsonify,
    render_template,
    request,
    send_file,
)
from openpyxl import Workbook

from gonggong.service.session_manager import session_manager
from gonggong.utils.error_handler import handle_errors, log_error, log_info, log_warning
from flask import session as flask_session, redirect, url_for
from gonggong.config.database import get_database_connection
from xunfang.service.xunfang_service import (
    calculate_xunfang_for_date_range,
    export_online_rate_for_date_range,
    export_police_force_for_date_range,
    export_quadrant_chart_for_date_range,
)


xunfang_bp = Blueprint("xunfang", __name__, template_folder="../templates")


@xunfang_bp.before_request
def _ensure_access() -> None:
    """请求前拦截，基于 IP 做巡防模块访问控制。"""
    if not flask_session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute('SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s', (flask_session["username"], "巡防"))
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)


@xunfang_bp.route("/")
def xunfang() -> str:
    """巡防统计模块首页。"""
    return render_template("xunfang.html")


@xunfang_bp.route("/calculate", methods=["POST"])
def calculate_xunfang() -> Response:
    """巡防批量计算任务。"""
    data = request.json or {}
    start_time = data.get("startTime")
    end_time = data.get("endTime")

    if not start_time or not end_time:
        return jsonify({"success": False, "message": "开始时间和结束时间不能为空"})

    try:
        result = calculate_xunfang_for_date_range(start_time, end_time)
        return jsonify(
            {
                "success": True,
                "message": "计算完成",
                "success_count": result.get("success_count", 0),
                "total_tasks": result.get("total_tasks", 0),
            }
        )
    except Exception as exc:  # noqa: BLE001
        log_error(f"巡防统计计算失败: {exc}")
        return jsonify({"success": False, "message": f"计算失败: {exc}"}), 500


@xunfang_bp.route("/export_online_rate", methods=["POST"])
def export_online_rate() -> Response:
    """导出在线率统计 Excel。"""
    data = request.json or {}
    start_time = data.get("startTime")
    end_time = data.get("endTime")

    if not start_time or not end_time:
        return jsonify({"success": False, "message": "开始时间和结束时间不能为空"}), 400

    try:
        workbook = export_online_rate_for_date_range(start_time, end_time)
        buffer = BytesIO()
        workbook.save(buffer)
        buffer.seek(0)
        filename = f"{start_time}至{end_time}在线率统计.xlsx"
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:  # noqa: BLE001
        log_error(f"导出在线率统计失败: {exc}")
        return jsonify({"success": False, "message": f"导出失败: {exc}"}), 500


@xunfang_bp.route("/export_quadrant_chart", methods=["POST"])
def export_quadrant_chart() -> Response:
    """导出象限图（ZIP，包含 PNG 和 Excel）。"""
    data = request.json or {}
    start_time = data.get("startTime")
    end_time = data.get("endTime")

    if not start_time or not end_time:
        return jsonify({"success": False, "message": "开始时间和结束时间不能为空"}), 400

    try:
        image_buffer, workbook = export_quadrant_chart_for_date_range(start_time, end_time)

        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            if image_buffer:
                image_buffer.seek(0)
                png_name = f"{start_time}至{end_time}巡防象限图.png"
                zip_file.writestr(png_name, image_buffer.read())

            excel_buffer = BytesIO()
            workbook.save(excel_buffer)
            excel_buffer.seek(0)
            xlsx_name = f"{start_time}至{end_time}巡防象限图.xlsx"
            zip_file.writestr(xlsx_name, excel_buffer.read())

        zip_buffer.seek(0)
        filename = f"{start_time}至{end_time}巡防象限图.zip"
        return send_file(zip_buffer, as_attachment=True, download_name=filename, mimetype="application/zip")
    except Exception as exc:  # noqa: BLE001
        log_error(f"导出象限图失败: {exc}")
        return jsonify({"success": False, "message": f"导出失败: {exc}"}), 500


@xunfang_bp.route("/export_police_force", methods=["POST"])
def export_police_force() -> Response:
    """导出巡防警力表 Excel。"""
    data = request.json or {}
    start_time = data.get("startTime")
    end_time = data.get("endTime")

    if not start_time or not end_time:
        return jsonify({"success": False, "message": "开始时间和结束时间不能为空"}), 400

    try:
        workbook = export_police_force_for_date_range(start_time, end_time)
        buffer = BytesIO()
        workbook.save(buffer)
        buffer.seek(0)
        filename = f"{start_time}至{end_time}巡防警力表.xlsx"
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:  # noqa: BLE001
        log_error(f"导出警力表失败: {exc}")
        return jsonify({"success": False, "message": f"导出失败: {exc}"}), 500


@xunfang_bp.route("/request_data", methods=["POST"])
@handle_errors("请求数据")
def request_data() -> Response:
    """
    代理调用外部接口并返回 JSON 数据。

    支持：
    - GET / POST
    - params 为字典或 URL 查询字符串
    - data / json 载荷
    - 结果为嵌套结构时自动抽取列表
    """
    payload = request.json or {}
    method = str(payload.get("method", "GET")).upper()
    url = payload.get("url")
    params = payload.get("params", {})
    data_body = payload.get("data")
    json_body = payload.get("json")
    headers = payload.get("headers") or {}

    if not url:
        return jsonify({"success": False, "message": "请求链接不能为空"}), 400

    session = session_manager.get_session() or requests.Session()

    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    default_headers.update(headers)

    # 解析字符串参数
    parsed_params = _ensure_mapping(params)
    parsed_data_body = _ensure_mapping(data_body)
    parsed_json_body = json_body if isinstance(json_body, (dict, list)) else _ensure_mapping(json_body)

    # GET URL 上已有 query 参数，也合并进 params
    if method == "GET":
        parsed_url = urlparse(url)
        if parsed_url.query:
            query_params = {k: v[-1] if len(v) == 1 else v for k, v in parse_qs(parsed_url.query).items()}
            parsed_params = {**query_params, **parsed_params}
            url = parsed_url._replace(query="").geturl()

    log_info(
        f"代理请求 {method} {url}, params={parsed_params}, data={parsed_data_body}, json={parsed_json_body}"
    )

    try:
        response = session.request(
            method=method,
            url=url,
            headers=default_headers,
            params=parsed_params if method == "GET" else None,
            data=parsed_data_body if method in {"POST", "PUT", "PATCH"} else None,
            json=parsed_json_body if method in {"POST", "PUT", "PATCH"} else None,
            timeout=30,
        )
    except Exception as exc:  # noqa: BLE001
        log_error(f"代理请求异常: {exc}")
        return jsonify({"success": False, "message": f"请求异常: {exc}"}), 500

    log_info(f"代理请求完成，状态码 {response.status_code}")

    if response.status_code != 200:
        return jsonify({"success": False, "message": f"请求失败，状态码 {response.status_code}"}), 400

    try:
        json_data = response.json()
    except ValueError:
        snippet = response.text[:500]
        log_warning(f"响应非 JSON 内容，截取前 500 字符: {snippet}")
        return jsonify({"success": False, "message": "响应内容不是合法的 JSON"}), 400

    flattened = extract_data_from_nested_json(json_data)
    if flattened is None:
        return jsonify({"success": False, "message": "未找到有效数据"}), 400

    if isinstance(flattened, dict):
        flattened = [flattened]

    if not isinstance(flattened, list):
        return jsonify({"success": False, "message": "数据格式不正确，期望列表或字典"}), 400

    log_info(f"数据解析完成，共 {len(flattened)} 条")
    return jsonify({"success": True, "data": flattened, "count": len(flattened)})


@xunfang_bp.route("/download_result", methods=["POST"])
def download_result() -> Response:
    """根据前端提交的数据导出 CSV 或 Excel 文件。"""
    form_data = request.form.get("data")
    if not form_data:
        return jsonify({"success": False, "message": "未收到要导出的数据"}), 400

    try:
        data_dict = json.loads(form_data)
    except json.JSONDecodeError as exc:
        return jsonify({"success": False, "message": f"数据格式错误: {exc}"}), 400

    rows = data_dict.get("data", [])
    export_format = str(data_dict.get("exportFormat", "csv")).lower()
    filename = data_dict.get("filename", f"巡防数据_{datetime.now().strftime('%Y%m%d%H%M%S')}")

    if export_format == "csv":
        return download_csv(rows, filename)
    if export_format in {"xlsx", "excel"}:
        return download_xlsx(rows, filename)

    return jsonify({"success": False, "message": f"不支持的导出格式: {export_format}"}), 400


# -----------------------------------------------------------------------------
# 辅助函数
# -----------------------------------------------------------------------------
def _ensure_mapping(data: Any) -> Dict[str, Any]:
    """将字符串或 None 参数转换为字典。"""
    if data is None or isinstance(data, dict):
        return data or {}
    if isinstance(data, str):
        data = data.strip()
        if not data:
            return {}
        return parse_form_data_string(data)
    return {}


def parse_form_data_string(form_string: str) -> Dict[str, Any]:
    """
    解析表单字符串为字典。

    支持嵌套键，例如 params[beginTime]=2025-10-01。
    """
    parsed: Dict[str, Any] = {}
    decoded = unquote_plus(form_string)
    for part in decoded.split("&"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        value = value or ""
        if "[" in key and key.endswith("]"):
            base_key, nested_key = key.split("[", 1)
            nested_key = nested_key.rstrip("]")
            parsed.setdefault(base_key, {})[nested_key] = value
        else:
            parsed[key] = value
    return parsed


def extract_data_from_nested_json(data: Any, depth: int = 6) -> Any:
    """从嵌套 JSON 中提取包含数据的列表或字典。"""
    if depth <= 0:
        return data
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        priority_keys = ["rows", "data", "list", "items", "result", "results", "records"]
        for key in priority_keys:
            if key in data:
                candidate = extract_data_from_nested_json(data[key], depth - 1)
                if candidate is not None:
                    return candidate
        for value in data.values():
            candidate = extract_data_from_nested_json(value, depth - 1)
            if candidate is not None:
                return candidate
    return data


def download_csv(data: List[Dict[str, Any]], filename: str) -> Response:
    """以 CSV 格式导出数据。"""
    output = StringIO()
    if data:
        headers = sorted({key for row in data if isinstance(row, dict) for key in row.keys()})
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        for row in data:
            writer.writerow({key: (row.get(key) if isinstance(row, dict) else "") for key in headers})
    else:
        output.write("无数据\n")

    buffer = BytesIO(output.getvalue().encode("utf-8-sig"))
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"{filename}.csv",
        mimetype="text/csv; charset=utf-8",
    )


def download_xlsx(data: List[Dict[str, Any]], filename: str) -> Response:
    """以 Excel 格式导出数据。"""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "巡防数据"

    if data:
        headers = sorted({key for row in data if isinstance(row, dict) for key in row.keys()})
        sheet.append(headers)
        for row in data:
            sheet.append([(row.get(key) if isinstance(row, dict) else "") for key in headers])
    else:
        sheet.append(["无数据"])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"{filename}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
# -----------------------------------------------------------------------------
# æ–°ç‰ˆä¸‹è½½ç»“æžœæŽ¥å£ï¼šé¿å…å‰ç«¯POSTå¤§æ•°æ®
# -----------------------------------------------------------------------------
@xunfang_bp.route("/download_result_v2", methods=["POST"])
def download_result_v2() -> Response:
    """
    æ ¹æ®å‰ç«¯æäº¤çš„â€œè¯·æ±‚å‚æ•°â€é‡æ–°è°ƒç”¨å¤–éƒ¨æŽ¥å£ï¼Œå¹¶å¯¼å‡º CSV / Excel æ–‡ä»¶ã€‚
    ä¸å†ä»Žå‰ç«¯æŽ¥æ”¶å®Œæ•´æ•°æ®é›†ï¼Œé¿å…è¯·æ±‚ä½“è¿‡å¤§å¼•å‘ 413ã€‚
    """
    form_data = request.form.get("data")
    if not form_data:
        return jsonify({"success": False, "message": "æœªæ”¶åˆ°å¯¼å‡ºå‚æ•°"}), 400

    try:
        data_dict = json.loads(form_data)
    except json.JSONDecodeError as exc:  # noqa: BLE001
        return jsonify({"success": False, "message": f"å‚æ•°æ ¼å¼é”™è¯¯: {exc}"}), 400

    method = str(data_dict.get("method", "GET")).upper()
    url = data_dict.get("url")
    params = data_dict.get("params", {})
    export_format = str(data_dict.get("exportFormat", "xlsx")).lower()
    filename = data_dict.get(
        "filename", f"å·¡é˜²æ•°æ®_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )

    if not url:
        return jsonify({"success": False, "message": "è¯·æ±‚é“¾æŽ¥ä¸èƒ½ä¸ºç©º"}), 400

    session = session_manager.get_session() or requests.Session()

    default_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    parsed_params = _ensure_mapping(params)

    if method == "GET":
        parsed_url = urlparse(url)
        if parsed_url.query:
            query_params = {
                k: (v[-1] if len(v) == 1 else v)
                for k, v in parse_qs(parsed_url.query).items()
            }
            parsed_params = {**query_params, **parsed_params}
            url = parsed_url._replace(query="").geturl()

    log_info(f"ä¸‹è½½å¯¼å‡ºï¼šä»£ç†è¯·æ±‚ {method} {url}, params={parsed_params}")

    try:
        response = session.request(
            method=method,
            url=url,
            headers=default_headers,
            params=parsed_params if method == "GET" else None,
            timeout=30,
        )
    except Exception as exc:  # noqa: BLE001
        log_error(f"ä¸‹è½½å¯¼å‡ºæ—¶ä»£ç†è¯·æ±‚å¼‚å¸¸: {exc}")
        return jsonify({"success": False, "message": f"è¯·æ±‚å¼‚å¸¸: {exc}"}), 500

    if response.status_code != 200:
        return jsonify(
            {
                "success": False,
                "message": f"è¯·æ±‚å¤±è´¥ï¼ŒçŠ¶æ€ç  {response.status_code}",
            }
        ), 400

    try:
        json_data = response.json()
    except ValueError:
        snippet = response.text[:500]
        log_warning(f"ä¸‹è½½å¯¼å‡ºæ—¶å“åº”éž JSON ï¼Œæˆªå–å‰ 500 å­—ç¬¦: {snippet}")
        return jsonify({"success": False, "message": "å“åº”å†…å®¹ä¸æ˜¯åˆæ³•çš„ JSON"}), 400

    flattened = extract_data_from_nested_json(json_data)
    if flattened is None:
        return jsonify({"success": False, "message": "æœªæ‰¾åˆ°æœ‰æ•ˆæ•°æ®"}), 400

    if isinstance(flattened, dict):
        flattened = [flattened]

    if not isinstance(flattened, list):
        return jsonify(
            {"success": False, "message": "æ•°æ®æ ¼å¼ä¸æ­£ç¡®ï¼ŒæœŸæœ›åˆ—è¡¨æˆ–å­—å…¸"}
        ), 400

    if export_format == "csv":
        return download_csv(flattened, filename)
    if export_format in {"xlsx", "excel"}:
        return download_xlsx(flattened, filename)

    return jsonify({"success": False, "message": f"ä¸æ”¯æŒçš„å¯¼å‡ºæ ¼å¼: {export_format}"}), 400

# -----------------------------------------------------------------------------
# è¾…åŠ©å‡½æ•°
