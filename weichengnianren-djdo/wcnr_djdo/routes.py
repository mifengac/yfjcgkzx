from __future__ import annotations

import io
import logging
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from flask import (
    Blueprint,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from gonggong.config.database import get_database_connection

from . import exporters, service
from .sms import SEND_SMS_PASSWORD, send_dashboard_sms


wcnr_djdo_bp = Blueprint(
    "wcnr_djdo",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


@wcnr_djdo_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "未成年人(打架斗殴)"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)


def _parse_dt(val: str) -> datetime:
    val = (val or "").strip()
    # 兼容前端不带秒的情况：如 "2026-01-18 00:00"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    raise ValueError(f"time data '{val}' does not match format '%Y-%m-%d %H:%M:%S'")


def _default_range() -> tuple[datetime, datetime]:
    now = datetime.now()
    end_time = datetime(now.year, now.month, now.day, 0, 0, 0)
    start_time = end_time - timedelta(days=7)
    return start_time, end_time


def _get_range_from_request() -> tuple[datetime, datetime]:
    start = request.args.get("start_time")
    end = request.args.get("end_time")
    if not start or not end:
        return _default_range()
    return _parse_dt(start), _parse_dt(end)


def _load_import_func():
    """
    动态加载 `weichengnianren-djdo/0125_wcnr_sfzxx_import.py` 中的导入函数，
    以便复用“导入送校数据”的逻辑。
    """
    import importlib.util

    script_path = Path(__file__).resolve().parents[1] / "0125_wcnr_sfzxx_import.py"
    spec = importlib.util.spec_from_file_location("wcnr_sfzxx_import", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("无法加载导入脚本")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    func = getattr(mod, "import_sfzxx_file", None)
    if func is None:
        raise RuntimeError("导入脚本缺少 import_sfzxx_file()")
    return func


@wcnr_djdo_bp.get("/")
def index():
    start_time, end_time = _default_range()
    return render_template(
        "wcnr_djdo/index.html",
        default_start=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        default_end=end_time.strftime("%Y-%m-%d %H:%M:%S"),
    )


@wcnr_djdo_bp.get("/api/metric/<metric_key>")
def api_metric(metric_key: str):
    try:
        s, e = _get_range_from_request()
        result = service.get_metric(metric_key, s, e)
        return jsonify(
            {
                "success": True,
                "title": result.title,
                "series": result.series,
                "chart_rows": result.chart_rows,
                "detail_rows": result.detail_rows,
                "count": len(result.detail_rows),
            }
        )
    except KeyError:
        return jsonify({"success": False, "message": f"未知指标: {metric_key}"}), 400
    except Exception as exc:
        logging.exception("metric api failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@wcnr_djdo_bp.get("/api/export/xlsx")
def api_export_metric_xlsx():
    metric_key = request.args.get("metric")
    if not metric_key:
        return jsonify({"success": False, "message": "缺少参数 metric"}), 400
    try:
        s, e = _get_range_from_request()
        result = service.get_metric(metric_key, s, e)
        data, filename = exporters.build_metric_xlsx(result, s, e)
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:
        logging.exception("export xlsx failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@wcnr_djdo_bp.get("/api/export/details")
def api_export_details():
    try:
        s, e = _get_range_from_request()
        results = [service.get_metric(k, s, e) for k in service.METRICS.keys()]
        data, filename = exporters.build_details_xlsx(results, s, e)
        return send_file(
            io.BytesIO(data),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:
        logging.exception("export details failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@wcnr_djdo_bp.get("/api/export/overview_pdf")
def api_export_overview_pdf():
    try:
        s, e = _get_range_from_request()
        results = [service.get_metric(k, s, e) for k in service.METRICS.keys()]
        data, filename = exporters.build_overview_pdf(results, s, e)
        return send_file(io.BytesIO(data), as_attachment=True, download_name=filename, mimetype="application/pdf")
    except Exception as exc:
        logging.exception("export overview pdf failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@wcnr_djdo_bp.post("/api/import/sx_xls")
def api_import_sx_xls():
    file = request.files.get("file")
    if not file:
        return jsonify({"success": False, "message": "未选择文件"}), 400
    filename = str(file.filename or "")
    if not filename.lower().endswith(".xls"):
        return jsonify({"success": False, "message": "仅支持 xls 格式文件"}), 400

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xls") as tmp:
            tmp.write(file.read())
            tmp_path = Path(tmp.name)

        import_func = _load_import_func()
        stats = import_func(tmp_path, sheet_name="累计招生", truncate=False)
        return jsonify({"success": True, "message": "导入成功", "stats": stats})
    except Exception as exc:
        logging.exception("import sx xls failed")
        return jsonify({"success": False, "message": str(exc)}), 500
    finally:
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


@wcnr_djdo_bp.post("/api/sms/send")
def api_send_sms():
    payload = request.get_json(silent=True) or {}
    password = str(payload.get("password") or "")
    if password != SEND_SMS_PASSWORD:
        return jsonify({"success": False, "message": "密码错误"}), 403

    try:
        start = payload.get("start_time")
        end = payload.get("end_time")
        if start and end:
            s, e = _parse_dt(str(start)), _parse_dt(str(end))
        else:
            s, e = _default_range()

        metric_order = ["jq_za", "jzjy", "sx_sx", "zljqjh", "cs_fa", "ng_zf"]
        results = [service.get_metric(k, s, e) for k in metric_order]

        out = send_dashboard_sms(start_time=s, end_time=e, results=results)
        return jsonify(
            {
                "success": True,
                "inserted": out.get("inserted", 0),
                "eid": out.get("eid"),
                "mobiles": out.get("mobiles", []),
                "preview": str(out.get("content") or "")[:300],
            }
        )
    except Exception as exc:
        logging.exception("send sms failed")
        return jsonify({"success": False, "message": str(exc)}), 500
