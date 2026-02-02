"""
数据提取模块路由。

能力：
1) 上传 xlsx/csv，解析列名与预览
2) 调用本地 llama.cpp（OpenAI 兼容接口）批量抽取字段并导出
3) 基于权限表 ywdata.jcgkzx_permission 做访问控制（module='数据提取'）
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, Optional

from flask import Blueprint, Response, abort, jsonify, render_template, request, send_file
from flask import session as flask_session, redirect, url_for
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from gonggong.config.database import get_database_connection
from gonggong.utils.error_handler import handle_errors, log_info
from shuju_tiqu.service.extract_service import extract_fields, parse_targets
from shuju_tiqu.service.file_io import FileFormatError, dataframe_preview, export_dataframe_bytes, read_table_file


shuju_tiqu_bp = Blueprint("shuju_tiqu", __name__, template_folder="../templates")


@dataclass
class UploadEntry:
    path: str
    filename: str
    created_at: float


UPLOAD_STORE: Dict[str, UploadEntry] = {}


def _uploads_dir() -> str:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    base = os.path.join(project_root, "tmp", "shuju_tiqu")
    os.makedirs(base, exist_ok=True)
    return base


def _cleanup_uploads() -> None:
    ttl = int(os.getenv("LLM_UPLOAD_TTL_SECONDS") or "7200")
    now = time.time()
    remove_ids = [k for k, v in UPLOAD_STORE.items() if now - v.created_at > ttl]
    for upload_id in remove_ids:
        entry = UPLOAD_STORE.pop(upload_id, None)
        if not entry:
            continue
        try:
            if os.path.exists(entry.path):
                os.remove(entry.path)
        except Exception:
            continue


def _get_file_ext(filename: str) -> str:
    return os.path.splitext(filename or "")[1].lower().lstrip(".")


def _validate_upload_file(file: Optional[FileStorage]) -> FileStorage:
    if not file or not file.filename:
        raise ValueError("请选择文件")
    ext = _get_file_ext(file.filename)
    if ext not in ("xlsx", "csv"):
        raise FileFormatError("仅支持 xlsx/csv 文件")
    return file


@shuju_tiqu_bp.before_request
def _ensure_access() -> Optional[Response]:
    if not flask_session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (flask_session["username"], "数据提取"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)
    return None


@shuju_tiqu_bp.route("/")
def index() -> str:
    return render_template("shuju_tiqu.html")


@shuju_tiqu_bp.route("/api/upload", methods=["POST"])
@handle_errors("上传并解析")
def api_upload() -> Response:
    _cleanup_uploads()
    file = _validate_upload_file(request.files.get("file"))
    safe_name = secure_filename(file.filename)
    if not safe_name:
        ext = _get_file_ext(file.filename)
        safe_name = f"upload.{ext or 'dat'}"
    upload_id = uuid.uuid4().hex
    save_path = os.path.join(_uploads_dir(), f"{upload_id}_{safe_name}")
    file.save(save_path)

    df, columns = read_table_file(save_path)
    preview_rows = dataframe_preview(df, n=20)

    UPLOAD_STORE[upload_id] = UploadEntry(path=save_path, filename=file.filename, created_at=time.time())
    log_info(f"数据提取 upload ok: id={upload_id} filename={file.filename} rows={len(df)} cols={len(columns)}")
    return jsonify(
        {
            "success": True,
            "upload_id": upload_id,
            "filename": file.filename,
            "columns": columns,
            "preview_rows": preview_rows,
            "row_count": int(len(df)),
        }
    )


@shuju_tiqu_bp.route("/api/extract", methods=["POST"])
@handle_errors("提取并导出")
def api_extract() -> Response:
    _cleanup_uploads()
    payload: Dict[str, Any] = request.json or {}
    upload_id = str(payload.get("upload_id", "")).strip()
    if not upload_id:
        raise ValueError("upload_id 不能为空")
    entry = UPLOAD_STORE.get(upload_id)
    if not entry:
        raise ValueError("upload_id 无效或已过期，请重新上传")

    source_columns = payload.get("source_columns") or []
    if not isinstance(source_columns, list):
        raise ValueError("source_columns 参数格式错误")
    source_columns = [str(c).strip() for c in source_columns if str(c).strip()]

    targets = parse_targets(payload.get("targets"))
    user_prompt = str(payload.get("prompt", "") or "")
    out_format = str(payload.get("out_format", "xlsx") or "xlsx").lower()

    df, _ = read_table_file(entry.path)
    default_batch_size = 1 if len(df) <= 200 else 10
    batch_size = int(payload.get("batch_size") or os.getenv("LLM_EXTRACT_BATCH_SIZE") or default_batch_size)
    concurrency = int(payload.get("concurrency") or os.getenv("LLM_EXTRACT_CONCURRENCY") or "2")
    df = extract_fields(
        df,
        source_columns=source_columns,
        targets=targets,
        user_prompt=user_prompt,
        batch_size=batch_size,
        concurrency=concurrency,
    )

    data, mimetype = export_dataframe_bytes(df, out_format=out_format)

    stem = os.path.splitext(os.path.basename(entry.filename))[0]
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    filename = f"{stem}_llm_{ts}.{out_format}"

    bio = BytesIO(data)
    bio.seek(0)
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype=mimetype,
    )
