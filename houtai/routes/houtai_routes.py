from __future__ import annotations

"""
后台模块（houtai）：
- 提供Excel模板下载（包含字段：username、password、module）
- 支持上传Excel并批量导入用户与权限（密码使用Werkzeug哈希）
- 所有数据库表位于 schema: ywdata；表名：jcgkzx_user、jcgkzx_permission
"""

import io
import os
from typing import List, Tuple

from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, session, jsonify
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash
from openpyxl import Workbook, load_workbook

from gonggong.config.database import get_database_connection
from houtai.service.sms_manage_service import (
    REQUIRED_IMPORT_FIELDS,
    SmsManageService,
    build_template_workbook,
    parse_rows_from_excel,
)


houtai_bp = Blueprint("houtai", __name__, template_folder="../../templates")


def _ensure_template_on_disk(path: str) -> None:
    """如模板不存在，则在磁盘生成一个Excel导入模板（UTF-8注释，列名固定）。"""
    if os.path.exists(path):
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "用户权限导入"
    ws.append(["username", "password", "module"])  # 表头
    # 示例数据（仅作演示，可删除）
    ws.append(["admin", "admin123", "后台"])  # 后台
    ws.append(["alice", "123456", "警情"])   # 警情
    ws.append(["bob", "654321", "巡防"])     # 巡防
    ws.append(["cathy", "passw0rd", "治综"]) # 治综
    wb.save(path)


@houtai_bp.route("/")
def import_page():
    """
    导入页面：显示模板下载与文件上传表单。
    需登录后访问。
    """
    if not session.get("username"):
        return redirect(url_for("login"))
    return render_template("houtai_import.html")


@houtai_bp.route("/download_sms_template")
def download_sms_template():
    """下载短信发送管理导入模板。"""
    if not session.get("username"):
        return redirect(url_for("login"))

    wb = build_template_workbook()
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return send_file(bio, as_attachment=True, download_name="短信发送管理导入模板.xlsx")


@houtai_bp.route("/download_template")
def download_template():
    """下载Excel导入模板（同时在当前目录生成一份模板文件）。"""
    if not session.get("username"):
        return redirect(url_for("login"))

    # 在当前目录生成模板文件
    filename = "user_permission_import_template.xlsx"
    abs_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), filename)
    _ensure_template_on_disk(abs_path)

    # 同时返回内存中的模板供下载
    wb = Workbook()
    ws = wb.active
    ws.title = "用户权限导入"
    ws.append(["username", "password", "module"])  # 表头
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return send_file(bio, as_attachment=True, download_name="用户与权限导入模板.xlsx")


@houtai_bp.route("/upload", methods=["POST"])
def upload_import():
    """
    处理Excel上传并导入：
    - 逐行读取 username、password、module 字段
    - jcgkzx_user: username 唯一，密码使用Werkzeug哈希；若已存在则更新密码
    - jcgkzx_permission: (username,module) 唯一，已存在则忽略重复
    """
    if not session.get("username"):
        return redirect(url_for("login"))

    file = request.files.get("file")
    if not file:
        flash("请选择Excel文件再上传")
        return redirect(url_for("houtai.import_page"))

    filename = secure_filename(file.filename)
    if not filename.lower().endswith((".xlsx", ".xlsm")):
        flash("文件格式错误，请上传xlsx/xlsm格式的Excel")
        return redirect(url_for("houtai.import_page"))

    try:
        wb = load_workbook(file, read_only=True, data_only=True)
        ws = wb.active
        # 假设第一行为表头
        headers = [str(c.value).strip() if c.value is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1))]
        required = ["username", "password", "module"]
        for col in required:
            if col not in headers:
                flash(f"缺少必要列：{col}")
                return redirect(url_for("houtai.import_page"))
        idx = {name: headers.index(name) for name in required}

        to_users: List[Tuple[str, str]] = []
        to_perms: List[Tuple[str, str]] = []
        for r in ws.iter_rows(min_row=2):
            u = str(r[idx["username"]].value).strip() if r[idx["username"]].value is not None else ""
            p = str(r[idx["password"]].value).strip() if r[idx["password"]].value is not None else ""
            m = str(r[idx["module"]].value).strip() if r[idx["module"]].value is not None else ""
            if not u or not p or not m:
                continue
            to_users.append((u, generate_password_hash(p)))
            to_perms.append((u, m))

        if not to_users:
            flash("文件中没有有效数据")
            return redirect(url_for("houtai.import_page"))

        conn = get_database_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    # 用户表：username唯一，存在则更新密码
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS "ywdata"."jcgkzx_user" (
                            id SERIAL PRIMARY KEY,
                            username VARCHAR(50) UNIQUE NOT NULL,
                            password VARCHAR(255) NOT NULL
                        )
                    ''')
                    # 权限表：(username,module)唯一
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS "ywdata"."jcgkzx_permission" (
                            id SERIAL PRIMARY KEY,
                            username VARCHAR(50) NOT NULL,
                            module VARCHAR(50) NOT NULL,
                            UNIQUE (username, module)
                        )
                    ''')

                    # 批量导入用户
                    for u, hp in to_users:
                        cur.execute(
                            'INSERT INTO "ywdata"."jcgkzx_user" (username, password) VALUES (%s, %s)\n'
                            'ON CONFLICT (username) DO UPDATE SET password = EXCLUDED.password',
                            (u, hp),
                        )

                    # 批量导入权限
                    for u, m in to_perms:
                        cur.execute(
                            'INSERT INTO "ywdata"."jcgkzx_permission" (username, module) VALUES (%s, %s)\n'
                            'ON CONFLICT (username, module) DO NOTHING',
                            (u, m),
                        )

            flash(f"导入完成，用户{len(to_users)}条，权限{len(to_perms)}条")
        finally:
            conn.close()

        return redirect(url_for("houtai.import_page"))
    except Exception as exc:
        flash(f"导入失败：{exc}")
        return redirect(url_for("houtai.import_page"))


@houtai_bp.route("/upload_sms", methods=["POST"])
def upload_sms():
    """处理短信发送管理 Excel 批量导入（按 sspcsdm+xm 覆盖）。"""
    if not session.get("username"):
        return redirect(url_for("login"))

    file = request.files.get("file")
    if not file:
        flash("请选择Excel文件再上传")
        return redirect(url_for("houtai.import_page"))

    filename = secure_filename(file.filename)
    if not filename.lower().endswith((".xlsx", ".xlsm")):
        flash("文件格式错误，请上传xlsx/xlsm格式的Excel")
        return redirect(url_for("houtai.import_page"))

    try:
        wb = load_workbook(file, read_only=True, data_only=True)
        ws = wb.active
        rows, errors = parse_rows_from_excel(ws)

        if errors:
            flash(f"导入失败：{errors[0]}")
            return redirect(url_for("houtai.import_page"))
        if not rows:
            flash("文件中没有有效数据")
            return redirect(url_for("houtai.import_page"))

        affected = SmsManageService.save_many(rows)
        flash(
            "短信发送管理导入完成，影响记录 "
            f"{affected} 条（按 sspcsdm+xm 覆盖）。"
            f"模板必填列: {', '.join(REQUIRED_IMPORT_FIELDS)}；xh 为展示列可为空。"
        )
        return redirect(url_for("houtai.import_page"))
    except Exception as exc:
        flash(f"导入失败：{exc}")
        return redirect(url_for("houtai.import_page"))


@houtai_bp.route("/api/sms/list")
def sms_list_api():
    """短信发送管理列表（分页）。"""
    if not session.get("username"):
        return jsonify({"success": False, "message": "请先登录"}), 401

    keyword = (request.args.get("keyword") or "").strip()
    page = int(request.args.get("page") or 1)
    page_size = int(request.args.get("page_size") or 20)

    try:
        rows, total = SmsManageService.list_rows(keyword=keyword, page=page, page_size=page_size)
        return jsonify(
            {
                "success": True,
                "data": rows,
                "total": total,
                "page": page,
                "page_size": page_size,
            }
        )
    except Exception as exc:
        return jsonify({"success": False, "message": f"查询失败: {exc}"}), 500


@houtai_bp.route("/api/sms/save", methods=["POST"])
def sms_save_api():
    """短信发送管理单条新增/修改（按 sspcsdm+xm 覆盖）。"""
    if not session.get("username"):
        return jsonify({"success": False, "message": "请先登录"}), 401

    payload = request.get_json(silent=True) or {}
    try:
        SmsManageService.save_one(payload)
        return jsonify({"success": True, "message": "保存成功"})
    except Exception as exc:
        return jsonify({"success": False, "message": f"保存失败: {exc}"}), 400


@houtai_bp.route("/api/sms/batch_delete", methods=["POST"])
def sms_batch_delete_api():
    """短信发送管理批量删除（按 sspcsdm+xm 删除）。"""
    if not session.get("username"):
        return jsonify({"success": False, "message": "请先登录"}), 401

    payload = request.get_json(silent=True) or {}
    rows = payload.get("rows") or []
    keys = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        keys.append((str(row.get("sspcsdm") or "").strip(), str(row.get("xm") or "").strip()))

    try:
        deleted = SmsManageService.delete_by_keys(keys)
        return jsonify({"success": True, "message": f"已删除 {deleted} 条"})
    except Exception as exc:
        return jsonify({"success": False, "message": f"删除失败: {exc}"}), 500

