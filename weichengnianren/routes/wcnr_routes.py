from __future__ import annotations

"""
未成年人模块路由。

功能说明：
- 访问前校验当前登录用户是否拥有“未成年人”模块权限；
- 首页支持条件筛选（数据类型、报警时间范围、立案日期范围）与分页；
- 支持将当前筛选结果导出为 CSV / Excel，文件名为时间戳；
- 列表中“姓名”字段在存在证件照地址时可点击下载证件照；
- “法律文书JSON列表”字段展示为可点击的文书名称列表，点击跳转到对应 URL。
"""

from datetime import datetime
from io import BytesIO, StringIO
from typing import Dict, List, Tuple

from flask import (
    Blueprint,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from gonggong.config.database import execute_query, get_database_connection
from weichengnianren.config import COLUMN_DEFINITIONS


weichengnianren_bp = Blueprint("weichengnianren", __name__, template_folder="../templates")


@weichengnianren_bp.before_request
def _check_access() -> None:
    """
    访问控制：
    - 需已登录；
    - 且在 jcgkzx_permission 中拥有 module = '未成年人' 的权限。
    """
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "未成年人"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)


def _build_base_sql() -> str:
    """
    构建未成年人模块的基础 SQL。

    说明：
    - 使用 WITH 语句拼接未成年警情、未成年案件及其人员、文书与处罚信息；
    - 最外层 SELECT 使用别名（中文列名），便于模板渲染；
    - 由外层再包一层 SELECT * FROM (...) 做筛选与分页。
    """
    return """
WITH
    -- A. 涉及未成年人的警情（根据警情标注）
    juv_jq AS (
        SELECT
            j.caseno                  AS jqbh,
            j.calltime                AS jq_calltime,
            j.neworicharasubclassname AS jq_yssx,
            j.casemark                AS jq_casemark,
            j.dutydeptno              AS jq_dutydeptno,
            j.dutydeptname            AS jq_dutydeptname,
            j.occuraddress            AS jq_occuraddress,
            j.casecontents            AS jq_casecontents,
            j.replies                 AS jq_replies
        FROM ywdata.zq_kshddpt_dsjfx_jq j
        WHERE j.casemark LIKE '%%未成年%%'
    ),

    -- B. 涉及未成年人的案件：先从案件+mv_minor_person 得到“未成年案件集合”
    minor_case AS (
        SELECT DISTINCT
            a."案件编号",
            a."地区",
            a."案件名称",
            a."警情编号",
            a."简要案情",
            a."办案单位名称",
            a."立案日期",
            a."案由",
            a."案件类型",
            a."案件状态名称"
        FROM ywdata.mv_zfba_all_ajxx a
        JOIN ywdata.mv_minor_person mp
          ON mp.asjbh = a."案件编号"
    ),

    -- C. 从 mv_minor_person 出发，按【案件编号 + 人员编号】聚合文书 + 行政处罚
    ws_xz_agg AS MATERIALIZED (
        SELECT
            mp.asjbh,
            mp.anjxgrybh,

            -- 文书聚合
            string_agg(
                w.flws_zlmc,
                ',' ORDER BY w.wsywxxid
            ) AS ws_name_list,        -- 文书种类名称列表

            string_agg(
                w.flwslldz,
                ',' ORDER BY w.wsywxxid
            ) AS ws_url_list,         -- 文书浏览地址列表

            json_agg(
                json_build_object(
                    'name', w.flws_zlmc,
                    'url',  w.flwslldz
                )
                ORDER BY w.wsywxxid
            ) AS ws_json_list,        -- JSON数组 [{name,url},...]

            -- 行政处罚聚合
            MAX(x.jlts)    AS jlts,       -- 拘留天数
            MAX(x.fk)      AS fk,         -- 罚款金额
            MAX(x.sfjlbzx) AS sfjlbzx     -- 是否拘留不执行

        FROM ywdata.mv_minor_person mp
        LEFT JOIN ywdata.zfba_ws_001 w
               ON w.asjbh      = mp.asjbh
              AND w.flws_dxbh  = mp.anjxgrybh
        LEFT JOIN ywdata.zfba_aj_009 x
               ON x.wsywxxid   = w.wsywxxid

        GROUP BY
            mp.asjbh,
            mp.anjxgrybh
    ),

    -- D. 涉及未成年人的案件明细（按案件+人员 一行）
    juv_case AS (
        SELECT
            a."案件编号",
            a."地区",
            a."案件名称",
            a."警情编号",
            a."简要案情",
            a."办案单位名称",
            a."立案日期",
            a."案由",
            a."案件类型",
            a."案件状态名称",

            mp.xm,
            mp.xbmc,
            mp.age_years,
            mp.zjhm,
            mp.hjd_xz,
            mp.xzd_xz,
            mp.zjzpckdz,
            mp.anjxgrybh,
            mp.role_names,

            wa.ws_name_list,
            wa.ws_url_list,
            wa.ws_json_list,
            wa.jlts,
            wa.fk,
            wa.sfjlbzx
        FROM minor_case a
        JOIN ywdata.mv_minor_person mp
          ON mp.asjbh = a."案件编号"
        LEFT JOIN ws_xz_agg wa
          ON wa.asjbh      = a."案件编号"
         AND wa.anjxgrybh  = mp.anjxgrybh
    )

    -- E. 最终：FULL JOIN，把“未成年警情”与“未成年案件”合并
    SELECT
        -- 统一警情编号
        COALESCE(j.jqbh, c."警情编号")          AS "警情编号",

        -- 警情信息（如果案件无警情，这些字段为 NULL）
        j.jq_calltime                           AS "报警时间",
        j.jq_yssx                               AS "原始警情性质",
        j.jq_casemark                           AS "警情标注",
        j.jq_dutydeptno                         AS "管辖单位代码",
        j.jq_dutydeptname                       AS "管辖单位名称",
        j.jq_occuraddress                       AS "警情地址",
        j.jq_casecontents                       AS "报警内容",
        j.jq_replies                            AS "处警回复",

        -- 案件信息（如果是“只警情无案件”，这些字段为 NULL）
        c."案件编号",
        c."地区",
        c."案件名称",
        c."简要案情",
        c."办案单位名称",
        c."立案日期",
        c."案由",
        c."案件类型",
        c."案件状态名称",

        -- 案件人员（只对有案件的行有值）
        c.xm                                    AS "姓名",
        c.xbmc                                  AS "性别",
        c.age_years                             AS "年龄",
        c.zjhm                                  AS "证件号码",
        c.hjd_xz                                AS "户籍地",
        c.xzd_xz                                AS "现住址",
        c.zjzpckdz                              AS "证件照",
        c.anjxgrybh                             AS "人员编号",
        c.role_names                            AS "角色名称",

        -- 文书+处罚
        c.ws_name_list                          AS "法律文书种类名称",
        c.ws_url_list                           AS "法律文书浏览地址",
        c.ws_json_list                          AS "法律文书JSON列表",
        c.jlts                                  AS "拘留天数",
        c.fk                                    AS "罚款金额",
        c.sfjlbzx                               AS "是否拘留不执行",

        -- 关联类型：方便前端区分是“只有警情、只有案件、还是都有”
        CASE
            WHEN j.jqbh IS NOT NULL AND c."案件编号" IS NOT NULL THEN '警情+案件'
            WHEN j.jqbh IS NOT NULL AND c."案件编号" IS NULL     THEN '仅警情'
            WHEN j.jqbh IS NULL     AND c."案件编号" IS NOT NULL THEN '仅案件'
            ELSE '未知'
        END                                      AS "关联类型"

    FROM juv_jq j
    FULL JOIN juv_case c
      ON c."警情编号" = j.jqbh
    """


def _build_filtered_sql(
    data_types: List[str],
    start_bjsj: str | None,
    end_bjsj: str | None,
    start_larq: str | None,
    end_larq: str | None,
    limit: int | None,
    offset: int | None,
) -> Tuple[str, List[object]]:
    """
    在基础 SQL 外包一层，拼接 WHERE 条件和分页，并附带 total_count 统计。
    """
    base_sql = _build_base_sql().strip().rstrip(";")
    sql = f"SELECT *, COUNT(*) OVER() AS total_count FROM ({base_sql}) t WHERE 1=1"
    params: List[object] = []

    if data_types:
        sql += ' AND "关联类型" = ANY(%s)'
        params.append(data_types)

    if start_bjsj:
        sql += ' AND "报警时间" >= %s'
        params.append(start_bjsj)
    if end_bjsj:
        sql += ' AND "报警时间" <= %s'
        params.append(end_bjsj)

    if start_larq:
        sql += ' AND "立案日期" >= %s'
        params.append(start_larq)
    if end_larq:
        sql += ' AND "立案日期" <= %s'
        params.append(end_larq)

    # 统一按报警时间倒序（为空的排最后）
    sql += ' ORDER BY "报警时间" DESC NULLS LAST'

    if limit is not None and limit > 0 and offset is not None and offset >= 0:
        sql += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    return sql, params


def _parse_ws_json_list(rows: List[Dict[str, object]]) -> None:
    """
    解析“法律文书JSON列表”字段，将其转换为 Python 对象列表，方便模板渲染。
    """
    import json

    for row in rows:
        raw = row.get("法律文书json列表")
        parsed = []
        if raw:
            try:
                # 若数据库驱动已经将 JSON 转为 Python 对象，则直接使用
                if isinstance(raw, (list, tuple)):
                    parsed = list(raw)
                else:
                    parsed = json.loads(raw)
            except Exception:
                parsed = []
        row["法律文书JSON列表_解析"] = parsed


def _query_cases(
    data_types: List[str],
    start_bjsj: str | None,
    end_bjsj: str | None,
    start_larq: str | None,
    end_larq: str | None,
    page: int,
    page_size: int | None,
) -> Tuple[List[Dict[str, object]], int]:
    """
    执行查询并返回结果与总条数。
    """
    limit = page_size if page_size and page_size > 0 else None
    offset = ((page - 1) * page_size) if page_size and page_size > 0 else None

    sql, params = _build_filtered_sql(data_types, start_bjsj, end_bjsj, start_larq, end_larq, limit, offset)
    rows = execute_query(sql, tuple(params))

    total = 0
    if rows:
        total = rows[0].get("total_count", 0) or 0
        for row in rows:
            row.pop("total_count", None)

    _parse_ws_json_list(rows)
    return rows, int(total)


@weichengnianren_bp.route("/")
def index() -> str:
    """
    未成年人模块首页。

    - 默认加载基础 SQL 的查询结果；
    - 根据前端传入的筛选条件拼接 WHERE；
    - 支持分页：page / page_size；page_size 支持 20/50/100/全部。
    """
    # 多选数据类型：警情+案件 / 仅警情 / 仅案件
    data_types = request.args.getlist("data_types")
    # 时间范围使用 ISO 字符串（datetime-local 的值），直接传入数据库进行比较
    start_bjsj = request.args.get("start_bjsj") or None
    end_bjsj = request.args.get("end_bjsj") or None
    start_larq = request.args.get("start_larq") or None
    end_larq = request.args.get("end_larq") or None

    page_str = request.args.get("page", "1")
    page_size_str = request.args.get("page_size", "20")
    try:
        page = max(int(page_str), 1)
    except ValueError:
        page = 1

    if page_size_str == "all":
        page_size: int | None = None
    else:
        try:
            page_size_val = int(page_size_str)
            page_size = page_size_val if page_size_val > 0 else 20
        except ValueError:
            page_size = 20

    rows, total = _query_cases(
        data_types=data_types,
        start_bjsj=start_bjsj,
        end_bjsj=end_bjsj,
        start_larq=start_larq,
        end_larq=end_larq,
        page=page,
        page_size=page_size,
    )

    total_pages = 1
    if page_size and page_size > 0:
        total_pages = (total + page_size - 1) // page_size if total > 0 else 1

    return render_template(
        "weichengnianren.html",
        columns=COLUMN_DEFINITIONS,
        rows=rows,
        total=total,
        page=page,
        page_size=page_size_str,
        total_pages=total_pages,
        data_types_selected=data_types,
        start_bjsj=start_bjsj or "",
        end_bjsj=end_bjsj or "",
        start_larq=start_larq or "",
        end_larq=end_larq or "",
    )


def _download_csv(rows: List[Dict[str, object]], filename: str) -> Response:
    """以 CSV 形式导出数据，包含所有字段。"""
    output = StringIO()
    if rows:
        headers = list(rows[0].keys())
        # 导出时不包含解析用的附加列
        headers = [h for h in headers if h not in ("法律文书JSON列表_解析",)]
        writer = csv.DictWriter(output, fieldnames=headers)  # type: ignore[name-defined]
        writer.writeheader()
        for row in rows:
            writer.writerow({key: (row.get(key) or "") for key in headers})
    else:
        output.write("无数据\n")

    buffer = BytesIO(output.getvalue().encode("utf-8-sig"))
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv; charset=utf-8",
    )


def _download_excel(rows: List[Dict[str, object]], filename: str) -> Response:
    """以 Excel 形式导出数据，包含所有字段。"""
    workbook = Workbook()  # type: ignore[name-defined]
    sheet = workbook.active
    sheet.title = "未成年人数据"

    if rows:
        headers = list(rows[0].keys())
        headers = [h for h in headers if h not in ("法律文书JSON列表_解析",)]
        sheet.append(headers)
        for row in rows:
            cleaned_row: List[object] = []
            for key in headers:
                value = row.get(key)
                # 避免将 list/dict 直接写入 Excel，统一转为字符串
                if isinstance(value, (list, dict)):
                    value = json.dumps(value, ensure_ascii=False)  # type: ignore[name-defined]
                if value is None:
                    value = ""
                cleaned_row.append(value)
            sheet.append(cleaned_row)
    else:
        sheet.append(["无数据"])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@weichengnianren_bp.route("/download")
def download() -> Response:
    """
    导出当前筛选条件下的全部数据。

    - format=csv / excel；
    - 文件名为时间戳（YYYYMMDDHHMMSS）加扩展名。
    """
    data_types = request.args.getlist("data_types")
    start_bjsj = request.args.get("start_bjsj") or None
    end_bjsj = request.args.get("end_bjsj") or None
    start_larq = request.args.get("start_larq") or None
    end_larq = request.args.get("end_larq") or None

    export_format = (request.args.get("format") or "csv").lower()

    # 下载时不做分页，page_size=None
    rows, _ = _query_cases(
        data_types=data_types,
        start_bjsj=start_bjsj,
        end_bjsj=end_bjsj,
        start_larq=start_larq,
        end_larq=end_larq,
        page=1,
        page_size=None,
    )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if export_format == "excel":
        return _download_excel(rows, f"{timestamp}.xlsx")
    return _download_csv(rows, f"{timestamp}.csv")


# 为类型检查抑制未定义名称错误所需的导入
import csv  # noqa: E402  # isort: skip
from openpyxl import Workbook  # noqa: E402  # isort: skip
import json  # noqa: E402  # isort: skip
