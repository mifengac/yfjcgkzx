# 服务层 - 警情案件总览
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional, Tuple

from gonggong.config.database import execute_query


BASE_SQL = """
WITH base_jq AS (
        SELECT
            jq.caseno,
            jq.calltime,
            jq.cmdname,
            jq.dutydeptname,
            jq.occuraddress,
            jq.replies,
            jq.lngoflocate,
            jq.latoflocate,
            jq.casemarkok,
            jq.leixing
        FROM ywdata.v_jq_optimized jq
        WHERE 1=1
{base_filters}
        ),
        aj AS (
        SELECT a.*
        FROM ywdata.mv_zfba_all_ajxx a
        JOIN base_jq b
        ON b.caseno = a."警情编号"
        ),
        aj_ids AS (
        SELECT DISTINCT a."案件编号"
        FROM aj a
        WHERE a."案件编号" IS NOT NULL
        ),
        ry_agg AS (
        SELECT
            r.asjbh,
            string_agg(
            (COALESCE(r.xm, '') || ',' || '年龄:' || COALESCE(r.nl::text, '') || '(' || COALESCE(r.asjxgry_jsmc, '') || ')'),
            ';'
            ) AS ry_list
        FROM ywdata.zfba_ry_002 r
        JOIN aj_ids a
        ON a."案件编号" = r.asjbh
        GROUP BY r.asjbh
        ),
        ws_agg AS (
        SELECT
            w.asjbh,
            json_agg(
            json_build_object('name', w.flws_zlmc, 'url', w.flwslldz)
            ORDER BY w.wsywxxid
            ) AS ws_json_list,
            count(*) FILTER (WHERE w.flws_zlmc LIKE '%%逮捕申请%%') AS daibu_cnt,
            count(*) FILTER (WHERE w.flws_zlmc LIKE '%%起诉意见书%%') AS qisu_cnt,
            count(*) FILTER (WHERE w.flws_dxlxdm = '01' AND w.flws_zlmc LIKE '%%起诉意见书%%') AS yisong_cnt
        FROM ywdata.zfba_ws_001 w
        JOIN aj_ids a
        ON a."案件编号" = w.asjbh
        WHERE w.flws_lzztdm = '04'
        GROUP BY w.asjbh
        ),
        aj009_agg AS (
        SELECT
            a.asjbh,
            count(*) FILTER (WHERE a.jlts::text ~ '^[0-9]+$' AND a.jlts::NUMERIC > 0) AS jvliu_cnt,
            count(*) FILTER (WHERE a.fk::text ~ '^[0-9]+$' AND a.fk::NUMERIC > 0) AS fakuan_cnt,
            count(*) FILTER (WHERE a.sfjg::text ~ '^[0-9]+$' AND a.sfjg::NUMERIC > 0) AS jinggao_cnt
        FROM ywdata.zfba_aj_009 a
        JOIN aj_ids aj
        ON aj."案件编号" = a.asjbh
        GROUP BY a.asjbh
        )
        SELECT
        b.caseno                               AS 警情编号,
        b.leixing                              AS 类型,
        b.calltime                             AS 报警时间,
        b.cmdname                              AS 地区,
        b.dutydeptname                         AS 派出所,
        b.occuraddress                         AS 警情地址,
        b.replies                              AS 处警情况,
        b.lngoflocate                          AS 经度,
        b.latoflocate                          AS 纬度,
        b.casemarkok                           AS 警情标注,
        a."案件编号"                            AS 案件编号,
        a."案件名称"                            AS 案件名称,
        a."简要案情"                            AS 简要案情,
        a."案件类型"                            AS 案件类型,
        a."立案日期"                            AS 立案日期,
        a."案由"                                AS 案由,
        a."案件状态名称"                        AS 案件状态名称,
        r.ry_list                              AS 人员列表,
        w.ws_json_list                         AS 文书列表,
        COALESCE(w.daibu_cnt, 0)               AS 逮捕人数,
        COALESCE(w.qisu_cnt, 0)                AS 起诉人数,
        COALESCE(w.yisong_cnt, 0)              AS 移送人数,
        COALESCE(a9.jvliu_cnt, 0)              AS 拘留人数,
        COALESCE(a9.fakuan_cnt, 0)             AS 罚款人数,
        COALESCE(a9.jinggao_cnt, 0)            AS 警告人数
        FROM base_jq b
        LEFT JOIN aj a
        ON b.caseno = a."警情编号"
        LEFT JOIN ry_agg r
        ON a."案件编号" = r.asjbh
        LEFT JOIN ws_agg w
        ON a."案件编号" = w.asjbh
        LEFT JOIN aj009_agg a9
        ON a."案件编号" = a9.asjbh
WHERE 1=1
"""



DETAIL_COLUMNS = [
    "警情编号",
    "类型",
    "报警时间",
    "地区",
    "派出所",
    "警情地址",
    "处警情况",
    "经度",
    "纬度",
    "警情标注",
    "案件编号",
    "案件名称",
    "简要案情",
    "案件类型",
    "立案日期",
    "案由",
    "案件状态名称",
    "人员列表",
    "文书列表",
    "逮捕人数",
    "起诉人数",
    "移送人数",
    "拘留人数",
    "罚款人数",
    "警告人数",
]


def _format_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def _clean_chujing_status(value):
    if not value:
        return "未反馈"
    text = str(value)
    for key in ("【结警反馈】", "【过程反馈】", "处理结果说明"):
        idx = text.find(key)
        if idx != -1:
            after = text[idx:]
            import re

            parts = re.split(r"\r?\n\s*\r?\n", after, 1)
            return parts[0].strip()
    if "关联重复报警" in text:
        return "重复报警"
    return "未反馈"


class JqajzlService:
    def _build_query(
        self,
        case_types: Optional[Iterable[str]],
        start_time: Optional[str],
        end_time: Optional[str],
        region: Optional[str],
        status_name: Optional[str],
        require_case: bool,
    ) -> Tuple[str, List[object]]:
        params: List[object] = []
        base_filters = ""

        if start_time and end_time:
            base_filters += " AND jq.calltime BETWEEN %s AND %s"
            params.extend([start_time, end_time])

        if case_types:
            base_filters += " AND jq.leixing = ANY(%s)"
            params.append(list(case_types))

        sql = BASE_SQL.format(base_filters=base_filters)

        if region:
            sql += " AND b.cmdname = %s"
            params.append(region)

        if require_case:
            sql += ' AND a."案件编号" IS NOT NULL'

        if status_name:
            sql += ' AND a."案件状态名称" = %s'
            params.append(status_name)

        return sql, params

    def _fetch_base_data(
        self,
        case_types: Optional[Iterable[str]],
        start_time: Optional[str],
        end_time: Optional[str],
        region: Optional[str] = None,
        status_name: Optional[str] = None,
        require_case: bool = False,
    ) -> List[dict]:
        sql, params = self._build_query(
            case_types, start_time, end_time, region, status_name, require_case
        )
        data = execute_query(sql, tuple(params) if params else None)

        for row in data:
            for key, value in row.items():
                row[key] = _format_value(value)
        return data

    def get_summary(
        self, case_types: Optional[Iterable[str]], start_time: str, end_time: str
    ) -> dict:
        data = self._fetch_base_data(case_types, start_time, end_time)

        status_values = set()
        for row in data:
            status = row.get("案件状态名称")
            if status:
                status_values.add(status)
        status_columns = sorted(status_values)

        columns = ["地区", "警情数", "案件数"] + status_columns

        summary_map = {}
        summary_order: List[str] = []
        status_sets = {}

        for row in data:
            region = row.get("地区") or "未知"
            if region not in summary_map:
                summary_map[region] = {"警情数": 0, "案件数": 0}
                status_sets[region] = {status: set() for status in status_columns}
                summary_order.append(region)

            if row.get("警情编号") is not None:
                summary_map[region]["警情数"] += 1

            case_no = row.get("案件编号")
            if case_no:
                summary_map[region]["案件数"] += 1

            status = row.get("案件状态名称")
            if status and case_no and status in status_sets[region]:
                status_sets[region][status].add(case_no)

        rows = []
        total_status_sets = {status: set() for status in status_columns}
        total_jq = 0
        total_case = 0

        for region in summary_order:
            row = {"地区": region}
            row["警情数"] = summary_map[region]["警情数"]
            row["案件数"] = summary_map[region]["案件数"]
            total_jq += row["警情数"]
            total_case += row["案件数"]

            for status in status_columns:
                value = len(status_sets[region][status])
                row[status] = value
                total_status_sets[status].update(status_sets[region][status])
            rows.append(row)

        total_row = {"地区": "合计", "警情数": total_jq, "案件数": total_case}
        for status in status_columns:
            total_row[status] = len(total_status_sets[status])
        rows.append(total_row)

        return {"columns": columns, "data": rows}

    def get_detail(
        self,
        case_types: Optional[Iterable[str]],
        start_time: Optional[str],
        end_time: Optional[str],
        region: Optional[str],
        status_name: Optional[str],
        require_case: bool,
    ) -> dict:
        data = self._fetch_base_data(
            case_types,
            start_time,
            end_time,
            region=region,
            status_name=status_name,
            require_case=require_case,
        )

        ordered_data = []
        for row in data:
            ordered_row = {}
            for col in DETAIL_COLUMNS:
                value = row.get(col, "")
                if col == "处警情况":
                    value = _clean_chujing_status(value)
                ordered_row[col] = value
            ordered_data.append(ordered_row)

        return {"columns": DETAIL_COLUMNS, "data": ordered_data}
