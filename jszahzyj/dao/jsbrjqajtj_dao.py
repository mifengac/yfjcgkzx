from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List

from gonggong.config.database import execute_query


def normalize_datetime_text(value: str) -> str:
    text = (value or "").strip().replace("T", " ")
    if not text:
        raise ValueError("时间不能为空")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    raise ValueError(f"时间格式错误: {text}，应为 YYYY-MM-DD HH:MM:SS")


def query_branch_options() -> List[Dict[str, Any]]:
    sql = """
    SELECT
      ssfjdm AS value,
      MAX(ssfj) AS label
    FROM
      stdata.b_dic_zzjgdm
    WHERE
      ssfjdm IS NOT NULL
      AND ssfj IS NOT NULL
    GROUP BY
      ssfjdm
    ORDER BY
      MAX(ssfj)
    """
    return execute_query(sql)


def query_jsbrjqajtj(
    *,
    start_time: str,
    end_time: str,
    branches: Iterable[str] | None = None,
) -> List[Dict[str, Any]]:
    start_text = normalize_datetime_text(start_time)
    end_text = normalize_datetime_text(end_time)
    start_dt = datetime.strptime(start_text, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_text, "%Y-%m-%d %H:%M:%S")
    if start_dt > end_dt:
        raise ValueError("开始时间不能大于结束时间")

    branch_list = [x.strip() for x in (branches or []) if x and x.strip()]
    branch_filter = ""
    params: List[Any] = [start_text, end_text]
    if branch_list:
        branch_filter = " AND cmdid = ANY(%s)"
        params.append(branch_list)

    sql = f"""
    WITH jq_jsb AS (
        SELECT
            caseno AS 警情编号,
            calltime AS 报警时间,
            cmdname AS 地区,
            dutydeptname AS 管辖单位,
            callerphone AS 报警电话,
            neworicharasubclassname AS 原始警情,
            newcharasubclassname AS 确认警情,
            occuraddress AS 发生地址,
            casecontents AS 报警内容,
            replies AS 处警情况,
            spiritcausetrouble AS 精神病肇事标识,
            cmdid AS 分局代码
        FROM ywdata.zq_kshddpt_dsjfx_jq
        WHERE
            calltime >= %s
            AND calltime <= %s
            {branch_filter}
            AND (
                spiritcausetrouble IS NOT NULL
                OR casecontents ~* '精神病|精神障碍|精神异常|精神发病|犯病|肇事肇祸'
                OR replies ~* '精神病|精神障碍|精神异常|精神发病|犯病|肇事肇祸'
                OR charaname ~* '精神'
                OR newcharasubclassname ~* '精神'
            )
    )
    SELECT
        jq.警情编号,
        jq.报警时间,
        jq.地区,
        jq.管辖单位,
        jq.报警电话,
        jq.原始警情,
        jq.确认警情,
        jq.发生地址,
        jq.报警内容,
        jq.处警情况,
        jq.精神病肇事标识,
        aj.ajxx_ajbh AS 案件编号,
        aj.ajxx_ajmc AS 案件名称,
        aj.ajxx_ajlx AS 案件类型,
        TO_CHAR(aj.ajxx_lasj, 'YYYY-MM-DD') AS 立案时间,
        CASE WHEN aj.ajxx_jqbh IS NOT NULL THEN '是' ELSE '否' END AS 是否立案
    FROM jq_jsb jq
    LEFT JOIN ywdata.zq_zfba_ajxx aj
        ON aj.ajxx_jqbh = jq.警情编号
    ORDER BY jq.报警时间 DESC
    """
    return execute_query(sql, tuple(params))
