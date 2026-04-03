from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection


DISPUTE_TYPE_CASE_SQL = """
CASE CAST(md."jflx" AS TEXT)
    WHEN '3' THEN '邻里纠纷'
    WHEN '8' THEN '土地（宅基地）纠纷'
    WHEN '7' THEN '其他'
    WHEN '1' THEN '家庭矛盾'
    WHEN '4' THEN '经济纠纷'
    WHEN '5' THEN '劳资纠纷'
    WHEN '2' THEN '感情纠纷'
    WHEN '6' THEN '青少年不良行为'
    WHEN '11' THEN '房地产纠纷'
    WHEN '9' THEN '医疗纠纷'
    WHEN '14' THEN '消费纠纷'
    WHEN '10' THEN '教育纠纷'
    WHEN '12' THEN '12'
    WHEN '15' THEN '行政纠纷'
    ELSE COALESCE(CAST(md."jflx" AS TEXT), '')
END
""".strip()


def _build_keyword_clause(field_names: Sequence[str], keywords: Sequence[str]) -> Tuple[str, List[str]]:
    parts: List[str] = []
    params: List[str] = []
    for field_name in field_names:
        for keyword in keywords:
            parts.append(f"COALESCE({field_name}, '') ILIKE %s")
            params.append(f"%{keyword}%")
    return " OR ".join(parts), params


def _fetch_rows(sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows_raw = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows_raw]
    finally:
        conn.close()


def query_workorder_rows(
    *,
    start_time: datetime,
    end_time: datetime,
    keywords: Sequence[str],
) -> List[Dict[str, Any]]:
    keyword_sql, keyword_params = _build_keyword_clause(['gd."sqnr"'], keywords)
    sql = f"""
    SELECT
        gd."ywbh" AS "业务编号",
        gd."ldbh" AS "来电编号",
        gd."jjcd" AS "紧急程度",
        gd."ldhm" AS "来电号码",
        gd."sqr" AS "诉求人",
        gd."sqnr" AS "诉求内容",
        gd."gdbt" AS "工单标题",
        gd."sxfl" AS "事项分类",
        gd."ldsj" AS "来电时间",
        gd."ssjg" AS "所属机构",
        gd."cldw1" AS "处理单位",
        gd."sqfw" AS "诉求范围",
        gd."gdzt" AS "工单状态"
    FROM ywdata.sh_yf_123gd_xx gd
    WHERE gd."ldsj" >= %s
      AND gd."ldsj" <= %s
      AND ({keyword_sql})
    ORDER BY gd."ldsj" DESC NULLS LAST
    """
    return _fetch_rows(sql, [start_time, end_time, *keyword_params])


def query_dispute_rows(
    *,
    start_time: datetime,
    end_time: datetime,
    keywords: Sequence[str],
) -> List[Dict[str, Any]]:
    keyword_sql, keyword_params = _build_keyword_clause(['md."jyqk"', 'md."jfyy"'], keywords)
    sql = f"""
    SELECT
        md."ywlsh" AS "业务流水号",
        md."jfmc" AS "纠纷名称",
        {DISPUTE_TYPE_CASE_SQL} AS "纠纷类型",
        md."sfgazzfw" AS "是否公安职责范围",
        md."fssj" AS "发生时间",
        md."fxdj" AS "风险等级",
        md."jyqk" AS "简要情况",
        md."jfyy" AS "纠纷缘由",
        md."sbrlxdh" AS "上报人联系电话",
        md."tcfa" AS "调处方案",
        md."fsdzmc" AS "发生地址",
        bdz."ssfj" AS "所属分局",
        bdz."sspcs" AS "所属派出所",
        md."djsj" AS "登记时间",
        md."xgr_xm" AS "修改人姓名"
    FROM stdata.b_per_mdjfjfsjgl md
    LEFT JOIN stdata.b_dic_zzjgdm bdz
      ON md."sspcs" = bdz."sspcsdm"
    WHERE md.deleteflag = '0'
      AND md."fssj" >= %s
      AND md."fssj" <= %s
      AND ({keyword_sql})
    ORDER BY md."fssj" DESC NULLS LAST
    """
    return _fetch_rows(sql, [start_time, end_time, *keyword_params])
