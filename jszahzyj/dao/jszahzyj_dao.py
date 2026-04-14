"""
精神障碍患者预警数据访问层
处理与精神障碍患者预警相关的数据库操作
"""
from typing import Dict, Any, List, Tuple
import logging
from gonggong.config.database import execute_query

logger = logging.getLogger(__name__)


_BRANCH_NAME_SQL_TEMPLATE = """
CASE
    WHEN substring({column}, 1, 6) = '445321' THEN '新兴县公安局'
    WHEN substring({column}, 1, 6) = '445302' THEN '云城分局'
    WHEN substring({column}, 1, 6) = '445381' THEN '罗定市公安局'
    WHEN substring({column}, 1, 6) = '445322' THEN '郁南县公安局'
    WHEN substring({column}, 1, 6) = '445303' THEN '云安分局'
    ELSE {column}
END
"""


def _branch_name_sql(column: str) -> str:
    return _BRANCH_NAME_SQL_TEMPLATE.format(column=column)


def _build_base_query_sql() -> str:
    liguan_branch_sql = _branch_name_sql("a.sjgsdwdm")
    dispute_branch_sql = _branch_name_sql("b.mdjfsjgsdwdm")
    return f"""
    SELECT
        a.xm AS 姓名,
        a.zjhm AS 证件号码,
        a.hjdz AS 户籍地址,
        a.lgsj AS 列管时间,
        {liguan_branch_sql} AS 列管分局名称,
        c.sspcs AS 列管派出所名称,
        b.jfmc AS 纠纷名称,
        b.jyqk AS 简要情况,
        b.mdjfdjsj AS 矛盾纠纷录入时间,
        {dispute_branch_sql} AS 矛盾纠纷录入分局名称,
        d.sspcs AS 矛盾纠纷录入派出所名称
    FROM
        (
            SELECT *
            FROM stdata.b_per_jszahzryxxwh
            WHERE sflg = '1' AND "deleteflag" = '0'
        ) a
    LEFT JOIN (
        SELECT
            a1.zjhm,
            b1.jfmc,
            b1.jyqk,
            b1.djsj AS mdjfdjsj,
            b1.sjgsdwdm AS mdjfsjgsdwdm
        FROM
            (
                SELECT *
                FROM stdata.b_zdry_ryxx_mdjf
                WHERE deleteflag = '0'
            ) a1
        LEFT JOIN (
            SELECT *
            FROM stdata.b_per_mdjfjfsjgl
            WHERE deleteflag = '0'
        ) b1 ON a1.mdjflsh = b1.ywlsh
    ) b ON a.zjhm = b.zjhm
    LEFT JOIN stdata.b_dic_zzjgdm c ON a.sjgsdwdm = c.sspcsdm
    LEFT JOIN stdata.b_dic_zzjgdm d ON b.mdjfsjgsdwdm = d.sspcsdm
    WHERE b.jfmc IS NOT NULL
    """


def _build_filter_clause(
    *,
    liguan_start: str = None,
    liguan_end: str = None,
    maodun_start: str = None,
    maodun_end: str = None,
    fenju_list: List[str] = None,
) -> Tuple[List[str], List[Any]]:
    conditions: List[str] = []
    params: List[Any] = []

    if liguan_start:
        conditions.append("a.lgsj >= %s")
        params.append(liguan_start)
    if liguan_end:
        conditions.append("a.lgsj <= %s")
        params.append(liguan_end)
    if maodun_start:
        conditions.append("b.mdjfdjsj >= %s")
        params.append(maodun_start)
    if maodun_end:
        conditions.append("b.mdjfdjsj <= %s")
        params.append(maodun_end)
    if fenju_list:
        conditions.append(f"({_branch_name_sql('a.sjgsdwdm')}) = ANY(%s)")
        params.append(fenju_list)

    return conditions, params


def _build_query_sql(
    *,
    liguan_start: str = None,
    liguan_end: str = None,
    maodun_start: str = None,
    maodun_end: str = None,
    fenju_list: List[str] = None,
) -> Tuple[str, List[Any]]:
    sql = _build_base_query_sql()
    conditions, params = _build_filter_clause(
        liguan_start=liguan_start,
        liguan_end=liguan_end,
        maodun_start=maodun_start,
        maodun_end=maodun_end,
        fenju_list=fenju_list,
    )
    if conditions:
        sql += " AND " + " AND ".join(conditions)
    sql += " ORDER BY a.lgsj DESC"
    return sql, params


def query_jszahzyj_data(
    liguan_start: str = None,
    liguan_end: str = None,
    maodun_start: str = None,
    maodun_end: str = None,
    fenju_list: List[str] = None,
    page: int = 1,
    page_size: int = 20
) -> Tuple[List[Dict[str, Any]], int]:
    """
    查询精神障碍患者预警数据

    参数:
        liguan_start: 列管时间开始 (格式: 'YYYY-MM-DD HH:MM:SS')，可选
        liguan_end: 列管时间结束 (格式: 'YYYY-MM-DD HH:MM:SS')，可选
        maodun_start: 矛盾纠纷录入时间开始 (格式: 'YYYY-MM-DD HH:MM:SS')，可选
        maodun_end: 矛盾纠纷录入时间结束 (格式: 'YYYY-MM-DD HH:MM:SS')，可选
        fenju_list: 分局列表，可选
        page: 页码 (从1开始)
        page_size: 每页记录数

    返回:
        (数据列表, 总记录数)
    """
    try:
        base_sql, params = _build_query_sql(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list,
        )

        # 包装为带总数统计的查询
        sql = f"""
        SELECT
            COUNT(*) OVER() AS total_count,
            t.*
        FROM ({base_sql}) t
        LIMIT %s OFFSET %s
        """

        # 计算偏移量
        offset = (page - 1) * page_size
        params.extend([page_size, offset])

        # 执行查询
        rows = execute_query(sql, tuple(params))

        # 提取总记录数
        total = 0
        if rows:
            total = rows[0].get('total_count', 0) or 0
            # 移除total_count字段
            for row in rows:
                row.pop('total_count', None)

        logger.info("查询到 %s 条数据，总记录数: %s", len(rows), total)
        return rows, int(total)

    except Exception as e:
        logger.error("查询精神障碍患者预警数据失败: %s", e)
        raise


def get_all_jszahzyj_data(
    liguan_start: str = None,
    liguan_end: str = None,
    maodun_start: str = None,
    maodun_end: str = None,
    fenju_list: List[str] = None
) -> List[Dict[str, Any]]:
    """
    获取所有精神障碍患者预警数据（用于导出，不分页）

    参数:
        liguan_start: 列管时间开始，可选
        liguan_end: 列管时间结束，可选
        maodun_start: 矛盾纠纷录入时间开始，可选
        maodun_end: 矛盾纠纷录入时间结束，可选
        fenju_list: 分局列表，可选

    返回:
        数据列表
    """
    try:
        sql, params = _build_query_sql(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list,
        )

        # 执行查询
        rows = execute_query(sql, tuple(params))
        logger.info("导出查询到 %s 条数据", len(rows))
        return rows

    except Exception as e:
        logger.error("查询全部数据失败: %s", e)
        raise
