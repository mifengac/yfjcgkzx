"""
精神障碍患者预警数据访问层
处理与精神障碍患者预警相关的数据库操作
"""
from typing import Dict, Any, List, Tuple
import logging
from gonggong.config.database import execute_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


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
        # 基础SQL查询
        base_sql = """
        SELECT
            a.xm AS 姓名,
            a.zjhm AS 证件号码,
            a.hjdz AS 户籍地址,
            a.lgsj AS 列管时间,
            CASE
                WHEN substring(a.sjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445302' THEN '云城分局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445303' THEN '云安分局'
                ELSE a.sjgsdwdm
            END AS 列管分局名称,
            c.sspcs AS 列管派出所名称,
            b.jfmc AS 纠纷名称,
            b.jyqk AS 简要情况,
            b.mdjfdjsj AS 矛盾纠纷录入时间,
            CASE
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445302' THEN '云城分局'
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445303' THEN '云安分局'
                ELSE b.mdjfsjgsdwdm
            END AS 矛盾纠纷录入分局名称,
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

        # 构建WHERE条件和参数列表
        where_conditions = []
        params = []

        # 列管时间筛选
        if liguan_start:
            where_conditions.append("a.lgsj >= %s")
            params.append(liguan_start)
        if liguan_end:
            where_conditions.append("a.lgsj <= %s")
            params.append(liguan_end)

        # 矛盾纠纷录入时间筛选
        if maodun_start:
            where_conditions.append("b.mdjfdjsj >= %s")
            params.append(maodun_start)
        if maodun_end:
            where_conditions.append("b.mdjfdjsj <= %s")
            params.append(maodun_end)

        # 分局筛选
        if fenju_list and len(fenju_list) > 0:
            where_conditions.append("""
                (CASE
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445302' THEN '云城分局'
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445303' THEN '云安分局'
                    ELSE a.sjgsdwdm
                END) = ANY(%s)
            """)
            params.append(fenju_list)

        # 拼接WHERE条件
        if where_conditions:
            base_sql += " AND " + " AND ".join(where_conditions)

        # 添加排序
        base_sql += " ORDER BY a.lgsj DESC"

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

        logging.info(f"查询到 {len(rows)} 条数据，总记录数: {total}")
        return rows, int(total)

    except Exception as e:
        logging.error(f"查询精神障碍患者预警数据失败: {e}")
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
        # 基础SQL查询
        sql = """
        SELECT
            a.xm AS 姓名,
            a.zjhm AS 证件号码,
            a.hjdz AS 户籍地址,
            a.lgsj AS 列管时间,
            CASE
                WHEN substring(a.sjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445302' THEN '云城分局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445303' THEN '云安分局'
                ELSE a.sjgsdwdm
            END AS 列管分局名称,
            c.sspcs AS 列管派出所名称,
            b.jfmc AS 纠纷名称,
            b.jyqk AS 简要情况,
            b.mdjfdjsj AS 矛盾纠纷录入时间,
            CASE
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445302' THEN '云城分局'
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
                WHEN substring(b.mdjfsjgsdwdm, 1, 6) = '445303' THEN '云安分局'
                ELSE b.mdjfsjgsdwdm
            END AS 矛盾纠纷录入分局名称,
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

        # 构建WHERE条件和参数列表
        where_conditions = []
        params = []

        # 列管时间筛选
        if liguan_start:
            where_conditions.append("a.lgsj >= %s")
            params.append(liguan_start)
        if liguan_end:
            where_conditions.append("a.lgsj <= %s")
            params.append(liguan_end)

        # 矛盾纠纷录入时间筛选
        if maodun_start:
            where_conditions.append("b.mdjfdjsj >= %s")
            params.append(maodun_start)
        if maodun_end:
            where_conditions.append("b.mdjfdjsj <= %s")
            params.append(maodun_end)

        # 分局筛选
        if fenju_list and len(fenju_list) > 0:
            where_conditions.append("""
                (CASE
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445302' THEN '云城分局'
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
                    WHEN substring(a.sjgsdwdm, 1, 6) = '445303' THEN '云安分局'
                    ELSE a.sjgsdwdm
                END) = ANY(%s)
            """)
            params.append(fenju_list)

        # 拼接WHERE条件
        if where_conditions:
            sql += " AND " + " AND ".join(where_conditions)

        # 添加排序
        sql += " ORDER BY a.lgsj DESC"

        # 执行查询
        rows = execute_query(sql, tuple(params))
        logging.info(f"导出查询到 {len(rows)} 条数据")
        return rows

    except Exception as e:
        logging.error(f"查询全部数据失败: {e}")
        raise
