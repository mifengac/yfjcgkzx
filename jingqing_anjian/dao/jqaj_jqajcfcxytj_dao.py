# -*- coding: utf-8 -*-
"""
警情案件处罚查询与统计 - 数据访问层
DAO (Data Access Object)
"""

from gonggong.config.database import execute_query


class JqajcfcxytjDAO:
    """警情案件处罚查询与统计数据访问对象"""

    def get_case_types(self):
        """
        获取警情类型列表

        Returns:
            list: 警情类型列表, 格式: [{"leixing": "类型名称"}, ...]
        """
        query = """
            SELECT ctc."leixing"
            FROM "ywdata"."case_type_config" ctc
        """
        return execute_query(query)

    def get_jingqings(self, tbkssj, jssj, leixing_list):
        """
        获取警情数据

        Args:
            tbkssj: 同比开始时间 (包含去年和今年的数据)
            jssj: 结束时间
            leixing_list: 警情类型列表

        Returns:
            list: 警情数据列表
        """
        # 构建 SQL
        if leixing_list:
            # 有选择类型时，使用 IN 条件
            leixing_placeholders = ','.join(['%s'] * len(leixing_list))
            query = f"""
                SELECT *, LEFT(vjo."cmdid", 6) AS "diqu"
                FROM "ywdata"."v_jq_optimized" vjo
                WHERE vjo."calltime" BETWEEN %s AND %s
                  AND vjo."leixing" IN ({leixing_placeholders})
            """
            params = [tbkssj, jssj] + leixing_list
        else:
            # 没有选择类型时，查询全部
            query = """
                SELECT *, LEFT(vjo."cmdid", 6) AS "diqu"
                FROM "ywdata"."v_jq_optimized" vjo
                WHERE vjo."calltime" BETWEEN %s AND %s
            """
            params = [tbkssj, jssj]

        return execute_query(query, params)

    def get_anjians(self, tbkssj, jssj, leixing_list):
        """
        获取案件数据

        Args:
            tbkssj: 同比开始时间 (包含去年和今年数据)
            jssj: 结束时间
            leixing_list: 警情类型列表

        Returns:
            list: 案件数据列表
        """
        # 先获取类型对应的案由模式
        type_query = """
            SELECT ay_pattern
            FROM "ywdata"."case_type_config" ctc
            WHERE ctc."leixing" = %s
        """

        patterns = []
        if leixing_list:
            # 有选择类型时，获取对应的案由模式
            for leixing in leixing_list:
                result = execute_query(type_query, [leixing])
                if result:
                    patterns.extend([r['ay_pattern'] for r in result])

        # 构建 SQL
        if patterns:
            # 有案由模式时，使用 SIMILAR TO 条件
            pattern_condition = ' OR '.join(['mzaa."案由" SIMILAR TO %s'] * len(patterns))
            query = f"""
                SELECT *
                FROM "ywdata"."mv_zfba_all_ajxx" mzaa
                WHERE mzaa."立案日期" BETWEEN %s AND %s
                  AND ({pattern_condition})
            """
            params = [tbkssj, jssj] + patterns
        else:
            # 没有选择类型时，查询全部
            query = """
                SELECT *
                FROM "ywdata"."mv_zfba_all_ajxx" mzaa
                WHERE mzaa."立案日期" BETWEEN %s AND %s
            """
            params = [tbkssj, jssj]

        return execute_query(query, params)

    def get_wenshus(self, tbkssj, jssj, leixing_list):
        """
        获取文书数据

        Args:
            tbkssj: 同比开始时间 (包含去年和今年数据)
            jssj: 结束时间
            leixing_list: 警情类型列表

        Returns:
            list: 文书数据列表
        """
        # 先获取类型对应的案由模式
        type_query = """
            SELECT ay_pattern
            FROM "ywdata"."case_type_config"
            WHERE "leixing" = %s
        """

        patterns = []
        if leixing_list:
            # 有选择类型时，获取对应的案由模式
            for leixing in leixing_list:
                result = execute_query(type_query, [leixing])
                if result:
                    patterns.extend([r['ay_pattern'] for r in result])

        # 构建 SQL
        if patterns:
            # 有案由模式时，使用 SIMILAR TO 条件
            pattern_condition = ' OR '.join(['aj.aymc SIMILAR TO %s'] * len(patterns))
            query = f"""
                WITH ws_dedup AS (
                    SELECT DISTINCT ON (ws.wsywxxid)
                        ws.*
                    FROM "ywdata"."mv_zfba_wenshu" ws
                    WHERE COALESCE(ws.spsj, ws.tfsj) >= %s::timestamp
                      AND COALESCE(ws.spsj, ws.tfsj) <= %s::timestamp
                    ORDER BY ws.wsywxxid, ws.tfsj DESC NULLS LAST
                ),
                aj_dedup AS (
                    SELECT DISTINCT ON (aj.asjbh)
                        aj.*
                    FROM "ywdata"."zfba_aj_003" aj
                    WHERE {pattern_condition}
                    ORDER BY aj.asjbh, aj.xgsj DESC NULLS LAST
                ),
                base AS (
                    SELECT
                        LEFT(ws.badwdm, 6) AS region,
                        ws.badwmc,
                        ws.flws_dxbh,
                        ws.flws_bt,
                        ws.tfsj,
                        ws.spsj,
                        ws.asjbh,
                        ws.asjmc,
                        ws.wsywxxid,
                        aj.aymc,
                        p.sfjg,
                        p.jlts,
                        p.fk,
                        ws.flws_zlmc,
                        ws.flws_dxlxdm,
                        ws.flws_dxbxm
                    FROM ws_dedup ws
                    LEFT JOIN aj_dedup aj ON ws.asjbh = aj.asjbh
                    LEFT JOIN "ywdata"."zfba_aj_009" p ON ws.wsywxxid = p.wsywxxid
                )
                SELECT
                    b.region::text AS region,
                    b.badwmc::text AS badwmc,
                    b.wsywxxid::TEXT AS wsywxxid,
                    b.flws_dxlxdm::TEXT AS dxlxdm,
                    b.flws_dxbh::text AS flws_dxbh,
                    b.flws_dxbxm::text AS flws_dxbxm,
                    b.flws_bt::text AS flws_bt,
                    COALESCE(b.spsj, b.tfsj) AS spsj,
                    b.asjbh::text AS asjbh,
                    b.asjmc::text AS asjmc,
                    COALESCE(b.sfjg, '0') AS jinggao,
                    COALESCE(b.fk, '0') AS fakuan,
                    COALESCE(b.jlts, '0') AS zhiju
                FROM base b
                WHERE b.aymc IS NOT NULL
            """
            params = [tbkssj, jssj] + patterns
        else:
            # 没有选择类型时，查询全部
            query = """
                WITH ws_dedup AS (
                    SELECT DISTINCT ON (ws.wsywxxid)
                        ws.*
                    FROM "ywdata"."mv_zfba_wenshu" ws
                    WHERE COALESCE(ws.spsj, ws.tfsj) >= %s::timestamp
                      AND COALESCE(ws.spsj, ws.tfsj) <= %s::timestamp
                    ORDER BY ws.wsywxxid, ws.tfsj DESC NULLS LAST
                ),
                aj_dedup AS (
                    SELECT DISTINCT ON (aj.asjbh)
                        aj.*
                    FROM "ywdata"."zfba_aj_003" aj
                    WHERE 1=1
                    ORDER BY aj.asjbh, aj.xgsj DESC NULLS LAST
                ),
                base AS (
                    SELECT
                        LEFT(ws.badwdm, 6) AS region,
                        ws.badwmc,
                        ws.flws_dxbh,
                        ws.flws_bt,
                        ws.tfsj,
                        ws.spsj,
                        ws.asjbh,
                        ws.asjmc,
                        ws.wsywxxid,
                        aj.aymc,
                        p.sfjg,
                        p.jlts,
                        p.fk,
                        ws.flws_zlmc,
                        ws.flws_dxlxdm,
                        ws.flws_dxbxm
                    FROM ws_dedup ws
                    LEFT JOIN aj_dedup aj ON ws.asjbh = aj.asjbh
                    LEFT JOIN "ywdata"."zfba_aj_009" p ON ws.wsywxxid = p.wsywxxid
                )
                SELECT
                    b.region::text AS region,
                    b.badwmc::text AS badwmc,
                    b.wsywxxid::TEXT AS wsywxxid,
                    b.flws_dxlxdm::TEXT AS dxlxdm,
                    b.flws_dxbh::text AS flws_dxbh,
                    b.flws_dxbxm::text AS flws_dxbxm,
                    b.flws_bt::text AS flws_bt,
                    COALESCE(b.spsj, b.tfsj) AS spsj,
                    b.asjbh::text AS asjbh,
                    b.asjmc::text AS asjmc,
                    COALESCE(b.sfjg, '0') AS jinggao,
                    COALESCE(b.fk, '0') AS fakuan,
                    COALESCE(b.jlts, '0') AS zhiju
                FROM base b
                WHERE b.aymc IS NOT NULL
            """
            params = [tbkssj, jssj]

        return execute_query(query, params)
