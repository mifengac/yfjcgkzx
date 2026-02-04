# 任务:在当前模块新增一个tab页,名为"矫治情况统计"
## 查询区域:
    - 时间范围控件:格式为'YYYY-MM-DD HH:MM:SS',"开始时间"默认为当天向前减7天,"结束时间"默认为当天,时间格式均为'00:00:00',如今天是'2026-01-27',那"开始时间"为'2026-01-20 00:00:00',"结束时间"为'2026-01-27 00:00:00'
    - "类型",多选下拉框,通过```SELECT "leixing"FROM "ywdata"."case_type_config";```获取,逻辑为通过wcn."xyrxx_ay_mc"字段进行模糊匹配
    - "查询",点击查询通过时间范围和类型过滤数据
    - "导出":单击按钮,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,数据为"数据展示区"显示的数据,文件名为"全市矫治教育统计"+{时间戳}.csv/xlsx
    - "导出详情":单击按钮,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,数据源为我提供的数据源的详细数据,文件名为"全市矫治教育详情"+{时间戳}.csv/xlsx
## 数据展示区:
    - 第一列为"分局":通过对数据源"分局"字段分组计数得到
    - 第二列为"违法人数":通过对数据源"案件类型"值为'行政'的数据过滤并分组计数得到,最后一行是不分组计数的数据,即全部数据
    - 第三列为"矫治教育文书开具数(行政)":通过对数据源"案件类型"值为'行政'且"是否开具矫治文书"值为'是'的数据过滤并分组计数得到
    - 第四列为"提请专门教育申请书数(行政):通过对数据源"案件类型"值为'行政'且"是否提请专门教育"值为'是'的数据过滤并分组计数得到
    - 第五列为"犯罪人数":通过对数据源"案件类型"值为'刑事'的数据过滤并分组计数得到,最后一行是不分组计数的数据,即全部数据
    - 第五列为"矫治教育文书开具数(刑事)":通过对数据源"案件类型"值为'刑事'且"是否开具矫治文书"值为'是'的数据过滤并分组计数得到
    - 第六列为"提请专门教育申请书数(刑事)":通过对数据源"案件类型"值为'刑事'且"是否提请专门教育"值为'是'的数据过滤并分组计数得到
    - 第七列为"刑拘数":通过对数据源"案件类型"值为'刑事'且"是否未刑拘"值为'否'的数据过滤并分组计数得到
    - 最后一行是不分组的数据源计数
## 数据源:
    ```
    WITH base_data AS (
        -- 基础数据（按身份证号和案件编号去重）
        SELECT DISTINCT ON (wcn."xyrxx_sfzh", wcn."ajxx_join_ajxx_ajbh")
            wcn."ajxx_join_ajxx_ajbh",
            wcn."ajxx_join_ajxx_ajlx",
            wcn."ajxx_join_ajxx_ajmc",
            wcn."ajxx_join_ajxx_cbqy_jc",
            wcn."ajxx_join_ajxx_cbdw_bh",
            wcn."ajxx_join_ajxx_lasj",
            wcn."xyrxx_sfzh",
            wcn."xyrxx_xm",
            wcn."xyrxx_nl",
            wcn."xyrxx_hjdxz",
            wcn."xyrxx_jzdxzqh",
            wcn."xyrxx_rybh"
        FROM "ywdata"."zq_zfba_wcnr_xyr" wcn
        WHERE wcn."ajxx_join_ajxx_lasj" BETWEEN '2026-01-01' AND '2026-02-04' AND wcn."xyrxx_ay_mc" SIMILAR TO (SELECT ctc."ay_pattern" FROM "case_type_config" ctc WHERE ctc."leixing" ='打架斗殴')
        ORDER BY wcn."xyrxx_sfzh", wcn."ajxx_join_ajxx_ajbh", wcn."ajxx_join_ajxx_lasj" DESC
    ),

    /* ✅ 收敛身份证集合，后续统计只算这批人，避免慢 */
    target_sfzh AS (
        SELECT DISTINCT bd."xyrxx_sfzh" AS xyrxx_sfzh
        FROM base_data bd
    ),

    /* 矫治文书判断 */
    jzws_info AS (
        SELECT
            bd."ajxx_join_ajxx_ajbh",
            bd."xyrxx_sfzh",
            CASE
                WHEN xjs.ajbh IS NOT NULL AND zltzs.zltzs_ajbh IS NOT NULL THEN '训诫书/责令通知书'
                WHEN xjs.ajbh IS NOT NULL THEN '训诫书'
                WHEN zltzs.zltzs_ajbh IS NOT NULL THEN '责令通知书'
                ELSE NULL
            END AS jzws_name,
            CASE
                WHEN xjs.ajbh IS NOT NULL OR zltzs.zltzs_ajbh IS NOT NULL THEN '是'
                ELSE '否'
            END AS has_jzws
        FROM base_data bd
        LEFT JOIN "ywdata"."zq_zfba_xjs2" xjs
            ON bd."ajxx_join_ajxx_ajbh" = xjs.ajbh
        AND bd."xyrxx_xm" = xjs.xgry_xm
        LEFT JOIN "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" zltzs
            ON bd."ajxx_join_ajxx_ajbh" = zltzs.zltzs_ajbh
        AND bd."xyrxx_sfzh" = zltzs.zltzs_sfzh
    ),

    /* 条件1：行政处罚决定书治拘天数 >= 5日 */
    fhss_xzcfjds AS (
        SELECT DISTINCT
            bd."ajxx_join_ajxx_ajbh",
            bd."xyrxx_sfzh"
        FROM base_data bd
        INNER JOIN "ywdata"."zq_zfba_xzcfjds" xzcf
            ON bd."ajxx_join_ajxx_ajbh" = xzcf.ajxx_ajbh
        AND bd."xyrxx_rybh" = xzcf.xzcfjds_rybh
        WHERE xzcf.xzcfjds_tj_jlts::INTEGER > 4
    ),

    /* ✅ 前科统计：只统计 target_sfzh 这些人的历史（按身份证） */
    ay_stats AS (
        SELECT
            w.xyrxx_sfzh,
            COUNT(*) AS total_cnt,
            COUNT(DISTINCT w.xyrxx_ay_mc) AS distinct_ay_cnt
        FROM "ywdata"."zq_zfba_wcnr_xyr" w
        INNER JOIN target_sfzh t
            ON t.xyrxx_sfzh = w.xyrxx_sfzh
        GROUP BY w.xyrxx_sfzh
    ),

    /* 记录证（用于判断刑事是否刑拘：有记录证 => 非未刑拘） */
    fhss_jlz AS (
        SELECT DISTINCT
            bd."ajxx_join_ajxx_ajbh",
            bd."xyrxx_sfzh"
        FROM base_data bd
        INNER JOIN "ywdata"."zq_zfba_jlz" jlz
            ON bd."ajxx_join_ajxx_ajbh" = jlz.ajxx_ajbh
        AND bd."xyrxx_rybh" = jlz.jlz_rybh
    ),

    /* ✅ 汇总：新增4列解释 + 总的是否符合送生 */
    fhss_detail AS (
        SELECT
            bd."ajxx_join_ajxx_ajbh",
            bd."xyrxx_sfzh",

            /* 1 行政：治拘>=5日 */
            CASE
                WHEN bd."ajxx_join_ajxx_ajlx" = '行政'
                AND xzcf."ajxx_join_ajxx_ajbh" IS NOT NULL
                THEN '是' ELSE '否'
            END AS "是否治拘5日及以上",

            /* 2 行政：2次前科且案由相同 */
            CASE
                WHEN bd."ajxx_join_ajxx_ajlx" = '行政'
                AND ay.total_cnt = 2
                AND ay.distinct_ay_cnt = 1
                THEN '是' ELSE '否'
            END AS "是否2次前科且案由相同",

            /* 3 行政：3次前科及以上 */
            CASE
                WHEN bd."ajxx_join_ajxx_ajlx" = '行政'
                AND ay.total_cnt >= 3
                THEN '是' ELSE '否'
            END AS "是否3次前科及以上",

            /* 4 刑事：是否未刑拘（按记录证反推：无记录证=未刑拘） */
            CASE
                WHEN bd."ajxx_join_ajxx_ajlx" = '刑事'
                AND jlz."ajxx_join_ajxx_ajbh" IS NULL
                THEN '是' ELSE '否'
            END AS "是否未刑拘",

            /* 总结：四个条件任意满足 => 是 */
            CASE
                WHEN (
                    (bd."ajxx_join_ajxx_ajlx" = '行政' AND xzcf."ajxx_join_ajxx_ajbh" IS NOT NULL)
                OR (bd."ajxx_join_ajxx_ajlx" = '行政' AND ay.total_cnt = 2 AND ay.distinct_ay_cnt = 1)
                OR (bd."ajxx_join_ajxx_ajlx" = '行政' AND ay.total_cnt >= 3)
                OR (bd."ajxx_join_ajxx_ajlx" = '刑事' AND jlz."ajxx_join_ajxx_ajbh" IS NULL)
                )
                THEN '是' ELSE '否'
            END AS is_fhss

        FROM base_data bd
        LEFT JOIN fhss_xzcfjds xzcf
            ON bd."ajxx_join_ajxx_ajbh" = xzcf."ajxx_join_ajxx_ajbh"
        AND bd."xyrxx_sfzh" = xzcf."xyrxx_sfzh"
        LEFT JOIN ay_stats ay
            ON bd."xyrxx_sfzh" = ay.xyrxx_sfzh
        LEFT JOIN fhss_jlz jlz
            ON bd."ajxx_join_ajxx_ajbh" = jlz."ajxx_join_ajxx_ajbh"
        AND bd."xyrxx_sfzh" = jlz."xyrxx_sfzh"
    )

    SELECT distinct
        bd."ajxx_join_ajxx_ajbh" AS 案件编号,
        bd."ajxx_join_ajxx_ajlx" AS 案件类型,
        bd."ajxx_join_ajxx_ajmc" AS 案件名称,
        bd."ajxx_join_ajxx_cbqy_jc" AS 分局,
        bd."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
        TO_CHAR(bd."ajxx_join_ajxx_lasj", 'YYYY-MM-DD HH24:MI:SS') AS 立案时间,
        bd."xyrxx_sfzh" AS 身份证号,
        bd."xyrxx_xm" AS 姓名,
        bd."xyrxx_nl" AS 年龄,
        bd."xyrxx_hjdxz" AS 户籍地,
        bd."xyrxx_jzdxzqh" AS 居住地,
        bd."xyrxx_rybh" AS 人员编号,

        COALESCE(jzws.has_jzws, '否') AS 是否开具矫治文书,
        jzws.jzws_name AS 矫治文书名称,

        /* ✅ 是否符合送生 + 4列解释 */
        COALESCE(fh.is_fhss, '否') AS 是否符合送生,
        fh."是否治拘5日及以上",
        fh."是否2次前科且案由相同",
        fh."是否3次前科及以上",
        fh."是否未刑拘",

        CASE
            WHEN EXISTS (
                SELECT 1
                FROM "ywdata"."zq_zfba_tqzmjy" tq
                WHERE bd."ajxx_join_ajxx_ajbh" = tq.ajbh
                AND bd."xyrxx_xm" = tq.xgry_xm
            ) THEN '是'
            ELSE '否'
        END AS 是否提请专门教育,

        CASE
            WHEN EXISTS (
                SELECT 1
                FROM "ywdata"."zq_wcnr_sfzxx" sfz
                WHERE bd."xyrxx_sfzh" = sfz.sfzhm
                AND bd."ajxx_join_ajxx_lasj" < sfz.rx_time
            ) THEN '是'
            ELSE '否'
        END AS 是否送校

    FROM base_data bd
    LEFT JOIN jzws_info jzws
        ON bd."ajxx_join_ajxx_ajbh" = jzws."ajxx_join_ajxx_ajbh"
    AND bd."xyrxx_sfzh" = jzws."xyrxx_sfzh"
    LEFT JOIN fhss_detail fh
        ON bd."ajxx_join_ajxx_ajbh" = fh."ajxx_join_ajxx_ajbh"
    AND bd."xyrxx_sfzh" = fh."xyrxx_sfzh"
    --ORDER BY bd."ajxx_join_ajxx_lasj" DESC, bd."xyrxx_xm";
    ```
# 任务4: 
# 任务1:
    - 在"获取综查数据"的"警情案件统计"中新增几个字段
## 新增字段及逻辑
    - 案件数,同比案件数:
         - 数据来源:
            ```
                SELECT
                    ajxx_ajbh AS "案件编号",
                    ajxx_jqbh AS "警情编号",
                    ajxx_ajmc AS "案件名称",
                    ajxx_ajlx AS "案件类型",
                    ajxx_ajzt AS "案件状态",
                    ajxx_ay AS "案由",
                    ajxx_ay_dm AS "案由代码",
                    ajxx_fasj AS "发案时间",
                    ajxx_lasj AS "立案时间",
                    ajxx_sldw_mc AS "受理单位",
                    ajxx_cbdw_mc AS "承办单位",
                    LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                    ajxx_zbbj AS "在办标记",
                    ajxx_ajly AS "案件来源"
                FROM "ywdata"."zq_zfba_ajxx"
                WHERE ajxx_lasj BETWEEN %s AND %s
                AND 1=1
            ```
        - 过滤规则,不需要过滤ajxx_ajlx条件,其实就是'行政'和'刑事'相加
    - 治拘、同比治拘改为治安处罚、同比治安处罚，同时在过滤区新增一个"多选下拉框",名为治安处罚类型,有'警告','罚款','拘留'三个选项,通过对xz.xzcfjds_cfzl ~ 进行判断,参考hqzcsj\templates\zfba_wcnr_jqaj_tab.html模块:
    - 办结、同比办结:
        - 数据源：
            ```
                SELECT
                    ajxx_ajbh AS "案件编号",
                    ajxx_jqbh AS "警情编号",
                    ajxx_ajmc AS "案件名称",
                    ajxx_ajlx AS "案件类型",
                    ajxx_ajzt AS "案件状态",
                    ajxx_ay AS "案由",
                    ajxx_ay_dm AS "案由代码",
                    ajxx_fasj AS "发案时间",
                    ajxx_lasj AS "立案时间",
                    ajxx_sldw_mc AS "受理单位",
                    ajxx_cbdw_mc AS "承办单位",
                    LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                    ajxx_zbbj AS "在办标记",
                    ajxx_ajly AS "案件来源"
                FROM "ywdata"."zq_zfba_ajxx"
                WHERE ajxx_lasj BETWEEN  %s AND %s
                AND "ajxx_ajzt" IN ('已立案','已受案','已受理')
                AND 1=1
            ```
    - 高质量、同比高质量:
        - 数据源
            ```
            SELECT zza."ajxx_ajbh"案件编号 ,count(zzj."jlz_id")刑拘人数,zza."ajxx_lasj"立案时间 ,zza."ajxx_cbqy_jc" 承办区域,left("ajxx_cbdw_bh_dm",6)地区,zza."ajxx_cbdw_bh"办案单位 ,zza."ajxx_ajzt" 案件状态,zza."ajxx_fadd"发案地点 ,zza."ajxx_fasj" 发案时间, zza."ajxx_jyaq"简要案情 
            FROM "zq_zfba_ajxx" zza LEFT JOIN "zq_zfba_jlz" zzj ON zza."ajxx_ajbh" =zzj."ajxx_ajbh" 
            WHERE zza."ajxx_ajlx" ='刑事'  AND zza."ajxx_lasj"  BETWEEN '2026-01-01' AND '2026-02-04' GROUP BY 
            zza."ajxx_ajbh" ,zza."ajxx_lasj" ,zza."ajxx_cbqy_jc" ,zza."ajxx_cbdw_bh" ,zza."ajxx_jyaq" ,zza."ajxx_ajzt" ,zza."ajxx_fadd" ,zza."ajxx_fasj" ,left("ajxx_cbdw_bh_dm",6)
            ```
        - 过滤条件:
            - 开始时间、结束时间:zza."ajxx_lasj"
            - 类型: 同之前的zq_zfba_ajxx一样
# 任务2: 在hqzcsj的"未成年人统计"模块新增一个字段:
## 新增字段及逻辑
    - 案件数(被侵害)、同比案件数(被侵害)
        - 数据源:
            ```
                SELECT
                    ajxx_ajbh AS "案件编号",
                    ajxx_jqbh AS "警情编号",
                    ajxx_ajmc AS "案件名称",
                    ajxx_ajlx AS "案件类型",
                    ajxx_ajzt AS "案件状态",
                    ajxx_ay AS "案由",
                    ajxx_ay_dm AS "案由代码",
                    ajxx_fasj AS "发案时间",
                    ajxx_lasj AS "立案时间",
                    ajxx_sldw_mc AS "受理单位",
                    ajxx_cbdw_mc AS "承办单位",
                    LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                    ajxx_zbbj AS "在办标记",
                    ajxx_ajly AS "案件来源"
                FROM "ywdata"."zq_zfba_wcnr_shr_ajxx" 
                WHERE ajxx_lasj BETWEEN '2026-01-01' AND '2026-02-01'
                AND 1=1
            ```
        - 过滤条件:
            - 开始时间、结束时间:ajxx_lasj
            - 类型:和之前的一样

