# 任务:帮我在当前项目中新增一个模块,名为"未成年人(打架斗殴)",相关代码文件全部放在weichengnianren-djdo文件夹中
# 页面布局:
    1. 页面格式为可视化数据大屏格式,左上角有时间范围控件,格式为"YYYY-MM-DD HH:MM:SS",开始时间默认为当前减7,如今天是'2026-01-23'那么开始时间默认为'2026-01-16 00:00:00',结束时间默认为今天0点,如'2026-01-23 00:00:00'
    2. 标题为"未成年人打架斗殴六项指标监测"
    3. 共分为6个小板块,一行3个,共2行
    4. 每个板块名称分别为"警情转案率","采取矫治教育措施率","涉刑人员送学率","责令加强监护率","场所发案率","纳管人员再犯率"
    5. 每个小板块都包含
        1. 时间范围控件,格式为"YYYY-MM-DD HH:MM:SS",开始时间默认为当前减7,如今天是'2026-01-23'那么开始时间默认为'2026-01-16 00:00:00',结束时间默认为今天0点,如'2026-01-23 00:00:00'
        2. 六个"地区"的柱状图,每个地区三个"柱子",地区固定为"云城","云安","罗定","新兴","郁南","全市"
        3. 排序按钮,单击排序按钮后可以选择三个"柱子"其中的一个进行排序
        4. "下载"按钮,单击"下载"会下载xlsx格式的文件,文件名格式为{开始时间}+"-"+{结束时间}+{小版块标题}+{时间戳}.xlsx
        5. 柱状图中的柱子名称可以通过点击来"显示"或"隐藏"
    6. 页面右上角有3个按钮
        1. "导出总览",点击"导出总览"导出6个小模块的表格数据和柱状图数据,分别是"警情转案率柱状图""警情转案率表格","采取矫治教育措施率柱状图","采取矫治教育措施率表格","涉刑人员送学率柱状图","涉刑人员送学率表格","责令加强监护率柱状图","责令加强监护率表格","场所发案率柱状图","场所发案率表格","纳管人员再犯率柱状图","纳管人员再犯率表格",格式为{开始时间}+'-'+{结束时间}+'全市未成年人打架斗殴六项指标监测.pdf
        2. "导出详情",点击"导出详情"导出六个小版块的xlsx表格,分为6个sheet,每个sheet名称分别为"警情转案率","采取矫治教育措施率","涉刑人员送学率","责令加强监护率","场所发案率","纳管人员再犯率"
        3. "导入送校数据",点击该按钮弹出文件选择框,可以选择文件,用户只能选择xls格式文件,导入后进行判断,如果不是xls格式文件则弹出提醒框并停止后面的步骤,如果是xls文件判断"sheet"和"第三行"的列是否对的上,对不上则提醒用户并停止,如果正确则执行weichengnianren-djdo\0125_wcnr_sfzxx_import.py代码的逻辑导入数据,最后导入成功则弹出框提醒导入情况
    7. 柱状图使用weichengnianren-djdo\static\chart.min.js,版本是4.4.1
# 数据源:
    1.警情转案率:
        1. ```SELECT jq."caseno" 警情编号,jq."calltime" 报警时间,jq."cmdname" 分局,jq."dutydeptname" 管辖单位,jq."casecontents" 报警内容,jq."replies" 处警情况,mza."案件编号" ,mza."案件名称" ,mza."地区" ,mza."办案单位名称" 
        FROM "zq_kshddpt_dsjfx_jq" jq LEFT JOIN "mv_zfba_ajxx" mza ON jq."caseno" =mza."警情编号" 
        WHERE jq."newcharasubclass" IN (SELECT UNNEST (ctc."newcharasubclass_list") FROM "case_type_config" ctc WHERE ctc."leixing"='打架斗殴')
        AND jq."calltime" BETWEEN {开始时间} AND {结束时间} AND jq."casemarkok" ~'未成年'```
        2. 其中"地区"通过:jq."cmdname" 分局,字段进行判断
            1. 所有数据为'全市'
            2. 包含'云城'的为云城
            3. 包含'云安'的为云安
            4. 包含'罗定'的为罗定
            5. 包含'新兴'的为新兴
            6. 包含'郁南'的为郁南
        3. 三个"柱子"分别为:
            1. "警情",通过jq."caseno" 警情编号计数
            2. "案件",通过mza."案件编号"计数(如果是"空"则不计数)
            3. "转案率",通过"案件"/"警情"*100%(取2位小数)
    2. 采取矫治教育措施率
        1. ```
        WITH minor_fight AS (
            SELECT
                mzaa."案件编号",
                mzaa."案件名称",
                mzaa."立案日期",
                CASE
                    WHEN mzaa."地区" ='445302' THEN '云城'
                    WHEN mzaa."地区" ='445303' THEN '云安'
                    WHEN mzaa."地区" ='445381' THEN '罗定'
                    WHEN mzaa."地区" ='445321' THEN '新兴'
                    WHEN mzaa."地区" ='445322' THEN '郁南'
                END AS "地区",
                mzaa."办案单位名称",
                mmp."xm"         AS "姓名",
                mmp."zjhm"       AS "证件号码",
                mmp."role_names" AS "人员类型",
                mmp."age_years"  AS "年龄",
                mmp."anjxgrybh"  AS "人员编号"
            FROM "mv_zfba_all_ajxx" mzaa
            INNER JOIN "mv_minor_person" mmp
                ON mzaa."案件编号" = mmp."asjbh"
            WHERE mzaa."案由" SIMILAR TO (
                SELECT ctc."ay_pattern"
                FROM "case_type_config" ctc
                WHERE ctc."leixing" = '打架斗殴'
            )
            AND mzaa."立案日期" >= '2026-01-01'
            AND mzaa."立案日期" < NOW()
            AND mmp."role_names" = '嫌疑人'
        ),

        /* ✅ 关键：收敛案件编号集合 */
        target_aj AS (
            SELECT DISTINCT "案件编号"
            FROM minor_fight
        ),

        doc_hit_raw AS (
            SELECT
                xjs_ajbh AS "案件编号",
                xjs_rybh AS "人员编号",
                '训诫书'  AS "文书名称",
                xjs_xjyy AS "训诫原因"
            FROM "ywdata"."zq_zfba_xjs"
            WHERE xjs_ajbh IS NOT NULL AND xjs_rybh IS NOT NULL

            UNION ALL

            SELECT
                zltzs_ajbh AS "案件编号",
                zltzs_rybh AS "人员编号",
                '加强监督教育/责令接受家庭教育指导通知书' AS "文书名称",
                zltzs_blxw AS "训诫原因"
            FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs"
            WHERE zltzs_ajbh IS NOT NULL AND zltzs_rybh IS NOT NULL
        ),

        doc_hit AS (
            SELECT
                "案件编号",
                "人员编号",
                string_agg(DISTINCT "文书名称", ',' ORDER BY "文书名称") AS "文书名称",
                string_agg(DISTINCT "训诫原因", ',' ORDER BY "训诫原因") AS "训诫原因"
            FROM doc_hit_raw
            GROUP BY "案件编号", "人员编号"
        ),

        /* ✅ 只对 target_aj 范围内的案件去重清洗 */
        baxgry_distinct AS (
            SELECT DISTINCT
                r.asjbh AS "案件编号",
                NULLIF(TRIM(r.baxgry_xm), '') AS "name",
                NULLIF(TRIM(r.lxdh), '')      AS "phone"
            FROM "zfba_ry_001" r
            INNER JOIN target_aj t
                ON r.asjbh = t."案件编号"
            WHERE NULLIF(TRIM(r.baxgry_xm), '') IS NOT NULL
            AND NULLIF(TRIM(r.lxdh), '') IS NOT NULL
        ),

        baxgry_json AS (
            SELECT
                d."案件编号",
                jsonb_agg(
                    jsonb_build_object('name', d."name", 'phone', d."phone")
                    ORDER BY d."name", d."phone"
                ) AS "办案联系人_json",
                jsonb_agg(
                    d."phone"
                    ORDER BY d."phone"
                ) AS "联系电话_json"
            FROM baxgry_distinct d
            GROUP BY d."案件编号"
        )

        SELECT
            mf.*,
            dh."训诫原因",
            dh."文书名称",
            CASE WHEN dh."案件编号" IS NOT NULL THEN '是' ELSE '否' END AS "是否开具文书",
            bx."办案联系人_json",
            bx."联系电话_json"
        FROM minor_fight mf
        LEFT JOIN doc_hit dh
            ON mf."案件编号" = dh."案件编号"
        AND mf."人员编号" = dh."人员编号"
        LEFT JOIN baxgry_json bx
            ON mf."案件编号" = bx."案件编号";
        ```
        2. 其中"地区"通过"地区"字段判断,所有数据即为"全市"
        3. 三个"柱子"分别为:
            1. "应采取矫治教育措施人数",对所有字段计数
            2. "已采取矫治教育措施人数",对"是否开具文书"的值为'是'进行计数
            3. "采取矫治教育措施率",通过"已采取矫治教育措施人数"/"应采取矫治教育措施人数"*100%,取2位小数
    3. 涉刑人员送学率
        1. ```
            SELECT
                mmp."anjxgrybh" AS 人员编号,
                mmp."xm" AS 姓名,
                mmp."zjhm" AS 证件号码,
                mmp."hjd_xz" AS 户籍地,
                mmp."xzd_xz" AS 现住地,
                mmp."age_years" AS 年龄,
                mmp."role_names" AS 人员类型,
                mzaa."案件编号",
                CASE
                    WHEN mzaa."地区" ='445302' THEN '云城'
                    WHEN mzaa."地区" ='445303' THEN '云安'
                    WHEN mzaa."地区" ='445381' THEN '罗定'
                    WHEN mzaa."地区" ='445321' THEN '新兴'
                    WHEN mzaa."地区" ='445322' THEN '郁南'
                END AS "地区",
                mzaa."案件名称",
                mzaa."简要案情",
                mzaa."立案日期",
                mzaa."办案单位名称",
                bx."办案联系人_json",
                bx."联系电话_json",

                /* ✅ 新增：是否送校 */
                CASE
                    WHEN sx.is_match = 1 THEN '是'
                    ELSE '否'
                END AS "是否送校"

            FROM "mv_minor_person" mmp
            INNER JOIN "mv_zfba_all_ajxx" mzaa
                ON mmp."asjbh" = mzaa."案件编号"

            /* 办案联系人聚合 */
            LEFT JOIN LATERAL (
                WITH d AS (
                    SELECT DISTINCT
                        NULLIF(TRIM(r.baxgry_xm), '') AS name,
                        NULLIF(TRIM(r.lxdh), '')      AS phone
                    FROM "zfba_ry_001" r
                    WHERE r.asjbh = mzaa."案件编号"
                    AND NULLIF(TRIM(r.baxgry_xm), '') IS NOT NULL
                    AND NULLIF(TRIM(r.lxdh), '') IS NOT NULL
                )
                SELECT
                    jsonb_agg(jsonb_build_object('name', d.name, 'phone', d.phone) ORDER BY d.name, d.phone) AS "办案联系人_json",
                    jsonb_agg(d.phone ORDER BY d.phone) AS "联系电话_json"
                FROM d
            ) bx ON TRUE

            /* ✅ 是否送校：存在 rx_time > 立案日期 即命中（不扩行） */
            LEFT JOIN LATERAL (
                SELECT 1 AS is_match
                FROM zq_wcnr_sfzxx z
                WHERE z."sfzhm" = mmp."zjhm"
                AND z.rx_time > mzaa."立案日期"
                LIMIT 1
            ) sx ON TRUE

            WHERE mmp."role_names" = '嫌疑人'
            AND mzaa."立案日期" BETWEEN '2026-01-01' AND NOW()
            AND mzaa."案由" SIMILAR TO (
                SELECT ctc."ay_pattern"
                FROM "case_type_config" ctc
                WHERE ctc."leixing" = '打架斗殴'
            )
            AND mzaa."案件类型" = '刑事';
        ```
    2. 其中"地区"通过"地区"列分类,"全市"则为所有数据
    3. 三个"柱子"分别为:
        1. "符合涉刑人员送学人数",对所有人员计数
        2. "实际送学人身",对"是否送校"值为'是'的进行计数
        3. "涉刑人员送学率","实际送学人身"/"符合涉刑人员送学人数"*100%取2位小数
    4. 责令加强监护率
        1. ```
            WITH minor_fight AS (
                SELECT
                    mzaa."案件编号",
                    mzaa."案件名称",
                    mzaa."立案日期",
                    mzaa."办案单位名称",
                    CASE
                        WHEN mzaa."地区" ='445302' THEN '云城'
                        WHEN mzaa."地区" ='445303' THEN '云安'
                        WHEN mzaa."地区" ='445381' THEN '罗定'
                        WHEN mzaa."地区" ='445321' THEN '新兴'
                        WHEN mzaa."地区" ='445322' THEN '郁南'
                    END AS "地区",
                    mmp."xm"         AS "姓名",
                    mmp."zjhm"       AS "证件号码",
                    mmp."role_names" AS "人员类型",
                    mmp."age_years"  AS "年龄",
                    mmp."anjxgrybh"  AS "人员编号"
                FROM "mv_zfba_all_ajxx" mzaa
                INNER JOIN "mv_minor_person" mmp
                    ON mzaa."案件编号" = mmp."asjbh"
                WHERE mzaa."案由" SIMILAR TO (
                    SELECT ctc."ay_pattern"
                    FROM "case_type_config" ctc
                    WHERE ctc."leixing" = '打架斗殴'
                )
                AND mzaa."立案日期" >= '2026-01-01'
                AND mzaa."立案日期" < NOW()
                AND mmp."role_names" = '嫌疑人'
            ),

            /* ✅ 关键：把本次结果涉及的案件编号先收敛出来 */
            target_aj AS (
                SELECT DISTINCT "案件编号"
                FROM minor_fight
            ),

            jtjyzdtzs_hit AS (
                SELECT DISTINCT
                    jqjhjyzljsjtjyzdtzs_ajbh AS "案件编号",
                    jqjhjyzljsjtjyzdtzs_rybh AS "人员编号"
                FROM "ywdata"."zq_zfba_jtjyzdtzs"
                WHERE jqjhjyzljsjtjyzdtzs_ajbh IS NOT NULL
                AND jqjhjyzljsjtjyzdtzs_rybh IS NOT NULL
            ),

            /* ✅ 只在 target_aj 范围内做去重清洗（不再扫全表） */
            baxgry_distinct AS (
                SELECT DISTINCT
                    r.asjbh AS "案件编号",
                    NULLIF(TRIM(r.baxgry_xm), '') AS "name",
                    NULLIF(TRIM(r.lxdh), '')      AS "phone"
                FROM "zfba_ry_001" r
                INNER JOIN target_aj t
                    ON r.asjbh = t."案件编号"
                WHERE NULLIF(TRIM(r.baxgry_xm), '') IS NOT NULL
                AND NULLIF(TRIM(r.lxdh), '') IS NOT NULL
            ),

            baxgry_json AS (
                SELECT
                    d."案件编号",
                    jsonb_agg(
                        jsonb_build_object('name', d."name", 'phone', d."phone")
                        ORDER BY d."name", d."phone"
                    ) AS "办案联系人_json",
                    jsonb_agg(
                        d."phone"
                        ORDER BY d."phone"
                    ) AS "联系电话_json"
                FROM baxgry_distinct d
                GROUP BY d."案件编号"
            )

            SELECT
                mf.*,
                CASE
                    WHEN jh."案件编号" IS NOT NULL THEN '是'
                    ELSE '否'
                END AS "是否开具文书",
                bx."办案联系人_json",
                bx."联系电话_json"
            FROM minor_fight mf
            LEFT JOIN jtjyzdtzs_hit jh
                ON mf."案件编号" = jh."案件编号"
            AND mf."人员编号" = jh."人员编号"
            LEFT JOIN baxgry_json bx
                ON mf."案件编号" = bx."案件编号";
        ```
            2. 其中"地区"通过"地区"字段判断,所有数据即为"全市"
            3. 三个"柱子"分别为:
                1. "应责令加强监护人数",对所有字段计数
                2. "已责令加强监护人数",对"是否开具文书"的值为'是'进行计数
                3. "责令加强监护率",通过"已责令加强监护人数"/"应责令加强监护人数"*100%,取2位小数
    5. 场所发案率
        1. ```WITH aj_list AS (
            SELECT DISTINCT
                mza.*
            FROM "mv_zfba_all_ajxx" mza
            INNER JOIN "mv_minor_person" mmp
                ON mza."案件编号" = mmp."asjbh"
            WHERE mza."案由" SIMILAR TO (
                SELECT ctc."ay_pattern"
                FROM "case_type_config" ctc
                WHERE ctc."leixing" = '打架斗殴'
            )
            AND mza."立案日期" BETWEEN '2026-01-01' AND NOW()
            AND mmp."role_names" = '嫌疑人'
        )
        SELECT
            a.*,
            bx."办案联系人_json",
            bx."联系电话_json"
        FROM aj_list a
        LEFT JOIN LATERAL (
            WITH d AS (
                SELECT DISTINCT
                    NULLIF(TRIM(r.baxgry_xm), '') AS name,
                    NULLIF(TRIM(r.lxdh), '')      AS phone
                FROM "zfba_ry_001" r
                WHERE r.asjbh = a."案件编号"
                AND NULLIF(TRIM(r.baxgry_xm), '') IS NOT NULL
                AND NULLIF(TRIM(r.lxdh), '') IS NOT NULL
            )
            SELECT
                jsonb_agg(jsonb_build_object('name', d.name, 'phone', d.phone) ORDER BY d.name, d.phone) AS "办案联系人_json",
                jsonb_agg(d.phone ORDER BY d.phone) AS "联系电话_json"
            FROM d
        ) bx ON TRUE;
        ```
        2. 查询到的数据需要再次通过xungfang/5lei_dizhi_model中的模型对"案件发生地址名称"进行分类,该模型是通过macbert训练的,其中新增一列"分类结果"
        3. 其中"地区"通过"地区"列分类,"全市"则为所有数据
        4. 三个"柱子"分别为:
            1. "娱乐场所案件数",对"分类结果"值为'重点管控场所'的值进行过滤计数
            2. "案件数",对所有值进行计数
            3. "场所发案率",通过"娱乐场所案件数"/"案件数"*100%取2位小时
    6. 纳管人员再犯率
        1. ```
            WITH fight_suspect AS (
                SELECT DISTINCT
                    mmp."zjhm" AS zjhm
                FROM "mv_minor_person" mmp
                INNER JOIN "mv_zfba_all_ajxx" mzaa
                    ON mmp."asjbh" = mzaa."案件编号"
                WHERE mzaa."案由" SIMILAR TO (
                    SELECT ctc."ay_pattern"
                    FROM "case_type_config" ctc
                    WHERE ctc."leixing" = '打架斗殴'
                )
                AND mzaa."立案日期" BETWEEN '2026-01-01' AND NOW()
                AND mmp."role_names" = '嫌疑人'
                AND mmp."zjhm" IS NOT NULL
            )

            SELECT
                bzr.*,
                CASE
                    WHEN bzr."ssfj_dm" ='445302000000' THEN '云城'
                    WHEN bzr."ssfj_dm" ='445303000000' THEN '云安'
                    WHEN bzr."ssfj_dm" ='445381000000' THEN '罗定'
                    WHEN bzr."ssfj_dm" ='445321000000' THEN '新兴'
                    WHEN bzr."ssfj_dm" ='445322000000' THEN '郁南'
                END AS "地区",
                CASE
                    WHEN fs.zjhm IS NOT NULL THEN '是'
                    ELSE '否'
                END AS "是否再犯"
            FROM "stdata"."b_zdry_ryxx" bzr
            LEFT JOIN fight_suspect fs
                ON bzr.zjhm = fs.zjhm
            WHERE bzr.sflg = '1'
            AND bzr."deleteflag" = '0';
        ```
        2. 其中"地区"通过"地区"列分类,"全市"则为所有数据
        3. 三个柱子分别为:
            1. "列管人数",对所有值计数
            2. "再犯人数",对"是否再犯"值为'是'的计数
            3. "再犯率","再犯人数"/"列管人数"*100%取两位小数