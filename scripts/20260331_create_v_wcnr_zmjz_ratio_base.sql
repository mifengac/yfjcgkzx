-- Create base view for 专门(矫治)教育占比
-- 说明：把固定不变的逻辑收敛到视图里，外层查询继续按时间、案由类型过滤

CREATE SCHEMA IF NOT EXISTS "ywdata";

CREATE OR REPLACE VIEW "ywdata"."v_wcnr_zmjz_ratio_base" AS
WITH wf_validated AS MATERIALIZED (
    SELECT
        x."xyrxx_sfzh",
        x."xyrxx_xm",
        x."xyrxx_xb",
        x."xyrxx_lrsj",
        x."ajxx_join_ajxx_ajbh",
        x."ajxx_join_ajxx_lasj",
        x."ajxx_join_ajxx_ajlx",
        x."ajxx_join_ajxx_ajmc",
        x."ajxx_join_ajxx_ay",
        x."xyrxx_hjdxzqh",
        x."xyrxx_hjdxzqh_dm",
        x."xyrxx_hjdxz",
        x."xyrxx_xzdxz",
        x."ajxx_join_ajxx_cbdw_bh",
        x."ajxx_join_ajxx_cbdw_bh_dm",
        x."xyrxx_rybh",
        TO_DATE(SUBSTRING(x."xyrxx_sfzh", 7, 8), 'YYYYMMDD') AS birthday
    FROM "ywdata"."zq_zfba_xyrxx" x
    WHERE x."ajxx_join_ajxx_isdel_dm" = '0'
      AND x."xyrxx_sfzh" IS NOT NULL
      AND LENGTH(x."xyrxx_sfzh") = 18
      AND x."xyrxx_sfzh" ~ '^\d{17}[\dXx]$'
      AND SUBSTRING(x."xyrxx_sfzh", 7, 8) ~ '^\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$'
    AND SUBSTRING(x."xyrxx_sfzh", 7, 8) >= TO_CHAR(x."xyrxx_lrsj" - INTERVAL '18 years', 'YYYYMMDD')
    AND SUBSTRING(x."xyrxx_sfzh", 7, 8) <= TO_CHAR(x."xyrxx_lrsj" - INTERVAL '12 years', 'YYYYMMDD')
),
xz_keys AS MATERIALIZED (
    SELECT DISTINCT
        b."ajxx_ajbh" AS ajbh,
        b."byxzcfjds_rybh" AS rybh
    FROM "ywdata"."zq_zfba_byxzcfjds" b
    WHERE COALESCE(b."byxzcfjds_cbryj", '') !~ '违法事实不能成立'

    UNION

    SELECT DISTINCT
        x."ajxx_ajbh" AS ajbh,
        x."xzcfjds_rybh" AS rybh
    FROM "ywdata"."zq_zfba_xzcfjds" x
),
wf_xz_filtered AS MATERIALIZED (
    SELECT v.*
    FROM wf_validated v
    WHERE v."ajxx_join_ajxx_ajlx" = '刑事'
      AND NOT EXISTS (
            SELECT 1
            FROM "ywdata"."zq_zfba_byxzcfjds" bxf
            WHERE bxf."ajxx_ajbh" = v."ajxx_join_ajxx_ajbh"
              AND bxf."byxzcfjds_rybh" = v."xyrxx_rybh"
              AND COALESCE(bxf."byxzcfjds_cbryj", '') ~ '违法事实不能成立'
        )
      AND NOT EXISTS (
            SELECT 1
            FROM "ywdata"."zq_zfba_wenshu" ws
            WHERE ws."ajbh" = v."ajxx_join_ajxx_ajbh"
              AND ws."xgry_xm" = v."xyrxx_xm"
        )

    UNION ALL

    SELECT v.*
    FROM wf_validated v
    INNER JOIN xz_keys k
        ON k.ajbh = v."ajxx_join_ajxx_ajbh"
       AND k.rybh = v."xyrxx_rybh"
    WHERE v."ajxx_join_ajxx_ajlx" = '行政'
),
base_raw AS MATERIALIZED (
    SELECT
        v."xyrxx_sfzh",
        v."xyrxx_xm",
        v."xyrxx_xb",
        v."xyrxx_lrsj",
        v."ajxx_join_ajxx_ajbh",
        v."ajxx_join_ajxx_lasj",
        v."ajxx_join_ajxx_ajlx",
        v."ajxx_join_ajxx_ajmc",
        v."ajxx_join_ajxx_ay",
        v."xyrxx_hjdxzqh",
        v."xyrxx_hjdxzqh_dm",
        v."xyrxx_hjdxz",
        v."xyrxx_xzdxz",
        v."ajxx_join_ajxx_cbdw_bh",
        v."ajxx_join_ajxx_cbdw_bh_dm",
        v."xyrxx_rybh",
        v.birthday,
        aj."ajxx_fasj",
        DATE_PART('year', AGE(aj."ajxx_fasj"::DATE, v.birthday))::INTEGER AS fasj_age,
        DATE_PART('year', AGE(CURRENT_DATE, v.birthday))::INTEGER AS current_age,
        LEFT(COALESCE(v."ajxx_join_ajxx_cbdw_bh_dm", ''), 6) AS "地区代码"
    FROM wf_xz_filtered v
    INNER JOIN "ywdata"."zq_zfba_ajxx" aj
        ON aj."ajxx_ajbh" = v."ajxx_join_ajxx_ajbh"
    WHERE DATE_PART('year', AGE(CURRENT_DATE, v.birthday)) < 18
),
base AS MATERIALIZED (
    SELECT DISTINCT ON (xyrxx_sfzh)
        *
    FROM base_raw
    WHERE NULLIF(BTRIM(COALESCE(xyrxx_sfzh, '')), '') IS NOT NULL
    ORDER BY xyrxx_sfzh, ajxx_join_ajxx_lasj DESC NULLS LAST, xyrxx_lrsj DESC NULLS LAST, ajxx_join_ajxx_ajbh DESC
),
base_sfzh AS MATERIALIZED (
    SELECT DISTINCT xyrxx_sfzh
    FROM base
    WHERE NULLIF(BTRIM(COALESCE(xyrxx_sfzh, '')), '') IS NOT NULL
),
admin_people AS MATERIALIZED (
    SELECT DISTINCT xyrxx_sfzh
    FROM base
    WHERE ajxx_join_ajxx_ajlx = '行政'
),
jlbzx_cte AS MATERIALIZED (
    SELECT DISTINCT
        b.xyrxx_sfzh,
        '是' AS "是否拘留不执行",
        '否' AS "是否2次违法犯罪且第一次开具了矫治文书",
        '否' AS "是否3次及以上违法犯罪",
        '否' AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
    FROM base_raw b
    INNER JOIN "ywdata"."zq_zfba_xzcfjds" xz
        ON  xz."ajxx_ajbh" = b.ajxx_join_ajxx_ajbh
        AND xz."xzcfjds_rybh" = b.xyrxx_rybh
    WHERE b.ajxx_join_ajxx_ajlx = '行政'
      AND COALESCE(xz."xzcfjds_cfzl", '') ~ '拘留'
      AND COALESCE(xz."xzcfjds_zxqk_text", '') ~ '不执行|不送'
      AND NULLIF(TRIM(xz."xzcfjds_tj_jlts"), '') IS NOT NULL
      AND CAST(xz."xzcfjds_tj_jlts" AS INTEGER) > 4
),
admin_history AS MATERIALIZED (
    SELECT
        x."xyrxx_sfzh",
        x."xyrxx_xm",
        x."ajxx_join_ajxx_ajbh",
        x."ajxx_join_ajxx_lasj",
        x."ajxx_join_ajxx_ay_dm"
    FROM "ywdata"."zq_zfba_xyrxx" x
    INNER JOIN admin_people p
        ON p.xyrxx_sfzh = x."xyrxx_sfzh"
    WHERE x."ajxx_join_ajxx_isdel_dm" = '0'
      AND x."ajxx_join_ajxx_ajlx" = '行政'
),
admin_agg AS MATERIALIZED (
    SELECT
        xyrxx_sfzh,
        COUNT(*) AS admin_case_cnt,
        MIN(ajxx_join_ajxx_ay_dm) AS min_ay_dm,
        MAX(ajxx_join_ajxx_ay_dm) AS max_ay_dm
    FROM admin_history
    GROUP BY xyrxx_sfzh
),
admin_first_case AS MATERIALIZED (
    SELECT DISTINCT ON (xyrxx_sfzh)
        xyrxx_sfzh,
        xyrxx_xm,
        ajxx_join_ajxx_ajbh,
        ajxx_join_ajxx_lasj
    FROM admin_history
    ORDER BY xyrxx_sfzh, ajxx_join_ajxx_lasj ASC NULLS LAST, ajxx_join_ajxx_ajbh ASC
),
twice_same_ay_with_doc_cte AS MATERIALIZED (
    SELECT DISTINCT
        f.xyrxx_sfzh,
        '否' AS "是否拘留不执行",
        '是' AS "是否2次违法犯罪且第一次开具了矫治文书",
        '否' AS "是否3次及以上违法犯罪",
        '否' AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
    FROM admin_agg a
    INNER JOIN admin_first_case f
        ON f.xyrxx_sfzh = a.xyrxx_sfzh
    WHERE a.admin_case_cnt = 2
      AND a.min_ay_dm IS NOT NULL
      AND a.min_ay_dm = a.max_ay_dm
      AND (
            EXISTS (
                SELECT 1
                FROM "ywdata"."zq_zfba_xjs2" xj
                WHERE xj."ajbh" = f.ajxx_join_ajxx_ajbh
                  AND xj."xgry_xm" = f.xyrxx_xm
            )
         OR EXISTS (
                SELECT 1
                FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" zl
                WHERE zl."zltzs_ajbh" = f.ajxx_join_ajxx_ajbh
                  AND zl."zltzs_ryxm" = f.xyrxx_xm
            )
        )
),
three_plus_cte AS MATERIALIZED (
    SELECT DISTINCT
        xyrxx_sfzh,
        '否' AS "是否拘留不执行",
        '否' AS "是否2次违法犯罪且第一次开具了矫治文书",
        '是' AS "是否3次及以上违法犯罪",
        '否' AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
    FROM admin_agg
    WHERE admin_case_cnt >= 3
),
criminal_no_detain_no_stop_cte AS MATERIALIZED (
    SELECT DISTINCT
        b.xyrxx_sfzh,
        '否' AS "是否拘留不执行",
        '否' AS "是否2次违法犯罪且第一次开具了矫治文书",
        '否' AS "是否3次及以上违法犯罪",
        '是' AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
    FROM base_raw b
    WHERE b.ajxx_join_ajxx_ajlx = '刑事'
      AND NOT EXISTS (
            SELECT 1
            FROM "ywdata"."zq_zfba_jlz" j
            WHERE j."ajxx_ajbh" = b.ajxx_join_ajxx_ajbh
              AND j."jlz_rybh" = b.xyrxx_rybh
        )
      AND NOT EXISTS (
            SELECT 1
            FROM "ywdata"."zq_zfba_wenshu" ws
            WHERE ws."ajbh" = b.ajxx_join_ajxx_ajbh
              AND ws."xgry_xm" = b.xyrxx_xm
              AND COALESCE(ws."wsmc", '') ~ '终止侦查决定书'
        )
),
all_condition_rows AS MATERIALIZED (
    SELECT * FROM jlbzx_cte
    UNION ALL
    SELECT * FROM twice_same_ay_with_doc_cte
    UNION ALL
    SELECT * FROM three_plus_cte
    UNION ALL
    SELECT * FROM criminal_no_detain_no_stop_cte
),
person_flags AS MATERIALIZED (
    SELECT
        xyrxx_sfzh,
        MAX("是否拘留不执行") AS "是否拘留不执行",
        MAX("是否2次违法犯罪且第一次开具了矫治文书") AS "是否2次违法犯罪且第一次开具了矫治文书",
        MAX("是否3次及以上违法犯罪") AS "是否3次及以上违法犯罪",
        MAX("是否犯罪且未刑拘且未开具《终止侦查决定书》") AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
    FROM all_condition_rows
    GROUP BY xyrxx_sfzh
),
qualified_people AS MATERIALIZED (
    SELECT
        xyrxx_sfzh,
        '是' AS "是否符合专门(矫治)教育"
    FROM person_flags
),
qualified_latest_case AS MATERIALIZED (
    SELECT DISTINCT ON (b."xyrxx_sfzh")
        b."xyrxx_sfzh",
        b."xyrxx_xm",
        b."ajxx_join_ajxx_ajbh"
    FROM base b
    INNER JOIN qualified_people q
        ON q.xyrxx_sfzh = b."xyrxx_sfzh"
    ORDER BY b."xyrxx_sfzh", b."ajxx_join_ajxx_lasj" DESC NULLS LAST, b."ajxx_join_ajxx_ajbh" DESC
),
tqzmjy_people AS MATERIALIZED (
    SELECT DISTINCT
        q."xyrxx_sfzh",
        '是' AS "是否开具专门(矫治)教育申请书"
    FROM qualified_latest_case q
    INNER JOIN "ywdata"."zq_zfba_tqzmjy" t
        ON  t."ajbh" = q."ajxx_join_ajxx_ajbh"
        AND t."xgry_xm" = q."xyrxx_xm"
),
final_flags AS MATERIALIZED (
    SELECT
        s.xyrxx_sfzh,
        COALESCE(p."是否拘留不执行", '否') AS "是否拘留不执行",
        COALESCE(p."是否2次违法犯罪且第一次开具了矫治文书", '否') AS "是否2次违法犯罪且第一次开具了矫治文书",
        COALESCE(p."是否3次及以上违法犯罪", '否') AS "是否3次及以上违法犯罪",
        COALESCE(p."是否犯罪且未刑拘且未开具《终止侦查决定书》", '否') AS "是否犯罪且未刑拘且未开具《终止侦查决定书》",
        CASE WHEN q.xyrxx_sfzh IS NOT NULL THEN '是' ELSE '否' END AS "是否符合专门(矫治)教育",
        CASE WHEN tq.xyrxx_sfzh IS NOT NULL THEN '是' ELSE '否' END AS "是否开具专门(矫治)教育申请书"
    FROM base_sfzh s
    LEFT JOIN person_flags p
        ON p.xyrxx_sfzh = s.xyrxx_sfzh
    LEFT JOIN qualified_people q
        ON q.xyrxx_sfzh = s.xyrxx_sfzh
    LEFT JOIN tqzmjy_people tq
        ON tq.xyrxx_sfzh = s.xyrxx_sfzh
)
SELECT
    b.*,
    ff."是否拘留不执行",
    ff."是否2次违法犯罪且第一次开具了矫治文书",
    ff."是否3次及以上违法犯罪",
    ff."是否犯罪且未刑拘且未开具《终止侦查决定书》",
    ff."是否符合专门(矫治)教育",
    ff."是否开具专门(矫治)教育申请书"
FROM base b
LEFT JOIN final_flags ff
    ON ff.xyrxx_sfzh = b.xyrxx_sfzh;

COMMENT ON VIEW "ywdata"."v_wcnr_zmjz_ratio_base" IS '专门(矫治)教育占比基础视图：固定的去重、资格判定和最终标记逻辑';
