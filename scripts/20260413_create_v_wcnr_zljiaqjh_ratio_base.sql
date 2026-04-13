-- Create base view for 责令加强监护率
-- 说明：把按案件编号聚合未成年嫌疑人数和家庭教育指导通知书数量的逻辑收敛到视图里，
--      外层查询继续按时间、案由类型过滤。
-- 分母（应责令加强监护数）：v_wcnr_wfry_jbxx_base 中按案件编号分组，
--   统计每案件中发案时 <18 岁的未成年嫌疑人数（去重身份证号）。
-- 分子（已责令加强监护数）：zq_zfba_jtjyzdtzs2 中按案件编号统计家庭教育指导通知书数量。

CREATE SCHEMA IF NOT EXISTS "ywdata";

CREATE OR REPLACE VIEW "ywdata"."v_wcnr_zljiaqjh_ratio_base" AS
WITH minor_agg AS MATERIALIZED (
    SELECT
        "ajxx_join_ajxx_ajbh" AS ajbh,
        MAX("xyrxx_lrsj") AS lrsj,
        MAX("ajxx_join_ajxx_ajlx") AS ajlx,
        MAX("ajxx_join_ajxx_ajmc") AS ajmc,
        MAX("ajxx_join_ajxx_ay") AS ay,
        MAX("ajxx_join_ajxx_cbdw_bh") AS cbdw_bh,
        MAX("ajxx_join_ajxx_cbdw_bh_dm") AS cbdw_bh_dm,
        MAX("ajxx_join_ajxx_lasj") AS lasj,
        COUNT(DISTINCT "xyrxx_sfzh") AS yzt_count
    FROM "ywdata"."v_wcnr_wfry_jbxx_base"
    GROUP BY "ajxx_join_ajxx_ajbh"
),
jgh_agg AS MATERIALIZED (
    SELECT
        ajbh,
        COUNT(*) AS yzt_done
    FROM "ywdata"."zq_zfba_jtjyzdtzs2"
    GROUP BY ajbh
)
SELECT
    m.ajbh                                      AS "案件编号",
    m.ajlx                                      AS "案件类型",
    m.ajmc                                      AS "案件名称",
    m.ay                                        AS "案由名称",
    m.cbdw_bh                                   AS "办案单位",
    m.cbdw_bh_dm                                AS "办案单位代码",
    LEFT(COALESCE(m.cbdw_bh_dm, ''), 6)         AS "地区代码",
    m.lrsj                                      AS "录入时间",
    m.lasj                                      AS "立案时间",
    m.yzt_count                                 AS "应责令加强监护数",
    COALESCE(j.yzt_done, 0)                     AS "已责令加强监护数"
FROM minor_agg m
LEFT JOIN jgh_agg j ON m.ajbh = j.ajbh;

COMMENT ON VIEW "ywdata"."v_wcnr_zljiaqjh_ratio_base"
    IS '责令加强监护率基础视图：按案件聚合未成年嫌疑人数（分母）和家庭教育指导通知书数（分子）';
