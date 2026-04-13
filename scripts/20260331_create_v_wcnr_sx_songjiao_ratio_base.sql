-- Create base view for 涉刑人员送生占比
-- 说明：把固定不变的年龄筛选、文书筛选和送校标记逻辑收敛到视图里，
--      外层查询继续按时间、案由类型过滤并完成最终聚合。

CREATE SCHEMA IF NOT EXISTS "ywdata";

CREATE OR REPLACE VIEW "ywdata"."v_wcnr_sx_songjiao_ratio_base" AS
WITH sx_minor AS MATERIALIZED (
    SELECT
        x."xyrxx_sfzh",
        x."xyrxx_xm",
        x."xyrxx_hjdxzqh",
        x."xyrxx_hjdxzqh_dm",
        x."xyrxx_xzdxz",
        x."xyrxx_lrsj",
        x."ajxx_join_ajxx_ajbh",
        x."ajxx_join_ajxx_lasj",
        x."ajxx_join_ajxx_ay",
        x."ajxx_join_ajxx_cbdw_bh_dm",
        TO_DATE(SUBSTRING(x."xyrxx_sfzh", 7, 8), 'YYYYMMDD') AS birthday
    FROM "ywdata"."zq_zfba_xyrxx" x
    WHERE x."ajxx_join_ajxx_isdel_dm" = '0'
      AND x."ajxx_join_ajxx_ajlx_dm" = '02'
      AND x."xyrxx_sfzh" IS NOT NULL
      AND LENGTH(x."xyrxx_sfzh") = 18
      AND x."xyrxx_sfzh" ~ '^\d{17}[\dXx]$'
      AND SUBSTRING(x."xyrxx_sfzh", 7, 8) ~ '^\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$'
),
sx_age16 AS MATERIALIZED (
    SELECT m.*
    FROM sx_minor m
    INNER JOIN "ywdata"."zq_zfba_ajxx" aj
      ON m."ajxx_join_ajxx_ajbh" = aj."ajxx_ajbh"
    WHERE DATE_PART('year', AGE(aj."ajxx_fasj"::DATE, m.birthday)) < 16
),
ws_pre AS MATERIALIZED (
    SELECT ajbh, xgry_xm, wsmc
    FROM "ywdata"."zq_zfba_wenshu"
    WHERE wsmc ~ '不予立案通知书|撤销案件决定书|不予行政处罚决定书|提请专门'
),
ws_a AS MATERIALIZED (
    SELECT DISTINCT ajbh
    FROM ws_pre
    WHERE wsmc ~ '不予立案通知书|撤销案件决定书'
),
ws_b AS MATERIALIZED (
    SELECT DISTINCT ajbh, xgry_xm
    FROM ws_pre
    WHERE wsmc ~ '不予行政处罚决定书'
),
ws_sx AS MATERIALIZED (
    SELECT DISTINCT ajbh, xgry_xm
    FROM ws_pre
    WHERE wsmc ~ '提请专门'
),
sx_labeled AS MATERIALIZED (
    SELECT
        m."xyrxx_sfzh",
        m."xyrxx_xm",
        m."xyrxx_hjdxzqh",
        m."xyrxx_hjdxzqh_dm",
        m."xyrxx_xzdxz",
        m."xyrxx_lrsj",
        m."ajxx_join_ajxx_ajbh",
        m."ajxx_join_ajxx_lasj",
        m."ajxx_join_ajxx_ay",
        m."ajxx_join_ajxx_cbdw_bh_dm",
        EXISTS (
            SELECT 1
            FROM ws_sx s
            WHERE s.ajbh = m."ajxx_join_ajxx_ajbh"
              AND TRIM(s.xgry_xm) = m."xyrxx_xm"
        ) AS is_songxue
    FROM sx_age16 m
    WHERE EXISTS (
              SELECT 1
              FROM ws_a a
              WHERE a.ajbh = m."ajxx_join_ajxx_ajbh"
          )
       OR EXISTS (
              SELECT 1
              FROM ws_b b
              WHERE b.ajbh = m."ajxx_join_ajxx_ajbh"
                AND TRIM(b.xgry_xm) = m."xyrxx_xm"
          )
)
SELECT
    s."xyrxx_sfzh",
    s."xyrxx_xm",
    s."xyrxx_hjdxzqh",
    s."xyrxx_hjdxzqh_dm",
    s."xyrxx_xzdxz",
    s."xyrxx_lrsj",
    s."ajxx_join_ajxx_ajbh",
    s."ajxx_join_ajxx_lasj",
    s."ajxx_join_ajxx_ay",
    s."ajxx_join_ajxx_cbdw_bh_dm",
    s.is_songxue,
    LEFT(COALESCE(s."ajxx_join_ajxx_cbdw_bh_dm", ''), 6) AS "地区代码"
FROM sx_labeled s;

COMMENT ON VIEW "ywdata"."v_wcnr_sx_songjiao_ratio_base" IS '涉刑人员送生占比基础视图：固定的年龄筛选、文书筛选和送校标记逻辑';