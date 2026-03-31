-- Create base view for 严重不良未成年人矫治教育占比
-- 说明：把固定不变的逻辑收敛到视图里，外层查询继续按时间、案由类型过滤

CREATE SCHEMA IF NOT EXISTS "ywdata";

CREATE OR REPLACE VIEW "ywdata"."v_wcnr_yzbl_ratio_base" AS
WITH wenshu_pre AS MATERIALIZED (
    SELECT ajbh, xgry_xm, wsmc
    FROM "ywdata"."zq_zfba_wenshu"
    WHERE wsmc ~ '训诫书|责令未成年'
),
jzws_agg AS MATERIALIZED (
    SELECT
        xy.xyrxx_sfzh,
        STRING_AGG(DISTINCT w.wsmc, ' / ') AS wsmc_list
    FROM wenshu_pre w
    JOIN "ywdata"."zq_zfba_xyrxx" xy
      ON w.ajbh = xy.ajxx_join_ajxx_ajbh
     AND w.xgry_xm = xy.xyrxx_xm
    GROUP BY xy.xyrxx_sfzh
)
SELECT
    v.*,
    LEFT(COALESCE(v."办案部门编码", ''), 6) AS "地区代码",
    CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM "ywdata"."zq_zfba_xzcfjds" xz
            JOIN "ywdata"."zq_zfba_xyrxx" xy
              ON xz.ajxx_ajbh = xy.ajxx_join_ajxx_ajbh
             AND xz.xzcfjds_rybh = xy.xyrxx_rybh
            WHERE xy.xyrxx_sfzh = v."身份证号"
              AND xz.xzcfjds_zxqk_text !~ '不执行|不送'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM "ywdata"."zq_zfba_jlz" jl
            JOIN "ywdata"."zq_zfba_xyrxx" xy
              ON jl.ajxx_ajbh = xy.ajxx_join_ajxx_ajbh
             AND jl.jlz_rybh = xy.xyrxx_rybh
            WHERE xy.xyrxx_sfzh = v."身份证号"
        )
        THEN '是' ELSE '否'
    END AS "是否应采取矫治教育措施",
    CASE
        WHEN jz.xyrxx_sfzh IS NOT NULL THEN '是' ELSE '否'
    END AS "是否开具矫治文书",
    COALESCE(jz.wsmc_list, '') AS "开具文书名称"
FROM "ywdata"."v_wcnr_wfry_jbxx" v
LEFT JOIN jzws_agg jz
  ON v."身份证号" = jz.xyrxx_sfzh;

COMMENT ON VIEW "ywdata"."v_wcnr_yzbl_ratio_base" IS '严重不良未成年人矫治教育占比基础视图：固定的去重、资格判定和文书标记逻辑';