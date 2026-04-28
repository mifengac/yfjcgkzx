-- Create test base view for 严重不良未成年人矫治教育占比.
-- 生产验证阶段先创建 v_wcnr_yzbl_ratio_base2；验证通过后再替换旧视图名。

CREATE SCHEMA IF NOT EXISTS "ywdata";

CREATE OR REPLACE VIEW "ywdata"."v_wcnr_yzbl_ratio_base2" AS
WITH wfzf_ranked AS MATERIALIZED (
    SELECT
        b.*,
        row_number() OVER (
            PARTITION BY b."xyrxx_sfzh"
            ORDER BY b."ajxx_join_ajxx_lasj" DESC NULLS LAST,
                     b."xyrxx_lrsj" DESC NULLS LAST,
                     b."ajxx_join_ajxx_ajbh" DESC
        ) AS rn_desc
    FROM "ywdata"."v_wcnr_wfry_jbxx_base" b
    WHERE NULLIF(BTRIM(COALESCE(b."xyrxx_sfzh", '')), '') IS NOT NULL
),
wfzf_people AS MATERIALIZED (
    SELECT
        MIN(r."xyrxx_xm") FILTER (WHERE r.rn_desc = 1) AS "姓名",
        r."xyrxx_sfzh" AS "身份证号",
        MIN(r."xyrxx_xb") FILTER (WHERE r.rn_desc = 1) AS "性别",
        MIN(r."fasj_age") FILTER (WHERE r.rn_desc = 1) AS "最近一次发案年龄",
        MIN(r."current_age") FILTER (WHERE r.rn_desc = 1) AS "现在年龄",
        MIN(r."xyrxx_lrsj") FILTER (WHERE r.rn_desc = 1) AS "录入时间",
        COUNT(*) AS "违法次数",
        STRING_AGG(r."ajxx_join_ajxx_ajlx", ' → ' ORDER BY r."ajxx_join_ajxx_lasj" DESC NULLS LAST) AS "案件类型",
        STRING_AGG(TO_CHAR(r."ajxx_join_ajxx_lasj", 'YYYY-MM-DD'), ' → ' ORDER BY r."ajxx_join_ajxx_lasj" DESC NULLS LAST) AS "立案时间链",
        STRING_AGG(r."ajxx_join_ajxx_ajbh", ' → ' ORDER BY r."ajxx_join_ajxx_lasj" DESC NULLS LAST) AS "案件编号",
        STRING_AGG(r."ajxx_join_ajxx_ay", ' → ' ORDER BY r."ajxx_join_ajxx_lasj" DESC NULLS LAST) AS "案由",
        STRING_AGG(DISTINCT r."xyrxx_hjdxzqh", '、') AS "户籍行政区",
        STRING_AGG(DISTINCT r."xyrxx_hjdxzqh_dm", '、') AS "户籍地代码",
        STRING_AGG(DISTINCT r."xyrxx_hjdxz", '、') AS "户籍地",
        STRING_AGG(DISTINCT r."xyrxx_xzdxz", '、') AS "现住地",
        STRING_AGG(DISTINCT r."ajxx_join_ajxx_cbdw_bh", ' → ') AS "办案部门",
        STRING_AGG(DISTINCT r."ajxx_join_ajxx_cbdw_bh_dm", ' → ') AS "办案部门编码",
        STRING_AGG(r."xyrxx_rybh", ' → ' ORDER BY r."ajxx_join_ajxx_lasj" DESC NULLS LAST) AS "人员编号",
        NULL::text AS "学校名称",
        NULL::text AS "年级名称",
        NULL::text AS "班级名称",
        NULL::text AS "就读状态"
    FROM wfzf_ranked r
    GROUP BY r."xyrxx_sfzh"
),
wenshu_pre AS MATERIALIZED (
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
     AND TRIM(w.xgry_xm) = TRIM(xy.xyrxx_xm)
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
FROM wfzf_people v
LEFT JOIN jzws_agg jz
  ON v."身份证号" = jz.xyrxx_sfzh;

COMMENT ON VIEW "ywdata"."v_wcnr_yzbl_ratio_base2" IS '严重不良未成年人矫治教育占比基础测试视图：基于 v_wcnr_wfry_jbxx_base 的去重、资格判定和文书标记逻辑';
