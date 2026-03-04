-- 未成年人10个率（wcnr_10lv）性能优化SQL
-- 目标：不改业务口径，降低 jzqk_compact 相关耗时，便于内网一次验证

-- =========================================================
-- 1) 建议补充索引（先在测试环境执行）
-- =========================================================

-- zq_zfba_wcnr_xyr：用于违法次数聚合、训诫关联、按时间过滤
CREATE INDEX IF NOT EXISTS idx_wcnr_xyr_sfzh
    ON ywdata.zq_zfba_wcnr_xyr (xyrxx_sfzh);

CREATE INDEX IF NOT EXISTS idx_wcnr_xyr_sfzh_ajbh_xm
    ON ywdata.zq_zfba_wcnr_xyr (xyrxx_sfzh, ajxx_join_ajxx_ajbh, xyrxx_xm);

CREATE INDEX IF NOT EXISTS idx_wcnr_xyr_lrsj
    ON ywdata.zq_zfba_wcnr_xyr (xyrxx_lrsj);

-- 处罚/拘留/文书表：用于 EXISTS/LEFT JOIN 键
CREATE INDEX IF NOT EXISTS idx_xzcfjds_ajbh_rybh
    ON ywdata.zq_zfba_xzcfjds (ajxx_ajbh, xzcfjds_rybh);

CREATE INDEX IF NOT EXISTS idx_byxzcfjds_ajbh_rybh
    ON ywdata.zq_zfba_byxzcfjds (ajxx_ajbh, byxzcfjds_rybh);

CREATE INDEX IF NOT EXISTS idx_jlz_ajbh_rybh
    ON ywdata.zq_zfba_jlz (ajxx_ajbh, jlz_rybh);

CREATE INDEX IF NOT EXISTS idx_zltzs_ajbh_rybh
    ON ywdata.zq_zfba_zlwcnrzstdxwgftzs (zltzs_ajbh, zltzs_rybh);

CREATE INDEX IF NOT EXISTS idx_xjs2_ajbh_xm
    ON ywdata.zq_zfba_xjs2 (ajbh, xgry_xm);

-- 送校表：用于身份证 + 入学时间
CREATE INDEX IF NOT EXISTS idx_sfzxx_sfzhm_rx_time
    ON ywdata.zq_wcnr_sfzxx (sfzhm, rx_time);

-- 其他明细文书（兼容明细接口）
CREATE INDEX IF NOT EXISTS idx_jtjyzdtzs2_ajbh_xgry_spsj
    ON ywdata.zq_zfba_jtjyzdtzs2 (ajbh, xgry_xm, spsj);

CREATE INDEX IF NOT EXISTS idx_tqzmjy_ajbh_xgry
    ON ywdata.zq_zfba_tqzmjy (ajbh, xgry_xm);


-- =========================================================
-- 2) 最小改动重构查询（对应 _fetch_jzqk_compact_rows）
-- 说明：
-- - 保持原业务口径
-- - 将重复 EXISTS 改为预聚合 CTE + LEFT JOIN
-- - 用 max(rx_time) + date 比较替代 TO_CHAR 比较
-- =========================================================

-- 替换为你的实际时间与类型
-- leixing_list 为空数组表示“全量”
WITH input_args AS (
    SELECT
        TIMESTAMP '2026-02-01 00:00:00' AS start_time,
        TIMESTAMP '2026-02-08 00:00:00' AS end_time,
        ARRAY['涉赌（含举报）']::text[] AS leixing_list
),
violation_counts AS (
    SELECT
        w.xyrxx_sfzh AS 身份证号,
        COUNT(*) AS 违法次数,
        COUNT(DISTINCT w.ajxx_join_ajxx_ay_dm) AS 不同案由数
    FROM ywdata.zq_zfba_wcnr_xyr w
    WHERE COALESCE(NULLIF(w.xyrxx_isdel_dm, ''), '0')::integer = 0
      AND COALESCE(NULLIF(w.ajxx_join_ajxx_isdel_dm, ''), '0')::integer = 0
    GROUP BY w.xyrxx_sfzh
),
xjs_case_pairs AS (
    SELECT DISTINCT
        w.xyrxx_sfzh AS 身份证号,
        w.ajxx_join_ajxx_ajbh AS 案件编号
    FROM ywdata.zq_zfba_wcnr_xyr w
    JOIN ywdata.zq_zfba_xjs2 x
      ON w.ajxx_join_ajxx_ajbh = x.ajbh
     AND w.xyrxx_xm = x.xgry_xm
    WHERE COALESCE(NULLIF(w.xyrxx_isdel_dm, ''), '0')::integer = 0
      AND COALESCE(NULLIF(w.ajxx_join_ajxx_isdel_dm, ''), '0')::integer = 0
),
xjs_case_stats AS (
    SELECT 身份证号, COUNT(*) AS xjs_case_cnt
    FROM xjs_case_pairs
    GROUP BY 身份证号
),
base_data AS (
    SELECT DISTINCT
        vw.案件编号,
        vw.人员编号,
        vw.案件类型,
        vw.案由,
        vw.地区,
        vw.立案时间,
        vw.姓名,
        vw.身份证号,
        CASE WHEN vw.年龄::text ~ '^\d+$' THEN CAST(vw.年龄 AS integer) END AS 年龄数值,
        COALESCE(vc.违法次数, 0) AS 违法次数,
        COALESCE(vc.不同案由数, 0) AS 不同案由数,
        COALESCE(xcs.xjs_case_cnt, 0) AS xjs_case_cnt,
        CASE WHEN xcp.身份证号 IS NULL THEN 0 ELSE 1 END AS current_case_has_xjs
    FROM ywdata.v_wcnr_wfry_base vw
    LEFT JOIN violation_counts vc ON vw.身份证号 = vc.身份证号
    LEFT JOIN xjs_case_stats xcs ON vw.身份证号 = xcs.身份证号
    LEFT JOIN xjs_case_pairs xcp
      ON vw.身份证号 = xcp.身份证号
     AND vw.案件编号 = xcp.案件编号
    JOIN input_args ia ON TRUE
    WHERE vw.录入时间 BETWEEN ia.start_time AND ia.end_time
      AND (
            cardinality(ia.leixing_list) = 0
            OR EXISTS (
                SELECT 1
                FROM ywdata.case_type_config ctc
                WHERE ctc.leixing = ANY(ia.leixing_list)
                  AND vw.案由 SIMILAR TO ctc.ay_pattern
            )
          )
),
xz_flags AS (
    SELECT
        x.ajxx_ajbh AS 案件编号,
        x.xzcfjds_rybh AS 人员编号,
        MAX(
            CASE
                WHEN COALESCE(NULLIF(x.xzcfjds_tj_jlts, ''), '0') ~ '^\d+$'
                     AND COALESCE(NULLIF(x.xzcfjds_tj_jlts, ''), '0')::integer > 4
                THEN 1 ELSE 0
            END
        ) AS has_zhiju_gt4,
        MAX(
            CASE
                WHEN COALESCE(NULLIF(x.xzcfjds_tj_jlts, ''), '0') ~ '^\d+$'
                     AND COALESCE(NULLIF(x.xzcfjds_tj_jlts, ''), '0')::integer > 4
                     AND x.xzcfjds_zxqk_text ~ '(不送|不执行)'
                THEN 1 ELSE 0
            END
        ) AS has_zhiju_busong
    FROM ywdata.zq_zfba_xzcfjds x
    GROUP BY x.ajxx_ajbh, x.xzcfjds_rybh
),
jlz_flags AS (
    SELECT DISTINCT
        j.ajxx_ajbh AS 案件编号,
        j.jlz_rybh AS 人员编号,
        1 AS has_xingju
    FROM ywdata.zq_zfba_jlz j
),
zltzs_flags AS (
    SELECT DISTINCT
        z.zltzs_ajbh AS 案件编号,
        z.zltzs_rybh AS 人员编号,
        1 AS has_zltzs
    FROM ywdata.zq_zfba_zlwcnrzstdxwgftzs z
),
xjs_current_flags AS (
    SELECT DISTINCT
        x.ajbh AS 案件编号,
        x.xgry_xm AS 姓名,
        1 AS has_xjs_current
    FROM ywdata.zq_zfba_xjs2 x
),
songxiao_person AS (
    SELECT
        s.sfzhm AS 身份证号,
        MAX(s.rx_time) AS max_rx_time
    FROM ywdata.zq_wcnr_sfzxx s
    WHERE s.rx_time IS NOT NULL
    GROUP BY s.sfzhm
)
SELECT
    bd.地区,
    bd.案件类型,
    bd.年龄数值,
    CASE WHEN bd.案件类型 = '行政' AND COALESCE(xzf.has_zhiju_gt4, 0) = 1 THEN 1 ELSE 0 END AS is_zhiju_gt4,
    CASE WHEN bd.案件类型 = '行政' AND COALESCE(xzf.has_zhiju_busong, 0) = 1 THEN 1 ELSE 0 END AS is_zhiju_busong,
    CASE
        WHEN bd.案件类型 = '行政'
         AND bd.违法次数 = 2
         AND bd.不同案由数 = 1
         AND (bd.xjs_case_cnt - bd.current_case_has_xjs) > 0
        THEN 1 ELSE 0
    END AS is_second_same_ay_with_xjs,
    CASE WHEN bd.案件类型 = '行政' AND bd.违法次数 > 2 THEN 1 ELSE 0 END AS is_third_plus,
    CASE WHEN bd.案件类型 = '刑事' AND COALESCE(jf.has_xingju, 0) = 1 THEN 1 ELSE 0 END AS is_xingju,
    CASE WHEN COALESCE(zf.has_zltzs, 0) = 1 OR COALESCE(xcf.has_xjs_current, 0) = 1 THEN 1 ELSE 0 END AS is_jiaozhi_wenshu,
    CASE
        WHEN sp.max_rx_time IS NOT NULL AND sp.max_rx_time::date >= bd.立案时间::date
        THEN 1 ELSE 0
    END AS is_songxiao
FROM base_data bd
LEFT JOIN xz_flags xzf
  ON xzf.案件编号 = bd.案件编号
 AND xzf.人员编号 = bd.人员编号
LEFT JOIN jlz_flags jf
  ON jf.案件编号 = bd.案件编号
 AND jf.人员编号 = bd.人员编号
LEFT JOIN zltzs_flags zf
  ON zf.案件编号 = bd.案件编号
 AND zf.人员编号 = bd.人员编号
LEFT JOIN xjs_current_flags xcf
  ON xcf.案件编号 = bd.案件编号
 AND xcf.姓名 = bd.姓名
LEFT JOIN songxiao_person sp
  ON sp.身份证号 = bd.身份证号;


-- =========================================================
-- 3) 统计口径验证SQL（wfzf_people / yzbl）
-- 说明：对应代码中的 _compact_rows_to_logic_rows + _is_yzbl_num
-- =========================================================
WITH compact AS (
    -- 占位结构（可执行，返回空结果）
    -- 内网验证时请将 compact CTE 替换为“第2节重构查询”并确保输出字段一致。
    SELECT
        NULL::text AS 地区,
        0::integer AS is_zhiju_gt4,
        0::integer AS is_zhiju_busong,
        0::integer AS is_xingju,
        0::integer AS is_jiaozhi_wenshu,
        0::integer AS is_songxiao
    WHERE FALSE
),
logic AS (
    SELECT
        地区,
        CASE WHEN is_zhiju_gt4 = 1 THEN '是' ELSE '否' END AS 治拘大于4天,
        CASE WHEN is_zhiju_busong = 1 THEN '是' ELSE '否' END AS 是否治拘不送,
        CASE WHEN is_xingju = 1 THEN '是' ELSE '否' END AS 是否刑拘,
        CASE WHEN is_jiaozhi_wenshu = 1 THEN '是' ELSE '否' END AS 是否开具矫治文书,
        CASE WHEN is_songxiao = 1 THEN '是' ELSE '否' END AS 是否送校
    FROM compact
)
SELECT
    地区,
    COUNT(*) AS wfzf_people,
    SUM(
        CASE
            WHEN 是否送校 = '是'
              OR (治拘大于4天 = '是' AND 是否治拘不送 = '否')
              OR 是否刑拘 = '是'
              OR 是否开具矫治文书 = '是'
            THEN 1 ELSE 0
        END
    ) AS yzbl_num
FROM logic
GROUP BY 地区
ORDER BY 地区;


-- =========================================================
-- 4) 执行计划建议
-- =========================================================
-- 在内网执行：
-- EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
-- <第2节完整查询SQL>;
