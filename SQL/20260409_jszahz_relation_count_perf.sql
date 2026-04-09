-- 精神患者主题库右滑页 6 类关联数量统计执行计划测试
-- 使用说明：
-- 1. 将 params.sample_ids 中的身份证号数组替换为能复现慢查询的真实样本
-- 2. 依次执行 6 段 EXPLAIN ANALYZE
-- 3. 对比每段 Execution Time、Buffers，以及是否出现 Seq Scan
-- 4. 当前库不支持 pg_trgm，默认最可能慢的是“关联警情统计执行计划”

-- 关联案件统计执行计划
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT ARRAY['身份证号1', '身份证号2', '身份证号3']::text[] AS sample_ids
),
ids AS (
    SELECT DISTINCT UNNEST(p.sample_ids) AS zjhm
    FROM params p
)
SELECT
    ids.zjhm AS "身份证号",
    COUNT(*) AS "数量"
FROM ids
JOIN "ywdata"."zq_zfba_xyrxx" zzx
  ON zzx."xyrxx_sfzh" = ids.zjhm
WHERE zzx."ajxx_join_ajxx_ajbh" IS NOT NULL
GROUP BY ids.zjhm;

-- 关联警情统计执行计划
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT ARRAY['身份证号1', '身份证号2', '身份证号3']::text[] AS sample_ids
),
ids AS (
    SELECT DISTINCT UNNEST(p.sample_ids) AS zjhm
    FROM params p
),
patterns AS (
    SELECT ARRAY_AGG('%' || zjhm || '%') AS like_patterns
    FROM ids
),
candidate AS (
    SELECT jq."replies"
    FROM "ywdata"."zq_kshddpt_dsjfx_jq" jq
    CROSS JOIN patterns p
    WHERE jq."replies" IS NOT NULL
      AND jq."replies" LIKE ANY (p.like_patterns)
)
SELECT
    ids.zjhm AS "身份证号",
    COUNT(*) AS "数量"
FROM ids
JOIN candidate c
  ON c."replies" LIKE ('%' || ids.zjhm || '%')
GROUP BY ids.zjhm;

-- 关联机动车统计执行计划
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT ARRAY['身份证号1', '身份证号2', '身份证号3']::text[] AS sample_ids
),
ids AS (
    SELECT DISTINCT UNNEST(p.sample_ids) AS zjhm
    FROM params p
)
SELECT
    ids.zjhm AS "身份证号",
    COUNT(*) AS "数量"
FROM ids
JOIN "ywdata"."t_qsjdc_jbxx" jdc
  ON jdc."sfzmhm" = ids.zjhm
GROUP BY ids.zjhm;

-- 关联视频云统计执行计划
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT ARRAY['身份证号1', '身份证号2', '身份证号3']::text[] AS sample_ids
)
SELECT
    spy."id_number" AS "身份证号",
    COUNT(*) AS "数量"
FROM "ywdata"."t_spy_ryrlgj_xx" spy
JOIN params p
  ON 1 = 1
WHERE spy."libname" = '精神病人'
  AND spy."id_number" = ANY (p.sample_ids)
GROUP BY spy."id_number";

-- 关联门诊统计执行计划
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT ARRAY['身份证号1', '身份证号2', '身份证号3']::text[] AS sample_ids
),
ids AS (
    SELECT DISTINCT UNNEST(p.sample_ids) AS zjhm
    FROM params p
)
SELECT
    ids.zjhm AS "身份证号",
    COUNT(*) AS "数量"
FROM ids
JOIN "ywdata"."sh_yf_mz_djxx" mz
  ON mz."zjhm" = ids.zjhm
GROUP BY ids.zjhm;

-- 关联飙车炸街统计执行计划
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT ARRAY['身份证号1', '身份证号2', '身份证号3']::text[] AS sample_ids
),
ids AS (
    SELECT DISTINCT UNNEST(p.sample_ids) AS zjhm
    FROM params p
)
SELECT
    ids.zjhm AS "身份证号",
    COUNT(*) AS "数量"
FROM ids
JOIN "ywdata"."b_evt_jjzdbczjajxx" jz
  ON jz."dsrsfzmhm" = ids.zjhm
GROUP BY ids.zjhm;
