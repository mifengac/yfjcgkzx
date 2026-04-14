-- =====================================================================
-- 精神患者主题库关联数据源索引 + 关联数量执行计划测试
-- 合并自：add_jszahz_relation_indexes, jszahz_relation_count_perf
-- =====================================================================

-- ■ 关联查询索引 -----------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_zq_zfba_xyrxx_sfzh_ajbh_lasj
ON "ywdata"."zq_zfba_xyrxx" ("xyrxx_sfzh", "ajxx_join_ajxx_ajbh", "ajxx_join_ajxx_lasj");

-- 当前本地 KingbaseES V008R006C009B0014 未提供 pg_trgm / gin_trgm_ops。
-- 关联警情已改为"源表抽取 -> 中间表 -> 页面查询"，此脚本不再为 replies 文本匹配创建索引。

CREATE INDEX IF NOT EXISTS idx_t_qsjdc_jbxx_sfzmhm_ccdjrq
ON "ywdata"."t_qsjdc_jbxx" ("sfzmhm", "ccdjrq");

CREATE INDEX IF NOT EXISTS idx_b_evt_jjzdbczjajxx_dsrsfzmhm_wfsj
ON "ywdata"."b_evt_jjzdbczjajxx" ("dsrsfzmhm", "wfsj");

CREATE INDEX IF NOT EXISTS idx_t_spy_ryrlgj_xx_id_number_libname_shot_time_ts
ON "ywdata"."t_spy_ryrlgj_xx" (
    "id_number",
    "libname",
    (CASE
        WHEN "shot_time" ~ '^\d{14}$' THEN ywdata.str_to_ts("shot_time")
        ELSE NULL
    END)
);

CREATE INDEX IF NOT EXISTS idx_t_yf_spy_qs_device_sbbm
ON "ywdata"."t_yf_spy_qs_device" ("sbbm");

CREATE INDEX IF NOT EXISTS idx_sh_yf_mz_djxx_zjhm_mzxx_jzsj
ON "ywdata"."sh_yf_mz_djxx" ("zjhm", "mzxx_jzsj");

-- 关联交通违法
-- 批量数量统计：JOIN vio_violation ON vv.jszh = ids.zjhm — 等值查找，需要 jszh 索引
-- 单人明细查询：WHERE jszh = %s ORDER BY wfsj DESC — 复合索引同时消除排序步骤
-- vio_violation 记录量通常较大（全市机动车违法），(jszh, wfsj DESC) 是最优前缀顺序：
--   • 等值谓词先收窄结果集，DESC 排序与 index scan direction 对齐，避免 filesort
CREATE INDEX IF NOT EXISTS idx_vio_violation_jszh_wfsj
ON "ywdata"."vio_violation" ("jszh", "wfsj" DESC);

-- ■ 关联数量统计执行计划 ---------------------------------------------------
-- 使用说明：
-- 1. 将 params.sample_ids 中的身份证号数组替换为能复现慢查询的真实样本
-- 2. 依次执行 6 段 EXPLAIN ANALYZE
-- 3. 对比每段 Execution Time、Buffers，以及是否出现 Seq Scan
-- 4. 关联警情已切到中间表，通常不再是最慢项；请以实际 Execution Time 为准

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
)
SELECT
    jqm."sfzh" AS "身份证号",
    COUNT(*) AS "数量"
FROM "jcgkzx_monitor"."jszahz_jq_sfzh_map" jqm
JOIN params p
  ON 1 = 1
WHERE jqm."sfzh" = ANY (p.sample_ids)
GROUP BY jqm."sfzh";

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

-- 关联交通违法 - 批量数量统计执行计划
-- 预期结果：idx_vio_violation_jszh_wfsj 上的 Index Scan，无 Seq Scan
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
JOIN "ywdata"."vio_violation" vv
  ON vv."jszh" = ids.zjhm
GROUP BY ids.zjhm;

-- 关联交通违法 - 单人明细查询执行计划
-- 预期结果：idx_vio_violation_jszh_wfsj 上的 Index Scan（前向扫描即为 DESC 顺序，无额外排序节点）
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
SELECT
    vv."wfbh"  AS "违法编号",
    vv."wfsj"  AS "违法时间",
    vv."wfdd"  AS "违法地点",
    vv."hphm"  AS "号牌号码",
    vv."hpzl"  AS "号牌种类",
    vv."wfxw"  AS "违法行为",
    vv."fkje"  AS "罚款金额",
    vv."wfjfs" AS "违法记分数"
FROM "ywdata"."vio_violation" vv
WHERE vv."jszh" = '身份证号1'
ORDER BY vv."wfsj" DESC;
