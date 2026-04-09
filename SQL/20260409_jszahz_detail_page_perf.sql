-- 精神患者主题库 detail_page 基础明细查询执行计划测试
-- 用途：
-- 1. 定位点击“去重患者数”后，右滑页首次加载是否卡在基础明细 SQL
-- 2. 覆盖 detail_page 的核心链路：先取 active batch，再查 snapshot 明细
-- 3. 对比“具体分局 / 汇总 / 加风险标签 / 加人员类型”几种场景的执行计划
--
-- 使用说明：
-- 1. 先把 params 里的时间、分局编码、标签值替换成能稳定复现慢查询的真实样本
-- 2. 依次执行每一段 EXPLAIN ANALYZE
-- 3. 重点关注：
--    - Execution Time
--    - Buffers
--    - 是否出现 Seq Scan / Bitmap Heap Scan / 大范围 Sort
--    - person_type EXISTS 子查询是否拖慢整体
--
-- 关键背景：
-- - 当前右滑页已改成“基础明细先返回，6 类关联统计异步补齐”
-- - 如果页面仍长时间停在“正在查询详细数据及关联统计...”，优先怀疑下面的基础明细 SQL

-- 1. 生效批次查询执行计划（对应 get_active_batch）
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
SELECT
    id,
    source_file_name,
    sheet_name,
    import_status,
    is_active,
    imported_row_count,
    matched_person_count,
    generated_tag_count,
    created_by,
    error_message,
    created_at,
    activated_at
FROM "jcgkzx_monitor"."jszahz_topic_batch"
WHERE is_active = TRUE
  AND import_status = 'success'
ORDER BY activated_at DESC NULLS LAST, created_at DESC, id DESC
LIMIT 1;

-- 2. detail_page：具体分局，无人员类型/风险标签（最接近常见点击场景）
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        1::integer AS batch_id,
        '2025-01-01 00:00:00'::timestamp AS start_time,
        '2026-04-09 00:00:00'::timestamp AS end_time,
        '445302000000'::text AS branch_code
)
SELECT
    COALESCE(s.xm, '') AS "姓名",
    COALESCE(s.zjhm, '') AS "身份证号",
    s.lgsj AS "列管时间",
    COALESCE(s.lgdw, '') AS "列管单位",
    COALESCE(s.ssfj, '未匹配分局') AS "分局",
    COALESCE(s.fxdj_label, '无数据') AS "人员风险",
    COALESCE(s.person_types_text, '') AS "人员类型"
FROM "jcgkzx_monitor"."jszahz_topic_snapshot" s
JOIN params p
  ON 1 = 1
WHERE s.batch_id = p.batch_id
  AND s.lgsj >= p.start_time
  AND s.lgsj <= p.end_time
  AND COALESCE(s.ssfjdm, '__UNMATCHED__') = p.branch_code
ORDER BY COALESCE(s.ssfj, '未匹配分局'), s.lgsj DESC NULLS LAST, s.xm, s.zjhm;

-- 3. detail_page：汇总明细（branch_code = __ALL__），无人员类型/风险标签
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        1::integer AS batch_id,
        '2025-01-01 00:00:00'::timestamp AS start_time,
        '2026-04-09 00:00:00'::timestamp AS end_time
)
SELECT
    COALESCE(s.xm, '') AS "姓名",
    COALESCE(s.zjhm, '') AS "身份证号",
    s.lgsj AS "列管时间",
    COALESCE(s.lgdw, '') AS "列管单位",
    COALESCE(s.ssfj, '未匹配分局') AS "分局",
    COALESCE(s.fxdj_label, '无数据') AS "人员风险",
    COALESCE(s.person_types_text, '') AS "人员类型"
FROM "jcgkzx_monitor"."jszahz_topic_snapshot" s
JOIN params p
  ON 1 = 1
WHERE s.batch_id = p.batch_id
  AND s.lgsj >= p.start_time
  AND s.lgsj <= p.end_time
ORDER BY COALESCE(s.ssfj, '未匹配分局'), s.lgsj DESC NULLS LAST, s.xm, s.zjhm;

-- 4. detail_page：具体分局 + 风险标签
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        1::integer AS batch_id,
        '2025-01-01 00:00:00'::timestamp AS start_time,
        '2026-04-09 00:00:00'::timestamp AS end_time,
        '445302000000'::text AS branch_code,
        ARRAY['1级患者', '2级患者']::text[] AS risk_labels
)
SELECT
    COALESCE(s.xm, '') AS "姓名",
    COALESCE(s.zjhm, '') AS "身份证号",
    s.lgsj AS "列管时间",
    COALESCE(s.lgdw, '') AS "列管单位",
    COALESCE(s.ssfj, '未匹配分局') AS "分局",
    COALESCE(s.fxdj_label, '无数据') AS "人员风险",
    COALESCE(s.person_types_text, '') AS "人员类型"
FROM "jcgkzx_monitor"."jszahz_topic_snapshot" s
JOIN params p
  ON 1 = 1
WHERE s.batch_id = p.batch_id
  AND s.lgsj >= p.start_time
  AND s.lgsj <= p.end_time
  AND COALESCE(s.ssfjdm, '__UNMATCHED__') = p.branch_code
  AND s.fxdj_label = ANY (p.risk_labels)
ORDER BY COALESCE(s.ssfj, '未匹配分局'), s.lgsj DESC NULLS LAST, s.xm, s.zjhm;

-- 5. detail_page：具体分局 + 人员类型（对应 EXISTS 子查询）
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        1::integer AS batch_id,
        '2025-01-01 00:00:00'::timestamp AS start_time,
        '2026-04-09 00:00:00'::timestamp AS end_time,
        '445302000000'::text AS branch_code,
        ARRAY['弱监护', '不规律服药']::text[] AS person_types
)
SELECT
    COALESCE(s.xm, '') AS "姓名",
    COALESCE(s.zjhm, '') AS "身份证号",
    s.lgsj AS "列管时间",
    COALESCE(s.lgdw, '') AS "列管单位",
    COALESCE(s.ssfj, '未匹配分局') AS "分局",
    COALESCE(s.fxdj_label, '无数据') AS "人员风险",
    COALESCE(s.person_types_text, '') AS "人员类型"
FROM "jcgkzx_monitor"."jszahz_topic_snapshot" s
JOIN params p
  ON 1 = 1
WHERE s.batch_id = p.batch_id
  AND s.lgsj >= p.start_time
  AND s.lgsj <= p.end_time
  AND COALESCE(s.ssfjdm, '__UNMATCHED__') = p.branch_code
  AND EXISTS (
      SELECT 1
      FROM "jcgkzx_monitor"."jszahz_topic_person_type" pt
      WHERE pt.batch_id = s.batch_id
        AND pt.zjhm = s.zjhm
        AND pt.person_type = ANY (p.person_types)
  )
ORDER BY COALESCE(s.ssfj, '未匹配分局'), s.lgsj DESC NULLS LAST, s.xm, s.zjhm;

-- 6. detail_page：具体分局 + 风险标签 + 人员类型（最重场景）
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        1::integer AS batch_id,
        '2025-01-01 00:00:00'::timestamp AS start_time,
        '2026-04-09 00:00:00'::timestamp AS end_time,
        '445302000000'::text AS branch_code,
        ARRAY['1级患者', '2级患者']::text[] AS risk_labels,
        ARRAY['弱监护', '不规律服药']::text[] AS person_types
)
SELECT
    COALESCE(s.xm, '') AS "姓名",
    COALESCE(s.zjhm, '') AS "身份证号",
    s.lgsj AS "列管时间",
    COALESCE(s.lgdw, '') AS "列管单位",
    COALESCE(s.ssfj, '未匹配分局') AS "分局",
    COALESCE(s.fxdj_label, '无数据') AS "人员风险",
    COALESCE(s.person_types_text, '') AS "人员类型"
FROM "jcgkzx_monitor"."jszahz_topic_snapshot" s
JOIN params p
  ON 1 = 1
WHERE s.batch_id = p.batch_id
  AND s.lgsj >= p.start_time
  AND s.lgsj <= p.end_time
  AND COALESCE(s.ssfjdm, '__UNMATCHED__') = p.branch_code
  AND s.fxdj_label = ANY (p.risk_labels)
  AND EXISTS (
      SELECT 1
      FROM "jcgkzx_monitor"."jszahz_topic_person_type" pt
      WHERE pt.batch_id = s.batch_id
        AND pt.zjhm = s.zjhm
        AND pt.person_type = ANY (p.person_types)
  )
ORDER BY COALESCE(s.ssfj, '未匹配分局'), s.lgsj DESC NULLS LAST, s.xm, s.zjhm;
