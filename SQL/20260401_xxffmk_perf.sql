-- =====================================================================
-- 学校赋分模块 5 维度性能测试合集
-- 合并自：dim1_perf ~ dim5_perf
-- 按需修改 params 中的时间。
-- =====================================================================

-- ■ 维度1：累计送生人数 ---------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        TIMESTAMP '2026-01-01 00:00:00' AS begin_time,
        TIMESTAMP '2026-03-31 23:59:59' AS end_time
)
SELECT
    BTRIM(COALESCE(z."yxx", '')) AS raw_school_name,
    COUNT(*) AS raw_count
FROM "ywdata"."zq_zfba_wcnr_sfzxx" z
JOIN params p
  ON 1 = 1
WHERE z."rx_time" >= p.begin_time
  AND z."rx_time" <= p.end_time
  AND NULLIF(BTRIM(COALESCE(z."yxx", '')), '') IS NOT NULL
GROUP BY BTRIM(COALESCE(z."yxx", ''))
ORDER BY raw_count DESC, raw_school_name;

-- ■ 维度2：涉校警情 -------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        '2026-01-01 00:00:00'::text AS begin_time,
        '2026-03-31 23:59:59'::text AS end_time,
        '(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校)'::text AS school_keyword_pattern,
        '([^,，。；;：:、 ]{2,40}(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校))'::text AS school_extract_pattern
),
source_rows AS (
    SELECT
        j."caseno",
        j."calltime",
        j."occuraddress",
        j."casecontents",
        j."replies",
        COALESCE(
            (REGEXP_MATCH(COALESCE(j."occuraddress", ''), p.school_extract_pattern))[1],
            (REGEXP_MATCH(COALESCE(j."casecontents", ''), p.school_extract_pattern))[1],
            (REGEXP_MATCH(COALESCE(j."replies", ''), p.school_extract_pattern))[1]
        ) AS extracted_school_name
    FROM "ywdata"."zq_kshddpt_dsjfx_jq" j
    JOIN params p
      ON 1 = 1
    WHERE j."calltime" >= p.begin_time
      AND j."calltime" <= p.end_time
      AND j."newcharasubclass" IN ('01','02','04','05','06','08','09')
      AND (
            COALESCE(j."occuraddress", '') ~ p.school_keyword_pattern
         OR COALESCE(j."casecontents", '') ~ p.school_keyword_pattern
         OR COALESCE(j."replies", '') ~ p.school_keyword_pattern
      )
)
SELECT
    s."caseno",
    s."calltime",
    s.extracted_school_name
FROM source_rows s
WHERE NULLIF(BTRIM(COALESCE(s.extracted_school_name, '')), '') IS NOT NULL
ORDER BY s."calltime" DESC, s."caseno";

-- ■ 维度3：案件团伙数（3 人及以上）-----------------------------------------

EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        TIMESTAMP '2026-01-01 00:00:00' AS begin_time,
        TIMESTAMP '2026-03-31 23:59:59' AS end_time
),
minor_people AS (
    SELECT
        x."ajxx_join_ajxx_ajbh" AS ajbh,
        x."xyrxx_sfzh" AS sfzjh,
        x."xyrxx_xm" AS xm,
        x."xyrxx_lrsj"
    FROM "ywdata"."zq_zfba_xyrxx" x
    JOIN params p
      ON 1 = 1
    WHERE x."xyrxx_lrsj" >= p.begin_time
      AND x."xyrxx_lrsj" <= p.end_time
      AND COALESCE(x."ajxx_join_ajxx_isdel_dm", '0') = '0'
      AND COALESCE(x."xyrxx_sfzh", '') ~ '^[0-9]{17}[0-9Xx]$'
      AND SUBSTRING(x."xyrxx_sfzh", 7, 8) ~ '^[0-9]{8}$'
      AND AGE(COALESCE(x."ajxx_join_ajxx_lasj", x."xyrxx_lrsj")::date, TO_DATE(SUBSTRING(x."xyrxx_sfzh", 7, 8), 'YYYYMMDD')) < INTERVAL '18 years'
),
gang_cases AS (
    SELECT
        m.ajbh
    FROM minor_people m
    GROUP BY m.ajbh
    HAVING COUNT(*) >= 3
)
SELECT
    r."xxbsm",
    r."xxmc",
    COUNT(*) AS gang_person_count
FROM minor_people m
JOIN gang_cases g
  ON g.ajbh = m.ajbh
JOIN "ywdata"."mv_xxffmk_student_school_rel" r
  ON r."sfzjh" = m.sfzjh
GROUP BY r."xxbsm", r."xxmc"
ORDER BY gang_person_count DESC, r."xxbsm";

-- ■ 维度4：辍学人数 --------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
SELECT
    r."xxbsm",
    r."xxmc",
    COUNT(*) AS dropout_count
FROM "ywdata"."b_per_qscxwcnr" q
JOIN "ywdata"."mv_xxffmk_student_school_rel" r
  ON r."sfzjh" = q."zjhm"
WHERE NULLIF(BTRIM(COALESCE(q."zjhm", '')), '') IS NOT NULL
GROUP BY r."xxbsm", r."xxmc"
ORDER BY dropout_count DESC, r."xxbsm";

-- ■ 维度5：夜不归宿学生人数 ------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        TIMESTAMP '2026-01-01 00:00:00' AS begin_time,
        TIMESTAMP '2026-03-31 23:59:59' AS end_time
),
night_days AS (
    SELECT
        n.sfzjh,
        MAX(n.xm) AS xm,
        COUNT(*) AS night_days
    FROM "ywdata"."mv_xxffmk_dim5_night_day" n
    JOIN params p
      ON n.shot_date >= p.begin_time::date
     AND n.shot_date <= p.end_time::date
    GROUP BY n.sfzjh
    HAVING COUNT(*) >= 10
)
SELECT
    r."xxbsm",
    r."xxmc",
    COUNT(*) AS no_return_count
FROM night_days q
JOIN "ywdata"."mv_xxffmk_student_school_rel" r
  ON r."sfzjh" = q.sfzjh
GROUP BY r."xxbsm", r."xxmc"
ORDER BY no_return_count DESC, r."xxbsm";
