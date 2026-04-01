-- 维度3性能测试：案件团伙数（3人及以上）
-- 重点观察 zq_zfba_xyrxx 时间过滤、未成年人判断、团伙聚合和学籍映射开销。
-- 按需修改 params 中的时间。

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
