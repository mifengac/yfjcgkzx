-- 维度3查询：案件团伙数（3人及以上）
-- 说明：
-- 1. 只改 params 里的时间即可。
-- 2. 这里直接按身份证映射到学校，再统计 3 人及以上的案件团伙人数。
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
