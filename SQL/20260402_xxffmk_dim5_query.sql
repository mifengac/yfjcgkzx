-- 维度5查询：夜不归宿学生人数
-- 说明：
-- 1. 只改 params 里的时间即可。
-- 2. 先读夜间日粒度预聚合 MV，再关联学校映射表。
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
    r."zgjyxzbmmc",
    COUNT(*) AS no_return_count
FROM night_days q
JOIN "ywdata"."mv_xxffmk_student_school_rel" r
  ON r."sfzjh" = q.sfzjh
GROUP BY r."xxbsm", r."xxmc", r."zgjyxzbmmc"
ORDER BY no_return_count DESC, r."xxbsm";
