-- 维度5性能测试：夜不归宿学生人数
-- 直接读取夜间日粒度预聚合 MV，重点观察预聚合过滤和最终学校 join 的成本。
-- 按需修改 params 中的时间。
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
