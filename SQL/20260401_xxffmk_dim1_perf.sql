-- 维度1性能测试：累计送生人数
-- 按需修改 params 中的时间。

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
