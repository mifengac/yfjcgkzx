-- 维度4性能测试：辍学人数
-- 该维度不做时间过滤，重点看 b_per_qscxwcnr 与学籍映射的连接性能。

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
