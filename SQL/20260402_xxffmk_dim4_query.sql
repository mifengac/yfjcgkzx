-- 维度4查询：辍学人数
-- 说明：
-- 1. 该维度不做时间过滤，始终按全量查询。
-- 2. 只按身份证号映射学校并统计人数。
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
