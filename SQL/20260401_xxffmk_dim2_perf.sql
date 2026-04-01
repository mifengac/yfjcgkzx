-- 维度2性能测试：涉校警情
-- 重点观察 zq_kshddpt_dsjfx_jq 的时间过滤和学校名抽取性能。
-- 按需修改 params 中的时间。
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT
        '2026-01-01 00:00:00'::text AS begin_time,
        '2026-03-31 23:59:59'::text AS end_time
),
source_rows AS (
    SELECT
        j."caseno",
        j."calltime",
        j."occuraddress",
        j."casecontents",
        j."replies",
        COALESCE(
            (REGEXP_MATCH(COALESCE(j."occuraddress", ''), '([A-Za-z0-9一-龥·（）()\-]{2,40}(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校))'))[1],
            (REGEXP_MATCH(COALESCE(j."casecontents", ''), '([A-Za-z0-9一-龥·（）()\-]{2,40}(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校))'))[1],
            (REGEXP_MATCH(COALESCE(j."replies", ''), '([A-Za-z0-9一-龥·（）()\-]{2,40}(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校))'))[1]
        ) AS extracted_school_name
    FROM "ywdata"."zq_kshddpt_dsjfx_jq" j
    JOIN params p
      ON 1 = 1
    WHERE j."calltime" >= p.begin_time
      AND j."calltime" <= p.end_time
      AND j."newcharasubclass" IN ('01','02','04','05','06','08','09')
      AND (
            COALESCE(j."occuraddress", '') ~ '(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校)'
         OR COALESCE(j."casecontents", '') ~ '(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校)'
         OR COALESCE(j."replies", '') ~ '(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校)'
      )
)
SELECT
    s."caseno",
    s."calltime",
    s.extracted_school_name
FROM source_rows s
WHERE NULLIF(BTRIM(COALESCE(s.extracted_school_name, '')), '') IS NOT NULL
ORDER BY s."calltime" DESC, s."caseno";
