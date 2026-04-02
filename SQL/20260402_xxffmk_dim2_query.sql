-- 维度2查询：涉校警情
-- 说明：
-- 1. 每次只需要修改 params 里的时间即可。
-- 2. 这里使用 newcharasubclass 过滤，和当前业务口径保持一致。
-- 3. 先抽取文本里出现的学校名，再按学校标准视图做精确归并。
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
),
matched_rows AS (
    SELECT
        s."caseno",
        s."calltime",
        s."occuraddress",
        s."casecontents",
        s."replies",
        s.extracted_school_name,
        COALESCE(sd."xxbsm", s.extracted_school_name) AS school_code,
        COALESCE(sd."xxmc", s.extracted_school_name) AS school_name,
        COALESCE(sd."zgjyxzbmmc", '') AS supervisor
    FROM source_rows s
    LEFT JOIN "ywdata"."mv_xxffmk_school_dim" sd
      ON sd.normalized_xxmc = UPPER(REGEXP_REPLACE(COALESCE(s.extracted_school_name, ''), '[[:space:][:punct:]]', '', 'g'))
    WHERE NULLIF(BTRIM(COALESCE(s.extracted_school_name, '')), '') IS NOT NULL
)
SELECT
    school_code,
    school_name,
    supervisor,
    COUNT(*) AS police_count,
    STRING_AGG(DISTINCT extracted_school_name, '；' ORDER BY extracted_school_name) AS raw_school_names
FROM matched_rows
GROUP BY school_code, school_name, supervisor
ORDER BY police_count DESC, school_code;
