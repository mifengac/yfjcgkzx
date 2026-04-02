-- 维度1查询：累计送生人数
-- 说明：
-- 1. 这里保留 raw_school_name，便于你在数据库里按不同时间窗口直接查看原始数据。
-- 2. 同时尝试按学校标准名做一次精确归并，方便快速看排名。
-- 3. 需要更改时间时，只改 params 里的 begin_time / end_time。
WITH params AS (
    SELECT
        TIMESTAMP '2026-01-01 00:00:00' AS begin_time,
        TIMESTAMP '2026-03-31 23:59:59' AS end_time
),
raw_counts AS (
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
)
SELECT
    COALESCE(s."xxbsm", raw.raw_school_name) AS school_code,
    COALESCE(s."xxmc", raw.raw_school_name) AS school_name,
    COALESCE(s."zgjyxzbmmc", '') AS supervisor,
    SUM(raw.raw_count) AS send_count,
    STRING_AGG(DISTINCT raw.raw_school_name, '；' ORDER BY raw.raw_school_name) AS raw_school_names
FROM raw_counts raw
LEFT JOIN "ywdata"."mv_xxffmk_school_dim" s
  ON s.normalized_xxmc = UPPER(REGEXP_REPLACE(raw.raw_school_name, '[[:space:][:punct:]]', '', 'g'))
GROUP BY
    COALESCE(s."xxbsm", raw.raw_school_name),
    COALESCE(s."xxmc", raw.raw_school_name),
    COALESCE(s."zgjyxzbmmc", '')
ORDER BY send_count DESC, school_code;
