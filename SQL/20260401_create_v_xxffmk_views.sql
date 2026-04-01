DROP MATERIALIZED VIEW IF EXISTS "ywdata"."mv_xxffmk_student_school_rel";
DROP VIEW IF EXISTS "ywdata"."mv_xxffmk_student_school_rel";
DROP MATERIALIZED VIEW IF EXISTS "ywdata"."mv_xxffmk_dim5_night_day";
DROP VIEW IF EXISTS "ywdata"."mv_xxffmk_dim5_night_day";
DROP MATERIALIZED VIEW IF EXISTS "ywdata"."mv_xxffmk_school_dim";
DROP VIEW IF EXISTS "ywdata"."mv_xxffmk_school_dim";

CREATE MATERIALIZED VIEW "ywdata"."mv_xxffmk_school_dim" AS
WITH school_sources AS (
    SELECT
        s."xxbsm",
        s."xxmc",
        s."zgjyxzbmmc",
        'zzxj' AS source_type,
        1 AS source_priority,
        MAX(COALESCE(s."gkrksj", s."bzkrksj", s."cd_time", s."add_time")) AS latest_time
    FROM "ywdata"."sh_yf_zzxj_xx" s
    WHERE NULLIF(BTRIM(COALESCE(s."xxbsm", '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(s."xxmc", '')), '') IS NOT NULL
    GROUP BY s."xxbsm", s."xxmc", s."zgjyxzbmmc"

    UNION ALL

    SELECT
        s."xxbsm",
        s."xxmc",
        s."zgjyxzbmmc",
        'zxxj' AS source_type,
        2 AS source_priority,
        MAX(COALESCE(s."bzkrksj", s."cd_time", s."add_time")) AS latest_time
    FROM "ywdata"."sh_gd_zxxxsxj_xx" s
    WHERE NULLIF(BTRIM(COALESCE(s."xxbsm", '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(s."xxmc", '')), '') IS NOT NULL
    GROUP BY s."xxbsm", s."xxmc", s."zgjyxzbmmc"
),
ranked AS (
    SELECT
        ss.*,
        ROW_NUMBER() OVER (
            PARTITION BY ss."xxbsm", ss."xxmc", ss."zgjyxzbmmc"
            ORDER BY ss.source_priority, ss.latest_time DESC NULLS LAST
        ) AS rn
    FROM school_sources ss
)
SELECT
    r."xxbsm",
    r."xxmc",
    r."zgjyxzbmmc",
    r.source_type,
    UPPER(REGEXP_REPLACE(COALESCE(r."xxmc", ''), '[[:space:][:punct:]]', '', 'g')) AS normalized_xxmc
FROM ranked r
WHERE r.rn = 1
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_xxffmk_school_dim_code_name
    ON "ywdata"."mv_xxffmk_school_dim" ("xxbsm", "xxmc", "zgjyxzbmmc");

COMMENT ON MATERIALIZED VIEW "ywdata"."mv_xxffmk_school_dim" IS '学校赋分模块学校标准维：中职优先的学校去重结果';

CREATE MATERIALIZED VIEW "ywdata"."mv_xxffmk_student_school_rel" AS
WITH student_sources AS (
    SELECT
        s."sfzjh",
        s."xxbsm",
        s."xxmc",
        s."zgjyxzbmmc",
        'zzxj' AS source_type,
        s."njmc",
        s."bjmc",
        1 AS source_priority,
        COALESCE(s."gkrksj", s."bzkrksj", s."cd_time", s."add_time") AS latest_time,
        s."id"
    FROM "ywdata"."sh_yf_zzxj_xx" s
    WHERE NULLIF(BTRIM(COALESCE(s."sfzjh", '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(s."xxbsm", '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(s."xxmc", '')), '') IS NOT NULL

    UNION ALL

    SELECT
        s."sfzjh",
        s."xxbsm",
        s."xxmc",
        s."zgjyxzbmmc",
        'zxxj' AS source_type,
        s."njmc",
        s."bjmc",
        2 AS source_priority,
        COALESCE(s."bzkrksj", s."cd_time", s."add_time") AS latest_time,
        s."id"
    FROM "ywdata"."sh_gd_zxxxsxj_xx" s
    WHERE NULLIF(BTRIM(COALESCE(s."sfzjh", '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(s."xxbsm", '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(s."xxmc", '')), '') IS NOT NULL
),
ranked AS (
    SELECT
        ss.*,
        ROW_NUMBER() OVER (
            PARTITION BY ss."sfzjh"
            ORDER BY ss.source_priority, ss.latest_time DESC NULLS LAST, ss."id" DESC
        ) AS rn
    FROM student_sources ss
)
SELECT
    r."sfzjh",
    r."xxbsm",
    r."xxmc",
    r."zgjyxzbmmc",
    r.source_type,
    r."njmc",
    r."bjmc"
FROM ranked r
WHERE r.rn = 1
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_xxffmk_student_school_rel_sfzjh
    ON "ywdata"."mv_xxffmk_student_school_rel" ("sfzjh");

CREATE INDEX IF NOT EXISTS idx_mv_xxffmk_student_school_rel_xxbsm
    ON "ywdata"."mv_xxffmk_student_school_rel" ("xxbsm");

COMMENT ON MATERIALIZED VIEW "ywdata"."mv_xxffmk_student_school_rel" IS '学校赋分模块身份证到学校映射：同身份证按中职优先';

CREATE MATERIALIZED VIEW "ywdata"."mv_xxffmk_dim5_night_day" AS
SELECT
    t."id_number" AS sfzjh,
    MAX(t."name") AS xm,
    TO_DATE(SUBSTRING(t."shot_time", 1, 8), 'YYYYMMDD') AS shot_date
FROM "ywdata"."t_spy_ryrlgj_xx" t
WHERE COALESCE(t."id_number", '') ~ '^[0-9]{17}[0-9Xx]$'
  AND SUBSTRING(t."shot_time", 9, 6) >= '000000'
  AND SUBSTRING(t."shot_time", 9, 6) <= '050000'
GROUP BY t."id_number", TO_DATE(SUBSTRING(t."shot_time", 1, 8), 'YYYYMMDD')
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_xxffmk_dim5_night_day_shot_date_sfzjh
    ON "ywdata"."mv_xxffmk_dim5_night_day" ("shot_date", "sfzjh");

COMMENT ON MATERIALIZED VIEW "ywdata"."mv_xxffmk_dim5_night_day" IS '夜不归宿学生日粒度预聚合：按身份证和日期去重的夜间轨迹';

CREATE INDEX IF NOT EXISTS idx_t_spy_ryrlgj_xx_night_shot_ts
    ON "ywdata"."t_spy_ryrlgj_xx" USING btree (ywdata.str_to_ts((shot_time)::text))
    WHERE SUBSTRING((shot_time)::text, 9, 6) >= '000000'
      AND SUBSTRING((shot_time)::text, 9, 6) <= '050000';
