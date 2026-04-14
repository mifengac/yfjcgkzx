-- =====================================================================
-- jcgkzx_monitor 公共基础表：自定义警情监测方案 + 表行数统计
-- 合并自：create_base_watch_custom_case_monitor, create_jcgkzx_monitor_b_count
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS "jcgkzx_monitor";

-- ■ 自定义警情监测方案表 ---------------------------------------------------

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."custom_case_monitor_scheme" (
    id SERIAL PRIMARY KEY,
    scheme_name VARCHAR(100) NOT NULL,
    scheme_code VARCHAR(120) NOT NULL,
    description VARCHAR(500) NOT NULL DEFAULT '',
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_custom_case_monitor_scheme_name UNIQUE (scheme_name),
    CONSTRAINT uq_custom_case_monitor_scheme_code UNIQUE (scheme_code)
);

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."custom_case_monitor_rule" (
    id SERIAL PRIMARY KEY,
    scheme_id INTEGER NOT NULL,
    field_name VARCHAR(64) NOT NULL,
    operator VARCHAR(64) NOT NULL,
    rule_values JSONB NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 1,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_custom_case_monitor_rule_scheme
        FOREIGN KEY (scheme_id)
        REFERENCES "jcgkzx_monitor"."custom_case_monitor_scheme" (id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_custom_case_monitor_scheme_enabled
    ON "jcgkzx_monitor"."custom_case_monitor_scheme" (is_enabled);

CREATE INDEX IF NOT EXISTS idx_custom_case_monitor_rule_scheme_id
    ON "jcgkzx_monitor"."custom_case_monitor_rule" (scheme_id, sort_order);

-- ■ 初始化方案数据 ---------------------------------------------------------

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_scheme" (
    scheme_name,
    scheme_code,
    description,
    is_enabled,
    created_at,
    updated_at
)
VALUES
    ('流浪/乞讨警情', 'wander_begging', '系统初始化方案：匹配流浪或乞讨相关警情', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('出租屋警情', 'rental_house', '系统初始化方案：匹配出租屋或租赁相关警情', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (scheme_code) DO UPDATE
SET scheme_name = EXCLUDED.scheme_name,
    description = EXCLUDED.description,
    is_enabled = EXCLUDED.is_enabled,
    updated_at = CURRENT_TIMESTAMP;

DELETE FROM "jcgkzx_monitor"."custom_case_monitor_rule"
 WHERE scheme_id IN (
    SELECT id
      FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
     WHERE scheme_code IN ('wander_begging', 'rental_house')
 );

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'contains_any', '["流浪", "乞讨"]'::jsonb, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'wander_begging';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'contains_any', '["出租屋", "租赁"]'::jsonb, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'rental_house';

-- ■ 清明专题监测方案 -------------------------------------------------------

BEGIN;

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_scheme" (
    scheme_name,
    scheme_code,
    description,
    is_enabled,
    created_at,
    updated_at
)
VALUES (
    '清明专题监测',
    'qingming_monitor',
    '清明期间涉林地、坟地纠纷监测',
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (scheme_code) DO UPDATE
SET scheme_name = EXCLUDED.scheme_name,
    description = EXCLUDED.description,
    is_enabled = EXCLUDED.is_enabled,
    updated_at = CURRENT_TIMESTAMP;

DELETE FROM "jcgkzx_monitor"."custom_case_monitor_rule"
 WHERE scheme_id IN (
    SELECT id
      FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
     WHERE scheme_code = 'qingming_monitor'
 );

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT
    id,
    'combined_text',
    'contains_any',
    '[
      "坟地","墓地","墓穴","祖坟","祖坟地","坟山","坟头","坟边","坟场","公墓",
      "骨灰","骨灰盒","墓碑","修坟","迁坟","扒坟","挖坟","占坟","争坟",
      "扫墓","祭祖","上坟","烧纸","纸钱","祭扫",
      "林地","山地","山场","山林","林场","林权","山界","地界","界址","边界",
      "承包地","责任山","荒山","林木","树木","砍树","伐树","毁林","占山","占地","圈地","开荒","林权证",
      "祭扫冲突","扫墓纠纷","上坟纠纷","修坟纠纷","迁坟纠纷","坟地纠纷","林地纠纷","山场纠纷","地界纠纷","权属纠纷",
      "阻拦扫墓","不让上坟","拦路","争执","冲突","打架","斗殴","推搡","辱骂"
    ]'::jsonb,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'qingming_monitor';

COMMIT;

-- ■ 表行数统计表 + 统计函数 ------------------------------------------------

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."b_count" (
    "bm" VARCHAR NULL,
    "bzw" VARCHAR NULL,
    "zs" VARCHAR NULL,
    "gxsj" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE "jcgkzx_monitor"."custom_case_monitor_scheme" IS U&'\81EA\5B9A\4E49\8B66\60C5\76D1\6D4B\65B9\6848\8868';
COMMENT ON TABLE "jcgkzx_monitor"."custom_case_monitor_rule" IS U&'\81EA\5B9A\4E49\8B66\60C5\76D1\6D4B\89C4\5219\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_batch" IS U&'\7CBE\795E\969C\788D\4E13\9898\6279\6B21\5BFC\5165\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_person_type" IS U&'\7CBE\795E\969C\788D\4E13\9898\4EBA\5458\6807\7B7E\660E\7EC6\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_snapshot" IS U&'\7CBE\795E\969C\788D\4E13\9898\5FEB\7167\7ED3\679C\8868';
COMMENT ON TABLE "jcgkzx_monitor"."b_count" IS U&'\8868\884C\6570\7EDF\8BA1\8868';

CREATE OR REPLACE FUNCTION "jcgkzx_monitor"."table_tj"(schema_name TEXT)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    rec RECORD;
    total_count BIGINT;
BEGIN
    TRUNCATE TABLE "jcgkzx_monitor"."b_count";

    FOR rec IN
        SELECT
            t.table_name AS table_name,
            obj_description((schema_name || '.' || t.table_name)::regclass, 'pg_class') AS table_comment
        FROM information_schema.tables t
        WHERE t.table_schema = schema_name
          AND t.table_type = 'BASE TABLE'
          AND t.table_name <> 'b_count'
        ORDER BY t.table_name
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', schema_name, rec.table_name) INTO total_count;

        INSERT INTO "jcgkzx_monitor"."b_count" ("bm", "bzw", "zs")
        VALUES (
            rec.table_name,
            COALESCE(rec.table_comment, ''),
            total_count::VARCHAR
        );
    END LOOP;

    INSERT INTO "jcgkzx_monitor"."b_count" ("bm", "bzw", "zs")
    SELECT
        'zzzz_hj',
        U&'\5408\8BA1',
        COALESCE(SUM(COALESCE(NULLIF("zs", ''), '0')::BIGINT), 0)::VARCHAR
    FROM "jcgkzx_monitor"."b_count";
END;
$function$;

SELECT "jcgkzx_monitor"."table_tj"('jcgkzx_monitor');
