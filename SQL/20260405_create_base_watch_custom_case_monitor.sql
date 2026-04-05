CREATE SCHEMA IF NOT EXISTS "jcgkzx_monitor";

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
---
BEGIN;

CREATE SCHEMA IF NOT EXISTS "jcgkzx_monitor";

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
