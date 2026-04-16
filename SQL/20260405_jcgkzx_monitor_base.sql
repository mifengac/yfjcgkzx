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
    group_no INTEGER NOT NULL DEFAULT 1,
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
    ON "jcgkzx_monitor"."custom_case_monitor_rule" (scheme_id, group_no, sort_order);

-- ■ 初始化方案数据 ---------------------------------------------------------

BEGIN;

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_scheme" (
    scheme_name,
    scheme_code,
    description,
    is_enabled,
    created_at,
    updated_at
)
VALUES
    ('暴力冲突倾向监测', 'violent_conflict_tendency', '系统初始化方案：监测持刀持械、打架斗殴、持物殴打、持械威胁、持械抢劫抢夺等高风险警情。', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('涉出租屋警情', 'rental_house_incident', '系统初始化方案：监测出租屋、房东租客、押金退租、租赁合同、群租等租住场景警情。', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('流浪/乞讨警情', 'wander_begging_incident', '系统初始化方案：监测流浪、乞讨、露宿街头、无家可归等警情，并排除流浪猫狗等动物类误报。', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (scheme_code) DO UPDATE
SET scheme_name = EXCLUDED.scheme_name,
    description = EXCLUDED.description,
    is_enabled = EXCLUDED.is_enabled,
    updated_at = CURRENT_TIMESTAMP;

DELETE FROM "jcgkzx_monitor"."custom_case_monitor_rule"
 WHERE scheme_id IN (
    SELECT id
      FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
     WHERE scheme_code IN ('violent_conflict_tendency', 'rental_house_incident', 'wander_begging_incident')
 );

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'regex_any', '["(?:持|拿|携带|手持).{0,6}(?:刀|匕首|砍刀|尖刀|菜刀|水果刀|械|钢管|铁棍|木棍|甩棍)"]'::jsonb, 1, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'violent_conflict_tendency';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'contains_any', '["打架","斗殴","伤人","殴打","打人","追打","砍人","捅人","刺伤","威胁","冲突"]'::jsonb, 1, 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'violent_conflict_tendency';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'contains_any', '["持刀伤人","持械伤人","持刀斗殴","持械斗殴","持刀打人","持械打人","持刀殴打","持械殴打","拿刀打人","拿刀殴打","拿械打人","拿械殴打","聚众斗殴","结伙斗殴","暴力冲突","激烈冲突"]'::jsonb, 2, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'violent_conflict_tendency';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'regex_any', '["(?:持|拿|抄起|挥舞|拿起|使用|用).{0,8}(?:刀|匕首|砍刀|尖刀|菜刀|水果刀|械|钢管|铁棍|木棍|甩棍|酒瓶|砖头|板凳).{0,6}(?:打人|殴打|伤人|追打|追砍|威胁|砍人|捅人)"]'::jsonb, 3, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'violent_conflict_tendency';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'regex_any', '["(?:持械|持刀|拿刀|拿械|手持器械).{0,6}(?:追打|追砍|威胁|伤人)"]'::jsonb, 4, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'violent_conflict_tendency';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'regex_any', '["(?:持|拿|携带|手持).{0,6}(?:刀|匕首|砍刀|尖刀|菜刀|水果刀|械|钢管|铁棍|木棍|甩棍)"]'::jsonb, 5, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'violent_conflict_tendency';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'contains_any', '["抢劫","抢夺","盗窃","入室","威胁"]'::jsonb, 5, 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'violent_conflict_tendency';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'contains_any', '["出租屋","出租房","群租房","日租房","租客","租户","房东","房客","退租","押金","租金","转租","租赁合同","清租","催租","欠租","合租"]'::jsonb, 1, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'rental_house_incident';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'occurAddress', 'contains_any', '["出租屋","出租房","群租房","日租房"]'::jsonb, 2, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'rental_house_incident';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'regex_any', '["(?:房东|房主|出租人).{0,8}(?:租客|租户|房客)","(?:租客|租户|房客).{0,8}(?:房东|房主|出租人)"]'::jsonb, 3, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'rental_house_incident';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'contains_any', '["乞讨","讨饭","讨钱","沿街乞讨","街头乞讨","乞讨人员","流浪乞讨","伸手要钱"]'::jsonb, 1, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'wander_begging_incident';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'contains_any', '["流浪","流浪人员","露宿","露宿街头","无家可归","睡街","睡桥洞","桥洞露宿","街头流浪"]'::jsonb, 2, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'wander_begging_incident';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'not_contains_any', '["流浪狗","流浪猫","流浪犬","猫狗","宠物","犬只","猫只"]'::jsonb, 2, 2, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'wander_begging_incident';

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_rule" (
    scheme_id,
    field_name,
    operator,
    rule_values,
    group_no,
    sort_order,
    is_enabled,
    created_at,
    updated_at
)
SELECT id, 'combined_text', 'regex_any', '["(?:流浪|乞讨).{0,4}(?:人员|男子|女子|老人|儿童)","(?:露宿|睡街|睡桥洞).{0,4}(?:人员|男子|女子|老人|儿童)?"]'::jsonb, 3, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
 WHERE scheme_code = 'wander_begging_incident';

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
