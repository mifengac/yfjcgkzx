BEGIN;

ALTER TABLE "jcgkzx_monitor"."custom_case_monitor_rule"
    ADD COLUMN IF NOT EXISTS group_no INTEGER;

UPDATE "jcgkzx_monitor"."custom_case_monitor_rule"
   SET group_no = 1
 WHERE group_no IS NULL;

ALTER TABLE "jcgkzx_monitor"."custom_case_monitor_rule"
    ALTER COLUMN group_no SET DEFAULT 1;

ALTER TABLE "jcgkzx_monitor"."custom_case_monitor_rule"
    ALTER COLUMN group_no SET NOT NULL;

DROP INDEX IF EXISTS "jcgkzx_monitor".idx_custom_case_monitor_rule_scheme_id;

CREATE INDEX IF NOT EXISTS idx_custom_case_monitor_rule_scheme_id
    ON "jcgkzx_monitor"."custom_case_monitor_rule" (scheme_id, group_no, sort_order);

TRUNCATE TABLE
    "jcgkzx_monitor"."custom_case_monitor_rule",
    "jcgkzx_monitor"."custom_case_monitor_scheme"
RESTART IDENTITY;

INSERT INTO "jcgkzx_monitor"."custom_case_monitor_scheme" (
    scheme_name,
    scheme_code,
    description,
    is_enabled,
    created_at,
    updated_at
)
VALUES
    (
        '暴力冲突倾向监测',
        'violent_conflict_tendency',
        '监测持刀持械、打架斗殴、持物殴打、持械威胁、持械抢劫抢夺等高风险警情。',
        TRUE,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ),
    (
        '涉出租屋警情',
        'rental_house_incident',
        '监测出租屋、房东租客、押金退租、租赁合同、群租等租住场景警情。',
        TRUE,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    ),
    (
        '流浪/乞讨警情',
        'wander_begging_incident',
        '监测流浪、乞讨、露宿街头、无家可归等警情，并排除流浪猫狗等动物类误报。',
        TRUE,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'regex_any',
    '["(?:持|拿|携带|手持).{0,6}(?:刀|匕首|砍刀|尖刀|菜刀|水果刀|械|钢管|铁棍|木棍|甩棍)"]'::jsonb,
    1,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'contains_any',
    '["打架","斗殴","伤人","殴打","打人","追打","砍人","捅人","刺伤","威胁","冲突"]'::jsonb,
    1,
    2,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'contains_any',
    '["持刀伤人","持械伤人","持刀斗殴","持械斗殴","持刀打人","持械打人","持刀殴打","持械殴打","拿刀打人","拿刀殴打","拿械打人","拿械殴打","聚众斗殴","结伙斗殴","暴力冲突","激烈冲突"]'::jsonb,
    2,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'regex_any',
    '["(?:持|拿|抄起|挥舞|拿起|使用|用).{0,8}(?:刀|匕首|砍刀|尖刀|菜刀|水果刀|械|钢管|铁棍|木棍|甩棍|酒瓶|砖头|板凳).{0,6}(?:打人|殴打|伤人|追打|追砍|威胁|砍人|捅人)"]'::jsonb,
    3,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'regex_any',
    '["(?:持械|持刀|拿刀|拿械|手持器械).{0,6}(?:追打|追砍|威胁|伤人)"]'::jsonb,
    4,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'regex_any',
    '["(?:持|拿|携带|手持).{0,6}(?:刀|匕首|砍刀|尖刀|菜刀|水果刀|械|钢管|铁棍|木棍|甩棍)"]'::jsonb,
    5,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'contains_any',
    '["抢劫","抢夺","盗窃","入室","威胁"]'::jsonb,
    5,
    2,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'contains_any',
    '["出租屋","出租房","群租房","日租房","租客","租户","房东","房客","退租","押金","租金","转租","租赁合同","清租","催租","欠租","合租"]'::jsonb,
    1,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'occurAddress',
    'contains_any',
    '["出租屋","出租房","群租房","日租房"]'::jsonb,
    2,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'regex_any',
    '["(?:房东|房主|出租人).{0,8}(?:租客|租户|房客)","(?:租客|租户|房客).{0,8}(?:房东|房主|出租人)"]'::jsonb,
    3,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'contains_any',
    '["乞讨","讨饭","讨钱","沿街乞讨","街头乞讨","乞讨人员","流浪乞讨","伸手要钱"]'::jsonb,
    1,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'contains_any',
    '["流浪","流浪人员","露宿","露宿街头","无家可归","睡街","睡桥洞","桥洞露宿","街头流浪"]'::jsonb,
    2,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'not_contains_any',
    '["流浪狗","流浪猫","流浪犬","猫狗","宠物","犬只","猫只"]'::jsonb,
    2,
    2,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
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
SELECT
    id,
    'combined_text',
    'regex_any',
    '["(?:流浪|乞讨).{0,4}(?:人员|男子|女子|老人|儿童)","(?:露宿|睡街|睡桥洞).{0,4}(?:人员|男子|女子|老人|儿童)?"]'::jsonb,
    3,
    1,
    TRUE,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM "jcgkzx_monitor"."custom_case_monitor_scheme"
WHERE scheme_code = 'wander_begging_incident';

COMMIT;
