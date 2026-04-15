-- Upgrade custom case monitor rules to support:
-- 1. AND inside one group
-- 2. OR between groups

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
