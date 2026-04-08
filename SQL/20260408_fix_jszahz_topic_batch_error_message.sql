ALTER TABLE "jcgkzx_monitor"."jszahz_topic_batch"
    ALTER COLUMN error_message SET DEFAULT '';

UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
SET error_message = ''
WHERE error_message IS NULL;
