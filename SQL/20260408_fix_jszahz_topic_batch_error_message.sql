ALTER TABLE "jcgkzx_monitor"."jszahz_topic_batch"
    ALTER COLUMN error_message DROP NOT NULL,
    ALTER COLUMN error_message DROP DEFAULT;

UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
SET error_message = NULL
WHERE error_message IS NOT NULL
  AND btrim(error_message) = '';
