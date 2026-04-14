ALTER TABLE "jcgkzx_monitor"."jszahz_topic_batch"
    ADD COLUMN IF NOT EXISTS source_kind VARCHAR(16) NOT NULL DEFAULT 'tag';

UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
SET source_kind = 'tag'
WHERE source_kind IS NULL OR BTRIM(source_kind) = '';

DROP INDEX IF EXISTS "jcgkzx_monitor".uq_jszahz_topic_batch_active_success;

CREATE UNIQUE INDEX IF NOT EXISTS uq_jszahz_topic_batch_active_success_kind
    ON "jcgkzx_monitor"."jszahz_topic_batch" (source_kind)
    WHERE is_active = TRUE AND import_status = 'success';

CREATE INDEX IF NOT EXISTS idx_jszahz_topic_batch_kind_active_created
    ON "jcgkzx_monitor"."jszahz_topic_batch" (source_kind, is_active, import_status, created_at DESC);

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."jszahz_topic_base_person" (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    zjhm VARCHAR(64) NOT NULL,
    xm VARCHAR(128) NULL,
    ssfjdm VARCHAR(32) NULL,
    source_sheet_name VARCHAR(32) NOT NULL,
    source_row_no INTEGER NOT NULL,
    source_seq_no VARCHAR(64) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_jszahz_topic_base_person_batch
        FOREIGN KEY (batch_id)
        REFERENCES "jcgkzx_monitor"."jszahz_topic_batch" (id)
        ON DELETE CASCADE,
    CONSTRAINT uq_jszahz_topic_base_person UNIQUE (batch_id, zjhm)
);

CREATE INDEX IF NOT EXISTS idx_jszahz_topic_base_person_lookup
    ON "jcgkzx_monitor"."jszahz_topic_base_person" (batch_id, ssfjdm, zjhm);

COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_base_person" IS U&'\7CBE\795E\969C\788D\4E13\9898\57FA\7840\4EBA\5458\5BFC\5165\660E\7EC6\8868';