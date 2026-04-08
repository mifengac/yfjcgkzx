CREATE SCHEMA IF NOT EXISTS "jcgkzx_monitor";

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."jszahz_topic_batch" (
    id SERIAL PRIMARY KEY,
    source_file_name VARCHAR(255) NOT NULL,
    sheet_name VARCHAR(64) NOT NULL DEFAULT '汇总',
    import_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    imported_row_count INTEGER NOT NULL DEFAULT 0,
    matched_person_count INTEGER NOT NULL DEFAULT 0,
    generated_tag_count INTEGER NOT NULL DEFAULT 0,
    created_by VARCHAR(64) NOT NULL DEFAULT '',
    error_message VARCHAR(1000) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TIMESTAMP NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_jszahz_topic_batch_active_success
    ON "jcgkzx_monitor"."jszahz_topic_batch" (import_status)
    WHERE is_active = TRUE AND import_status = 'success';

CREATE INDEX IF NOT EXISTS idx_jszahz_topic_batch_active_created
    ON "jcgkzx_monitor"."jszahz_topic_batch" (is_active, import_status, created_at DESC);

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."jszahz_topic_person_type" (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    zjhm VARCHAR(64) NOT NULL,
    person_type VARCHAR(64) NOT NULL,
    source_row_no INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_jszahz_topic_person_type_batch
        FOREIGN KEY (batch_id)
        REFERENCES "jcgkzx_monitor"."jszahz_topic_batch" (id)
        ON DELETE CASCADE,
    CONSTRAINT uq_jszahz_topic_person_type UNIQUE (batch_id, zjhm, person_type)
);

CREATE INDEX IF NOT EXISTS idx_jszahz_topic_person_type_lookup
    ON "jcgkzx_monitor"."jszahz_topic_person_type" (batch_id, person_type, zjhm);

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."jszahz_topic_snapshot" (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    zjhm VARCHAR(64) NOT NULL,
    xm VARCHAR(128) NULL,
    lgsj TIMESTAMP NULL,
    lgdw VARCHAR(128) NULL,
    fxdj VARCHAR(16) NULL,
    fxdj_label VARCHAR(32) NOT NULL DEFAULT '无数据',
    person_types_text VARCHAR(255) NOT NULL,
    ssfjdm VARCHAR(32) NULL,
    ssfj VARCHAR(128) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_jszahz_topic_snapshot_batch
        FOREIGN KEY (batch_id)
        REFERENCES "jcgkzx_monitor"."jszahz_topic_batch" (id)
        ON DELETE CASCADE,
    CONSTRAINT uq_jszahz_topic_snapshot UNIQUE (batch_id, zjhm)
);

CREATE INDEX IF NOT EXISTS idx_jszahz_topic_snapshot_query
    ON "jcgkzx_monitor"."jszahz_topic_snapshot" (batch_id, lgsj, fxdj_label, ssfjdm, zjhm);

COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_batch" IS U&'\7CBE\795E\969C\788D\4E13\9898\6279\6B21\5BFC\5165\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_person_type" IS U&'\7CBE\795E\969C\788D\4E13\9898\4EBA\5458\6807\7B7E\660E\7EC6\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_snapshot" IS U&'\7CBE\795E\969C\788D\4E13\9898\5FEB\7167\7ED3\679C\8868';
