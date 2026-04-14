-- =====================================================================
-- 精神患者主题库：表结构 + 约束 + 索引（含历史修复脚本）
-- 合并自：create_jszahz_topic_tables, fix_error_message, harden_defaults
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS "jcgkzx_monitor";

-- ■ jszahz_topic_batch 批次导入表 ------------------------------------------

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

-- ■ jszahz_topic_person_type 人员标签明细 ----------------------------------

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

-- ■ jszahz_topic_snapshot 快照结果表 ---------------------------------------

CREATE TABLE IF NOT EXISTS "jcgkzx_monitor"."jszahz_topic_snapshot" (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    zjhm VARCHAR(64) NOT NULL,
    xm VARCHAR(128) NULL,
    lgsj TIMESTAMP NULL,
    lgdw VARCHAR(128) NULL,
    fxdj VARCHAR(16) NULL,
    fxdj_label VARCHAR(32) NOT NULL DEFAULT '无数据',
    person_types_text VARCHAR(255) NOT NULL DEFAULT ' ',
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

-- ■ 表注释 ------------------------------------------------------------------

COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_batch" IS U&'\7CBE\795E\969C\788D\4E13\9898\6279\6B21\5BFC\5165\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_person_type" IS U&'\7CBE\795E\969C\788D\4E13\9898\4EBA\5458\6807\7B7E\660E\7EC6\8868';
COMMENT ON TABLE "jcgkzx_monitor"."jszahz_topic_snapshot" IS U&'\7CBE\795E\969C\788D\4E13\9898\5FEB\7167\7ED3\679C\8868';

-- ■ 历史修复：error_message 允许 NULL --------------------------------------

ALTER TABLE "jcgkzx_monitor"."jszahz_topic_batch"
    ALTER COLUMN error_message DROP NOT NULL,
    ALTER COLUMN error_message DROP DEFAULT;

UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
SET error_message = NULL
WHERE error_message IS NOT NULL
  AND btrim(error_message) = '';

-- ■ 历史修复：默认值加固与空值清洗 -----------------------------------------

ALTER TABLE "jcgkzx_monitor"."jszahz_topic_batch"
    ALTER COLUMN sheet_name SET DEFAULT '汇总',
    ALTER COLUMN import_status SET DEFAULT 'pending',
    ALTER COLUMN is_active SET DEFAULT FALSE,
    ALTER COLUMN imported_row_count SET DEFAULT 0,
    ALTER COLUMN matched_person_count SET DEFAULT 0,
    ALTER COLUMN generated_tag_count SET DEFAULT 0,
    ALTER COLUMN created_by SET DEFAULT 'system',
    ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE "jcgkzx_monitor"."jszahz_topic_snapshot"
    ALTER COLUMN xm DROP NOT NULL,
    ALTER COLUMN xm DROP DEFAULT,
    ALTER COLUMN lgdw DROP NOT NULL,
    ALTER COLUMN lgdw DROP DEFAULT,
    ALTER COLUMN fxdj DROP NOT NULL,
    ALTER COLUMN fxdj DROP DEFAULT,
    ALTER COLUMN person_types_text SET DEFAULT ' ',
    ALTER COLUMN ssfjdm DROP NOT NULL,
    ALTER COLUMN ssfjdm DROP DEFAULT,
    ALTER COLUMN ssfj DROP NOT NULL,
    ALTER COLUMN ssfj DROP DEFAULT;

UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
SET
    sheet_name = COALESCE(sheet_name, '汇总'),
    import_status = COALESCE(import_status, 'pending'),
    is_active = COALESCE(is_active, FALSE),
    imported_row_count = COALESCE(imported_row_count, 0),
    matched_person_count = COALESCE(matched_person_count, 0),
    generated_tag_count = COALESCE(generated_tag_count, 0),
    created_by = CASE
        WHEN created_by IS NULL OR btrim(created_by) = '' THEN 'system'
        ELSE created_by
    END,
    created_at = COALESCE(created_at, CURRENT_TIMESTAMP);

UPDATE "jcgkzx_monitor"."jszahz_topic_snapshot"
SET
    xm = NULLIF(btrim(xm), ''),
    lgdw = NULLIF(btrim(lgdw), ''),
    fxdj = NULLIF(btrim(fxdj), ''),
    ssfjdm = NULLIF(btrim(ssfjdm), ''),
    ssfj = NULLIF(btrim(ssfj), '');
