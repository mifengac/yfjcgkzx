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
