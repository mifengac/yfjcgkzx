-- Performance: index on b_per_jszahzryxxwh for DISTINCT ON (zjhm) ordering
-- Speeds up the snapshot rebuild which sorts by (zjhm, lgsj DESC, xgsj DESC, djsj DESC, systemid DESC)
CREATE INDEX IF NOT EXISTS idx_jszahzryxxwh_zjhm_lgsj
    ON "stdata"."b_per_jszahzryxxwh" (zjhm, lgsj DESC NULLS LAST, xgsj DESC NULLS LAST, djsj DESC NULLS LAST, systemid DESC)
    WHERE sflg = '1' AND deleteflag = '0';

-- Allow person_types_text to default to empty string for patients not in Excel
ALTER TABLE "jcgkzx_monitor"."jszahz_topic_snapshot"
    ALTER COLUMN person_types_text SET DEFAULT ' ';
