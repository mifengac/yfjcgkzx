-- Purpose:
-- 1. Replace the previous full-table DISTINCT ON strategy on ywdata.vio_surveil.
-- 2. Match the optimized traffic-detail SQL that filters vio_violation first,
--    then looks up the latest vio_surveil row per wfbh through LATERAL.
--
-- Notes:
-- - V2 analysis showed the main bottleneck was the query shape, not just
--   the missing indexes. The old WITH surveil AS (SELECT DISTINCT ON ...)
--   forced a full scan + external sort on vio_surveil before touching the
--   filtered vio_violation rows.
-- - This script keeps the same business result, but aligns indexes with the
--   new lookup pattern.

-- Rebuild the violation-side index so it matches the query's sort semantics
-- and skips rows that can never be matched by the business filter.
DROP INDEX IF EXISTS "ywdata"."idx_vio_violation_jszh_wfsj";

CREATE INDEX IF NOT EXISTS idx_vio_violation_jszh_wfsj
    ON "ywdata"."vio_violation" ("jszh", "wfsj" DESC NULLS LAST)
    WHERE "jszh" IS NOT NULL;

-- Replace the broad surveil index with a narrower partial index tailored for:
-- WHERE vs.wfbh = vv.wfbh
-- ORDER BY vs.gxsj DESC NULLS LAST, vs.lrsj DESC NULLS LAST, vs.xh DESC
-- LIMIT 1
DROP INDEX IF EXISTS "ywdata"."idx_vio_surveil_wfbh_gxsj_lrsj_xh";
DROP INDEX IF EXISTS "ywdata"."idx_vio_surveil_wfbh_latest_partial";

CREATE INDEX IF NOT EXISTS idx_vio_surveil_wfbh_latest_partial
    ON "ywdata"."vio_surveil" (
        "wfbh",
        "gxsj" DESC NULLS LAST,
        "lrsj" DESC NULLS LAST,
        "xh" DESC
    )
    WHERE "wfbh" IS NOT NULL;

ANALYZE "ywdata"."vio_violation";
ANALYZE "ywdata"."vio_surveil";

-- Optional verification for the single-person traffic detail query.
-- Replace ID_CARD_1 with a real sample ID number before running.
-- Expected direction:
-- - Index Scan on idx_vio_violation_jszh_wfsj
-- - Nested Loop Left Join
-- - Index Scan or Index Only Scan on idx_vio_surveil_wfbh_latest_partial
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
SELECT
    vv."wfbh" AS violation_no,
    vv."wfsj" AS violation_time,
    COALESCE(surveil."wfdz", vv."wfdz", vv."wfdd") AS violation_location,
    vv."hphm" AS plate_no,
    vv."hpzl" AS plate_type,
    COALESCE(surveil."jdcsyr", vv."jdcsyr") AS vehicle_owner,
    vv."wfxw" AS violation_action,
    vv."fkje" AS fine_amount,
    vv."wfjfs" AS penalty_points
FROM "ywdata"."vio_violation" vv
LEFT JOIN LATERAL (
    SELECT
        vs."wfdz",
        vs."jdcsyr"
    FROM "ywdata"."vio_surveil" vs
    WHERE vs."wfbh" = vv."wfbh"
    ORDER BY
        vs."gxsj" DESC NULLS LAST,
        vs."lrsj" DESC NULLS LAST,
        vs."xh" DESC
    LIMIT 1
) surveil
    ON TRUE
WHERE vv."jszh" = 'ID_CARD_1'
ORDER BY vv."wfsj" DESC NULLS LAST;

-- Optional verification for the batch export traffic detail query.
-- Replace the sample ID numbers before running.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
SELECT
    UPPER(vv."jszh") AS id_card,
    vv."wfbh" AS violation_no,
    vv."wfsj" AS violation_time,
    COALESCE(surveil."wfdz", vv."wfdz", vv."wfdd") AS violation_location,
    vv."hphm" AS plate_no,
    vv."hpzl" AS plate_type,
    COALESCE(surveil."jdcsyr", vv."jdcsyr") AS vehicle_owner,
    vv."wfxw" AS violation_action,
    vv."fkje" AS fine_amount,
    vv."wfjfs" AS penalty_points
FROM "ywdata"."vio_violation" vv
LEFT JOIN LATERAL (
    SELECT
        vs."wfdz",
        vs."jdcsyr"
    FROM "ywdata"."vio_surveil" vs
    WHERE vs."wfbh" = vv."wfbh"
    ORDER BY
        vs."gxsj" DESC NULLS LAST,
        vs."lrsj" DESC NULLS LAST,
        vs."xh" DESC
    LIMIT 1
) surveil
    ON TRUE
WHERE vv."jszh" = ANY(ARRAY['ID_CARD_1', 'ID_CARD_2', 'ID_CARD_3']::text[])
ORDER BY UPPER(vv."jszh"), vv."wfsj" DESC NULLS LAST;
