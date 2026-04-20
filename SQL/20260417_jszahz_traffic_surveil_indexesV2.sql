-- Purpose:
-- 1. Speed up traffic-violation detail queries in jszahzyj by filtering
--    ywdata.vio_violation on (jszh, wfsj DESC).
-- 2. Speed up the latest-row lookup on ywdata.vio_surveil by supporting
--    DISTINCT ON (wfbh) ORDER BY (wfbh, gxsj DESC, lrsj DESC, xh DESC).

CREATE INDEX IF NOT EXISTS idx_vio_violation_jszh_wfsj
    ON "ywdata"."vio_violation" ("jszh", "wfsj" DESC);

CREATE INDEX IF NOT EXISTS idx_vio_surveil_wfbh_gxsj_lrsj_xh
    ON "ywdata"."vio_surveil" ("wfbh", "gxsj" DESC, "lrsj" DESC, "xh" DESC);

-- Refresh planner statistics after creating indexes.
ANALYZE "ywdata"."vio_violation";
ANALYZE "ywdata"."vio_surveil";

-- Optional verification for single-person traffic detail query.
-- Replace ID_CARD_1 with a real sample ID number before running.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH surveil AS (
    SELECT DISTINCT ON (vs."wfbh")
        vs."wfbh",
        vs."wfdz",
        vs."jdcsyr",
        vs."gxsj",
        vs."lrsj",
        vs."xh"
    FROM "ywdata"."vio_surveil" vs
    WHERE vs."wfbh" IS NOT NULL
    ORDER BY
        vs."wfbh",
        vs."gxsj" DESC NULLS LAST,
        vs."lrsj" DESC NULLS LAST,
        vs."xh" DESC
)
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
LEFT JOIN surveil
    ON surveil."wfbh" = vv."wfbh"
WHERE vv."jszh" = 'ID_CARD_1'
ORDER BY vv."wfsj" DESC;
---
Sort  (cost=1084362.89..1084362.92 rows=9 width=119) (actual time=42204.634..42204.668 rows=0 loops=1)
  Output: vv.wfbh, vv.wfsj, (COALESCE(vs.wfdz, vv.wfdz, vv.wfdd)), vv.hphm, vv.hpzl, (COALESCE(vs.jdcsyr, vv.jdcsyr)), vv.wfxw, vv.fkje, vv.wfjfs
  Sort Key: vv.wfsj DESC
  Sort Method: quicksort  Memory: 25kB
  Buffers: shared hit=418272 read=3, temp read=3800 written=49295
  ->  Merge Right Join  (cost=1028323.44..1084362.75 rows=9 width=119) (actual time=42204.627..42204.655 rows=0 loops=1)
        Output: vv.wfbh, vv.wfsj, COALESCE(vs.wfdz, vv.wfdz, vv.wfdd), vv.hphm, vv.hpzl, COALESCE(vs.jdcsyr, vv.jdcsyr), vv.wfxw, vv.fkje, vv.wfjfs
        Inner Unique: true
        Merge Cond: ((vs.wfbh)::text = (vv.wfbh)::text)
        Buffers: shared hit=418272 read=3, temp read=3800 written=49295
        ->  Unique  (cost=1028282.64..1045688.56 rows=3090663 width=100) (actual time=42204.474..42204.488 rows=1 loops=1)
              Output: vs.wfbh, vs.wfdz, vs.jdcsyr, vs.gxsj, vs.lrsj, vs.xh
              Buffers: shared hit=418271, temp read=3800 written=49295
              ->  Sort  (cost=1028282.64..1036985.60 rows=3481185 width=100) (actual time=42204.468..42204.476 rows=1 loops=1)
                    Output: vs.wfbh, vs.wfdz, vs.jdcsyr, vs.gxsj, vs.lrsj, vs.xh
                    Sort Key: vs.wfbh, vs.gxsj DESC NULLS LAST, vs.lrsj DESC NULLS LAST, vs.xh DESC
                    Sort Method: external merge  Disk: 394168kB
                    Buffers: shared hit=418271, temp read=3800 written=49295
                    ->  Seq Scan on ywdata.vio_surveil vs  (cost=0.00..459652.92 rows=3481185 width=100) (actual time=0.017..11064.451 rows=3483271 loops=1)
                          Output: vs.wfbh, vs.wfdz, vs.jdcsyr, vs.gxsj, vs.lrsj, vs.xh
                          Filter: (vs.wfbh IS NOT NULL)
                          Rows Removed by Filter: 655766
                          Buffers: shared hit=418271
        ->  Sort  (cost=40.81..40.83 rows=9 width=106) (actual time=0.139..0.147 rows=0 loops=1)
              Output: vv.wfbh, vv.wfsj, vv.wfdz, vv.wfdd, vv.hphm, vv.hpzl, vv.jdcsyr, vv.wfxw, vv.fkje, vv.wfjfs
              Sort Key: vv.wfbh
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=1 read=3
              ->  Index Scan using idx_vio_violation_jszh_wfsj on ywdata.vio_violation vv  (cost=0.56..40.67 rows=9 width=106) (actual time=0.117..0.119 rows=0 loops=1)
                    Output: vv.wfbh, vv.wfsj, vv.wfdz, vv.wfdd, vv.hphm, vv.hpzl, vv.jdcsyr, vv.wfxw, vv.fkje, vv.wfjfs
                    Index Cond: ((vv.jszh)::text = 'ID_CARD_1'::text)
                    Buffers: shared hit=1 read=3
Planning Time: 1.026 ms
Execution Time: 42234.815 ms

-- Optional verification for batch export traffic detail query.
-- Replace the sample ID numbers before running.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, COSTS, TIMING, SUMMARY)
WITH params AS (
    SELECT ARRAY['ID_CARD_1', 'ID_CARD_2', 'ID_CARD_3']::text[] AS sample_ids
),
surveil AS (
    SELECT DISTINCT ON (vs."wfbh")
        vs."wfbh",
        vs."wfdz",
        vs."jdcsyr",
        vs."gxsj",
        vs."lrsj",
        vs."xh"
    FROM "ywdata"."vio_surveil" vs
    WHERE vs."wfbh" IS NOT NULL
    ORDER BY
        vs."wfbh",
        vs."gxsj" DESC NULLS LAST,
        vs."lrsj" DESC NULLS LAST,
        vs."xh" DESC
)
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
LEFT JOIN surveil
    ON surveil."wfbh" = vv."wfbh"
JOIN params p
    ON 1 = 1
WHERE vv."jszh" = ANY(p.sample_ids)
ORDER BY UPPER(vv."jszh"), vv."wfsj" DESC;

---
Sort  (cost=1084445.53..1084445.60 rows=27 width=635) (actual time=42689.641..42689.675 rows=0 loops=1)
  Output: ((upper((vv.jszh)::text))::character varying(8000 char)), vv.wfbh, vv.wfsj, (COALESCE(vs.wfdz, vv.wfdz, vv.wfdd)), vv.hphm, vv.hpzl, (COALESCE(vs.jdcsyr, vv.jdcsyr)), vv.wfxw, vv.fkje, vv.wfjfs
  Sort Key: ((upper((vv.jszh)::text))::character varying(8000 char)), vv.wfsj DESC
  Sort Method: quicksort  Memory: 25kB
  Buffers: shared hit=418289, temp read=3800 written=49295
  ->  Merge Right Join  (cost=1028405.26..1084444.89 rows=27 width=635) (actual time=42689.614..42689.643 rows=0 loops=1)
        Output: (upper((vv.jszh)::text))::character varying(8000 char), vv.wfbh, vv.wfsj, COALESCE(vs.wfdz, vv.wfdz, vv.wfdd), vv.hphm, vv.hpzl, COALESCE(vs.jdcsyr, vv.jdcsyr), vv.wfxw, vv.fkje, vv.wfjfs
        Inner Unique: true
        Merge Cond: ((vs.wfbh)::text = (vv.wfbh)::text)
        Buffers: shared hit=418286, temp read=3800 written=49295
        ->  Unique  (cost=1028282.64..1045688.56 rows=3090663 width=100) (actual time=42689.434..42689.447 rows=1 loops=1)
              Output: vs.wfbh, vs.wfdz, vs.jdcsyr, vs.gxsj, vs.lrsj, vs.xh
              Buffers: shared hit=418271, temp read=3800 written=49295
              ->  Sort  (cost=1028282.64..1036985.60 rows=3481185 width=100) (actual time=42689.427..42689.435 rows=1 loops=1)
                    Output: vs.wfbh, vs.wfdz, vs.jdcsyr, vs.gxsj, vs.lrsj, vs.xh
                    Sort Key: vs.wfbh, vs.gxsj DESC NULLS LAST, vs.lrsj DESC NULLS LAST, vs.xh DESC
                    Sort Method: external merge  Disk: 394168kB
                    Buffers: shared hit=418271, temp read=3800 written=49295
                    ->  Seq Scan on ywdata.vio_surveil vs  (cost=0.00..459652.92 rows=3481185 width=100) (actual time=0.027..10655.826 rows=3483271 loops=1)
                          Output: vs.wfbh, vs.wfdz, vs.jdcsyr, vs.gxsj, vs.lrsj, vs.xh
                          Filter: (vs.wfbh IS NOT NULL)
                          Rows Removed by Filter: 655766
                          Buffers: shared hit=418271
        ->  Sort  (cost=122.63..122.70 rows=27 width=124) (actual time=0.167..0.176 rows=0 loops=1)
              Output: vv.jszh, vv.wfbh, vv.wfsj, vv.wfdz, vv.wfdd, vv.hphm, vv.hpzl, vv.jdcsyr, vv.wfxw, vv.fkje, vv.wfjfs
              Sort Key: vv.wfbh
              Sort Method: quicksort  Memory: 25kB
              Buffers: shared hit=15
              ->  Index Scan using idx_vio_violation_jszh_wfsj on ywdata.vio_violation vv  (cost=0.56..121.99 rows=27 width=124) (actual time=0.148..0.151 rows=0 loops=1)
                    Output: vv.jszh, vv.wfbh, vv.wfsj, vv.wfdz, vv.wfdd, vv.hphm, vv.hpzl, vv.jdcsyr, vv.wfxw, vv.fkje, vv.wfjfs
                    Index Cond: ((vv.jszh)::text = ANY ('{ID_CARD_1,ID_CARD_2,ID_CARD_3}'::text[]))
                    Buffers: shared hit=15
Planning Time: 2.612 ms
Execution Time: 42725.237 ms