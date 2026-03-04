-- ====================================================
-- 各派出所警情案件综合统计（含类型过滤）
-- 修改 start_time / end_time 切换统计时间范围
-- 修改 leixing 按类型过滤（空数组或 NULL 表示不限类型）
--   单类型：ARRAY['打架斗殴']
--   多类型：ARRAY['打架斗殴', '赌博', '涉黄']
--   不限类型：NULL 或 ARRAY[]::text[]
-- ====================================================
WITH
params AS (
    SELECT
        '2026-01-01 00:00:00'::timestamp AS start_time,
        '2026-03-04 00:00:00'::timestamp AS end_time,
        NULL::text[]                      AS leixing   -- 按类型过滤；NULL 或空数组不限
),

-- 高质量案件：同一 ajxx_ajbh 在拘留证表中出现 ≥3 条
high_quality_cases AS (
    SELECT ajxx_ajbh
    FROM   ywdata.zq_zfba_jlz
    GROUP  BY ajxx_ajbh
    HAVING COUNT(*) >= 3
),

-- 警情 + 转案
-- 类型过滤：通过 case_type_config.newcharasubclass_list 匹配 neworicharasubclass
jq_stats AS (
    SELECT
        left(jq.dutydeptno, 8) || '0000'        AS pcsdm,
        COUNT(*)                                  AS jq_cnt,
        COUNT(aj.ajxx_jqbh)                       AS za_cnt
    FROM   ywdata.zq_kshddpt_dsjfx_jq jq
    CROSS  JOIN params p
    LEFT   JOIN ywdata.zq_zfba_ajxx aj
           ON  jq.caseno = aj.ajxx_jqbh
    WHERE  jq.calltime::timestamp >= p.start_time
      AND  jq.calltime::timestamp <  p.end_time
      AND  (
               p.leixing IS NULL OR p.leixing = ARRAY[]::text[]
               OR EXISTS (
                   SELECT 1
                   FROM   ywdata.case_type_config ctc
                   WHERE  ctc.leixing = ANY(p.leixing)
                     AND  jq.neworicharasubclass = ANY(ctc.newcharasubclass_list)
               )
           )
    GROUP  BY left(jq.dutydeptno, 8) || '0000'
),

-- 行政、刑事、办结行政、破案、高质量
-- 类型过滤：通过 case_type_config.ay_pattern 模糊匹配 ajxx_aymc（案由）
aj_stats AS (
    SELECT
        left(aj.ajxx_cbdw_bh_dm, 8) || '0000'                          AS pcsdm,
        COUNT(*) FILTER (WHERE aj.ajxx_ajlx = '行政')                  AS xz_cnt,
        COUNT(*) FILTER (WHERE aj.ajxx_ajlx = '刑事')                  AS xs_cnt,
        COUNT(*) FILTER (
            WHERE aj.ajxx_ajlx = '行政'
              AND aj.ajxx_ajzt NOT IN ('已立案', '已受理')
        )                                                                AS bjxz_cnt,
        COUNT(*) FILTER (
            WHERE aj.ajxx_ajlx = '刑事'
              AND aj.ajxx_ajzt NOT IN ('已立案', '已受理')
        )                                                                AS pa_cnt,
        COUNT(DISTINCT aj.ajxx_ajbh) FILTER (
            WHERE hq.ajxx_ajbh IS NOT NULL
        )                                                                AS gzl_cnt
    FROM   ywdata.zq_zfba_ajxx aj
    CROSS  JOIN params p
    LEFT   JOIN high_quality_cases hq ON hq.ajxx_ajbh = aj.ajxx_ajbh
    WHERE  aj.ajxx_lasj >= p.start_time
      AND  aj.ajxx_lasj <  p.end_time
      AND  (
               p.leixing IS NULL OR p.leixing = ARRAY[]::text[]
               OR EXISTS (
                   SELECT 1
                   FROM   ywdata.case_type_config ctc
                   WHERE  ctc.leixing = ANY(p.leixing)
                     AND  COALESCE(aj.ajxx_aymc, '') SIMILAR TO ctc.ay_pattern
               )
           )
    GROUP  BY left(aj.ajxx_cbdw_bh_dm, 8) || '0000'
),

-- 治拘（行政拘留）
-- 前置条件：xzcfjds_cfzl ~ '拘留'
-- 类型过滤：JOIN 案件表获取 ajxx_aymc，再通过 ay_pattern 匹配
zhiju_stats AS (
    SELECT
        left(xz.xzcfjds_cbdw_bh_dm, 8) || '0000'  AS pcsdm,
        COUNT(*)                                     AS zhiju_cnt
    FROM   ywdata.zq_zfba_xzcfjds xz
    CROSS  JOIN params p
    LEFT   JOIN ywdata.zq_zfba_ajxx aj_xz
           ON   aj_xz.ajxx_ajbh = xz.ajxx_ajbh
    WHERE  xz.xzcfjds_cfzl ~ '拘留'
      AND  xz.xzcfjds_spsj >= p.start_time
      AND  xz.xzcfjds_spsj <  p.end_time
      AND  (
               p.leixing IS NULL OR p.leixing = ARRAY[]::text[]
               OR EXISTS (
                   SELECT 1
                   FROM   ywdata.case_type_config ctc
                   WHERE  ctc.leixing = ANY(p.leixing)
                     AND  COALESCE(aj_xz.ajxx_aymc, '') SIMILAR TO ctc.ay_pattern
               )
           )
    GROUP  BY left(xz.xzcfjds_cbdw_bh_dm, 8) || '0000'
),

-- 刑拘
-- 类型过滤：通过 jlz_ay_mc 匹配 ay_pattern
xingju_stats AS (
    SELECT
        left(jlz.jlz_cbdw_bh_dm, 8) || '0000'      AS pcsdm,
        COUNT(*)                                      AS xingju_cnt
    FROM   ywdata.zq_zfba_jlz jlz
    CROSS  JOIN params p
    WHERE  jlz.jlz_pzsj >= p.start_time
      AND  jlz.jlz_pzsj <  p.end_time
      AND  (
               p.leixing IS NULL OR p.leixing = ARRAY[]::text[]
               OR EXISTS (
                   SELECT 1
                   FROM   ywdata.case_type_config ctc
                   WHERE  ctc.leixing = ANY(p.leixing)
                     AND  COALESCE(jlz.jlz_ay_mc, '') SIMILAR TO ctc.ay_pattern
               )
           )
    GROUP  BY left(jlz.jlz_cbdw_bh_dm, 8) || '0000'
),

-- 逮捕
-- 类型过滤：通过 dbz_ay_mc 匹配 ay_pattern
daibu_stats AS (
    SELECT
        left(dbz.dbz_cbqy_bh_dm, 8) || '0000'       AS pcsdm,
        COUNT(*)                                       AS daibu_cnt
    FROM   ywdata.zq_zfba_dbz dbz
    CROSS  JOIN params p
    WHERE  dbz.dbz_pzsj >= p.start_time
      AND  dbz.dbz_pzsj <  p.end_time
      AND  (
               p.leixing IS NULL OR p.leixing = ARRAY[]::text[]
               OR EXISTS (
                   SELECT 1
                   FROM   ywdata.case_type_config ctc
                   WHERE  ctc.leixing = ANY(p.leixing)
                     AND  COALESCE(dbz.dbz_ay_mc, '') SIMILAR TO ctc.ay_pattern
               )
           )
    GROUP  BY left(dbz.dbz_cbqy_bh_dm, 8) || '0000'
),

-- 起诉
-- 类型过滤：通过 ajxx_ay 匹配 ay_pattern
qisu_stats AS (
    SELECT
        left(qsryxx.qsryxx_cbdw_bh_dm, 8) || '0000'  AS pcsdm,
        COUNT(*)                                        AS qisu_cnt
    FROM   ywdata.zq_zfba_qsryxx qsryxx
    CROSS  JOIN params p
    WHERE  qsryxx.qsryxx_tfsj >= p.start_time
      AND  qsryxx.qsryxx_tfsj <  p.end_time
      AND  (
               p.leixing IS NULL OR p.leixing = ARRAY[]::text[]
               OR EXISTS (
                   SELECT 1
                   FROM   ywdata.case_type_config ctc
                   WHERE  ctc.leixing = ANY(p.leixing)
                     AND  COALESCE(qsryxx.ajxx_ay, '') SIMILAR TO ctc.ay_pattern
               )
           )
    GROUP  BY left(qsryxx.qsryxx_cbdw_bh_dm, 8) || '0000'
)

SELECT
    d.ssfj                           AS 所属分局,
    d.sspcs                          AS 派出所名称,
    d.sspcsdm                        AS 派出所代码,
    COALESCE(j.jq_cnt,    0)         AS 警情,
    COALESCE(j.za_cnt,    0)         AS 转案,
    COALESCE(a.xz_cnt,    0)         AS 行政,
    COALESCE(a.xs_cnt,    0)         AS 刑事,
    COALESCE(a.bjxz_cnt,  0)         AS 办结,
    COALESCE(a.pa_cnt,    0)         AS 破案,
    COALESCE(a.gzl_cnt,   0)         AS 高质量,
    COALESCE(z.zhiju_cnt, 0)         AS 治拘,
    COALESCE(x.xingju_cnt,0)         AS 刑拘,
    COALESCE(db.daibu_cnt,0)         AS 逮捕,
    COALESCE(q.qisu_cnt,  0)         AS 起诉
FROM       stdata.b_dic_zzjgdm d
LEFT JOIN  jq_stats     j  ON j.pcsdm  = d.sspcsdm
LEFT JOIN  aj_stats     a  ON a.pcsdm  = d.sspcsdm
LEFT JOIN  zhiju_stats  z  ON z.pcsdm  = d.sspcsdm
LEFT JOIN  xingju_stats x  ON x.pcsdm  = d.sspcsdm
LEFT JOIN  daibu_stats  db ON db.pcsdm = d.sspcsdm
LEFT JOIN  qisu_stats   q  ON q.pcsdm  = d.sspcsdm
ORDER BY   d.sspcs;
