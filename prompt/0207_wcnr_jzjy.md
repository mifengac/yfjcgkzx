# 帮我分析下这个SQL执行慢的原因
## 修改严重不良未成年人矫治教育覆盖率的数据源为
	```
	WITH base_data AS (
		-- 基础案件-人员数据
		SELECT 
			zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
			zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
			zzwx."ajxx_join_ajxx_ay" AS 案由,
			zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
			LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
			zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
			zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
			zzwx.xyrxx_xm AS 姓名,
			zzwx."xyrxx_sfzh" AS 身份证号,
			zzwx."xyrxx_rybh" AS 人员编号,
			zzwx."xyrxx_hjdxz" AS 户籍地,
			zzwx."xyrxx_nl" AS 年龄,
			zzwx."xyrxx_isdel" AS 是否删除,
			zzwx."xyrxx_jzdxzqh" AS 居住地
		FROM ywdata."zq_zfba_wcnr_xyr" zzwx
		WHERE zzwx."ajxx_join_ajxx_isdel_dm" = 0 
			AND zzwx."xyrxx_isdel_dm" = 0 
			AND zzwx."xyrxx_sfda_dm" = 1
			AND zzwx."ajxx_join_ajxx_lasj" BETWEEN '2026-01-01' AND '2026-02-06'
			AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (SELECT ctc."ay_pattern"   FROM "case_type_config" ctc  WHERE ctc."leixing" = '打架斗殴')
	),
	filtered_data AS (
		-- 根据案件类型匹配相应的文书表
		SELECT bd.*
		FROM base_data bd
		WHERE 
			-- 行政案件:匹配行政处罚决定书或不予行政处罚决定书
			(bd.案件类型 = '行政' AND (
				EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
					WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
				)
				OR EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_byxzcfjds" b
					WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
				)
			))
			-- 刑事案件:匹配拘留证
			OR bd.案件类型 = '刑事' 
	)
	SELECT zws.*,CASE WHEN zzx."id" IS NOT NULL THEN '是' ELSE '否'  END AS 是否开具矫治文书
	FROM filtered_data zws LEFT JOIN zq_zfba_xjs2 zzx ON zws.姓名 =zzx."xgry_xm" AND zws.案件编号=zzx."ajbh" ```

## SQL
```
WITH base_data AS (
    -- 基础案件-人员数据
    SELECT 
        zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
        zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
        zzwx."ajxx_join_ajxx_ay" AS 案由,
        zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
        LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
        zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
        zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
        zzwx.xyrxx_xm AS 姓名,
        zzwx."xyrxx_sfzh" AS 身份证号,
        zzwx."xyrxx_rybh" AS 人员编号,
        zzwx."xyrxx_hjdxz" AS 户籍地,
        zzwx."xyrxx_nl" AS 年龄,
        zzwx."xyrxx_isdel" AS 是否删除,
        zzwx."xyrxx_jzdxzqh" AS 居住地
    FROM ywdata."zq_zfba_wcnr_xyr" zzwx
    WHERE zzwx."ajxx_join_ajxx_isdel_dm" = 0 
        AND zzwx."xyrxx_isdel_dm" = 0 
        AND zzwx."xyrxx_sfda_dm" = 1
--        AND zzwx."xyrxx_lrsj" BETWEEN '2026-01-01' AND '2026-02-06'
--        AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (
--            SELECT ctc."ay_pattern" 
--            FROM "case_type_config" ctc 
--            WHERE ctc."leixing" = '打架斗殴'
--        )
),
filtered_data AS (
    -- 根据案件类型匹配相应的文书表
    SELECT bd.*
    FROM base_data bd
    WHERE 
        -- 行政案件:匹配行政处罚决定书或不予行政处罚决定书
        (bd.案件类型 = '行政' AND (
            EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
                WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
            )
            OR EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_byxzcfjds" b
                WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
            )
        ))
        -- 刑事案件:匹配拘留证
        OR bd.案件类型 = '刑事' 
),
violation_counts AS (
    -- 计算每个人的违法次数和案由
    SELECT 
        xyrxx_sfzh AS 身份证号,
        COUNT(*) AS 违法次数,
        COUNT(DISTINCT ajxx_join_ajxx_ay_dm) AS 不同案由数,
        MIN(ajxx_join_ajxx_ay_dm) AS 案由代码
    FROM ywdata."zq_zfba_wcnr_xyr"
    WHERE xyrxx_isdel_dm = 0 AND ajxx_join_ajxx_isdel_dm = 0
    GROUP BY xyrxx_sfzh
),
first_case_xjs AS (
    -- 查找第一次违法是否开具了训诫书
    SELECT DISTINCT
        fd.身份证号,
        fd.案件编号 AS 当前案件编号,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM ywdata."zq_zfba_wcnr_xyr" w
                JOIN ywdata."zq_zfba_xjs2" x 
                    ON w."ajxx_join_ajxx_ajbh" = x.ajbh 
                    AND w.xyrxx_xm = x.xgry_xm
                WHERE w.xyrxx_sfzh = fd.身份证号
                    AND w."ajxx_join_ajxx_ajbh" != fd.案件编号
                    AND w.xyrxx_isdel_dm = 0
                    AND w.ajxx_join_ajxx_isdel_dm = 0
            ) THEN 1 
            ELSE 0 
        END AS 有训诫书
    FROM filtered_data fd
)
SELECT DISTINCT
    fd.案件编号,
    fd.人员编号,
    fd.案件类型,
    fd.案由,
    fd.地区,
    fd.办案单位,
    fd.立案时间,
    fd.姓名,
    fd.身份证号,
    fd.户籍地,
    fd.年龄,
    fd.居住地,
    
    -- 治拘大于4天(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' AND EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
            WHERE x.ajxx_ajbh = fd.案件编号 
                AND x.xzcfjds_rybh = fd.人员编号
                AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
        ) THEN '是'
        ELSE '否'
    END AS 治拘大于4天,
    
    -- 2次违法且案由相同且第一次违法开具了训诫书(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' 
            AND vc.违法次数 = 2 
            AND vc.不同案由数 = 1
            AND fcx.有训诫书 = 1
        THEN '是'
        ELSE '否'
    END AS "2次违法且案由相同且第一次违法开具了训诫书",
    
    -- 3次及以上违法(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' AND vc.违法次数 > 2 THEN '是'
        ELSE '否'
    END AS "3次及以上违法",
    
    -- 是否刑拘(仅刑事案件)
    CASE 
        WHEN fd.案件类型 = '刑事' AND EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_jlz" j
            WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
        ) THEN '是'
        ELSE '否'
    END AS 是否刑拘,
    
    -- 是否开具矫治文书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_zlwcnrzstdxwgftzs" z
            WHERE z.zltzs_ajbh = fd.案件编号 AND z.zltzs_rybh = fd.人员编号
        ) OR EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_xjs2" x
            WHERE x.ajbh = fd.案件编号 AND x.xgry_xm = fd.姓名
        ) THEN '是'
        ELSE '否'
    END AS 是否开具矫治文书,
    
    -- 是否开具加强监督教育/责令接受家庭教育指导通知书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_jtjyzdtzs" j
            WHERE j.jqjhjyzljsjtjyzdtzs_ajbh = fd.案件编号 
                AND j.jqjhjyzljsjtjyzdtzs_rybh = fd.人员编号
        ) THEN '是'
        ELSE '否'
    END AS "是否开具加强/责令接受家庭教育指导通知书",
    
    -- 是否开具提请专门教育申请书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_tqzmjy" t
            WHERE t.ajbh = fd.案件编号 AND t.xgry_xm = fd.姓名
        ) THEN '是'
        ELSE '否'
    END AS 是否开具提请专门教育申请书,
    
    -- 是否符合送生
    CASE 
        WHEN CAST(fd.年龄 AS INTEGER) > 11 AND (
            -- 治拘大于4天
            (fd.案件类型 = '行政' AND EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
                WHERE x.ajxx_ajbh = fd.案件编号 
                    AND x.xzcfjds_rybh = fd.人员编号
                    AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
            ))
            -- 2次违法且案由相同且第一次违法开具了训诫书
            OR (fd.案件类型 = '行政' AND vc.违法次数 = 2 AND vc.不同案由数 = 1 AND fcx.有训诫书 = 1)
            -- 3次及以上违法
            OR (fd.案件类型 = '行政' AND vc.违法次数 > 2)
            -- 是否刑拘
            OR (fd.案件类型 = '刑事' AND EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_jlz" j
                WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
            ))
        ) THEN '是'
        ELSE '否'
    END AS 是否符合送生,
    
    -- 是否送校
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_wcnr_sfzxx" s
            WHERE s.sfzhm = fd.身份证号 
                AND s.rx_time > fd.立案时间
        ) THEN '是'
        ELSE '否'
    END AS 是否送校

FROM filtered_data fd
LEFT JOIN violation_counts vc ON fd.身份证号 = vc.身份证号
LEFT JOIN first_case_xjs fcx ON fd.身份证号 = fcx.身份证号 AND fd.案件编号 = fcx.当前案件编号

ORDER BY fd.案件编号, fd.人员编号;
```
## 执行计划
Unique  (cost=50408972.44..50408972.49 rows=1 width=648) (actual time=7482.421..7487.312 rows=2255 loops=1)
  CTE filtered_data
    ->  Seq Scan on zq_zfba_wcnr_xyr zzwx  (cost=0.00..50372504.78 rows=1 width=277) (actual time=76.649..89.628 rows=2256 loops=1)
          Filter: (((ajxx_join_ajxx_isdel_dm)::integer = 0) AND ((xyrxx_isdel_dm)::integer = 0) AND ((xyrxx_sfda_dm)::integer = 1) AND ((ajxx_join_ajxx_ajlx = '刑事'::text) OR ((ajxx_join_ajxx_ajlx = '行政'::text) AND ((alternatives: SubPlan 1 or hashed SubPlan 2) OR (alternatives: SubPlan 3 or hashed SubPlan 4)))))
          Rows Removed by Filter: 967
          SubPlan 1
            ->  Seq Scan on zq_zfba_xzcfjds x  (cost=0.00..14961.25 rows=1 width=0) (never executed)
                  Filter: ((ajxx_ajbh = zzwx.ajxx_join_ajxx_ajbh) AND (xzcfjds_rybh = zzwx.xyrxx_rybh))
          SubPlan 2
            ->  Seq Scan on zq_zfba_xzcfjds x_1  (cost=0.00..14811.17 rows=30017 width=48) (actual time=0.006..61.708 rows=29053 loops=1)
          SubPlan 3
            ->  Seq Scan on zq_zfba_byxzcfjds b  (cost=0.00..667.58 rows=1 width=0) (never executed)
                  Filter: ((ajxx_ajbh = zzwx.ajxx_join_ajxx_ajbh) AND (byxzcfjds_rybh = zzwx.xyrxx_rybh))
          SubPlan 4
            ->  Seq Scan on zq_zfba_byxzcfjds b_1  (cost=0.00..658.05 rows=1905 width=48) (actual time=0.016..3.218 rows=1905 loops=1)
  ->  Sort  (cost=36467.66..36467.66 rows=1 width=648) (actual time=7482.419..7482.616 rows=2256 loops=1)
        Sort Key: fd."案件编号", fd."人员编号", fd."案件类型", fd."案由", fd."地区", fd."办案单位", fd."立案时间", fd."姓名", fd."身份证号", fd."户籍地", fd."年龄", fd."居住地", (CASE WHEN ((fd."案件类型" = '行政'::text) AND (alternatives: SubPlan 6 or hashed SubPlan 7)) THEN '是'::text ELSE '否'::text END), (CASE WHEN ((fd."案件类型" = '行政'::text) AND (vc."违法次数" = 2) AND (vc."不同案由数" = 1) AND ((CASE WHEN (SubPlan 23) THEN 1 ELSE 0 END) = 1)) THEN '是'::text ELSE '否'::text END), (CASE WHEN ((fd."案件类型" = '行政'::text) AND (vc."违法次数" > 2)) THEN '是'::text ELSE '否'::text END), (CASE WHEN ((fd."案件类型" = '刑事'::text) AND (alternatives: SubPlan 8 or hashed SubPlan 9)) THEN '是'::text ELSE '否'::text END), (CASE WHEN ((alternatives: SubPlan 10 or hashed SubPlan 11) OR (alternatives: SubPlan 12 or hashed SubPlan 13)) THEN '是'::text ELSE '否'::text END), (CASE WHEN (alternatives: SubPlan 14 or hashed SubPlan 15) THEN '是'::text ELSE '否'::text END), (CASE WHEN (alternatives: SubPlan 16 or hashed SubPlan 17) THEN '是'::text ELSE '否'::text END), (CASE WHEN (((fd."年龄")::integer > 11) AND (((fd."案件类型" = '行政'::text) AND (alternatives: SubPlan 18 or hashed SubPlan 19)) OR ((fd."案件类型" = '行政'::text) AND (vc."违法次数" = 2) AND (vc."不同案由数" = 1) AND ((CASE WHEN (SubPlan 23) THEN 1 ELSE 0 END) = 1)) OR ((fd."案件类型" = '行政'::text) AND (vc."违法次数" > 2)) OR ((fd."案件类型" = '刑事'::text) AND (alternatives: SubPlan 20 or hashed SubPlan 21)))) THEN '是'::text ELSE '否'::text END), (CASE WHEN (SubPlan 22) THEN '是'::text ELSE '否'::text END)
        Sort Method: quicksort  Memory: 1259kB
        ->  Nested Loop Left Join  (cost=1107.25..36467.65 rows=1 width=648) (actual time=2383.584..7471.331 rows=2256 loops=1)
              Join Filter: ((fd."案件编号" = fd_1."案件编号") AND (fd_1."身份证号" = fd."身份证号"))
              Rows Removed by Join Filter: 5085024
              ->  Merge Left Join  (cost=753.54..753.57 rows=1 width=376) (actual time=124.412..128.491 rows=2256 loops=1)
                    Merge Cond: (fd."身份证号" = vc."身份证号")
                    ->  Sort  (cost=0.03..0.04 rows=1 width=360) (actual time=100.158..100.752 rows=2256 loops=1)
                          Sort Key: fd."身份证号"
                          Sort Method: quicksort  Memory: 970kB
                          ->  CTE Scan on filtered_data fd  (cost=0.00..0.02 rows=1 width=360) (actual time=76.656..92.213 rows=2256 loops=1)
                    ->  Sort  (cost=753.51..753.52 rows=1 width=34) (actual time=24.242..24.433 rows=2536 loops=1)
                          Sort Key: vc."身份证号"
                          Sort Method: quicksort  Memory: 295kB
                          ->  Subquery Scan on vc  (cost=753.47..753.50 rows=1 width=34) (actual time=16.673..22.775 rows=2537 loops=1)
                                ->  GroupAggregate  (cost=753.47..753.49 rows=1 width=66) (actual time=16.672..22.390 rows=2537 loops=1)
                                      Group Key: zq_zfba_wcnr_xyr.xyrxx_sfzh
                                      ->  Sort  (cost=753.47..753.48 rows=1 width=27) (actual time=16.640..16.900 rows=3220 loops=1)
                                            Sort Key: zq_zfba_wcnr_xyr.xyrxx_sfzh
                                            Sort Method: quicksort  Memory: 348kB
                                            ->  Seq Scan on zq_zfba_wcnr_xyr  (cost=0.00..753.46 rows=1 width=27) (actual time=0.017..5.615 rows=3220 loops=1)
                                                  Filter: (((xyrxx_isdel_dm)::integer = 0) AND ((ajxx_join_ajxx_isdel_dm)::integer = 0))
                                                  Rows Removed by Filter: 3
              ->  Unique  (cost=353.70..353.72 rows=1 width=68) (actual time=0.931..2.195 rows=2255 loops=2256)
                    ->  Sort  (cost=353.70..353.71 rows=1 width=68) (actual time=0.931..1.083 rows=2256 loops=2256)
                          Sort Key: fd_1."身份证号", fd_1."案件编号", (CASE WHEN (SubPlan 23) THEN 1 ELSE 0 END)
                          Sort Method: quicksort  Memory: 273kB
                          ->  CTE Scan on filtered_data fd_1  (cost=0.00..353.69 rows=1 width=68) (actual time=1.022..2092.505 rows=2256 loops=1)
                                SubPlan 23
                                  ->  Nested Loop  (cost=0.28..353.68 rows=1 width=0) (actual time=0.927..0.927 rows=0 loops=2256)
                                        Join Filter: ((w.ajxx_join_ajxx_ajbh = x_8.ajbh) AND (w.xyrxx_xm = x_8.xgry_xm))
                                        Rows Removed by Join Filter: 241
                                        ->  Index Scan using zq_zfba_wcnr_xyr_pkey on zq_zfba_wcnr_xyr w  (cost=0.28..248.48 rows=1 width=33) (actual time=0.638..0.757 rows=1 loops=2256)
                                              Index Cond: (xyrxx_sfzh = fd_1."身份证号")
                                              Filter: ((ajxx_join_ajxx_ajbh <> fd_1."案件编号") AND ((xyrxx_isdel_dm)::integer = 0) AND ((ajxx_join_ajxx_isdel_dm)::integer = 0))
                                              Rows Removed by Filter: 1
                                        ->  Seq Scan on zq_zfba_xjs2 x_8  (cost=0.00..99.08 rows=408 width=33) (actual time=0.004..0.058 rows=392 loops=1391)
              SubPlan 6
                ->  Seq Scan on zq_zfba_xzcfjds x_2  (cost=0.00..15111.34 rows=1 width=0) (never executed)
                      Filter: ((ajxx_ajbh = fd."案件编号") AND (xzcfjds_rybh = fd."人员编号") AND ((xzcfjds_tj_jlts)::integer > 4))
              SubPlan 7
                ->  Seq Scan on zq_zfba_xzcfjds x_3  (cost=0.00..14961.25 rows=10006 width=48) (actual time=0.017..73.340 rows=11157 loops=1)
                      Filter: ((xzcfjds_tj_jlts)::integer > 4)
                      Rows Removed by Filter: 17896
              SubPlan 8
                ->  Seq Scan on zq_zfba_jlz j  (cost=0.00..2397.12 rows=1 width=0) (never executed)
                      Filter: ((ajxx_ajbh = fd."案件编号") AND (jlz_rybh = fd."人员编号"))
              SubPlan 9
                ->  Seq Scan on zq_zfba_jlz j_1  (cost=0.00..2362.75 rows=6875 width=48) (actual time=0.010..11.104 rows=6875 loops=1)
              SubPlan 10
                ->  Seq Scan on zq_zfba_zlwcnrzstdxwgftzs z  (cost=0.00..13.54 rows=1 width=0) (never executed)
                      Filter: ((zltzs_ajbh = fd."案件编号") AND (zltzs_rybh = fd."人员编号"))
              SubPlan 11
                ->  Seq Scan on zq_zfba_zlwcnrzstdxwgftzs z_1  (cost=0.00..13.36 rows=36 width=48) (actual time=0.019..0.084 rows=36 loops=1)
              SubPlan 12
                ->  Seq Scan on zq_zfba_xjs2 x_4  (cost=0.00..101.12 rows=1 width=0) (never executed)
                      Filter: ((ajbh = fd."案件编号") AND (xgry_xm = fd."姓名"))
              SubPlan 13
                ->  Seq Scan on zq_zfba_xjs2 x_5  (cost=0.00..99.08 rows=408 width=33) (actual time=0.031..0.532 rows=408 loops=1)
              SubPlan 14
                ->  Seq Scan on zq_zfba_jtjyzdtzs j_2  (cost=0.00..143.90 rows=1 width=0) (never executed)
                      Filter: ((jqjhjyzljsjtjyzdtzs_ajbh = fd."案件编号") AND (jqjhjyzljsjtjyzdtzs_rybh = fd."人员编号"))
              SubPlan 15
                ->  Seq Scan on zq_zfba_jtjyzdtzs j_3  (cost=0.00..141.93 rows=393 width=48) (actual time=0.024..0.638 rows=393 loops=1)
              SubPlan 16
                ->  Seq Scan on zq_zfba_tqzmjy t  (cost=0.00..38.64 rows=1 width=0) (never executed)
                      Filter: ((ajbh = fd."案件编号") AND (xgry_xm = fd."姓名"))
              SubPlan 17
                ->  Seq Scan on zq_zfba_tqzmjy t_1  (cost=0.00..36.76 rows=376 width=33) (actual time=0.014..0.297 rows=376 loops=1)
              SubPlan 18
                ->  Seq Scan on zq_zfba_xzcfjds x_6  (cost=0.00..15111.34 rows=1 width=0) (never executed)
                      Filter: ((ajxx_ajbh = fd."案件编号") AND (xzcfjds_rybh = fd."人员编号") AND ((xzcfjds_tj_jlts)::integer > 4))
              SubPlan 19
                ->  Seq Scan on zq_zfba_xzcfjds x_7  (cost=0.00..14961.25 rows=10006 width=48) (actual time=0.009..74.012 rows=11157 loops=1)
                      Filter: ((xzcfjds_tj_jlts)::integer > 4)
                      Rows Removed by Filter: 17896
              SubPlan 20
                ->  Seq Scan on zq_zfba_jlz j_4  (cost=0.00..2397.12 rows=1 width=0) (never executed)
                      Filter: ((ajxx_ajbh = fd."案件编号") AND (jlz_rybh = fd."人员编号"))
              SubPlan 21
                ->  Seq Scan on zq_zfba_jlz j_5  (cost=0.00..2362.75 rows=6875 width=48) (actual time=0.020..11.048 rows=6875 loops=1)
              SubPlan 22
                ->  Seq Scan on zq_wcnr_sfzxx s  (cost=0.00..46.16 rows=1 width=0) (actual time=0.206..0.206 rows=0 loops=2256)
                      Filter: (((rx_time)::timestamp without time zone > fd."立案时间") AND ((sfzhm)::text = fd."身份证号"))
                      Rows Removed by Filter: 564
Planning Time: 5.581 ms
Execution Time: 7487.838 ms

# 任务:根据我以下需求生成开发清单,如有疑问则向我提出,如有更好意见则向我提出
## 在yfjcgkzx\hqzcsj模块新增一个tab页,名为"未成年人1393指标"
### 查询区域:
    - 时间范围控件:格式为'YYYY-MM-DD HH:MM:SS',"开始时间"默认为当天向前减7天,"结束时间"默认为当天,时间格式均为'00:00:00',如今天是'2026-01-27',那"开始时间"为'2026-01-20 00:00:00',"结束时间"为'2026-01-27 00:00:00'
    - "类型",多选下拉框,通过```SELECT "leixing"FROM "ywdata"."case_type_config";```获取,逻辑为通过wcn."xyrxx_ay_mc"字段进行模糊匹配
    - "查询",点击查询通过时间范围和类型过滤数据
    - "导出":单击按钮,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,数据为"数据展示区"显示的数据,文件名为"全市矫治教育统计"+{时间戳}.csv/xlsx
    - "导出详情":单击按钮,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,数据源为我提供的数据源的详细数据,文件名为"全市矫治教育详情"+{时间戳}.csv/xlsx
## 数据展示区:数据均为数字,通过对各数据源"地区"列分组得到,且数字可点击,点击后弹出新页面显示详细数据,且详细数据可下载
    - 第一列为"地区":通过对数据源"地区"字段分组计数得到
    - 第二列为"违法犯罪未成年人":通过对数据源"违法犯罪未成年人"分组计数得到,最后一行是不分组计数的数据,即全部数据
    - 第三列为"专门教育学生结业后犯罪率":通过对数据源"专门教育学生结业后犯罪率"的"地区"字段分组计数得到,最后一行是不分组计数的数据,即全部数据
    - 第四列为"专门教育学生结业后违法犯罪率":通过对数据源"专门教育学生结业后违法犯罪率"的"地区"字段分组计数得到,最后一行是不分组计数的数据,即全部数据
    - 第五列为"未成年人场所被侵害发案数":通过对数据源"未成年人场所被侵害发案数"的"地区"字段分组计数得到,,最后一行是不分组计数的数据,即全部数据
	- 第六列为"未成年人被侵害发案数":不使用xunfang\5lei_dizhi_model模型对"发案地点"进行分类的原始数据,通过对数据源"未成年人场所被侵害发案数"的"地区"字段分组计数得到,最后一行是不分组计数的数据,即全部数据
    - 第七列为"严重不良未成年人矫治教育覆盖率":通过对数据源"严重不良未成年人矫治教育覆盖率"的"地区"字段分组计数得到,最后一行是不分组计数的数据,即全部数据
    - 第八列为"适用专门（矫治）教育情形送矫率":通过对数据源"适用专门（矫治）教育情形送矫率"的"地区"字段分组计数得到,最后一行是不分组计数的数据,即全部数据
    - 最后一行是不分组的数据源计数
## 数据源
### 违法犯罪未成年人:其中"地区"通过地区字段分组,时间范围通过zzwx."xyrxx_lrsj"字段过滤,"类型"字段通过ctc."leixing"字段过滤,当用户不选择任何类型时:删除```AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (   SELECT ctc."ay_pattern" FROM "case_type_config" ctc  WHERE ctc."leixing" = '打架斗殴' )```条件
```
	WITH base_data AS (
		-- 基础案件-人员数据
		SELECT 
			zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
			zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
			zzwx."ajxx_join_ajxx_ay" AS 案由,
			zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
			LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
			zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
			zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
			zzwx.xyrxx_xm AS 姓名,
			zzwx."xyrxx_sfzh" AS 身份证号,
			zzwx."xyrxx_rybh" AS 人员编号,
			zzwx."xyrxx_hjdxz" AS 户籍地,
			zzwx."xyrxx_nl" AS 年龄,
			zzwx."xyrxx_isdel" AS 是否删除,
			zzwx."xyrxx_jzdxzqh" AS 居住地
		FROM ywdata."zq_zfba_wcnr_xyr" zzwx
		WHERE zzwx."ajxx_join_ajxx_isdel_dm" = 0 
			AND zzwx."xyrxx_isdel_dm" = 0 
			AND zzwx."xyrxx_sfda_dm" = 1
			AND zzwx."xyrxx_lrsj" BETWEEN '2026-01-01' AND '2026-02-06'
			AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (
				SELECT ctc."ay_pattern" 
				FROM "case_type_config" ctc 
				WHERE ctc."leixing" = '打架斗殴'
			)
	)
	--filtered_data AS (
		-- 根据案件类型匹配相应的文书表
		SELECT bd.*
		FROM base_data bd
		WHERE 
			-- 行政案件:匹配行政处罚决定书或不予行政处罚决定书
			(bd.案件类型 = '行政' AND (
				EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
					WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
				)
				OR EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_byxzcfjds" b
					WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
				)
			))
			-- 刑事案件:匹配拘留证
			OR bd.案件类型 = '刑事' 
```
### 专门教育学生结业后犯罪率:其中"地区"通过hjdq字段分组,时间范围通过zws."lx_time"字段过滤,"类型"字段通过ctc."leixing"字段过滤,当用户不选择任何类型时:删除``` AND zzx.案由 SIMILAR  TO ( SELECT ctc."ay_pattern"   FROM "case_type_config" ctc  WHERE ctc."leixing" = '打架斗殴')```过滤条件
```
	WITH base_data AS (
		-- 基础案件-人员数据
		SELECT 
			zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
			zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
			zzwx."ajxx_join_ajxx_ay" AS 案由,
			zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
			LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
			zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
			zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
			zzwx.xyrxx_xm AS 姓名,
			zzwx."xyrxx_sfzh" AS 身份证号,
			zzwx."xyrxx_rybh" AS 人员编号,
			zzwx."xyrxx_hjdxz" AS 户籍地,
			zzwx."xyrxx_nl" AS 年龄,
			zzwx."xyrxx_isdel" AS 是否删除,
			zzwx."xyrxx_jzdxzqh" AS 居住地
		FROM ywdata."zq_zfba_xyrxx" zzwx
		WHERE zzwx."ajxx_join_ajxx_isdel_dm" = 0 
			AND zzwx."xyrxx_isdel_dm" = 0 
			AND zzwx."xyrxx_sfda_dm" = 1
	),
	filtered_data AS (
		-- 根据案件类型匹配相应的文书表
		SELECT bd.*
		FROM base_data bd
		WHERE 
			-- 行政案件:匹配行政处罚决定书或不予行政处罚决定书
			(bd.案件类型 = '行政' AND (
				EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
					WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
				)
				OR EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_byxzcfjds" b
					WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
				)
			))
			-- 刑事案件:匹配拘留证
			OR bd.案件类型 = '刑事' 
	)
	SELECT zws."xm" ,zws."xm" ,zws."xb" ,zws."sfzhm" ,zws."hjdq" ,zws."hjdz" ,zws."nj" ,zws."rx_time" ,zws."lx_time",zws."lxdh" ,zws."yxx" 
	FROM "zq_wcnr_sfzxx" zws LEFT JOIN filtered_data zzx ON zws."sfzhm" =zzx.身份证号
	WHERE zzx.案件类型 ='刑事' AND zws."lx_time" BETWEEN '2026-01-01' AND '2026-02-08'  AND zws."lx_time" <zzx.立案时间 AND zzx.案由
	SIMILAR  TO (            SELECT ctc."ay_pattern"   FROM "case_type_config" ctc  WHERE ctc."leixing" = '打架斗殴')
```
### 专门教育学生结业后违法犯罪率:其中"地区"通过hjdq字段分组,时间范围通过zws."lx_time"字段过滤,"类型"字段通过ctc."leixing"字段过滤,当用户不选择任何类型时:删除``` AND zzx.案由 SIMILAR  TO (SELECT ctc."ay_pattern"   FROM "case_type_config" ctc  WHERE ctc."leixing" = '打架斗殴')```
	```	WITH base_data AS (
			-- 基础案件-人员数据
			SELECT 
				zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
				zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
				zzwx."ajxx_join_ajxx_ay" AS 案由,
				zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
				LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
				zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
				zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
				zzwx.xyrxx_xm AS 姓名,
				zzwx."xyrxx_sfzh" AS 身份证号,
				zzwx."xyrxx_rybh" AS 人员编号,
				zzwx."xyrxx_hjdxz" AS 户籍地,
				zzwx."xyrxx_nl" AS 年龄,
				zzwx."xyrxx_isdel" AS 是否删除,
				zzwx."xyrxx_jzdxzqh" AS 居住地
			FROM ywdata."zq_zfba_xyrxx" zzwx
			WHERE zzwx."ajxx_join_ajxx_isdel_dm" = 0 
				AND zzwx."xyrxx_isdel_dm" = 0 
				AND zzwx."xyrxx_sfda_dm" = 1
		),
		filtered_data AS (
			-- 根据案件类型匹配相应的文书表
			SELECT bd.*
			FROM base_data bd
			WHERE 
				-- 行政案件:匹配行政处罚决定书或不予行政处罚决定书
				(bd.案件类型 = '行政' AND (
					EXISTS (
						SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
						WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
					)
					OR EXISTS (
						SELECT 1 FROM ywdata."zq_zfba_byxzcfjds" b
						WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
					)
				))
				-- 刑事案件:匹配拘留证
				OR bd.案件类型 = '刑事' 
		)
		SELECT zws."xm" ,zws."xm" ,zws."xb" ,zws."sfzhm" ,zws."hjdq" ,zws."hjdz" ,zws."nj" ,zws."rx_time" ,zws."lx_time",zws."lxdh" ,zws."yxx" 
		FROM "zq_wcnr_sfzxx" zws LEFT JOIN filtered_data zzx ON zws."sfzhm" =zzx.身份证号
		WHERE  zws."lx_time" BETWEEN '2026-01-01' AND '2026-02-08'  AND zws."lx_time" <zzx.立案时间 AND zzx.案由
		SIMILAR  TO (SELECT ctc."ay_pattern"   FROM "case_type_config" ctc  WHERE ctc."leixing" = '打架斗殴')
	```
### 未成年人场所被侵害发案数:其中"地区"通过地区字段分组,时间范围通过zzws."ajxx_lasj"字段过滤,"类型"字段通过ctc."leixing"字段过滤,当用户不选择任何类型时:删除``` zzws."ajxx_aymc" SIMILAR  TO ( SELECT ctc."ay_pattern"   FROM "case_type_config" ctc  WHERE ctc."leixing" = '打架斗殴')```这个条件
#### 二次过滤:使用xunfang\5lei_dizhi_model模型对未成年人场所被侵害发案数SQL结果集的"发案地点"字段进行分类,分类结果中值为'重点管控行业'的数据即为最终的未成年人场所被侵害发案数
	SELECT  zzws."ajxx_ajbh" 案件编号,zzws."ajxx_ajlx" 案件类型,zzws."ajxx_ajmc"案件名称 ,LEFT(zzws."ajxx_cbdw_bh_dm" ,6) 地区,zzws."ajxx_cbdw_mc" 办案单位,zzws."ajxx_jyaq"简要案情 ,zzws."ajxx_lasj"立案时间 ,
	zzws."ajxx_fadd" 发案地点,zzws."ajxx_ajzt"案件状态 ,zzws."ajxx_fasj" 发案时间 FROM "zq_zfba_wcnr_shr_ajxx" zzws WHERE zzws."ajxx_lasj" BETWEEN  '2026-01-01' AND '2026-02-08' AND zzws."ajxx_aymc" SIMILAR  TO ( SELECT ctc."ay_pattern"   FROM "case_type_config" ctc  WHERE ctc."leixing" = '打架斗殴')
	```
### 严重不良未成年人矫治教育覆盖率:其中"地区"通过地区字段分组,时间范围通过bzr."lgsj"字段过滤,不需要"类型"过滤
#### 二次过滤:其中"是否开具矫治文书"字段值为'是'的计数值除以所有数据计数值即为严重不良未成年人矫治教育覆盖率
	```
 	SELECT  CASE WHEN xjs."id" IS NOT NULL THEN '是' ELSE '否' END AS 是否开具矫治文书, bzr."xm" 姓名,bzr."xb" 性别,bzr."zjhm" 证件号码,bzr."lxdh" 联系电话,LEFT(bzr."sspcs_dm",6)地区,bzr."sspcs_dm"管辖单位,
 	bzr."hjdz" 户籍地,bzr."jzdz" 居住地址 FROM "stdata"."b_zdry_ryxx" bzr  LEFT  JOIN zq_zfba_xjs2 xjs ON bzr."xm" =xjs."xgry_xm" 
	WHERE bzr."deleteflag" =0 AND bzr."sflg" =1 AND bzr."lgsj" BETWEEN  '2026-01-01' AND '2026-02-08'   
	```
### 专门（矫治）教育送矫率:其中"地区"通过地区字段分组,时间范围通过zzwx."xyrxx_lrsj"字段过滤,"类型"通过```AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (SELECT ctc."ay_pattern" FROM "case_type_config" ctc WHERE ctc."leixing" = '打架斗殴')```过滤,如果没选"类型"则不使用该条件
#### 二次过滤:其中"是否送校"字段值为'是'的计数值除以所有数据计数值即为专门（矫治）教育送矫率
	```
	WITH base_data AS (
		-- 基础案件-人员数据
		SELECT 
			zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
			zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
			zzwx."ajxx_join_ajxx_ay" AS 案由,
			zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
			LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
			zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
			zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
			zzwx.xyrxx_xm AS 姓名,
			zzwx."xyrxx_sfzh" AS 身份证号,
			zzwx."xyrxx_rybh" AS 人员编号,
			zzwx."xyrxx_hjdxz" AS 户籍地,
			zzwx."xyrxx_nl" AS 年龄,
			zzwx."xyrxx_isdel" AS 是否删除,
			zzwx."xyrxx_jzdxzqh" AS 居住地
		FROM ywdata."zq_zfba_wcnr_xyr" zzwx
		WHERE zzwx."ajxx_join_ajxx_isdel_dm" = 0 
			AND zzwx."xyrxx_isdel_dm" = 0 
			AND zzwx."xyrxx_sfda_dm" = 1
			AND zzwx."xyrxx_lrsj" BETWEEN '2026-01-01' AND '2026-02-06'
			AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (SELECT ctc."ay_pattern" FROM "case_type_config" ctc WHERE ctc."leixing" = '打架斗殴')
	),
	filtered_data AS (
		-- 根据案件类型匹配相应的文书表
		SELECT bd.*
		FROM base_data bd
		WHERE 
			-- 行政案件:匹配行政处罚决定书或不予行政处罚决定书
			(bd.案件类型 = '行政' AND (
				EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
					WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
				)
				OR EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_byxzcfjds" b
					WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
				)
			))
			-- 刑事案件:匹配拘留证
			OR bd.案件类型 = '刑事' 
	),
	violation_counts AS (
		-- 计算每个人的违法次数和案由
		SELECT 
			xyrxx_sfzh AS 身份证号,
			COUNT(*) AS 违法次数,
			COUNT(DISTINCT ajxx_join_ajxx_ay_dm) AS 不同案由数,
			MIN(ajxx_join_ajxx_ay_dm) AS 案由代码
		FROM ywdata."zq_zfba_wcnr_xyr"
		WHERE xyrxx_isdel_dm = 0 AND ajxx_join_ajxx_isdel_dm = 0
		GROUP BY xyrxx_sfzh
	),
	first_case_xjs AS (
		-- 查找第一次违法是否开具了训诫书
		SELECT DISTINCT
			fd.身份证号,
			fd.案件编号 AS 当前案件编号,
			CASE 
				WHEN EXISTS (
					SELECT 1 
					FROM ywdata."zq_zfba_wcnr_xyr" w
					JOIN ywdata."zq_zfba_xjs2" x 
						ON w."ajxx_join_ajxx_ajbh" = x.ajbh 
						AND w.xyrxx_xm = x.xgry_xm
					WHERE w.xyrxx_sfzh = fd.身份证号
						AND w."ajxx_join_ajxx_ajbh" != fd.案件编号
						AND w.xyrxx_isdel_dm = 0
						AND w.ajxx_join_ajxx_isdel_dm = 0
				) THEN 1 
				ELSE 0 
			END AS 有训诫书
		FROM filtered_data fd
	)
	SELECT DISTINCT
		fd.案件编号,
		fd.人员编号,
		fd.案件类型,
		fd.案由,
		fd.地区,
		fd.办案单位,
		fd.立案时间,
		fd.姓名,
		fd.身份证号,
		fd.户籍地,
		fd.年龄,
		fd.居住地,
		
		-- 治拘大于4天(仅行政案件)
		CASE 
			WHEN fd.案件类型 = '行政' AND EXISTS (
				SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
				WHERE x.ajxx_ajbh = fd.案件编号 
					AND x.xzcfjds_rybh = fd.人员编号
					AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
			) THEN '是'
			ELSE '否'
		END AS 治拘大于4天,
		
		-- 2次违法且案由相同且第一次违法开具了训诫书(仅行政案件)
		CASE 
			WHEN fd.案件类型 = '行政' 
				AND vc.违法次数 = 2 
				AND vc.不同案由数 = 1
				AND fcx.有训诫书 = 1
			THEN '是'
			ELSE '否'
		END AS "2次违法且案由相同且第一次违法开具了训诫书",
		
		-- 3次及以上违法(仅行政案件)
		CASE 
			WHEN fd.案件类型 = '行政' AND vc.违法次数 > 2 THEN '是'
			ELSE '否'
		END AS "3次及以上违法",
		
		-- 是否刑拘(仅刑事案件)
		CASE 
			WHEN fd.案件类型 = '刑事' AND EXISTS (
				SELECT 1 FROM ywdata."zq_zfba_jlz" j
				WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
			) THEN '是'
			ELSE '否'
		END AS 是否刑拘,
		
		-- 是否开具矫治文书
		CASE 
			WHEN EXISTS (
				SELECT 1 FROM ywdata."zq_zfba_zlwcnrzstdxwgftzs" z
				WHERE z.zltzs_ajbh = fd.案件编号 AND z.zltzs_rybh = fd.人员编号
			) OR EXISTS (
				SELECT 1 FROM ywdata."zq_zfba_xjs2" x
				WHERE x.ajbh = fd.案件编号 AND x.xgry_xm = fd.姓名
			) THEN '是'
			ELSE '否'
		END AS 是否开具矫治文书,
		
		-- 是否开具加强监督教育/责令接受家庭教育指导通知书
		CASE 
			WHEN EXISTS (
				SELECT 1 FROM ywdata."zq_zfba_jtjyzdtzs" j
				WHERE j.jqjhjyzljsjtjyzdtzs_ajbh = fd.案件编号 
					AND j.jqjhjyzljsjtjyzdtzs_rybh = fd.人员编号
			) THEN '是'
			ELSE '否'
		END AS "是否开具加强/责令接受家庭教育指导通知书",
		
		-- 是否开具提请专门教育申请书
		CASE 
			WHEN EXISTS (
				SELECT 1 FROM ywdata."zq_zfba_tqzmjy" t
				WHERE t.ajbh = fd.案件编号 AND t.xgry_xm = fd.姓名
			) THEN '是'
			ELSE '否'
		END AS 是否开具提请专门教育申请书,
		
		-- 是否符合送生
		CASE 
			WHEN CAST(fd.年龄 AS INTEGER) > 11 AND (
				-- 治拘大于4天
				(fd.案件类型 = '行政' AND EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
					WHERE x.ajxx_ajbh = fd.案件编号 
						AND x.xzcfjds_rybh = fd.人员编号
						AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
				))
				-- 2次违法且案由相同且第一次违法开具了训诫书
				OR (fd.案件类型 = '行政' AND vc.违法次数 = 2 AND vc.不同案由数 = 1 AND fcx.有训诫书 = 1)
				-- 3次及以上违法
				OR (fd.案件类型 = '行政' AND vc.违法次数 > 2)
				-- 是否刑拘
				OR (fd.案件类型 = '刑事' AND EXISTS (
					SELECT 1 FROM ywdata."zq_zfba_jlz" j
					WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
				))
			) THEN '是'
			ELSE '否'
		END AS 是否符合送生,
		
		-- 是否送校
		CASE 
			WHEN EXISTS (
				SELECT 1 FROM ywdata."zq_wcnr_sfzxx" s
				WHERE s.sfzhm = fd.身份证号 
					AND s.rx_time > fd.立案时间
			) THEN '是'
			ELSE '否'
		END AS 是否送校

	FROM filtered_data fd
	LEFT JOIN violation_counts vc ON fd.身份证号 = vc.身份证号
	LEFT JOIN first_case_xjs fcx ON fd.身份证号 = fcx.身份证号 AND fd.案件编号 = fcx.当前案件编号

	ORDER BY fd.案件编号, fd.人员编号;
	```


## 修改yfjcgkzx\hqzcsj模块的"矫治教育统计"页面逻辑
### 数据源修改为下方sql,其中zzwx."xyrxx_lrsj"和"ajxx_join_ajxx_ay"是变量,如果用户在前端页面没选"类型"则不需要```AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (
            SELECT ctc."ay_pattern" 
            FROM "case_type_config" ctc 
            WHERE ctc."leixing" = '打架斗殴'
        )```查询条件
```sql
WITH base_data AS (
    -- 基础案件-人员数据
    SELECT 
        zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
        zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
        zzwx."ajxx_join_ajxx_ay" AS 案由,
        zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
        LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
        zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
        zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
        zzwx.xyrxx_xm AS 姓名,
        zzwx."xyrxx_sfzh" AS 身份证号,
        zzwx."xyrxx_rybh" AS 人员编号,
        zzwx."xyrxx_hjdxz" AS 户籍地,
        zzwx."xyrxx_nl" AS 年龄,
        zzwx."xyrxx_isdel" AS 是否删除,
        zzwx."xyrxx_jzdxzqh" AS 居住地
    FROM ywdata."zq_zfba_wcnr_xyr" zzwx
    WHERE zzwx."ajxx_join_ajxx_isdel_dm" = 0 
        AND zzwx."xyrxx_isdel_dm" = 0 
        AND zzwx."xyrxx_sfda_dm" = 1
        AND zzwx."xyrxx_lrsj" BETWEEN '2026-01-01' AND '2026-02-06'
        AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (
            SELECT ctc."ay_pattern" 
            FROM "case_type_config" ctc 
            WHERE ctc."leixing" = '打架斗殴'
        )
),
filtered_data AS (
    -- 根据案件类型匹配相应的文书表
    SELECT bd.*
    FROM base_data bd
    WHERE 
        -- 行政案件:匹配行政处罚决定书或不予行政处罚决定书
        (bd.案件类型 = '行政' AND (
            EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
                WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
            )
            OR EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_byxzcfjds" b
                WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
            )
        ))
        -- 刑事案件:匹配拘留证
        OR (bd.案件类型 = '刑事' AND EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_jlz" j
            WHERE j.ajxx_ajbh = bd.案件编号 AND j.jlz_rybh = bd.人员编号
        ))
),
violation_counts AS (
    -- 计算每个人的违法次数和案由
    SELECT 
        xyrxx_sfzh AS 身份证号,
        COUNT(*) AS 违法次数,
        COUNT(DISTINCT ajxx_join_ajxx_ay_dm) AS 不同案由数,
        MIN(ajxx_join_ajxx_ay_dm) AS 案由代码
    FROM ywdata."zq_zfba_wcnr_xyr"
    WHERE xyrxx_isdel_dm = 0 AND ajxx_join_ajxx_isdel_dm = 0
    GROUP BY xyrxx_sfzh
),
first_case_xjs AS (
    -- 查找第一次违法是否开具了训诫书
    SELECT DISTINCT
        fd.身份证号,
        fd.案件编号 AS 当前案件编号,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM ywdata."zq_zfba_wcnr_xyr" w
                JOIN ywdata."zq_zfba_xjs2" x 
                    ON w."ajxx_join_ajxx_ajbh" = x.ajbh 
                    AND w.xyrxx_xm = x.xgry_xm
                WHERE w.xyrxx_sfzh = fd.身份证号
                    AND w."ajxx_join_ajxx_ajbh" != fd.案件编号
                    AND w.xyrxx_isdel_dm = 0
                    AND w.ajxx_join_ajxx_isdel_dm = 0
            ) THEN 1 
            ELSE 0 
        END AS 有训诫书
    FROM filtered_data fd
)
SELECT DISTINCT
    fd.案件编号,
    fd.人员编号,
    fd.案件类型,
    fd.案由,
    fd.地区,
    fd.办案单位,
    fd.立案时间,
    fd.姓名,
    fd.身份证号,
    fd.户籍地,
    fd.年龄,
    fd.居住地,
    
    -- 治拘大于4天(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' AND EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
            WHERE x.ajxx_ajbh = fd.案件编号 
                AND x.xzcfjds_rybh = fd.人员编号
                AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
        ) THEN '是'
        ELSE '否'
    END AS 治拘大于4天,
    
    -- 2次违法且案由相同且第一次违法开具了训诫书(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' 
            AND vc.违法次数 = 2 
            AND vc.不同案由数 = 1
            AND fcx.有训诫书 = 1
        THEN '是'
        ELSE '否'
    END AS "2次违法且案由相同且第一次违法开具了训诫书",
    
    -- 3次及以上违法(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' AND vc.违法次数 > 2 THEN '是'
        ELSE '否'
    END AS "3次及以上违法",
    
    -- 是否刑拘(仅刑事案件)
    CASE 
        WHEN fd.案件类型 = '刑事' AND EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_jlz" j
            WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
        ) THEN '是'
        ELSE '否'
    END AS 是否刑拘,
    
    -- 是否开具矫治文书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_zlwcnrzstdxwgftzs" z
            WHERE z.zltzs_ajbh = fd.案件编号 AND z.zltzs_rybh = fd.人员编号
        ) OR EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_xjs2" x
            WHERE x.ajbh = fd.案件编号 AND x.xgry_xm = fd.姓名
        ) THEN '是'
        ELSE '否'
    END AS 是否开具矫治文书,
    
    -- 是否开具加强监督教育/责令接受家庭教育指导通知书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_jtjyzdtzs" j
            WHERE j.jqjhjyzljsjtjyzdtzs_ajbh = fd.案件编号 
                AND j.jqjhjyzljsjtjyzdtzs_rybh = fd.人员编号
        ) THEN '是'
        ELSE '否'
    END AS "是否开具加强监督教育/责令接受家庭教育指导通知书",
    
    -- 是否开具提请专门教育申请书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_tqzmjy" t
            WHERE t.ajbh = fd.案件编号 AND t.xgry_xm = fd.姓名
        ) THEN '是'
        ELSE '否'
    END AS 是否开具提请专门教育申请书,
    
    -- 是否符合送生
    CASE 
        WHEN CAST(fd.年龄 AS INTEGER) > 11 AND (
            -- 治拘大于4天
            (fd.案件类型 = '行政' AND EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
                WHERE x.ajxx_ajbh = fd.案件编号 
                    AND x.xzcfjds_rybh = fd.人员编号
                    AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
            ))
            -- 2次违法且案由相同且第一次违法开具了训诫书
            OR (fd.案件类型 = '行政' AND vc.违法次数 = 2 AND vc.不同案由数 = 1 AND fcx.有训诫书 = 1)
            -- 3次及以上违法
            OR (fd.案件类型 = '行政' AND vc.违法次数 > 2)
            -- 是否刑拘
            OR (fd.案件类型 = '刑事' AND EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_jlz" j
                WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
            ))
        ) THEN '是'
        ELSE '否'
    END AS 是否符合送生,
    
    -- 是否送校
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_wcnr_sfzxx" s
            WHERE s.sfzhm = fd.身份证号 
                AND s.rx_time > fd.立案时间
        ) THEN '是'
        ELSE '否'
    END AS 是否送校

FROM filtered_data fd
LEFT JOIN violation_counts vc ON fd.身份证号 = vc.身份证号
LEFT JOIN first_case_xjs fcx ON fd.身份证号 = fcx.身份证号 AND fd.案件编号 = fcx.当前案件编号

ORDER BY fd.案件编号, fd.人员编号;
```
### 分组字段修改为"地区"分组:且按照{445302:云城,445303:云安,445381:罗定,445321:新兴,445322:郁南}映射
### 违法人数和犯罪人数的统计逻辑不变:仍按照"案件类型"统计,其中"案件类型":'行政'则为违法人数,"案件类型":'刑事'则为犯罪人数
### 符合送生（行政）和符合送生（刑事）的统计逻辑不变:仍按照"案件类型","是否符合送生"字段值统计,其中"案件类型":'行政'且"是否符合送生":'是'则为符合送生（行政）,"案件类型":'刑事'且"是否符合送生":'是'则为符合送生（刑事）
### 矫治教育文书开具数(行政)和矫治教育文书开具数(刑事):按照"案件类型","是否开具矫治文书"字段值统计,其中"案件类型":'行政'且"是否开具矫治文书":'是'则为矫治教育文书开具数(行政),"案件类型":'刑事'且"矫治教育文书开具数(刑事)":'是'则为矫治教育文书开具数(刑事)
### 提请专门教育申请书数(行政)和提请专门教育申请书数(刑事):按照"案件类型","是否开具提请专门教育申请书"字段值统计,其中"案件类型":'行政'且"是否开具提请专门教育申请书":'是'则为提请专门教育申请书数(行政),"案件类型":'刑事'且"是否开具提请专门教育申请书":'是'则为提请专门教育申请书数(刑事)
### 送生数（行政）和送生数（刑事）:按照"案件类型","是否送校"字段值统计,其中"案件类型":'行政'且"是否送校":'是'则为送生数（行政）,"案件类型":'刑事'且"是否送校":'是'则为送生数（刑事）
### 刑拘数:按照"案件类型","是否刑拘"字段值统计,其中"案件类型":'刑事'且"是否刑拘":'是'则为刑拘数

# 这一篇材料是为您准备的2月7日下午全市治安“保平安”专项行动每日调度会的通报讲话稿。

这份材料结合了您提供的CSV数据（截止2月6日15时）以及专项行动的目标要求，语气严厉且具体，旨在指出问题、压实责任。

2026年春节“保平安”专项行动治安每日调度会通报

时间： 2026年2月7日
主持： 治安支队支队长

同志们：

今天是“保平安”专项行动开展的第8天，距离除夕还有9天。根据市局统一部署，我们治安战线必须守住底线，要在2月15日前打赢这场硬仗。今天下午调度会，主要通报行动首周（1月31日至2月6日15时）的治安态势，重点对打架斗殴压降不力、积案清理滞后、黄赌打击短板进行点名剖析。

一、打架斗殴警情：总量虽控，但同比反弹，部分派出所管控存漏洞

（一）整体情况
行动以来（1月31日至2月6日15时），全市共接报打架斗殴警情14起。
从数据看，有两个特点：
一是环比下降，态势暂稳。较行动前一周环比下降26.32%，目前总数控制在36宗的目标红线以内（当前14宗）。
二是同比上升，隐患突出。较去年同期（11起）上升27.27%。在全警动员的情况下，警情不降反升，说明我们的防控措施在某些区域没有落地。

（二）地区分析与问题点名
目前压降形势较差的地区主要集中在B区（同比翻倍）、D县（净增2起）。结合具体警情内容，我重点点名以下问题：

重点部位巡防不到位（B分局）。
B分局镇安派出所，2月6日深夜23时，在辖区星悦酒吧门口发生多人殴打一人的恶性警情（警情标注“盯办”）。

问题所在： 既然是专项行动期间，为什么酒吧这种重点夜场周边的巡防警力没有形成震慑？这是典型的重点部位巡防管控不到位。B分局要立即倒查当晚勤务安排。

矛盾纠纷排查化解不实（C市局、E县局、D县局）。
行动以来，因纠纷引发的打架警情占比最高。

C市局：Shengjiang派出所（2月6日装修纠纷、2月2日路边被打）、Chuanbu派出所（2月6日未成年人因狗只纠纷）、Taiping派出所（2月4日醉酒土地纠纷）。C市警情总量7起，全市最多。反映出你们的矛盾纠纷排查流于形式，社区民警底数不清。

E县局：Dong'ba派出所（2月6日快递点经济纠纷）、Nanjiangkou派出所（2月1日劳资纠纷持刀）。

D县局：Rencun派出所（2月1日选票纠纷）、Tiantang派出所（2月1日工人打架）。

问题所在： 无论是装修、劳资还是选举纠纷，都不是突发的，前期必有苗头。这些警情暴露出基层所矛盾纠纷排查不足，情报信息收集滞后，导致“民转刑、刑转命”风险增加。

重点人员与酒后管控缺失（A分局）。
A分局Hekou派出所，2月1日发生一起宵夜档兄弟酒后持刀伤人案件。

问题所在： 凌晨2点的宵夜档，又是熟人酒后作案，反映出对重点时段、酗酒高危人员的管控存在盲区。

二、打架斗殴积案清理：最后冲刺，严禁拖延

我们要清理2025年以来的34宗积案，这是硬指标。绝大多数单位已经完成，但仍有尾巴：

D县局： 还有3宗未办结；

A分局： 还有2宗未办结。

A区、D县听好了： 距离春节只剩一周，请你们的分管局长亲自督办，务必在未来两天内全部清零，不要让积案跨年，更不要拖全市后腿。

三、现案打击：行政处罚力度加大，刑事打击需精准

（一）行政案件
行动以来，全市受理打架斗殴行政案件12起（同比上升20%），行政拘留28人（同比上升366%，去年同期仅6人）。
表扬： 全市治安拘留人数大幅上升，说明各地落实了“严打”方针，对打架斗殴行为形成了有力震慑。特别是C市局，治安拘留14人，同比增加13人，打击效能显著。

（二）刑事案件
行动以来，全市打架斗殴立刑事案件1起（A分局Hekou所），刑事拘留1人。
A分局虽然警情有反弹，但在个案处置上性质定得准，转刑及时，值得肯定。其他单位在接处警中，凡符合立案标准的，必须坚决立案，严禁降格处理。

四、黄赌打击效能：目标基本达成，紧盯移诉短板

关于黄赌打击，对照我们设定的“2月10日节点目标”，目前进展如下：

整体进度： 破案数、刑拘人数、入所人数、移送起诉人数这4项指标已提前超额完成。

存在短板： “移送起诉案件” 完成率仅为64.29%（目标15起，现完成9起）。

具体情况：

现案打击： 行动以来共立黄赌刑事案件5起，已破3起。目前仍有2起赌博案件未破（A区1起、D县1起）。A区、D县要集中精力，务必在2月10日前破案。

强力手段： 行动以来已刑拘8人（C市6人、A区1人、D县1人），同比上升明显。C市局在黄赌刑拘上贡献最大，占全市75%。

移诉攻坚： 行动以来移诉案件4起（D县3起、B区1起）。D县虽然有未破案件，但在移诉工作上走在前面。

五、工作要求

同志们，还有9天就是除夕。今天的通报暴露出的问题，各单位要对号入座：

B区要严防死守酒吧、夜市等重点部位，把警灯亮起来。

C市、E县要结合“百万警进千万家”，把矛盾纠纷排查到底，不要让邻里纠纷演变成血案。

A区、D县要死磕那几宗未破的积案和黄赌现案，必须见底清零。

今天的会就开到这里，散会后，被点名的派出所向属地分局治安大队提交整改报告。
# 我来帮你编写这个复杂的SQL查询。这个查询需要判断未成年人是否符合专门矫治教育的条件。

```sql
WITH base_data AS (
    -- 基础案件-人员数据
    SELECT 
        zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
        zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
        zzwx."ajxx_join_ajxx_ay" AS 案由,
        zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
        LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
        zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
        zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
        zzwx.xyrxx_xm AS 姓名,
        zzwx."xyrxx_sfzh" AS 身份证号,
        zzwx."xyrxx_rybh" AS 人员编号,
        zzwx."xyrxx_hjdxz" AS 户籍地,
        zzwx."xyrxx_nl" AS 年龄,
        zzwx."xyrxx_isdel" AS 是否删除,
        zzwx."xyrxx_jzdxzqh" AS 居住地
    FROM ywdata."zq_zfba_wcnr_xyr" zzwx
    WHERE zzwx."ajxx_join_ajxx_isdel_dm" = 0 
        AND zzwx."xyrxx_isdel_dm" = 0 
        AND zzwx."xyrxx_sfda_dm" = 1
        AND zzwx."xyrxx_lrsj" BETWEEN '2026-01-01' AND '2026-02-06'
        AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO (
            SELECT ctc."ay_pattern" 
            FROM "case_type_config" ctc 
            WHERE ctc."leixing" = '打架斗殴'
        )
),
filtered_data AS (
    -- 根据案件类型匹配相应的文书表
    SELECT bd.*
    FROM base_data bd
    WHERE 
        -- 行政案件:匹配行政处罚决定书或不予行政处罚决定书
        (bd.案件类型 = '行政' AND (
            EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
                WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
            )
            OR EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_byxzcfjds" b
                WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
            )
        ))
        -- 刑事案件:匹配拘留证
        OR (bd.案件类型 = '刑事' AND EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_jlz" j
            WHERE j.ajxx_ajbh = bd.案件编号 AND j.jlz_rybh = bd.人员编号
        ))
),
violation_counts AS (
    -- 计算每个人的违法次数和案由
    SELECT 
        xyrxx_sfzh AS 身份证号,
        COUNT(*) AS 违法次数,
        COUNT(DISTINCT ajxx_join_ajxx_ay_dm) AS 不同案由数,
        MIN(ajxx_join_ajxx_ay_dm) AS 案由代码
    FROM ywdata."zq_zfba_wcnr_xyr"
    WHERE xyrxx_isdel_dm = 0 AND ajxx_join_ajxx_isdel_dm = 0
    GROUP BY xyrxx_sfzh
),
first_case_xjs AS (
    -- 查找第一次违法是否开具了训诫书
    SELECT DISTINCT
        fd.身份证号,
        fd.案件编号 AS 当前案件编号,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM ywdata."zq_zfba_wcnr_xyr" w
                JOIN ywdata."zq_zfba_xjs2" x 
                    ON w."ajxx_join_ajxx_ajbh" = x.ajbh 
                    AND w.xyrxx_xm = x.xgry_xm
                WHERE w.xyrxx_sfzh = fd.身份证号
                    AND w."ajxx_join_ajxx_ajbh" != fd.案件编号
                    AND w.xyrxx_isdel_dm = 0
                    AND w.ajxx_join_ajxx_isdel_dm = 0
            ) THEN 1 
            ELSE 0 
        END AS 有训诫书
    FROM filtered_data fd
)
SELECT DISTINCT
    fd.案件编号,
    fd.人员编号,
    fd.案件类型,
    fd.案由,
    fd.地区,
    fd.办案单位,
    fd.立案时间,
    fd.姓名,
    fd.身份证号,
    fd.户籍地,
    fd.年龄,
    fd.居住地,
    
    -- 治拘大于4天(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' AND EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
            WHERE x.ajxx_ajbh = fd.案件编号 
                AND x.xzcfjds_rybh = fd.人员编号
                AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
        ) THEN '是'
        ELSE '否'
    END AS 治拘大于4天,
    
    -- 2次违法且案由相同且第一次违法开具了训诫书(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' 
            AND vc.违法次数 = 2 
            AND vc.不同案由数 = 1
            AND fcx.有训诫书 = 1
        THEN '是'
        ELSE '否'
    END AS "2次违法且案由相同且第一次违法开具了训诫书",
    
    -- 3次及以上违法(仅行政案件)
    CASE 
        WHEN fd.案件类型 = '行政' AND vc.违法次数 > 2 THEN '是'
        ELSE '否'
    END AS "3次及以上违法",
    
    -- 是否刑拘(仅刑事案件)
    CASE 
        WHEN fd.案件类型 = '刑事' AND EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_jlz" j
            WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
        ) THEN '是'
        ELSE '否'
    END AS 是否刑拘,
    
    -- 是否开具矫治文书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_zlwcnrzstdxwgftzs" z
            WHERE z.zltzs_ajbh = fd.案件编号 AND z.zltzs_rybh = fd.人员编号
        ) OR EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_xjs2" x
            WHERE x.ajbh = fd.案件编号 AND x.xgry_xm = fd.姓名
        ) THEN '是'
        ELSE '否'
    END AS 是否开具矫治文书,
    
    -- 是否开具加强监督教育/责令接受家庭教育指导通知书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_jtjyzdtzs" j
            WHERE j.jqjhjyzljsjtjyzdtzs_ajbh = fd.案件编号 
                AND j.jqjhjyzljsjtjyzdtzs_rybh = fd.人员编号
        ) THEN '是'
        ELSE '否'
    END AS "是否开具加强监督教育/责令接受家庭教育指导通知书",
    
    -- 是否开具提请专门教育申请书
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_zfba_tqzmjy" t
            WHERE t.ajbh = fd.案件编号 AND t.xgry_xm = fd.姓名
        ) THEN '是'
        ELSE '否'
    END AS 是否开具提请专门教育申请书,
    
    -- 是否符合送生
    CASE 
        WHEN CAST(fd.年龄 AS INTEGER) > 11 AND (
            -- 治拘大于4天
            (fd.案件类型 = '行政' AND EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_xzcfjds" x
                WHERE x.ajxx_ajbh = fd.案件编号 
                    AND x.xzcfjds_rybh = fd.人员编号
                    AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
            ))
            -- 2次违法且案由相同且第一次违法开具了训诫书
            OR (fd.案件类型 = '行政' AND vc.违法次数 = 2 AND vc.不同案由数 = 1 AND fcx.有训诫书 = 1)
            -- 3次及以上违法
            OR (fd.案件类型 = '行政' AND vc.违法次数 > 2)
            -- 是否刑拘
            OR (fd.案件类型 = '刑事' AND EXISTS (
                SELECT 1 FROM ywdata."zq_zfba_jlz" j
                WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
            ))
        ) THEN '是'
        ELSE '否'
    END AS 是否符合送生,
    
    -- 是否送校
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM ywdata."zq_wcnr_sfzxx" s
            WHERE s.sfzhm = fd.身份证号 
                AND s.rx_time > fd.立案时间
        ) THEN '是'
        ELSE '否'
    END AS 是否送校

FROM filtered_data fd
LEFT JOIN violation_counts vc ON fd.身份证号 = vc.身份证号
LEFT JOIN first_case_xjs fcx ON fd.身份证号 = fcx.身份证号 AND fd.案件编号 = fcx.当前案件编号

ORDER BY fd.案件编号, fd.人员编号;
```

这个SQL查询的主要逻辑:

1. **base_data CTE**: 获取基础案件-人员数据
2. **filtered_data CTE**: 根据案件类型(行政/刑事)匹配相应文书表进行过滤
3. **violation_counts CTE**: 计算每个人的违法次数
4. **first_case_xjs CTE**: 判断第一次违法是否开具训诫书
5. **主查询**: 计算所有派生字段并按案件编号和人员编号去重

关键特点:
- 使用DISTINCT确保按案件编号和人员编号去重
- 根据案件类型(行政/刑事)分别处理不同条件
- 所有字段均以"是"/"否"标识
- 正确处理年龄判断和日期比较
===
# 任务:帮我写一段SQL,主要是从不同条件判断违法犯罪的未成年人是否符合专门矫治教育(送到专门教育学校)
## 要求:
    - 最后结果集要以"案件编号"和"人员编号"去重
## 基础数据
### 违法犯罪未成年:由基础"案件-人员"作为底数据,使用案件编号和人员编号与"ywdata"."zq_zfba_xzcfjds","ywdata"."zq_zfba_byxzcfjds","ywdata"."zq_zfba_jlz"三个表匹配过滤
    - "基础"案件-人员"数据:其中zzwx."xyrxx_lrsj"是变量,zzwx."ajxx_join_ajxx_ay"中的'打架斗殴'也是变量
        ```
SELECT zzwx."ajxx_join_ajxx_ajbh"案件编号 ,zzwx."ajxx_join_ajxx_ajlx" 案件类型,zzwx."ajxx_join_ajxx_ay" 案由,LEFT (zzwx."ajxx_join_ajxx_cbqy_bh_dm" ,6)地区,zzwx."ajxx_join_ajxx_cbdw_bh"办案单位 ,
zzwx."ajxx_join_ajxx_lasj" 立案时间 ,zzwx.xyrxx_xm 姓名,zzwx."xyrxx_sfzh"身份证号 ,zzwx."xyrxx_rybh" 人员编号,zzwx."xyrxx_hjdxz"户籍地 ,zzwx."xyrxx_nl" 年龄,zzwx."xyrxx_isdel" 是否删除,zzwx."xyrxx_jzdxzqh" 居住地
 FROM ywdata."zq_zfba_wcnr_xyr" zzwx WHERE zzwx."ajxx_join_ajxx_isdel_dm" =0 AND zzwx."xyrxx_isdel_dm" =0 AND zzwx."xyrxx_sfda_dm" =1
AND zzwx."xyrxx_lrsj" BETWEEN '2026-01-01' AND '2026-02-06'
AND zzwx."ajxx_join_ajxx_ay" SIMILAR  TO (SELECT ctc."ay_pattern" FROM "case_type_config" ctc WHERE ctc."leixing" ='打架斗殴')
        ```
        - 如果zzwx."ajxx_join_ajxx_ajlx"的值为'行政'
            - "基础"案件-人员"数据的案件编号和人员编号要与"ywdata"."zq_zfba_xzcfjds"(行政处罚决定书)表的ajxx_ajbh和xzcfjds_rybh匹配,如果匹配到说明有违法事实,则保留,没有匹配到则删除
            - "基础"案件-人员"数据的案件编号和人员编号要与"ywdata"."zq_zfba_byxzcfjds"(不予行政处罚决定书)表的ajxx_ajbh和byxzcfjds_rybh匹配,如果匹配到说明有违法事实,则保留,没有匹配到则删除
        - 如果zzwx."ajxx_join_ajxx_ajlx"的值为'刑事'
            - "基础"案件-人员"数据的案件编号和人员编号要与 "ywdata"."zq_zfba_jlz"(拘留证)表的ajxx_ajbh和jlz_rybh匹配,如果匹配到说明有违法事实,则保留,没有匹配到则删除
    - 最终匹配的结果字段以基础"案件-人员"数据的字段数据为准
## 添加字段
### 当zzwx."ajxx_join_ajxx_ajlx"的值为'行政'时:
    - 字段"治拘大于4天":与"ywdata"."zq_zfba_xzcfjds"(行政处罚决定书)表的ajxx_ajbh和xzcfjds_rybh匹配,且xzcfjds_tj_jlts>4则设置值为'是'否则为'否'
    - 字段"2次违法且案由相同且第一次违法开具了'训诫书'":以下2个条件均满足则值为'是'否则为'否'
        - 2次违法且案由相同:通过"身份证号"到ywdata."zq_zfba_wcnr_xyr"匹配计数,如果计数等于2且2次的"ajxx_join_ajxx_ay_dm"值相同
        - 第一次违法开具了训诫书:通过"身份证号"到ywdata."zq_zfba_wcnr_xyr"匹配"案件编号",将匹配到的案件编号不等于"基础数据"中的那个案件编号拿出来到"ywdata"."zq_zfba_xjs2"中匹配,通过不等于"基础数据"中的那个案件编号和姓名与"ywdata"."zq_zfba_xjs2"的ajbh(案件编号)与xgry_xm(姓名)匹配,如果匹配到值则说明第一次违法开具了训诫书
    - 字段"3次及以上违法":通过"身份证号"到ywdata."zq_zfba_wcnr_xyr"匹配计数,如果计数大于2则说明有3次前科,则设置值为'是'否则为'否'
### 当zzwx."ajxx_join_ajxx_ajlx"的值为'刑事'时:
    - 字段"是否刑拘": 通过基础"案件-人员"数据的案件编号和人员编号要与 "ywdata"."zq_zfba_jlz"(拘留证)表的ajxx_ajbh和jlz_rybh匹配,如果匹配到则值为'是'否则为'否'
### 不过滤任何条件
    - 字段"是否开具矫治文书":如果满足以下任一条件(匹配到值)则值为'是'否则为'否'
        - "基础"案件-人员"数据的案件编号与人员编号与"ywdata"."zq_zfba_zlwcnrzstdxwgftzs"的zltzs_ajbh与zltzs_rybh匹配
        - "基础"案件-人员"数据的案件编号与姓名与"ywdata"."zq_zfba_xjs2"的ajbh与xgry_xm匹配
    - 字段"是否开具加强监督教育/责令接受家庭教育指导通知书":"基础"案件-人员"数据的案件编号与人员编号与"ywdata"."zq_zfba_jtjyzdtzs"的jqjhjyzljsjtjyzdtzs_ajbh与jqjhjyzljsjtjyzdtzs_rybh匹配,如果匹配到值则为'是'否则为'否'
    - 字段"是否开具提请专门教育申请书":"基础"案件-人员"数据的案件编号与姓名与"ywdata"."zq_zfba_tqzmjy"的ajbh与xgry_xm匹配,如果匹配到则值为'是'否则为'否'
    - 字段"是否符合送生:年龄>11且("治拘大于4天"为'是'或者"2次违法且案由相同且第一次违法开具了'训诫书'"为'是'或者"3次及以上违法"为'是'或者"是否刑拘"为否)则值为'是',否则为'否'
    - 字段"是否送校":"基础"案件-人员"数据的身份证号与"ywdata"."zq_wcnr_sfzxx"表的sfzhm匹配,且rx_time大于"基础"案件-人员"数据的立案时间则值为'是'否则为'否'
## 表结构:
### "ywdata"."zq_wcnr_sfzxx"
CREATE TABLE "ywdata"."zq_wcnr_sfzxx" (
	"id" integer AUTO_INCREMENT,
	"xh" integer NULL,
	"bh" character varying(50 char) NOT NULL,
	"xm" character varying(50 char) NULL,
	"xb" character varying(50 char) NULL,
	"mz" character varying(50 char) NULL,
	"csrq" date NULL,
	"sfzhm" character varying(50 char) NULL,
	"hjdq" character varying(50 char) NULL,
	"hjdz" character varying(50 char) NULL,
	"jhr" character varying(50 char) NULL,
	"lxdh" character varying(50 char) NULL,
	"yxx" character varying(50 char) NULL,
	"nj" character varying(50 char) NULL,
	"ssbm" character varying(50 char) NULL,
	"jzyy" text NULL,
	"whdj" character varying(50 char) NULL,
	"rx_time" date NULL,
	"jz_time" date NULL,
	"lx_time" date NULL,
	"bz" text NULL,
	CONSTRAINT "zq_wcnr_sfzxx_pkey" PRIMARY KEY (bh)
)TABLESPACE sys_default;
### "ywdata"."zq_zfba_tqzmjy"
CREATE TABLE "ywdata"."zq_zfba_tqzmjy" (
	"id" text NOT NULL,
	"ajbh" text NULL,
	"ajmc" text NULL,
	"bd_id" text NULL,
	"cbdw_mc" text NULL,
	"cbr_xm" text NULL,
	"dycs" text NULL,
	"dysj" timestamp without time zone NULL,
	"kjsj" timestamp without time zone NULL,
	"lrsj" timestamp without time zone NULL,
	"spr_xm" text NULL,
	"spsj" timestamp without time zone NULL,
	"wsmc" text NULL,
	"wszh" text NULL,
	"wszt" text NULL,
	"WSZTName" text NULL,
	"ws_id" text NULL,
	"xgry_xm" text NULL,
	"zjz" text NULL,
	CONSTRAINT "zq_zfba_tqzmjy_pkey" PRIMARY KEY (id)
)TABLESPACE sys_default;
### "ywdata"."zq_zfba_zlwcnrzstdxwgftzs"
CREATE TABLE "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" (
	"zltzs_ajbh" text NULL,
	"zltzs_blxw" text NULL,
	"zltzs_btzrxm" text NULL,
	"zltzs_byxxw" text NULL,
	"zltzs_cbdw_bh" text NULL,
	"zltzs_cbdw_bh_dm" text NULL,
	"zltzs_cbdw_jc" text NULL,
	"zltzs_cbdw_mc" text NULL,
	"zltzs_cbqy_bh" text NULL,
	"zltzs_cbqy_bh_dm" text NULL,
	"zltzs_cbqy_mc" text NULL,
	"zltzs_cbr_sfzh" text NULL,
	"zltzs_cbr_xm" text NULL,
	"zltzs_cflx" text NULL,
	"zltzs_cfx" text NULL,
	"zltzs_dataversion" text NULL,
	"zltzs_ghq" text NULL,
	"zltzs_hjk_rksj" timestamp without time zone NULL,
	"zltzs_hjk_scrksj" timestamp without time zone NULL,
	"zltzs_id" text NOT NULL,
	"zltzs_isdel" text NULL,
	"zltzs_jdxx" text NULL,
	"zltzs_jhrsfzh" text NULL,
	"zltzs_jhrsfzzl" text NULL,
	"zltzs_jhrsfzzl_dm" text NULL,
	"zltzs_jhrxm" text NULL,
	"zltzs_lrr_sfzh" text NULL,
	"zltzs_lrsj" timestamp without time zone NULL,
	"zltzs_lx" text NULL,
	"zltzs_lxfs" text NULL,
	"zltzs_pcssje" text NULL,
	"zltzs_psignname" text NULL,
	"zltzs_qtpcxx" text NULL,
	"zltzs_rybh" text NULL,
	"zltzs_ryxm" text NULL,
	"zltzs_sfzh" text NULL,
	"zltzs_sfzzl" text NULL,
	"zltzs_sfzzl_dm" text NULL,
	"zltzs_shdwbh2" text NULL,
	"zltzs_shdwbh2_dm" text NULL,
	"zltzs_shdwbh3" text NULL,
	"zltzs_shdwbh3_dm" text NULL,
	"zltzs_shdwbh4" text NULL,
	"zltzs_shdwbh5" text NULL,
	"zltzs_shdwbh6" text NULL,
	"zltzs_shdwbh7" text NULL,
	"zltzs_shdwmc2" text NULL,
	"zltzs_shdwmc3" text NULL,
	"zltzs_shdwmc4" text NULL,
	"zltzs_shdwmc5" text NULL,
	"zltzs_shdwmc6" text NULL,
	"zltzs_shdwmc7" text NULL,
	"zltzs_shjg" text NULL,
	"zltzs_signname" text NULL,
	"zltzs_sjly" text NULL,
	"zltzs_sqhfxm" text NULL,
	"zltzs_szjg" text NULL,
	"zltzs_tfsj" timestamp without time zone NULL,
	"zltzs_wsh" text NULL,
	"zltzs_wszt" text NULL,
	"zltzs_wszt_dm" text NULL,
	"zltzs_xgr_sfzh" text NULL,
	"zltzs_xgsj" timestamp without time zone NULL,
	"zltzs_xzz" text NULL,
	"zltzs_ygzstl" text NULL,
	"zltzs_zjzs" text NULL,
	"zltzs_zlnr1" text NULL,
	"zltzs_zlnr2" text NULL,
	"zltzs_zlnr3" text NULL,
	"zltzs_zlnr4" text NULL,
	"zltzs_zlnr5" text NULL,
	"zltzs_zlnr6" text NULL,
	"zltzs_zlnr7" text NULL,
	"zltzs_zlnrrq2" timestamp without time zone NULL,
	"zltzs_zlnrrq3" timestamp without time zone NULL,
	"zltzs_zlnrrq4" timestamp without time zone NULL,
	CONSTRAINT "zq_zfba_zlwcnrzstdxwgftzs_pkey" PRIMARY KEY (zltzs_id)
)TABLESPACE sys_default;
### "ywdata"."zq_zfba_jtjyzdtzs"
CREATE TABLE "ywdata"."zq_zfba_jtjyzdtzs" (
	"jqjhjyzljsjtjyzdtzs_ajbh" text NULL,
	"jqjhjyzljsjtjyzdtzs_ajmc" text NULL,
	"jqjhjyzljsjtjyzdtzs_cbdw_bh" text NULL,
	"jqjhjyzljsjtjyzdtzs_cbdw_bh_dm" text NULL,
	"jqjhjyzljsjtjyzdtzs_cbdw_jc" text NULL,
	"jqjhjyzljsjtjyzdtzs_cbdw_mc" text NULL,
	"jqjhjyzljsjtjyzdtzs_cbqy_bh" text NULL,
	"jqjhjyzljsjtjyzdtzs_cbqy_bh_dm" text NULL,
	"jqjhjyzljsjtjyzdtzs_cbr_sfzh" text NULL,
	"jqjhjyzljsjtjyzdtzs_cbr_xm" text NULL,
	"jqjhjyzljsjtjyzdtzs_dataversion" text NULL,
	"jqjhjyzljsjtjyzdtzs_gzdw" text NULL,
	"jqjhjyzljsjtjyzdtzs_hjk_rksj" timestamp without time zone NULL,
	"jqjhjyzljsjtjyzdtzs_hjk_scrksj" timestamp without time zone NULL,
	"jqjhjyzljsjtjyzdtzs_id" text NOT NULL,
	"jqjhjyzljsjtjyzdtzs_isdel" text NULL,
	"jqjhjyzljsjtjyzdtzs_isdel_dm" text NULL,
	"jqjhjyzljsjtjyzdtzs_jtzz" text NULL,
	"jqjhjyzljsjtjyzdtzs_lrr_sfzh" text NULL,
	"jqjhjyzljsjtjyzdtzs_lrsj" timestamp without time zone NULL,
	"jqjhjyzljsjtjyzdtzs_lxfs" text NULL,
	"jqjhjyzljsjtjyzdtzs_psignname" text NULL,
	"jqjhjyzljsjtjyzdtzs_rybh" text NULL,
	"jqjhjyzljsjtjyzdtzs_ryxm" text NULL,
	"jqjhjyzljsjtjyzdtzs_sex" text NULL,
	"jqjhjyzljsjtjyzdtzs_sfzh" text NULL,
	"jqjhjyzljsjtjyzdtzs_signname" text NULL,
	"jqjhjyzljsjtjyzdtzs_sjly" text NULL,
	"jqjhjyzljsjtjyzdtzs_tfsj" timestamp without time zone NULL,
	"jqjhjyzljsjtjyzdtzs_wcnrrybh" text NULL,
	"jqjhjyzljsjtjyzdtzs_wcnrryxm" text NULL,
	"jqjhjyzljsjtjyzdtzs_wsh" text NULL,
	"jqjhjyzljsjtjyzdtzs_wszt" text NULL,
	"jqjhjyzljsjtjyzdtzs_wszt_dm" text NULL,
	"jqjhjyzljsjtjyzdtzs_xgr_sfzh" text NULL,
	"jqjhjyzljsjtjyzdtzs_xgsj" timestamp without time zone NULL,
	"jqjhjyzljsjtjyzdtzs_zdcs" text NULL,
	"jqjhjyzljsjtjyzdtzs_zddd" text NULL,
	"jqjhjyzljsjtjyzdtzs_zdyf" text NULL,
	"jqjhjyzljsjtjyzdtzs_zjzl" text NULL,
	"jqjhjyzljsjtjyzdtzs_zjzl_dm" text NULL,
	"jqjhjyzljsjtjyzdtzs_zlnr1" text NULL,
	"jqjhjyzljsjtjyzdtzs_zlnr2" text NULL,
	CONSTRAINT "zq_zfba_jtjyzdtzs_pkey" PRIMARY KEY (jqjhjyzljsjtjyzdtzs_id)
)TABLESPACE sys_default;
###  "ywdata"."zq_zfba_jlz"
CREATE TABLE "ywdata"."zq_zfba_jlz" (
	"jlz_id" text NOT NULL,
	"ajxx_ajbh" text NULL,
	"jlz_ajmc" text NULL,
	"jlz_ay_bh" text NULL,
	"jlz_ay_bh_dm" text NULL,
	"jlz_ay_mc" text NULL,
	"jlz_cbdw_bh" text NULL,
	"jlz_cbdw_bh_dm" text NULL,
	"jlz_cbdw_jc" text NULL,
	"jlz_cbdw_mc" text NULL,
	"jlz_cbqy_bh" text NULL,
	"jlz_cbqy_bh_dm" text NULL,
	"jlz_cbr_sfzh" text NULL,
	"jlz_cbr_xm" text NULL,
	"jlz_dasj" timestamp without time zone NULL,
	"jlz_dataversion" text NULL,
	"jlz_dksssj" text NULL,
	"jlz_fltk" text NULL,
	"jlz_isdel" text NULL,
	"jlz_isdel_dm" text NULL,
	"jlz_jdws" text NULL,
	"jlz_jlyy" text NULL,
	"jlz_jlyy_c" text NULL,
	"jlz_jsmj_sfbh" text NULL,
	"jlz_jsmj_xm" text NULL,
	"jlz_kss_bh" text NULL,
	"jlz_kss_mc" text NULL,
	"jlz_lrr_sfzh" text NULL,
	"jlz_lrsj" timestamp without time zone NULL,
	"jlz_lshj" text NULL,
	"jlz_psignname" text NULL,
	"jlz_pzr_sfzh" text NULL,
	"jlz_pzr_xm" text NULL,
	"jlz_pzsj" timestamp without time zone NULL,
	"jlz_rybh" text NULL,
	"jlz_ryxm" text NULL,
	"jlz_sfda" text NULL,
	"jlz_sfda_dm" text NULL,
	"jlz_sfxzbl" text NULL,
	"jlz_sfxzbl_dm" text NULL,
	"jlz_signname" text NULL,
	"jlz_sxzm" text NULL,
	"jlz_tfr_sfzh" text NULL,
	"jlz_tfr_xm" text NULL,
	"jlz_tfsj" timestamp without time zone NULL,
	"jlz_wsh" text NULL,
	"jlz_wszt" text NULL,
	"jlz_wszt_dm" text NULL,
	"jlz_xbsj" text NULL,
	"jlz_xgr_sfzh" text NULL,
	"jlz_xgsj" timestamp without time zone NULL,
	"jlz_xyrcsrq" timestamp without time zone NULL,
	"jlz_xyrxb" text NULL,
	"jlz_xyrxb_dm" text NULL,
	"jlz_xyrzz" text NULL,
	"jlz_ywid" text NULL,
	"jlz_zxjlsj" timestamp without time zone NULL,
	"data" jsonb NULL,
	"fetched_at" timestamp without time zone NULL,
	CONSTRAINT "zq_zfba_jlz_pkey" PRIMARY KEY (jlz_id)
)TABLESPACE sys_default;
### "ywdata"."zq_zfba_byxzcfjds" 
CREATE TABLE "ywdata"."zq_zfba_byxzcfjds" (
	"byxzcfjds_id" text NOT NULL,
	"ajxx_ajbh" text NULL,
	"byxzcfjds_ajmc" text NULL,
	"byxzcfjds_cbdw_bh" text NULL,
	"byxzcfjds_cbdw_bh_dm" text NULL,
	"byxzcfjds_cbdw_jc" text NULL,
	"byxzcfjds_cbdw_mc" text NULL,
	"byxzcfjds_cbqy_bh" text NULL,
	"byxzcfjds_cbr_sfzh" text NULL,
	"byxzcfjds_cbr_xm" text NULL,
	"byxzcfjds_cbryj" text NULL,
	"byxzcfjds_cflx" text NULL,
	"byxzcfjds_cqr_sfzh" text NULL,
	"byxzcfjds_cqr_xm" text NULL,
	"byxzcfjds_cqsj" text NULL,
	"byxzcfjds_cqyj" text NULL,
	"byxzcfjds_dataversion" text NULL,
	"byxzcfjds_dwbh" text NULL,
	"byxzcfjds_dwmc" text NULL,
	"byxzcfjds_fj" text NULL,
	"byxzcfjds_flyj" text NULL,
	"byxzcfjds_flyj1" text NULL,
	"byxzcfjds_flyj2" text NULL,
	"byxzcfjds_fyjg" text NULL,
	"byxzcfjds_fyjg_dz_lx" text NULL,
	"byxzcfjds_hjk_rksj" timestamp without time zone NULL,
	"byxzcfjds_hjk_sclrsj" timestamp without time zone NULL,
	"byxzcfjds_isdel" text NULL,
	"byxzcfjds_isdel_dm" text NULL,
	"byxzcfjds_lrr_sfzh" text NULL,
	"byxzcfjds_lrsj" timestamp without time zone NULL,
	"byxzcfjds_psignname" text NULL,
	"byxzcfjds_qzsj" text NULL,
	"byxzcfjds_rmfy" text NULL,
	"byxzcfjds_rybh" text NULL,
	"byxzcfjds_rymc" text NULL,
	"byxzcfjds_ryxx" text NULL,
	"byxzcfjds_signname" text NULL,
	"byxzcfjds_sj" text NULL,
	"byxzcfjds_sjly" text NULL,
	"byxzcfjds_splx" text NULL,
	"byxzcfjds_tfsj" timestamp without time zone NULL,
	"byxzcfjds_wfss" text NULL,
	"byxzcfjds_wsh" text NULL,
	"byxzcfjds_wszt" text NULL,
	"byxzcfjds_wszt_dm" text NULL,
	"byxzcfjds_xgr_sfzh" text NULL,
	"byxzcfjds_xgsj" timestamp without time zone NULL,
	"byxzcfjds_xgzj" text NULL,
	"byxzcfjds_zj" text NULL,
	"byxzcfjds_zllx" text NULL,
	CONSTRAINT "zq_zfba_byxzcfjds_pkey" PRIMARY KEY (byxzcfjds_id)
)TABLESPACE sys_default;
### "ywdata"."zq_zfba_xzcfjds" 
CREATE TABLE "ywdata"."zq_zfba_xzcfjds" (
	"xzcfjds_id" text NOT NULL,
	"ajxx_ajbh" text NULL,
	"xzcfjds_ajmc" text NULL,
	"xzcfjds_bf" text NULL,
	"xzcfjds_cbdw_bh" text NULL,
	"xzcfjds_cbdw_bh_dm" text NULL,
	"xzcfjds_cbdw_jc" text NULL,
	"xzcfjds_cbdw_mc" text NULL,
	"xzcfjds_cbqy_bh" text NULL,
	"xzcfjds_cbqy_bh_dm" text NULL,
	"xzcfjds_cbqy_mc" text NULL,
	"xzcfjds_cbr_sfzh" text NULL,
	"xzcfjds_cbr_xm" text NULL,
	"xzcfjds_cfjg" text NULL,
	"xzcfjds_cfjg_html" text NULL,
	"xzcfjds_cfjg_text" text NULL,
	"xzcfjds_cflx" text NULL,
	"xzcfjds_cflx_dm" text NULL,
	"xzcfjds_cfzl" text NULL,
	"xzcfjds_cqcz" text NULL,
	"xzcfjds_cqyj" text NULL,
	"xzcfjds_dataversion" text NULL,
	"xzcfjds_dwbh" text NULL,
	"xzcfjds_dwmc" text NULL,
	"xzcfjds_flyj" text NULL,
	"xzcfjds_fyjg" text NULL,
	"xzcfjds_gajgname_bt" text NULL,
	"xzcfjds_is_cf" text NULL,
	"xzcfjds_is_cf_dm" text NULL,
	"xzcfjds_isdel" text NULL,
	"xzcfjds_isdel_dm" text NULL,
	"xzcfjds_jlbzx" text NULL,
	"xzcfjds_kss_bh" text NULL,
	"xzcfjds_kss_bh_dm" text NULL,
	"xzcfjds_kss_mc" text NULL,
	"xzcfjds_lrr_sfzh" text NULL,
	"xzcfjds_lrsj" timestamp without time zone NULL,
	"xzcfjds_lxfs" text NULL,
	"xzcfjds_memo" text NULL,
	"xzcfjds_psignname" text NULL,
	"xzcfjds_qd" text NULL,
	"xzcfjds_qd1" text NULL,
	"xzcfjds_qdfs" text NULL,
	"xzcfjds_qdlx" text NULL,
	"xzcfjds_qzsj" text NULL,
	"xzcfjds_rmfy" text NULL,
	"xzcfjds_rybh" text NULL,
	"xzcfjds_ryxm" text NULL,
	"xzcfjds_ryxx" text NULL,
	"xzcfjds_sfgk" text NULL,
	"xzcfjds_sfgk_dm" text NULL,
	"xzcfjds_signname" text NULL,
	"xzcfjds_signname_dm" text NULL,
	"xzcfjds_sprxm" text NULL,
	"xzcfjds_spsj" timestamp without time zone NULL,
	"xzcfjds_tfsj" timestamp without time zone NULL,
	"xzcfjds_tj_dx" text NULL,
	"xzcfjds_tj_dx_dm" text NULL,
	"xzcfjds_tj_fk" text NULL,
	"xzcfjds_tj_fk_dm" text NULL,
	"xzcfjds_tj_fkje" text NULL,
	"xzcfjds_tj_jg" text NULL,
	"xzcfjds_tj_jg_dm" text NULL,
	"xzcfjds_tj_jl" text NULL,
	"xzcfjds_tj_jl_dm" text NULL,
	"xzcfjds_tj_jlts" text NULL,
	"xzcfjds_tj_qt" text NULL,
	"xzcfjds_tj_zdtk" text NULL,
	"xzcfjds_tj_zdts" text NULL,
	"xzcfjds_tj_zdts_cn" text NULL,
	"xzcfjds_tj_zltknr" text NULL,
	"xzcfjds_wfss" text NULL,
	"xzcfjds_wfss1" text NULL,
	"xzcfjds_wsh" text NULL,
	"xzcfjds_wszt" text NULL,
	"xzcfjds_wszt_dm" text NULL,
	"xzcfjds_xgr_sfzh" text NULL,
	"xzcfjds_xgsj" timestamp without time zone NULL,
	"xzcfjds_xzcfjd" text NULL,
	"xzcfjds_zj" text NULL,
	"xzcfjds_zj1" text NULL,
	"xzcfjds_zs" text NULL,
	"xzcfjds_zxqk" text NULL,
	"xzcfjds_zxqk_html" text NULL,
	"xzcfjds_zxqk_text" text NULL,
	"data" jsonb NULL,
	"fetched_at" timestamp without time zone NULL,
	CONSTRAINT "zq_zfba_xzcfjds_pkey" PRIMARY KEY (xzcfjds_id)
)TABLESPACE sys_default;
### "ywdata"."zq_zfba_wcnr_xyr"
CREATE TABLE "ywdata"."zq_zfba_wcnr_xyr" (
	"ajxx_ajbhs" text NOT NULL,
	"xyrxx_sfzh" text NOT NULL,
	"ajxx_join_ajxx_ajbh" text NULL,
	"ajxx_join_ajxx_ajlx" text NULL,
	"ajxx_join_ajxx_ajlx_dm" text NULL,
	"ajxx_join_ajxx_ajmc" text NULL,
	"ajxx_join_ajxx_ay" text NULL,
	"ajxx_join_ajxx_ay_dm" text NULL,
	"ajxx_join_ajxx_cbdw_bh" text NULL,
	"ajxx_join_ajxx_cbdw_bh_dm" text NULL,
	"ajxx_join_ajxx_cbqy_bh" text NULL,
	"ajxx_join_ajxx_cbqy_bh_dm" text NULL,
	"ajxx_join_ajxx_cbqy_jc" text NULL,
	"ajxx_join_ajxx_isdel" text NULL,
	"ajxx_join_ajxx_isdel_dm" text NULL,
	"ajxx_join_ajxx_lasj" timestamp without time zone NULL,
	"xyrxx_ay_bh" text NULL,
	"xyrxx_ay_bh_dm" text NULL,
	"xyrxx_ay_mc" text NULL,
	"xyrxx_bh" text NULL,
	"xyrxx_bz" text NULL,
	"xyrxx_bzdzk" text NULL,
	"xyrxx_c_cssj" text NULL,
	"xyrxx_cbdw_bh" text NULL,
	"xyrxx_cbdw_bh_dm" text NULL,
	"xyrxx_cbqy_bh" text NULL,
	"xyrxx_cbqy_bh_dm" text NULL,
	"xyrxx_ch" text NULL,
	"xyrxx_crj_zjhm" text NULL,
	"xyrxx_crj_zjlx" text NULL,
	"xyrxx_crj_zjlx_dm" text NULL,
	"xyrxx_cskssj" text NULL,
	"xyrxx_csrq" timestamp without time zone NULL,
	"xyrxx_cym" text NULL,
	"xyrxx_dataversion" text NULL,
	"xyrxx_dlr" text NULL,
	"xyrxx_dlrdh" text NULL,
	"xyrxx_dna" text NULL,
	"xyrxx_dwry" text NULL,
	"xyrxx_fzjl" text NULL,
	"xyrxx_gasj" text NULL,
	"xyrxx_gatsf" text NULL,
	"xyrxx_gatsf_dm" text NULL,
	"xyrxx_gcbh" text NULL,
	"xyrxx_gj" text NULL,
	"xyrxx_gj_dm" text NULL,
	"xyrxx_gjgzry" text NULL,
	"xyrxx_gjgzry_dm" text NULL,
	"xyrxx_grxg" text NULL,
	"xyrxx_gzdw" text NULL,
	"xyrxx_gzry" text NULL,
	"xyrxx_gzry_dm" text NULL,
	"xyrxx_hjd" text NULL,
	"xyrxx_hjdxz" text NULL,
	"xyrxx_hjdxz_x" text NULL,
	"xyrxx_hjdxz_y" text NULL,
	"xyrxx_hjdxzqh" text NULL,
	"xyrxx_hjdxzqh_dm" text NULL,
	"xyrxx_hyzk" text NULL,
	"xyrxx_hyzk_dm" text NULL,
	"xyrxx_id" text NULL,
	"xyrxx_isdel" text NULL,
	"xyrxx_isdel_dm" text NULL,
	"xyrxx_isfc" text NULL,
	"xyrxx_isfc_dm" text NULL,
	"xyrxx_isgzry" text NULL,
	"xyrxx_isgzry_dm" text NULL,
	"xyrxx_isxg" text NULL,
	"xyrxx_isxg_dm" text NULL,
	"xyrxx_jg" text NULL,
	"xyrxx_jg_dm" text NULL,
	"xyrxx_jtzk" text NULL,
	"xyrxx_jzdxzqh" text NULL,
	"xyrxx_jzdxzqh_dm" text NULL,
	"xyrxx_jzdz" text NULL,
	"xyrxx_ky" text NULL,
	"xyrxx_lrr_sfzh" text NULL,
	"xyrxx_lrsj" timestamp without time zone NULL,
	"xyrxx_lxfs" text NULL,
	"xyrxx_mgry" text NULL,
	"xyrxx_mgry_dm" text NULL,
	"xyrxx_mgrylx" text NULL,
	"xyrxx_mgrylx_dm" text NULL,
	"xyrxx_mz" text NULL,
	"xyrxx_mz_dm" text NULL,
	"xyrxx_nl" text NULL,
	"xyrxx_nlsx" text NULL,
	"xyrxx_qkqk" text NULL,
	"xyrxx_qq" text NULL,
	"xyrxx_qtlxfs" text NULL,
	"xyrxx_qtzjhm1" text NULL,
	"xyrxx_qtzjhm2" text NULL,
	"xyrxx_qtzjhm3" text NULL,
	"xyrxx_qtzjlx1" text NULL,
	"xyrxx_qtzjlx1_dm" text NULL,
	"xyrxx_qtzjlx2" text NULL,
	"xyrxx_qtzjlx2_dm" text NULL,
	"xyrxx_qtzjlx3" text NULL,
	"xyrxx_qtzjlx3_dm" text NULL,
	"xyrxx_qzcs" text NULL,
	"xyrxx_qzcsjssj" timestamp without time zone NULL,
	"xyrxx_qzcskssj" timestamp without time zone NULL,
	"xyrxx_r_rssj" text NULL,
	"xyrxx_rddb" text NULL,
	"xyrxx_rddb_dm" text NULL,
	"xyrxx_rdjb" text NULL,
	"xyrxx_rdjb_dm" text NULL,
	"xyrxx_rdsj" text NULL,
	"xyrxx_rybh" text NULL,
	"xyrxx_ryzt" text NULL,
	"xyrxx_ryzt_dm" text NULL,
	"xyrxx_scspzt" text NULL,
	"xyrxx_scspzt_dm" text NULL,
	"xyrxx_sf" text NULL,
	"xyrxx_sf_dm" text NULL,
	"xyrxx_sfbk" text NULL,
	"xyrxx_sfbk_dm" text NULL,
	"xyrxx_sfbmsf" text NULL,
	"xyrxx_sfbmsf_dm" text NULL,
	"xyrxx_sfda" text NULL,
	"xyrxx_sfda_dm" text NULL,
	"xyrxx_sfdy" text NULL,
	"xyrxx_sfdy_dm" text NULL,
	"xyrxx_sfga" text NULL,
	"xyrxx_sfga_dm" text NULL,
	"xyrxx_sfgatjm" text NULL,
	"xyrxx_sfgatjm_dm" text NULL,
	"xyrxx_sflar" text NULL,
	"xyrxx_sflar_dm" text NULL,
	"xyrxx_sftb" text NULL,
	"xyrxx_sftb_dm" text NULL,
	"xyrxx_sftsqt" text NULL,
	"xyrxx_sftsqt_dm" text NULL,
	"xyrxx_sfxd" text NULL,
	"xyrxx_sfxd_dm" text NULL,
	"xyrxx_sfythcj" text NULL,
	"xyrxx_sfythcj_dm" text NULL,
	"xyrxx_sfzbm" text NULL,
	"xyrxx_sfzbm_dm" text NULL,
	"xyrxx_sg" text NULL,
	"xyrxx_shgx" text NULL,
	"xyrxx_szdzb" text NULL,
	"xyrxx_szssrd" text NULL,
	"xyrxx_szsszx" text NULL,
	"xyrxx_tbbj" text NULL,
	"xyrxx_tbsj" text NULL,
	"xyrxx_tmtz" text NULL,
	"xyrxx_tsqt" text NULL,
	"xyrxx_tsqt_dm" text NULL,
	"xyrxx_tx" text NULL,
	"xyrxx_tx_dm" text NULL,
	"xyrxx_wfss" text NULL,
	"xyrxx_whcd" text NULL,
	"xyrxx_whcd_dm" text NULL,
	"xyrxx_wx" text NULL,
	"xyrxx_xb" text NULL,
	"xyrxx_xb_dm" text NULL,
	"xyrxx_xdjyjg" text NULL,
	"xyrxx_xdjyjg_dm" text NULL,
	"xyrxx_xgr_sfzh" text NULL,
	"xyrxx_xgsj" timestamp without time zone NULL,
	"xyrxx_xm" text NULL,
	"xyrxx_xx" text NULL,
	"xyrxx_xx_dm" text NULL,
	"xyrxx_xyr_nl" text NULL,
	"xyrxx_xzd" text NULL,
	"xyrxx_xzdxz" text NULL,
	"xyrxx_xzdxz_x" text NULL,
	"xyrxx_xzdxz_y" text NULL,
	"xyrxx_xzq" text NULL,
	"xyrxx_xzqdm" text NULL,
	"xyrxx_ywjsxx" text NULL,
	"xyrxx_ywjsxx_dm" text NULL,
	"xyrxx_ywm" text NULL,
	"xyrxx_ywx" text NULL,
	"xyrxx_yxzh" text NULL,
	"xyrxx_zagj" text NULL,
	"xyrxx_zagj_dm" text NULL,
	"xyrxx_zatd" text NULL,
	"xyrxx_zatd_dm" text NULL,
	"xyrxx_zc" text NULL,
	"xyrxx_zfzh" text NULL,
	"xyrxx_zhdd" text NULL,
	"xyrxx_zhjg" text NULL,
	"xyrxx_zhr" text NULL,
	"xyrxx_zhsj" timestamp without time zone NULL,
	"xyrxx_zhxzb" text NULL,
	"xyrxx_zhyzb" text NULL,
	"xyrxx_zm" text NULL,
	"xyrxx_zmbh" text NULL,
	"xyrxx_zmbh_dm" text NULL,
	"xyrxx_zpid" text NULL,
	"xyrxx_zszt" text NULL,
	"xyrxx_zw" text NULL,
	"xyrxx_zwxx" text NULL,
	"xyrxx_zxjb" text NULL,
	"xyrxx_zxjb_dm" text NULL,
	"xyrxx_zxwy" text NULL,
	"xyrxx_zxwy_dm" text NULL,
	"xyrxx_zy" text NULL,
	"xyrxx_zy_dm" text NULL,
	"xyrxx_zylb" text NULL,
	"xyrxx_zzmm" text NULL,
	"xyrxx_zzmm_dm" text NULL,
	CONSTRAINT "zq_zfba_wcnr_xyr_pkey" PRIMARY KEY (ajxx_ajbhs, xyrxx_sfzh)
)TABLESPACE sys_default;
        - 条件:年龄>13，且zzwx."ajxx_join_ajxx_ajlx"的值为'行政'
            - 条件1(拘留大于4天)：与"ywdata"."zq_zfba_xzcfjds"(行政处罚决定书)表的ajxx_ajbh和xzcfjds_rybh匹配,且xzcfjds_tj_jlts>4
            - 条件2(2次违法且案由相同且第一次违法开具了"训诫书"): 
                - 2次违法且案由相同:通过"身份证号"到ywdata."zq_zfba_wcnr_xyr"匹配计数,如果计数等于2且2次的"ajxx_join_ajxx_ay_dm"值相同
                - 第一次违法开具了训诫书:通过"身份证号"到ywdata."zq_zfba_wcnr_xyr"匹配"案件编号",将匹配到的案件编号不等于"基础数据"中的那个案件编号拿出来到"ywdata"."zq_zfba_xjs2"中匹配,通过不等于"基础数据"中的那个案件编号和姓名与"ywdata"."zq_zfba_xjs2"的ajbh(案件编号)与xgry_xm(姓名)匹配,如果匹配到值则说明第一次违法开具了训诫书
            - 条件3(3次及以上违法):通过"身份证号"到ywdata."zq_zfba_wcnr_xyr"匹配计数,如果计数大于2则说明有3次前科