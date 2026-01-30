# 帮我写一段SQL,zq_zfba_wcnr_xyr为主表
## zq_zfba_wcnr_xyr表最终查询字段zq_zfba_wcnr_xyr表的:"ajxx_ajbhs" 案件编号,"xyrxx_xm"姓名,"xyrxx_sfzh"证件号码,"ajxx_join_ajxx_ajlx"案件类型,"ajxx_join_ajxx_ajmc"案件名称,"ajxx_join_ajxx_cbdw_bh"办案单位,"ajxx_join_ajxx_cbdw_bh_dm"办案单位代码,"ajxx_join_ajxx_lasj"立案时间,"xyrxx_ay_mc"案由,"xyrxx_hjdxz"户籍地,"xyrxx_rybh"人员编号,"xyrxx_xzdxz"现住地
## 基数"jishu":```SELECT DISTINCT "xyrxx_sfzh" FROM "ywdata"."zq_zfba_wcnr_xyr" zzwx WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN  '2026-01-01' AND '2026-01-30'```
## 同时增加如下列
    1. "治拘5日及以上":zq_zfba_xzcfjds表中"xzcfjds_tj_jlts"值>4的,通过zq_zfba_wcnr_xyr表的"ajxx_ajbhs"与"xyrxx_rybh"字段与zq_zfba_xzcfjds表的"ajxx_ajbh"与"xzcfjds_rybh"字段匹配,如匹配到则值为'是'否则为'否'
    2. "连续2次同样违法/3次及以上违法":用"jishu"中的"xyrxx_sfzh"字段到zq_zfba_wcnr_xyr表中用"xyrxx_sfzh"分组:
        1. 次数=2且2次的"xyrxx_ay_mc"字段值相同
        2. 次数>2的
    3. "刑事刑拘":在zq_zfba_jlz表中通过zq_zfba_wcnr_xyr表的"ajxx_ajbhs"与"xyrxx_rybh"字段与zq_zfba_xzcfjds表的"ajxx_ajbh"与""jlz_rybh""字段匹配,如匹配到则值则为'是'否则为'否'
## zq_zfba_jlz
-- "ywdata"."zq_zfba_jlz" definition

-- Drop table

-- DROP TABLE "ywdata"."zq_zfba_jlz";

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
## zq_zfba_xzcfjds
-- "ywdata"."zq_zfba_xzcfjds" definition

-- Drop table

-- DROP TABLE "ywdata"."zq_zfba_xzcfjds";

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
## zq_zfba_wcnr_xyr
-- "ywdata"."zq_zfba_wcnr_xyr" definition

-- Drop table

-- DROP TABLE "ywdata"."zq_zfba_wcnr_xyr";

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
===
我来帮您编写这个SQL查询。根据您的需求,这是一个比较复杂的查询,需要关联多个表并进行条件判断。

```sql
WITH 
-- 基数CTE
jishu AS (
    SELECT DISTINCT "xyrxx_sfzh" 
    FROM "ywdata"."zq_zfba_wcnr_xyr" zzwx 
    WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN '2026-01-01' AND '2026-01-30'
),
-- 连续违法判断CTE
lianxu_wf AS (
    SELECT 
        "xyrxx_sfzh",
        COUNT(*) as wf_count,
        COUNT(DISTINCT "xyrxx_ay_mc") as distinct_ay_count,
        CASE 
            WHEN COUNT(*) = 2 AND COUNT(DISTINCT "xyrxx_ay_mc") = 1 THEN '是'
            WHEN COUNT(*) > 2 THEN '是'
            ELSE '否'
        END as is_lianxu_wf
    FROM "ywdata"."zq_zfba_wcnr_xyr"
    WHERE "xyrxx_sfzh" IN (SELECT "xyrxx_sfzh" FROM jishu)
        AND "ajxx_join_ajxx_lasj" BETWEEN '2026-01-01' AND '2026-01-30'
    GROUP BY "xyrxx_sfzh"
)
SELECT 
    main."ajxx_ajbhs" AS "案件编号",
    main."xyrxx_xm" AS "姓名",
    main."xyrxx_sfzh" AS "证件号码",
    main."ajxx_join_ajxx_ajlx" AS "案件类型",
    main."ajxx_join_ajxx_ajmc" AS "案件名称",
    main."ajxx_join_ajxx_cbdw_bh" AS "办案单位",
    main."ajxx_join_ajxx_cbdw_bh_dm" AS "办案单位代码",
    main."ajxx_join_ajxx_lasj" AS "立案时间",
    main."xyrxx_ay_mc" AS "案由",
    main."xyrxx_hjdxz" AS "户籍地",
    main."xyrxx_rybh" AS "人员编号",
    main."xyrxx_xzdxz" AS "现住地",
    -- 治拘5日及以上
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM "ywdata"."zq_zfba_xzcfjds" xzcf
            WHERE xzcf."ajxx_ajbh" = main."ajxx_ajbhs"
                AND xzcf."xzcfjds_rybh" = main."xyrxx_rybh"
                AND CAST(xzcf."xzcfjds_tj_jlts" AS INTEGER) > 4
        ) THEN '是'
        ELSE '否'
    END AS "治拘5日及以上",
    -- 连续2次同样违法/3次及以上违法
    COALESCE(lw.is_lianxu_wf, '否') AS "连续2次同样违法/3次及以上违法",
    -- 刑事刑拘
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM "ywdata"."zq_zfba_jlz" jlz
            WHERE jlz."ajxx_ajbh" = main."ajxx_ajbhs"
                AND jlz."jlz_rybh" = main."xyrxx_rybh"
        ) THEN '是'
        ELSE '否'
    END AS "刑事刑拘"
FROM "ywdata"."zq_zfba_wcnr_xyr" main
LEFT JOIN lianxu_wf lw ON lw."xyrxx_sfzh" = main."xyrxx_sfzh"
WHERE main."ajxx_join_ajxx_lasj" BETWEEN '2026-01-01' AND '2026-01-30'
    AND main."xyrxx_sfzh" IN (SELECT "xyrxx_sfzh" FROM jishu)
ORDER BY main."ajxx_ajbhs", main."xyrxx_sfzh";
```

**SQL说明:**

1. **jishu CTE**: 获取基数,即2026年1月1日至1月30日期间的不重复证件号码

2. **lianxu_wf CTE**: 判断连续违法情况
   - 统计每个证件号码的违法次数
   - 统计不同案由的数量
   - 如果次数=2且案由相同,或次数>2,则标记为'是'

3. **主查询**: 
   - 从主表获取所有基础字段
   - **治拘5日及以上**: 通过EXISTS子查询匹配zq_zfba_xzcfjds表,判断xzcfjds_tj_jlts > 4
   - **连续2次同样违法/3次及以上违法**: 左连接lianxu_wf CTE获取结果
   - **刑事刑拘**: 通过EXISTS子查询匹配zq_zfba_jlz表

**注意事项:**
- 假设xzcfjds_tj_jlts字段存储的是数值,使用CAST转换为INTEGER进行比较
- 使用COALESCE处理NULL值,确保返回'是'或'否'
- 所有日期范围都使用2026-01-01到2026-01-30
- 可以根据实际数据类型调整CAST转换