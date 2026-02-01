我来帮你编写这个SQL查询。这是一个相对复杂的统计查询，需要多表关联和聚合。

```sql
WITH base_data AS (
    -- 基础数据：2026年1月打架斗殴未成年人
    SELECT 
        zzwx."xyrxx_xm",
        zzwx."xyrxx_sfzh",
        zzwx."ajxx_join_ajxx_ajbh",
        zzwx."ajxx_join_ajxx_ajmc",
        zzwx."ajxx_join_ajxx_cbdw_bh",
        zzwx."ajxx_join_ajxx_cbqy_jc",
        zzwx."ajxx_join_ajxx_lasj",
        zzwx."xyrxx_rybh"         
    FROM "zq_zfba_wcnr_xyr" zzwx 
    WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN '2026-01-01' AND '2026-01-31' 
    AND zzwx."xyrxx_ay_mc" ~ '(殴打|打架|滋事|斗殴|故意伤害)'
),
fight_stats AS (
    -- 打架次数和历次立案时间
    SELECT 
        "xyrxx_sfzh",
        COUNT(*) AS fight_count,
        STRING_AGG(TO_CHAR("ajxx_join_ajxx_lasj", 'YYYY-MM-DD'), ',' ORDER BY "ajxx_join_ajxx_lasj") AS all_lasj
    FROM "zq_zfba_wcnr_xyr"
    WHERE "xyrxx_ay_mc" ~ '(殴打|打架|滋事|斗殴|故意伤害)'
    GROUP BY "xyrxx_sfzh"
)
SELECT 
    bd."xyrxx_xm" AS 姓名,
    bd."xyrxx_sfzh" AS 身份证号,
    bd."ajxx_join_ajxx_ajbh" AS 案件编号,
    bd."ajxx_join_ajxx_ajmc" AS 案件名称,
    bd."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
    bd."ajxx_join_ajxx_cbqy_jc" AS 分局,
    TO_CHAR(bd."ajxx_join_ajxx_lasj", 'YYYY-MM-DD') AS 立案时间,
    COALESCE(fs.fight_count, 0) AS 打架次数,
    COALESCE(fs.all_lasj, '') AS 历次立案时间,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "zq_zfba_xjs" xjs 
            WHERE xjs."xjs_ajbh" = bd."ajxx_join_ajxx_ajbh" 
            AND xjs."xjs_rybh" = bd."xyrxx_rybh"
        ) THEN '是' 
        ELSE '否' 
    END AS 是否开具训诫书,
    TO_CHAR(wcnr.rx_time, 'YYYY-MM-DD') AS 入校时间,
    TO_CHAR(wcnr.lx_time, 'YYYY-MM-DD') AS 离校时间,
    CASE 
        WHEN wcnr.lx_time IS NOT NULL 
        AND bd."ajxx_join_ajxx_lasj" > wcnr.lx_time 
        THEN '是' 
        ELSE '否' 
    END AS 离校再犯,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "ywdata"."b_per_qslswcnr" ls 
            WHERE ls."zjhm" = bd."xyrxx_sfzh"
        ) THEN '是' 
        ELSE '否' 
    END AS 是否留守未成年,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "ywdata"."b_per_qskjwcnr" kj 
            WHERE kj."zjhm" = bd."xyrxx_sfzh"
        ) THEN '是' 
        ELSE '否' 
    END AS 是否困境未成年,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "ywdata"."b_per_yfsjyjblxwwcnr" yb 
            WHERE yb."zjhm" = bd."xyrxx_sfzh"
        ) THEN '是' 
        ELSE '否' 
    END AS 是否一般不良未成年,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "stdata"."b_zdry_ryxx" bzr 
            WHERE bzr."zjhm" = bd."xyrxx_sfzh" 
            AND bzr."deleteflag" = '0' 
            AND bzr."sflg" = '1'
        ) THEN '是' 
        ELSE '否' 
    END AS 是否严重不良未成年
FROM base_data bd
LEFT JOIN fight_stats fs ON bd."xyrxx_sfzh" = fs."xyrxx_sfzh"
LEFT JOIN "zq_wcnr_sfzxx" wcnr ON bd."xyrxx_sfzh" = wcnr."sfzhm"
ORDER BY bd."ajxx_join_ajxx_lasj", bd."xyrxx_xm";
```

这个SQL查询的主要特点：

1. **使用CTE（公共表表达式）**提高可读性和性能
2. **base_data CTE**：获取2026年1月打架斗殴案件的基础信息
3. **fight_stats CTE**：统计每个人的打架次数和历次立案时间
4. **主查询**：关联所有需要的表，使用CASE语句判断各种状态
5. **使用EXISTS子查询**：高效判断是否存在匹配记录
6. **STRING_AGG函数**：将多个立案时间用逗号连接
7. **COALESCE函数**：处理NULL值

查询结果将包含所有要求的字段，并按立案时间和姓名排序。

# 任务:帮我写一段SQL
## 结果集:名为"2026年1月打架斗殴未成年人统计"基础数据为```SELECT * FROM "zq_zfba_wcnr_xyr" zzwx WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN '2026-01-01' AND '2026-01-31' AND zzwx."xyrxx_ay_mc"  ~'(殴打|打架|滋事|斗殴|故意伤害)'```
    - "xyrxx_xm"姓名,"xyrxx_sfzh"身份证号,"ajxx_join_ajxx_ajbh"案件编号,"ajxx_join_ajxx_ajmc"案件名称,"ajxx_join_ajxx_cbdw_bh"办案单位,"ajxx_join_ajxx_cbqy_jc"分局,"ajxx_join_ajxx_lasj""立案时间"从基础数据中获取
    - 打架次数:通过zq_zfba_wcnr_xyr表再次匹配,然后使用"xyrxx_ay_mc"  ~'(殴打|打架|滋事|斗殴|故意伤害)'过滤再使用"xyrxx_sfzh"身份证号分组,分组计数值则为"打架次数"
    - 历次立案时间:通过zq_zfba_wcnr_xyr表再次匹配,然后使用"xyrxx_ay_mc"  ~'(殴打|打架|滋事|斗殴|故意伤害)'过滤再使用"xyrxx_sfzh"身份证号分组,最后将"ajxx_join_ajxx_lasj"立案时间使用','分隔拼接,即可
    - 是否开具训诫书:与zq_zfba_xjs表的"xjs_ajbh"与"xjs_rybh"字段匹配,如匹配到值则为"是"否则为"否"
    - 入校时间,离校时间:通过"xyrxx_sfzh"身份证号与zq_wcnr_sfzxx表的sfzhm关联,获取rx_time入校时间,lx_time离校时间,
    - 离校再犯:通过zq_wcnr_sfzxx表的lx_time离校时间与"ajxx_join_ajxx_lasj"立案时间判断,如果"ajxx_join_ajxx_lasj"大于lx_time离校时间则为'是',否则为'否'
    - 是否留守未成年:通过"xyrxx_sfzh"身份证号与ywdata.b_per_qslswcnr表的"zjhm"匹配,如匹配到则为'是'否则为'否'
    - 是否困境未成年:通过"xyrxx_sfzh"身份证号与ywdata.b_per_qskjwcnr表的"zjhm"匹配,如匹配到则为'是'否则为'否'
    - 是否一般不良未成年:通过"xyrxx_sfzh"身份证号与ywdata.b_per_yfsjyjblxwwcnr表的"zjhm"匹配,如匹配到则为'是'否则为'否'
    - 是否严重不良未成年:通过"xyrxx_sfzh"身份证号与"stdata"."b_zdry_ryxx" 表的"zjhm"匹配,如匹配到则为'是'否则为'否'```SELECT * FROM "stdata"."b_zdry_ryxx" bzr WHERE bzr."deleteflag" ='0' AND bzr."sflg" ='1'```
## 表结构
### "stdata"."b_zdry_ryxx"
    CREATE TABLE "stdata"."b_zdry_ryxx" (
        "systemid" character varying(50 char) NOT NULL,
        "rylx" character varying(50 char) NULL,
        "xm" character varying(50 char) NULL,
        "cym" character varying(50 char) NULL,
        "xb" character varying(50 char) NULL,
        "zjlx" character varying(50 char) NULL,
        "zjhm" character varying(50 char) NULL,
        "csrq" timestamp without time zone NULL,
        "mz" character varying(50 char) NULL,
        "lxdh" character varying(50 char) NULL,
        "zzmm" character varying(50 char) NULL,
        "whcd" character varying(50 char) NULL,
        "hhzt" character varying(50 char) NULL,
        "gjdq" character varying(50 char) NULL,
        "hjdz" character varying(50 char) NULL,
        "hjdz_dm" character varying(50 char) NULL,
        "hjsfbzdz" character varying(50 char) NULL,
        "jzdz" character varying(50 char) NULL,
        "jzdz_dm" character varying(50 char) NULL,
        "jzsfbzdz" character varying(50 char) NULL,
        "jzszs" character varying(50 char) NULL,
        "jzszqx" character varying(50 char) NULL,
        "jzszjd" character varying(50 char) NULL,
        "jzszsq" character varying(50 char) NULL,
        "dadz" character varying(50 char) NULL,
        "dadz_dm" character varying(50 char) NULL,
        "dasfbzdz" character varying(50 char) NULL,
        "cphm" character varying(50 char) NULL,
        "jssj" timestamp without time zone NULL,
        "zrdw" character varying(50 char) NULL,
        "zrmj" character varying(50 char) NULL,
        "zrld" character varying(50 char) NULL,
        "sjly" character varying(50 char) NULL,
        "hszt" character varying(50 char) NULL,
        "sflg" character varying(50 char) NULL,
        "lgdw" character varying(50 char) NULL,
        "lgmj" character varying(50 char) NULL,
        "lgsj" timestamp without time zone NULL,
        "lgspld" character varying(50 char) NULL,
        "lgspsj" timestamp without time zone NULL,
        "gkzt" character varying(50 char) NULL,
        "gkzz" character varying(50 char) NULL,
        "sfzy" character varying(50 char) NULL,
        "sfsk" character varying(50 char) NULL,
        "sfdqtb" character varying(50 char) NULL,
        "sfglld" character varying(50 char) NULL,
        "cgdw" character varying(50 char) NULL,
        "cgmj" character varying(50 char) NULL,
        "cgsj" timestamp without time zone NULL,
        "cgspld" character varying(50 char) NULL,
        "cgspsj" timestamp without time zone NULL,
        "fxdj" character varying(50 char) NULL,
        "djsj" timestamp without time zone NULL,
        "djr_id" character varying(50 char) NULL,
        "djdw_id" character varying(50 char) NULL,
        "djdw_dm" character varying(50 char) NULL,
        "xgsj" timestamp without time zone NULL,
        "xgr_id" character varying(50 char) NULL,
        "xgdw_id" character varying(50 char) NULL,
        "xgdw_dm" character varying(50 char) NULL,
        "djr_xm" character varying(50 char) NULL,
        "xgr_xm" character varying(50 char) NULL,
        "deleteflag" character varying(50 char) NULL,
        "sssj_dm" character varying(50 char) NULL,
        "ssfj_dm" character varying(50 char) NULL,
        "sspcs_dm" character varying(50 char) NULL,
        "xgdw" character varying(50 char) NULL,
        "xgmj" character varying(50 char) NULL,
        "xgld" character varying(50 char) NULL,
        "sjgsdwdm" character varying(50 char) NULL,
        "jgsf" character varying(50 char) NULL,
        "gxdzlx" character varying(50 char) NULL,
        "gxdw" character varying(50 char) NULL,
        "gxdz" character varying(50 char) NULL,
        "xzqh_sf" character varying(50 char) NULL,
        "xzqh_ds" character varying(50 char) NULL,
        "xzqh_qx" character varying(50 char) NULL,
        "bq" character varying(50 char) NULL,
        "hjdz_hzb" character varying(50 char) NULL,
        "hjdz_zzb" character varying(50 char) NULL,
        "hjdz_sspcsdm" character varying(50 char) NULL,
        "hjdz_sssqcjdm" character varying(50 char) NULL,
        "hjdz_ssxqdm" character varying(50 char) NULL,
        "hjdz_ssxzjddm" character varying(50 char) NULL,
        "jzdz_hzb" character varying(50 char) NULL,
        "jzdz_zzb" character varying(50 char) NULL,
        "jzdz_sspcsdm" character varying(50 char) NULL,
        "jzdz_sssqcjdm" character varying(50 char) NULL,
        "jzdz_ssxqdm" character varying(50 char) NULL,
        "jzdz_ssxzjddm" character varying(50 char) NULL,
        "hjdz_wdz" character varying(50 char) NULL,
        "jzdz_wdz" character varying(50 char) NULL,
        "hjdz_type" character varying(50 char) NULL,
        "jzdz_type" character varying(50 char) NULL,
        "dadz_hzb" character varying(50 char) NULL,
        "dadz_zzb" character varying(50 char) NULL,
        "dadz_sspcsdm" character varying(50 char) NULL,
        "dadz_sssqcjdm" character varying(50 char) NULL,
        "dadz_ssxqdm" character varying(50 char) NULL,
        "dadz_ssxzjddm" character varying(50 char) NULL,
        "dadz_type" character varying(50 char) NULL,
        "dadz_wdz" character varying(50 char) NULL,
        "dadz_dz" character varying(50 char) NULL,
        "dadz_dzdm" character varying(50 char) NULL,
        "gxdzdm" character varying(50 char) NULL,
        "whmyy" character varying(50 char) NULL,
        "sfysjhm" character varying(50 char) NULL,
        "sfcg" character varying(50 char) NULL,
        "jzdz_sssj" character varying(50 char) NULL,
        "jzdz_ssfj" character varying(50 char) NULL,
        "hjdz_sssj" character varying(50 char) NULL,
        "hjdz_ssfj" character varying(50 char) NULL,
        "hjdz_ds" character varying(50 char) NULL,
        "lxdh_gs" character varying(50 char) NULL,
        "rybq" character varying(50 char) NULL,
        "hdzt" character varying(50 char) NULL,
        "zxwz" character varying(50 char) NULL,
        "zxwzgxsj" timestamp without time zone NULL,
        "zxwzxz" character varying(50 char) NULL,
        "sfygpyj" character varying(50 char) NULL,
        "gpyj_wz" character varying(50 char) NULL,
        "gpyj_wzxz" character varying(50 char) NULL,
        "gpyj_yjsj" timestamp without time zone NULL,
        "gpyj_ccrq" timestamp without time zone NULL,
        "gzdwmc" character varying(50 char) NULL,
        "gzdwdz" character varying(50 char) NULL,
        "gzdwdz_dm" character varying(50 char) NULL,
        "gzdwdz_hzb" character varying(50 char) NULL,
        "gzdwdz_zzb" character varying(50 char) NULL,
        "gzdwdz_sssj" character varying(50 char) NULL,
        "gzdwdz_ssfj" character varying(50 char) NULL,
        "gzdwdz_sspcsdm" character varying(50 char) NULL,
        "gzdwdz_ssxqdm" character varying(50 char) NULL,
        "gzdwdz_ssxzjddm" character varying(50 char) NULL,
        "gzdwdz_sssqcjdm" character varying(50 char) NULL,
        "gzdwdz_type" character varying(50 char) NULL,
        "gzdwdz_wdz" character varying(50 char) NULL,
        "zy" character varying(50 char) NULL,
        "last_zxwz" character varying(50 char) NULL,
        "last_zxwzgxsj" timestamp without time zone NULL,
        "last_zxwzxz" character varying(50 char) NULL,
        "grjdfx" character varying(50 char) NULL,
        "fzfx" character varying(50 char) NULL,
        "yxshwd" character varying(50 char) NULL,
        "zxhdfw" character varying(50 char) NULL,
        "wqhx" character varying(50 char) NULL,
        "jzckdz" character varying(50 char) NULL,
        "hjckdz" character varying(50 char) NULL,
        "jjlxr" character varying(50 char) NULL,
        "jjlxrdh" character varying(50 char) NULL,
        "yjjlxrgx" character varying(50 char) NULL,
        "jzdz_ssjwqdm" character varying(50 char) NULL,
        "hjdz_ssjwqdm" character varying(50 char) NULL,
        CONSTRAINT "con_stdata_b_zdry_ryxx_constraint_1" PRIMARY KEY (systemid)
    )TABLESPACE sys_default;
### b_per_yfsjyjblxwwcnr表
    CREATE TABLE "ywdata"."b_per_yfsjyjblxwwcnr" (
        "xm" varchar NULL,
        "zjhm" varchar NOT NULL,
        "hjszxq" varchar NULL,
        "jhr1xm" varchar NULL,
        "jhr1lxdh" varchar NULL,
        "jhr2xm" varchar NULL,
        "jhr2lxdh" varchar NULL,
        "jzdz" varchar NULL,
        "sfyjhrgtsh" varchar NULL,
        "xjh" varchar NULL,
        "xxmc" varchar NULL,
        "njbj" varchar NULL,
        "xxszxq" varchar NULL,
        "jdqk" varchar NULL,
        "xzxm" varchar NULL,
        "xzlxdh" varchar NULL,
        "dyzrxm" varchar NULL,
        "dyzrlxdh" varchar NULL,
        "bzrxm" varchar NULL,
        "bzrlxdh" varchar NULL,
        "blxw1" varchar NULL,
        "blxw2" varchar NULL,
        "blxw3" varchar NULL,
        "blxw4" varchar NULL,
        "blxw5" varchar NULL,
        "blxw6" varchar NULL,
        "blxw7" varchar NULL,
        "blxw8" varchar NULL,
        "blxw9" varchar NULL,
        "blxw10" varchar NULL,
        "rksj" timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT "con_ywdata_b_per_yfsjyjblxwwcnr_constraint_1" PRIMARY KEY (zjhm)
    )TABLESPACE sys_default;
### b_per_qskjwcnr
    CREATE TABLE "ywdata"."b_per_qskjwcnr" (
        "xm" character varying(255 char) NULL,
        "sfdjhk" character varying(255 char) NULL,
        "zjhm" character varying(255 char) NULL,
        "xb" character varying(255 char) NULL,
        "mz" character varying(255 char) NULL,
        "csrq" character varying(255 char) NULL,
        "nl" character varying(255 char) NULL,
        "gj" character varying(255 char) NULL,
        "hkxz" character varying(255 char) NULL,
        "hjszd" character varying(255 char) NULL,
        "hjxxdz" character varying(255 char) NULL,
        "xzd" character varying(255 char) NULL,
        "xzdxz" character varying(255 char) NULL,
        "qttsqk" character varying(255 char) NULL,
        "zt" character varying(255 char) NULL,
        "etlb" character varying(255 char) NULL,
        "knjtlx" character varying(255 char) NULL,
        "knjtlxqt" character varying(255 char) NULL,
        "azbqk" character varying(255 char) NULL,
        "azbbcsm" character varying(255 char) NULL,
        "etjkzk" character varying(255 char) NULL,
        "cjlbydj" character varying(255 char) NULL,
        "hblx" character varying(255 char) NULL,
        "yscj" character varying(255 char) NULL,
        "qtqksm" character varying(255 char) NULL,
        "xyzk" character varying(255 char) NULL,
        "jxqk" character varying(255 char) NULL,
        "jsqk" character varying(255 char) NULL,
        "jdxx" character varying(255 char) NULL,
        "xz" character varying(255 char) NULL,
        "rxsj" character varying(255 char) NULL,
        "qtsm1" character varying(255 char) NULL,
        "jhlx" character varying(255 char) NULL,
        "ywfszsjhrqhdqk" character varying(255 char) NULL,
        "jhqkpg" character varying(255 char) NULL,
        "lsxtskn" character varying(255 char) NULL,
        "qtsm2" character varying(255 char) NULL,
        "jhr1gx" character varying(255 char) NULL,
        "jhr1sfdq" character varying(255 char) NULL,
        "jhr1sfjhr" character varying(255 char) NULL,
        "jhr1lx" character varying(255 char) NULL,
        "jhr1jhrzjhm" character varying(255 char) NULL,
        "jhr1xm" character varying(255 char) NULL,
        "jhr1csrq" character varying(255 char) NULL,
        "jhr1lxdh" character varying(255 char) NULL,
        "jhr1stqk" character varying(255 char) NULL,
        "jhr1cjlbydj" character varying(255 char) NULL,
        "jhrdbbz" character varying(255 char) NULL,
        "jhr1qtbz" character varying(255 char) NULL,
        "jhr1hjszd" character varying(255 char) NULL,
        "jhr1hjdxxdz" character varying(255 char) NULL,
        "jhr1xjzdszd" character varying(255 char) NULL,
        "jhr1xjzxxdz" character varying(255 char) NULL,
        "jhr1wcwgrq" character varying(255 char) NULL,
        "jhr1dqwgszd" character varying(255 char) NULL,
        "jhr1dqwgxxdz" character varying(255 char) NULL,
        "jhr1bz" character varying(255 char) NULL,
        "jhr2gx" character varying(255 char) NULL,
        "jhr2sfdq" character varying(255 char) NULL,
        "jhr2sfjhr" character varying(255 char) NULL,
        "jhr2lx" character varying(255 char) NULL,
        "jhr2zjhm" character varying(255 char) NULL,
        "jhr2xm" character varying(255 char) NULL,
        "jhr2csrq" character varying(255 char) NULL,
        "jhr2lxdh" character varying(255 char) NULL,
        "jhr2stqk" character varying(255 char) NULL,
        "jhr2cjlbydj" character varying(255 char) NULL,
        "jhr2dbbz" character varying(255 char) NULL,
        "jhr2qtbz" character varying(255 char) NULL,
        "jhr2hjszd" character varying(255 char) NULL,
        "jhr2gjdxxdz" character varying(255 char) NULL,
        "jhr2xjzdszd" character varying(255 char) NULL,
        "jhr2xjzxxdz" character varying(255 char) NULL,
        "jhr2wcwgrq" character varying(255 char) NULL,
        "jhr2dqwgszd" character varying(255 char) NULL,
        "jhr2dwwgxxdz" character varying(255 char) NULL,
        "jhr2bz" character varying(255 char) NULL,
        "jtjjly" character varying(255 char) NULL,
        "shfljzfplsqk" character varying(255 char) NULL,
        "fmsftswc" character varying(255 char) NULL,
        "fmyfwclyfwjhnl" character varying(255 char) NULL,
        "fmsfwjhnl" character varying(255 char) NULL,
        "fmwcywwtqtjhr" character varying(255 char) NULL,
        "jhfl" character varying(255 char) NULL,
        "yetgx" character varying(255 char) NULL,
        "swtjhrxm" character varying(255 char) NULL,
        "swtjhrsflx" character varying(255 char) NULL,
        "swtjhrsfhm" character varying(255 char) NULL,
        "swtjhrdh" character varying(255 char) NULL,
        "swtjhrjkqk" character varying(255 char) NULL,
        "swtjhrqttsqk" character varying(255 char) NULL,
        "xjjdzd" character varying(255 char) NULL,
        "xjjxxdz1" character varying(255 char) NULL,
        "jgmc" character varying(255 char) NULL,
        "jggddh" character varying(255 char) NULL,
        "dwxz" character varying(255 char) NULL,
        "fzr" character varying(255 char) NULL,
        "lxr" character varying(255 char) NULL,
        "lxrdh" character varying(255 char) NULL,
        "jgdz" character varying(255 char) NULL,
        "xjjxxdz2" character varying(255 char) NULL,
        "bfjy" character varying(255 char) NULL,
        "bz2" character varying(255 char) NULL,
        "fmqk" character varying(255 char) NULL,
        "jhqk" character varying(255 char) NULL,
        "jyqk" character varying(255 char) NULL,
        "xljk" character varying(255 char) NULL,
        "shbh" character varying(255 char) NULL,
        "cjqk" character varying(255 char) NULL,
        "qt" character varying(255 char) NULL,
        "fxdj" character varying(255 char) NULL,
        "cjdw" character varying(255 char) NULL,
        "cjsj" character varying(255 char) NULL,
        "cjr" character varying(255 char) NULL,
        "gkrksj" timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP
    )TABLESPACE sys_default;
### b_per_qslswcnr
    CREATE TABLE "ywdata"."b_per_qslswcnr" (
        "xm" varchar NULL,
        "zjhm" varchar NOT NULL,
        "ly" varchar NULL,
        "gkrksj" timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT "con_ywdata_b_per_qslswcnr_constraint_1" PRIMARY KEY (zjhm)
    )TABLESPACE sys_default;
    CREATE UNIQUE INDEX index_ywdata_b_per_qslswcnr_index_1 ON ywdata.b_per_qslswcnr USING btree (zjhm) TABLESPACE sys_default;
### zq_wcnr_sfzxx
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
### zq_zfba_xjs
    CREATE TABLE "ywdata"."zq_zfba_xjs" (
        "xjs_ajbh" text NULL,
        "xjs_ajmc" text NULL,
        "xjs_cbdw_bh" text NULL,
        "xjs_cbdw_bh_dm" text NULL,
        "xjs_cbdw_jc" text NULL,
        "xjs_cbdw_mc" text NULL,
        "xjs_cbqy_bh" text NULL,
        "xjs_cbqy_bh_dm" text NULL,
        "xjs_cbr_sfzh" text NULL,
        "xjs_cbr_xm" text NULL,
        "xjs_cqyj" text NULL,
        "xjs_csrq" text NULL,
        "xjs_dataversion" text NULL,
        "xjs_gzdw" text NULL,
        "xjs_hjk_rksj" timestamp without time zone NULL,
        "xjs_hjk_sclrsj" timestamp without time zone NULL,
        "xjs_hjszd" text NULL,
        "xjs_id" text NOT NULL,
        "xjs_isdel" text NULL,
        "xjs_jtzz" text NULL,
        "xjs_lrr_sfzh" text NULL,
        "xjs_lrsj" timestamp without time zone NULL,
        "xjs_psignname" text NULL,
        "xjs_rybh" text NULL,
        "xjs_ryxm" text NULL,
        "xjs_sex" text NULL,
        "xjs_sex_dm" text NULL,
        "xjs_sfzh" text NULL,
        "xjs_signname" text NULL,
        "xjs_sjly" text NULL,
        "xjs_sszj" text NULL,
        "xjs_tfsj" timestamp without time zone NULL,
        "xjs_wsh" text NULL,
        "xjs_wszt" text NULL,
        "xjs_wszt_dm" text NULL,
        "xjs_xgr_sfzh" text NULL,
        "xjs_xgsj" timestamp without time zone NULL,
        "xjs_xjyy" text NULL,
        "xjs_zjzl" text NULL,
        "xjs_zjzl_dm" text NULL,
        CONSTRAINT "zq_zfba_xjs_pkey" PRIMARY KEY (xjs_id)
    )TABLESPACE sys_default;
### zq_zfba_wcnr_xyr
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
### zq_zfba_tqzmjy
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
===
我来帮你编写这个"列管未成年人统计"的SQL查询。

```sql
WITH base_data AS (
    -- 基础数据：列管未成年人
    SELECT 
        bzr."xm",
        bzr."zjhm",
        bzr."lxdh",
        bzr."hjdz",
        bzr."lgsj",
        bzr."ssfj_dm",
        bzr."gxdw",
        bzr."jzdz"
    FROM "stdata"."b_zdry_ryxx" bzr 
    WHERE bzr."deleteflag" = '0' 
    AND bzr."sflg" = '1'
),
fight_stats AS (
    -- 打架次数和历次立案时间
    SELECT 
        "xyrxx_sfzh",
        COUNT(*) AS fight_count,
        STRING_AGG(TO_CHAR("ajxx_join_ajxx_lasj", 'YYYY-MM-DD'), ',' ORDER BY "ajxx_join_ajxx_lasj") AS all_lasj
    FROM "zq_zfba_wcnr_xyr"
    WHERE "xyrxx_ay_mc" ~ '(殴打|打架|滋事|斗殴|故意伤害)'
    GROUP BY "xyrxx_sfzh"
),
latest_fight AS (
    -- 获取最近一次打架案件的立案时间（用于判断离校再犯）
    SELECT 
        "xyrxx_sfzh",
        MAX("ajxx_join_ajxx_lasj") AS latest_lasj
    FROM "zq_zfba_wcnr_xyr"
    WHERE "xyrxx_ay_mc" ~ '(殴打|打架|滋事|斗殴|故意伤害)'
    GROUP BY "xyrxx_sfzh"
)
SELECT 
    bd."xm" AS 姓名,
    bd."zjhm" AS 身份证号,
    bd."lxdh" AS 联系电话,
    bd."hjdz" AS 户籍地址,
    TO_CHAR(bd."lgsj", 'YYYY-MM-DD') AS 列管时间,
    bd."ssfj_dm" AS 所属分局代码,
    bd."gxdw" AS 管辖单位代码,
    bd."jzdz" AS 居住地址,
    COALESCE(fs.fight_count, 0) AS 打架次数,
    COALESCE(fs.all_lasj, '') AS 历次立案时间,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "zq_zfba_xjs" xjs 
            WHERE xjs."xjs_sfzh" = bd."zjhm"
        ) THEN '是' 
        ELSE '否' 
    END AS 是否开具训诫书,
    TO_CHAR(wcnr.rx_time, 'YYYY-MM-DD') AS 入校时间,
    TO_CHAR(wcnr.lx_time, 'YYYY-MM-DD') AS 离校时间,
    CASE 
        WHEN wcnr.lx_time IS NOT NULL 
        AND lf.latest_lasj IS NOT NULL
        AND lf.latest_lasj > wcnr.lx_time 
        THEN '是' 
        ELSE '否' 
    END AS 离校再犯,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "ywdata"."b_per_qslswcnr" ls 
            WHERE ls."zjhm" = bd."zjhm"
        ) THEN '是' 
        ELSE '否' 
    END AS 是否留守未成年,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "ywdata"."b_per_qskjwcnr" kj 
            WHERE kj."zjhm" = bd."zjhm"
        ) THEN '是' 
        ELSE '否' 
    END AS 是否困境未成年,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM "ywdata"."b_per_yfsjyjblxwwcnr" yb 
            WHERE yb."zjhm" = bd."zjhm"
        ) THEN '是' 
        ELSE '否' 
    END AS 是否一般不良未成年,
    '是' AS 是否严重不良未成年  -- 基础数据本身就是从b_zdry_ryxx筛选的，所以都是"是"
FROM base_data bd
LEFT JOIN fight_stats fs ON bd."zjhm" = fs."xyrxx_sfzh"
LEFT JOIN latest_fight lf ON bd."zjhm" = lf."xyrxx_sfzh"
LEFT JOIN "zq_wcnr_sfzxx" wcnr ON bd."zjhm" = wcnr."sfzhm"
ORDER BY bd."lgsj" DESC, bd."xm";
```

这个SQL查询的主要特点：

1. **base_data CTE**：从`b_zdry_ryxx`表获取列管未成年人的基础信息（已过滤`deleteflag='0'`和`sflg='1'`）

2. **fight_stats CTE**：统计每个人的打架次数和历次立案时间（用逗号分隔）

3. **latest_fight CTE**：获取每个人最近一次打架案件的立案时间，用于准确判断"离校再犯"

4. **主查询**：
   - 关联所有相关表
   - 使用EXISTS子查询判断是否开具训诫书、是否留守/困境/一般不良未成年
   - "是否严重不良未成年"直接返回"是"，因为基础数据本身就是从严重不良未成年表筛选的
   - 离校再犯的判断：使用最近一次立案时间与离校时间比较

5. **结果排序**：按列管时间降序、姓名排序

注意：由于基础数据本身就是从`b_zdry_ryxx`（严重不良未成年人表）中筛选的，所以"是否严重不良未成年"字段对所有记录都是"是"。