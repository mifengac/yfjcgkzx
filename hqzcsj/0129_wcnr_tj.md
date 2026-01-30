# 任务: 在hqzcsj模块新增一个tab页,名为"未成年人统计",数据从zq_zfba_wcnr_ajxx,zq_zfba_wcnr_xyr,zq_zfba_xjs,zq_zfba_jtjyzdtzs,zq_wcnr_sfzxx获取,相关代码文件在hqzcsj中新建,前缀全部为zfba_wcnr_jqaj_*,包含html,route,service,dao等文件全部新建

## 开发清单（2026-01-30）

### 目标与范围
- 在 `hqzcsj` 模块新增 Tab：“未成年人统计”。
- 汇总表：固定 6 个地区行；每个指标展示“当前值 + 同比值”，支持点击进入明细页。
- 导出：
  - 汇总导出：`未成年人统计{时间戳}.csv/.xlsx`
  - 导出详细：导出“当前页面所有数据源”的明细数据：`未成年人详细{时间戳}.xlsx`

### 文件与结构（按约定全部新建）
- 前端模板：`hqzcsj/templates/zfba_wcnr_jqaj_tab.html`、`hqzcsj/templates/zfba_wcnr_jqaj_detail.html`
- 路由：`hqzcsj/routes/zfba_wcnr_jqaj_routes.py`（Blueprint）
- Service：`hqzcsj/service/zfba_wcnr_jqaj_service.py`
- DAO：`hqzcsj/dao/zfba_wcnr_jqaj_dao.py`
- 入口挂载：在 `app.py` 注册 Blueprint；在 `hqzcsj/templates/hqzcsj_zongcha.html` 增加新 Tab 并 include 新 tab 模板（参考现有 `zfba_jq_aj_tab.html` 的接入方式）。

### 前端（Tab 页）
- 过滤区组件
  - [ ] 开始时间/结束时间：`YYYY-MM-DD HH:MM:SS`
  - [ ] 类型（多选）：数据源 `SELECT leixing FROM "ywdata"."case_type_config"`
    - [ ] 未选择类型时：所有数据源 **不拼接** 类型过滤（需要同时删除对应的 `AND ...`）
  - [ ] 治安处罚类型（多选）：固定值 `警告/罚款/拘留`
    - [ ] 选中则按 `xzcfjds_cfzl` 正则匹配（示例：`~ '(警告|拘留)'`）；未选中不加条件
  - [ ] 导出：csv/xlsx
  - [ ] 导出详细：仅 xlsx（导出所有数据源明细）
- 展示区
  - [ ] 列：地区 + 指标（含同比列）
  - [ ] 数字单元格可点击：打开明细页（同指标、同地区、同时间段；同比列用上一年同周期时间段）

### 后端接口（建议对齐 zfba_jq_aj 的接口形态）
- [ ] `GET /zfba_wcnr_jqaj/api/leixing`：类型下拉数据
- [ ] `GET /zfba_wcnr_jqaj/api/summary`：返回 `{meta, rows}`
  - [ ] `meta`：`start_time/end_time/yoy_start_time/yoy_end_time`
  - [ ] `rows`：6 行地区（含“地区代码”用于明细查询）
- [ ] `GET /zfba_wcnr_jqaj/detail`：明细页渲染（参数：`metric/diqu/start_time/end_time/leixing/za_types`）
- [ ] `GET /zfba_wcnr_jqaj/export`：汇总导出（csv/xlsx）
- [ ] `GET /zfba_wcnr_jqaj/detail/export`：单指标明细导出（csv/xlsx）
- [ ] `GET /zfba_wcnr_jqaj/detail/export_all`：全指标明细导出（xlsx）

### DAO/SQL 实现要点（按本文口径）
- [ ] 警情：`zq_kshddpt_dsjfx_jq`
  - 固定条件：`casemarkok ~ '未成年' AND LEFT(neworicharasubclass,2) IN ('01','02')`
  - 类型：`neworicharasubclass IN (SELECT UNNEST(ctc.newcharasubclass_list) ... WHERE leixing IN (...))`（未选类型不拼接）
  - 地区：`LEFT(cmdid, 6)`
- [ ] 行政/刑事：`ywdata.zq_zfba_wcnr_ajxx`（`ajxx_lasj`；地区 `LEFT(ajxx_cbdw_bh_dm,6)`；类型 `ajxx_aymc SIMILAR TO ay_pattern`）
- [ ] 行政嫌疑人/刑事嫌疑人：`ywdata.zq_zfba_wcnr_xyr`
  - 时间：行政用 `ajxx_join_ajxx_lasj`（文中“行政嫌疑人”写的是 `ajxx_lasj`，建议统一按 join 字段）
  - 地区：按 `LEFT(ajxx_join_ajxx_cbdw_bh_dm,6)`（或与案件表 join 后按 `LEFT(ajxx_cbdw_bh_dm,6)`，需确认）
- [ ] 治安处罚/治安处罚(不执行)：`ywdata.zq_zfba_xzcfjds` + 关联 `zq_zfba_wcnr_xyr`（`ajxx_ajbh`+`xzcfjds_rybh` ↔ `ajxx_ajbhs`+`xyrxx_bh`）
  - 时间：`xzcfjds_spsj`
  - 地区：`LEFT(xzcfjds_cbdw_bh_dm,6)`
  - 类型：通过关联 `zq_zfba_wcnr_ajxx` 的 `ajxx_aymc` 按 `ay_pattern` 过滤
  - 治安处罚类型：按 `xzcfjds_cfzl` 正则匹配（示例：`AND xzcfjds_cfzl ~ '(警告|拘留)'`；未选不拼接）
  - 治安处罚(不执行)：在“治安处罚”条件基础上额外追加 `AND xzcfjds_zxqk_text ~ '不送'`
- [ ] 刑拘：`ywdata.zq_zfba_jlz` + 关联 `zq_zfba_wcnr_xyr`（`ajxx_ajbh`+`jlz_rybh` ↔ `ajxx_ajbhs`+`xyrxx_bh`）
  - 时间：`jlz_pzsj`；地区：`LEFT(jlz_cbdw_bh_dm,6)`；类型：`jlz_ay_mc` 按 `ay_pattern` 过滤
- [ ] 训诫书：`ywdata.zq_zfba_xjs`
  - 固定过滤：`xjs_wszt='审批通过' AND xjs_isdel='0'`
  - 时间：`xjs_tfsj`；类型：通过 `xjs_ajbh` 关联 `zq_zfba_wcnr_ajxx` 的 `ajxx_aymc`
  - 地区：建议通过关联案件表的 `LEFT(ajxx_cbdw_bh_dm,6)`（文中写 `dbz_cbqy_bh_dm` 疑似笔误）
- [ ] 加强监督教育：`ywdata.zq_zfba_jtjyzdtzs`
  - 固定过滤：`jqjhjyzljsjtjyzdtzs_wszt='审批通过' AND jqjhjyzljsjtjyzdtzs_isdel_dm='0'`
  - 时间：`jqjhjyzljsjtjyzdtzs_tfsj`；地区：`LEFT(jqjhjyzljsjtjyzdtzs_cbdw_bh_dm,6)`；类型：通过 `*_ajbh` 关联案件表过滤
- [ ] 符合送校：按本文给定 CTE SQL 落地；将 SQL 中时间常量替换为变量；类型过滤按文中要求插入到基数 CTE
- [ ] 送校：`ywdata.zq_wcnr_sfzxx`（时间 `rx_time`；类型：`jzyy SIMILAR TO ay_pattern`；地区：`ssbm` 模糊映射到 6 地区）

### 验收点
- [ ] 未选择“类型”时：所有指标查询均不加类型过滤（不出现多余 `AND`）
- [ ] 治安处罚类型选择组合正确（正则拼接正确）
- [ ] 同比时间段：按上一年同周期计算，所有指标同比值可正常查询
- [ ] 6 个地区行数据可点开明细；导出汇总与导出详细文件名、格式正确

### 口径确认（已确认）
1. 治安处罚类型正则映射：按所选值直接拼接（示例：选择“警告、拘留” -> `AND xzcfjds_cfzl ~ '(警告|拘留)'`）。
2. 治安处罚(不执行)口径：在“治安处罚”的条件基础上，额外追加 `AND xzcfjds_zxqk_text ~ '不送'`。
3. 导出详细：删除 CSV，仅保留 xlsx。
4. 符合送校统计口径：按“人数（证件号码去重）”，且满足：
   - “治拘5日及以上”= '是'
   - “连续2次同样违法/3次及以上违法”= '是'
   - “刑事刑拘”= '否'

### 建议（可选）
- 建议复用 `zfba_jq_aj_tab.html` 的多选下拉、导出下拉、明细弹窗（iframe）交互，以减少重复 JS 与样式。
- 多数据源明细导出建议用 xlsx 多 sheet（每个指标一个 sheet），避免不同数据源字段不一致导致的歧义。
    1. 页面布局
        1. 数据过滤区
            1. "开始时间","结束时间"时间范围空间,格式是"YYYY-MM-DD HH:MM:SS"
            2. "类型":多选下拉框,数据源```SELECT leixing FROM "ywdata"."case_type_config" ctc ```,如果用户不选择类型,则不需要该条件,AND 及后面的过滤条件都删除
            3. "治安处罚类型",多选下拉框,值固定为'警告','罚款','拘留',通过zq_zfba_xzcfjds表的xzcfjds_cfzl字段进行模糊匹配,如选择'警告','拘留'则通过 zzx."xzcfjds_cfzl"  ~'(拘留|罚款)'过滤
            4. "导出":单击按钮,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,文件名为"未成年人统计"+{时间戳}.csv/xlsx
            5. "导出详细"单击按钮,,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,导出当前页面所有数据源的详细数据,文件名为"未成年人详细"+{时间戳}.csv/xlsx
        2. 数据展示区
            1. 第一列为地区: 为固定的6个值
            2. 表格的数据均为数字,通过SQL查询到后再计数得到,可以点击:
                1. 点击后弹出新页面显示详细数据
                2. 弹出页面右上角有一个"导出"按钮,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,文件名为{行标题}+"未成年人详细数据"+{时间戳}.csv/xlsx
            3. 列标题分别为地区 警情 同比警情 行政 同比行政 刑事 同比刑事 行政嫌疑人 同比行政嫌疑人 刑事嫌疑人 同比刑事嫌疑人 治安处罚 同比 治安处罚  治安处罚(不执行) 同比 治安处罚(不执行)  刑拘 同比刑拘 训诫书 同比训诫书 加强监督教育 同比加强监督教育 符合送校 送校 同比送校
        3. 数据来源及过滤字段:
            1. 警情:
                ```SELECT left("cmdid",6) diqu, jq."calltime" 报警时间,jq."caseno" 警情编号,jq."dutydeptname" 管辖单位, jq."cmdname"  分局 , jq."occuraddress"警情地址 ,jq."casecontents" 报警内容,jq."replies" 处警情况,jq."casemarkok"警情标注,jq."lngofcriterion" 经度,jq."latofcriterion"纬度
                FROM "zq_kshddpt_dsjfx_jq" jq WHERE jq."casemarkok" ~ '未成年'  AND LEFT (jq."neworicharasubclass" ,2)in('01','02') ```
                1. 时间段:calltime
                2. 类型: AND jq."neworicharasubclass"  IN ( SELECT UNNEST(ctc."newcharasubclass_list") FROM "case_type_config" ctc WHERE ctc."leixing"  IN ({类型}))
                3. 地区:通过"diqu"判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            2. 行政:```SELECT * FROM "ywdata"."zq_zfba_wcnr_ajxx" WHERE "ajxx_ajlx"='行政'```
                1. 时间段:ajxx_lasj
                2. 类型:AND ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过LEFT("ajxx_cbdw_bh_dm",6)判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            3. 刑事:```SELECT * FROM "ywdata"."zq_zfba_wcnr_ajxx" WHERE "ajxx_ajlx"='刑事'```
                1. 时间段:ajxx_lasj
                2. 类型:ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过LEFT("ajxx_cbdw_bh_dm",6)判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            4. 行政嫌疑人:```SELECT * FROM "ywdata"."zq_zfba_wcnr_xyr" WHERE ""ajxx_join_ajxx_ajlx""='行政'```
                1. 时间段:ajxx_lasj
                2. 类型:ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过LEFT("ajxx_cbdw_bh_dm",6)判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            5. 刑事嫌疑人:```SELECT * FROM "ywdata"."zq_zfba_wcnr_xyr" WHERE ""ajxx_join_ajxx_ajlx""='刑事'```
                1. 时间段:ajxx_join_ajxx_lasj
                2. 类型:ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过LEFT("ajxx_cbdw_bh_dm",6)判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'              
            6. 治安处罚:通过zq_zfba_xzcfjds表获取,需要与zq_zfba_wcnr_xyr关联,zq_zfba_xzcfjds表通过ajxx_ajbh与xzcfjds_rybh字段与zq_zfba_wcnr_xyr表的ajxx_ajbhs与xyrxx_bh字段匹配
                1. 时间段:xzcfjds_spsj
                2. "ajxx_ajbh"与"ywdata"."zq_zfba_wcnr_ajxx"表的"ajxx_ajbh"关联后通过"ywdata"."zq_zfba_wcnr_ajxx"表的ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))过滤
                3. 地区:通过LEFT("xzcfjds_cbdw_bh_dm",6)判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
                4. 治安处罚类型:通过"xzcfjds_cfzl"字段过滤
            7. 刑拘:通过"ywdata"."zq_zfba_jlz"获取,需要与zq_zfba_wcnr_xyr关联,zq_zfba_xzcfjds表通过ajxx_ajbh与jlz_rybh字段与zq_zfba_wcnr_xyr表的ajxx_ajbhs与xyrxx_bh字段匹配
                1. 时间段:jlz_pzsj
                2. 类型:jlz_ay_mc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过LEFT("jlz_cbdw_bh_dm",6)判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            8. 训诫书:```SELECT * FROM "ywdata"."zq_zfba_xjs" WHERE 1=1 AND  xjs_wszt='审批通过' AND xjs_isdel='0'```
                1. 时间段:"xjs_tfsj"
                2. 类型:"ajxx_ajbh"与"ywdata"."zq_zfba_wcnr_ajxx"表的"ajxx_ajbh"关联后通过"ywdata"."zq_zfba_wcnr_ajxx"表的ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))过滤
                3. 地区:通过"dbz_cbqy_bh_dm"判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            9. 加强监督教育:```SELECT  * FROM "ywdata"."zq_zfba_jtjyzdtzs" WHERE 1=1 AND jqjhjyzljsjtjyzdtzs_wszt_dm ='03' AND jqjhjyzljsjtjyzdtzs_isdel_dm ='0' ```
                1. 时间段:"jqjhjyzljsjtjyzdtzs_tfsj"
                2. 类型:"jqjhjyzljsjtjyzdtzs_ajbh"与"ywdata"."zq_zfba_wcnr_ajxx"表的"ajxx_ajbh"关联后通过"ywdata"."zq_zfba_wcnr_ajxx"表的ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))过滤
                3. 地区:通过LEFT("jqjhjyzljsjtjyzdtzs_cbdw_bh_dm",6)判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            10. 符合送校:
                查询SQL:```WITH 
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
                    ORDER BY main."ajxx_ajbhs", main."xyrxx_sfzh";```
                1. 时间段:ajxx_join_ajxx_lasj,SQL中所有ajxx_join_ajxx_lasj均要配置变量
                2. 类型:在基数的CTE最后增加条件:similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过LEFT("办案单位代码",6)判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'                
            11. 送校:```SELECT * FROM "ywdata"."zq_wcnr_sfzxx"```
                1. 时间段:"rx_time"
                2. 类型: jzyy similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))过滤
                3. 地区:通过"ssbm"模糊判断,值为包含'云城'的则为'云城,包含'云安'的则为'云安',包含'罗定'的则为'罗定',包含'新兴'的则为'新兴',包含'郁南'的则为'郁南',包含'市局'的则为'市局',{所有}='全市'
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
## zq_wcnr_sfzxx
    -- "ywdata"."zq_wcnr_sfzxx" definition

    -- Drop table

    -- DROP TABLE "ywdata"."zq_wcnr_sfzxx";

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
## zq_zfba_jtjyzdtzs
    -- "ywdata"."zq_zfba_jtjyzdtzs" definition

    -- Drop table

    -- DROP TABLE "ywdata"."zq_zfba_jtjyzdtzs";

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
## zq_zfba_xjs
    -- "ywdata"."zq_zfba_xjs" definition

    -- Drop table

    -- DROP TABLE "ywdata"."zq_zfba_xjs";

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
## zq_zfba_wcnr_ajxx
-- "ywdata"."zq_zfba_wcnr_ajxx" definition

-- Drop table

-- DROP TABLE "ywdata"."zq_zfba_wcnr_ajxx";

CREATE TABLE "ywdata"."zq_zfba_wcnr_ajxx" (
 "ajxx_ajbh" text NOT NULL,
 "ajxx_ab" text NULL,
 "ajxx_ab_dm" text NULL,
 "ajxx_abxl" text NULL,
 "ajxx_abxl1" text NULL,
 "ajxx_abxl1_name" text NULL,
 "ajxx_abxl2" text NULL,
 "ajxx_abxl2_name" text NULL,
 "ajxx_abxl3" text NULL,
 "ajxx_abxl3_name" text NULL,
 "ajxx_abxl_name" text NULL,
 "ajxx_ajdf" text NULL,
 "ajxx_ajjf" text NULL,
 "ajxx_ajlb" text NULL,
 "ajxx_ajlb_dm" text NULL,
 "ajxx_ajlx" text NULL,
 "ajxx_ajlx_dm" text NULL,
 "ajxx_ajly" text NULL,
 "ajxx_ajly_dm" text NULL,
 "ajxx_ajmc" text NULL,
 "ajxx_ajzt" text NULL,
 "ajxx_ajzt_dm" text NULL,
 "ajxx_ay" text NULL,
 "ajxx_ay_dm" text NULL,
 "ajxx_aybh" text NULL,
 "ajxx_aymc" text NULL,
 "ajxx_bgkly" text NULL,
 "ajxx_bz" text NULL,
 "ajxx_bzdzk" text NULL,
 "ajxx_cbdw_bh" text NULL,
 "ajxx_cbdw_bh_dm" text NULL,
 "ajxx_cbdw_mc" text NULL,
 "ajxx_cbqy_bh" text NULL,
 "ajxx_cbqy_bh_dm" text NULL,
 "ajxx_cbqy_jc" text NULL,
 "ajxx_cfsj" timestamp without time zone NULL,
 "ajxx_dataversion" text NULL,
 "ajxx_fadd" text NULL,
 "ajxx_fasd" text NULL,
 "ajxx_fasd_dm" text NULL,
 "ajxx_fasj" timestamp without time zone NULL,
 "ajxx_fasj1" timestamp without time zone NULL,
 "ajxx_fasj2" timestamp without time zone NULL,
 "ajxx_faxq" text NULL,
 "ajxx_faxq_dm" text NULL,
 "ajxx_fxdj" text NULL,
 "ajxx_gkfqr_sfzh" text NULL,
 "ajxx_gkfqr_xm" text NULL,
 "ajxx_gkfqsj" text NULL,
 "ajxx_gkspr_sfzh" text NULL,
 "ajxx_gkspr_xm" text NULL,
 "ajxx_gkspsj" text NULL,
 "ajxx_gkzt" text NULL,
 "ajxx_gkzt_dm" text NULL,
 "ajxx_id" text NULL,
 "ajxx_is_sw" text NULL,
 "ajxx_is_sw_dm" text NULL,
 "ajxx_is_tj" text NULL,
 "ajxx_is_tj_dm" text NULL,
 "ajxx_is_ysshse" text NULL,
 "ajxx_is_ysshse_dm" text NULL,
 "ajxx_isagreejoin" text NULL,
 "ajxx_isagreejoin_dm" text NULL,
 "ajxx_isdel" text NULL,
 "ajxx_isdel_dm" text NULL,
 "ajxx_isfc" text NULL,
 "ajxx_isfc_dm" text NULL,
 "ajxx_jasj" text NULL,
 "ajxx_jcksblsj" text NULL,
 "ajxx_jcksblyy" text NULL,
 "ajxx_jdzt" text NULL,
 "ajxx_jdzt_dm" text NULL,
 "ajxx_jqbh" text NULL,
 "ajxx_jsyj" text NULL,
 "ajxx_jtwfbh" text NULL,
 "ajxx_jtwfbh_glsj" text NULL,
 "ajxx_jyaq" text NULL,
 "ajxx_ksbllx" text NULL,
 "ajxx_ksbllx_dm" text NULL,
 "ajxx_kyjssj" text NULL,
 "ajxx_lasj" timestamp without time zone NULL,
 "ajxx_lrr_sfzh" text NULL,
 "ajxx_lrsj" timestamp without time zone NULL,
 "ajxx_noagreereason" text NULL,
 "ajxx_phajs" text NULL,
 "ajxx_pxjabs" text NULL,
 "ajxx_pxjabs_dm" text NULL,
 "ajxx_pxjarq" timestamp without time zone NULL,
 "ajxx_pxsj" timestamp without time zone NULL,
 "ajxx_qtclsm" text NULL,
 "ajxx_sary_xm" text NULL,
 "ajxx_sfbpaj" text NULL,
 "ajxx_sfbpaj_dm" text NULL,
 "ajxx_sfgk" text NULL,
 "ajxx_sfgk_dm" text NULL,
 "ajxx_sfksbl" text NULL,
 "ajxx_sfksbl_dm" text NULL,
 "ajxx_sfsj" text NULL,
 "ajxx_sfsj_dm" text NULL,
 "ajxx_sfsm" text NULL,
 "ajxx_sfsm_dm" text NULL,
 "ajxx_sfysasp" text NULL,
 "ajxx_sfysasp_dm" text NULL,
 "ajxx_sfzj" text NULL,
 "ajxx_sfzj_dm" text NULL,
 "ajxx_sldw_bh" text NULL,
 "ajxx_sldw_bh_dm" text NULL,
 "ajxx_sldw_mc" text NULL,
 "ajxx_sllajgts_double" text NULL,
 "ajxx_slsj" timestamp without time zone NULL,
 "ajxx_smckrybgzt" text NULL,
 "ajxx_smfqr_sfzh" text NULL,
 "ajxx_smfqr_xm" text NULL,
 "ajxx_smgkzt" text NULL,
 "ajxx_smgkzt_dm" text NULL,
 "ajxx_smjb" text NULL,
 "ajxx_smjb_dm" text NULL,
 "ajxx_smly" text NULL,
 "ajxx_smzt" text NULL,
 "ajxx_smzt_dm" text NULL,
 "ajxx_spyj" text NULL,
 "ajxx_ssjqdm" text NULL,
 "ajxx_ssjqdm_dm" text NULL,
 "ajxx_sssj" timestamp without time zone NULL,
 "ajxx_ssyj" text NULL,
 "ajxx_ssyj_dm" text NULL,
 "ajxx_tjsj" timestamp without time zone NULL,
 "ajxx_xgr_sfzh" text NULL,
 "ajxx_xgsj" timestamp without time zone NULL,
 "ajxx_xyr_xm" text NULL,
 "ajxx_xzcs" text NULL,
 "ajxx_xzcs_dm" text NULL,
 "ajxx_xzdqzqxbr_sfzh" text NULL,
 "ajxx_xzdqzqzbr_sfzh" text NULL,
 "ajxx_xzdx" text NULL,
 "ajxx_xzdx_dm" text NULL,
 "ajxx_xzsqbm" text NULL,
 "ajxx_xzsqmc" text NULL,
 "ajxx_xzwp" text NULL,
 "ajxx_ysxbmj_sfzh" text NULL,
 "ajxx_ysxbmj_xm" text NULL,
 "ajxx_ysyszt" text NULL,
 "ajxx_ysyszt_dm" text NULL,
 "ajxx_yszbmj_sfzh" text NULL,
 "ajxx_yszbmj_xm" text NULL,
 "ajxx_yszt" text NULL,
 "ajxx_yszt_dm" text NULL,
 "ajxx_zabs" text NULL,
 "ajxx_zabs_dm" text NULL,
 "ajxx_zasdtdms" text NULL,
 "ajxx_zbbj" text NULL,
 "ajxx_zbbj_dm" text NULL,
 "ajxx_zbdwjz" text NULL,
 "ajxx_zbdwjz_dm" text NULL,
 "ajxx_zbr_sfzh" text NULL,
 "ajxx_zbr_xm" text NULL,
 "ajxx_zbx" text NULL,
 "ajxx_zby" text NULL,
 "ajxx_zhlayy" text NULL,
 "ajxx_zksbl_sj" text NULL,
 CONSTRAINT "zq_zfba_wcnr_ajxx_pkey" PRIMARY KEY (ajxx_ajbh)
)TABLESPACE sys_default;
