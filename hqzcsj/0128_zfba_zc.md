1. 案件信息:"ajxx_lasj","ajxx_aymc","ajxx_cbqy_jc","ajxx_cbdw_mc","ajxx_jqbh"
2. 行政处罚:"xzcfjds_spsj",("ajxx_aymc"),"xzcfjds_cbqy_mc","xzcfjds_cbdw_mc","xzcfjds_cfzl"
3. 拘留:"jlz_pzsj","jlz_ay_mc","jlz_cbqy_bh","jlz_cbdw_mc",
4. 逮捕:dbz_pzdbsj,"dbz_dbyy","dbz_cbqy_bh","dbz_cbdw_mc",
5. 移交:"ysajtzs_pzsj",("ajxx_aymc"),"ysajtzs_cbqy_mc","ysajtzs_cbdw_mc",
6. 起诉:"qsryxx_tfsj",("ajxx_aymc"),"qsryxx_cbqy_bh","qsryxx_cbdw_mc",
# 任务: 帮我在hqzcsj模块新增一个tab页,名为"警情案件统计",tab页参考gzrzdd
    1. 页面布局
        1. 数据过滤区
            1. "开始时间","结束时间"时间范围空间,格式是"YYYY-MM-DD HH:MM:SS"
            2. "类型":多选下拉框,数据源```SELECT leixing FROM "ywdata"."case_type_config" ctc ```
            3. "导出":单击按钮,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,文件名为"警情案件统计"+{时间戳}.csv/xlsx
        2. 数据展示区
            1. 第一列为地区: 为固定的6个值
            2. 表格的数据均为数字,通过SQL查询到后再计数得到,可以点击:
                1. 点击后弹出新页面显示详细数据
                2. 弹出页面右上角有一个"导出"按钮,单击"导出"弹出'csv','xlsx'两个下拉按钮,单击对应按钮下载对应格式文件,文件名为{行标题}+"警情案件详细数据"+{时间戳}.csv/xlsx
            3. 列标题分别为地区	警情	同比警情	行政	同比行政	刑事	同比刑事	治拘	同比治拘	刑拘	同比刑拘	逮捕	同比逮捕	起诉	同比起诉	移送案件	同比移送人员	移送案件	同比移送案件
        3. 数据来源及过滤字段:
            1. 警情:```SELECT vjo."calltime" 报警时间,vjo."caseno" 警情编号,vjo."dutydeptname" 管辖单位, vjo."cmdname"  分局 , vjo."occuraddress"警情地址 ,vjo."casecontents" 报警内容,vjo."replies" 处警情况,vjo."casemarkok"警情标注,vjo."lngofcriterion" 经度,vjo."latofcriterion"纬度, LEFT(vjo."cmdid", 6) AS "diqu" FROM "ywdata"."v_jq_optimized" vjo WHERE vjo."calltime" BETWEEN %s AND %s AND vjo."leixing" IN ({类型})```
                1. 时间段:calltime
                2. 类型:"leixing" IN ({类型})
                3. 地区:通过"diqu"判断,值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            2. 行政:```SELECT * FROM "ywdata"."zq_zfba_ajxx" WHERE "ajxx_ajlx"='行政'```
                1. 时间段:ajxx_lasj
                2. 类型:ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过"ajxx_cbdw_bh_dm"判断(LEFT(...,6)),值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            3. 刑事:```SELECT * FROM "ywdata"."zq_zfba_ajxx" WHERE "ajxx_ajlx"='刑事'```
                1. 时间段:ajxx_lasj
                2. 类型:ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过"ajxx_cbdw_bh_dm"判断(LEFT(...,6)),值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            4. 治拘:```SELECT * FROM "ywdata"."zq_zfba_xzcfjds" WHERE xzcfjds_cfzl ~ '拘留'```
                1. 时间段:xzcfjds_spsj
                2. "ajxx_ajbh"与"ywdata"."zq_zfba_ajxx"表的"ajxx_ajbh"关联后通过"ywdata"."zq_zfba_ajxx"表的ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))过滤
                3. 地区:通过"xzcfjds_cbdw_bh_dm"判断(LEFT(...,6)),值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            5. 刑拘:```SELECT * FROM "ywdata"."zq_zfba_jlz"```
                1. 时间段:jlz_pzsj
                2. 类型:jlz_ay_mc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 通过"jlz_cbdw_bh_dm"判断(LEFT(...,6)),值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            6. 逮捕:```SELECT * FROM "ywdata"."zq_zfba_dbz"```
                1. 时间段:dbz_pzdbsj
                2. 类型:dbz_dbyy similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))
                3. 地区:通过"dbz_cbdw_bh_dm"判断(LEFT(...,6)),值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            7. 起诉(人员):```SELECT  * FROM "ywdata"."zq_zfba_qsryxx"```
                1. 时间段:ysajtzs_pzsj
                2. 类型:"ajxx_ajbh"与"ywdata"."zq_zfba_ajxx"表的"ajxx_ajbh"关联后通过"ywdata"."zq_zfba_ajxx"表的ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))过滤
                3. 地区:关联案件表后通过"ajxx_cbdw_bh_dm"判断(LEFT(...,6)),值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
            8. 移送(案件):```SELECT * FROM "ywdata"."zq_zfba_ysajtzs"```
                1. 时间段:qsryxx_tfsj
                2. 类型:"ajxx_ajbh"与"ywdata"."zq_zfba_ajxx"表的"ajxx_ajbh"关联后通过"ywdata"."zq_zfba_ajxx"表的ajxx_aymc similar to (SELECT ctc.ay_pattern FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" IN ({类型}))过滤
                3. 地区:通过"ysajtzs_cbdw_bh_dm"判断(LEFT(...,6)),值为'445302'='云城',值为'445303'='云安',值为'445381'='罗定',值为'445321'='新兴',值为'445322'='郁南',值为'445300'='市局',{所有}='全市'
    2. 目录结构:相关代码文件在hqzcsj中新建,前缀全部为zfba_jq_aj_*,包含html,route,service,dao等文件全部新建
===
# 任务:根据以下要求生成开放清单,如有疑问则向我提问,如有更好的提议则向我提出
## 1. 当hqzcsj\templates\zfba_jq_aj_tab.html页面初始化时,类型什么都不选择时，所有数据源后台查询条件不加类型的过滤条件,即把类型相关的过滤条件删除,在所有数据源查询的where条件后面加1=1,如果类型有值则将'AND leixing =ANY(...)'拼接上去,如果类型没值则不拼接
## 地区的统计字段修改:
### 警情不变,行政、刑事改为LEFT("ajxx_cbdw_bh_dm",6),治拘改为LEFT("xzcfjds_cbdw_bh_dm",6),刑拘改为LEFT("jlz_cbdw_bh_dm",6),逮捕改为LEFT("dbz_cbdw_bh_dm",6),起诉改为LEFT("ajxx_cbdw_bh_dm",6),移送改为LEFT("ysajtzs_cbdw_bh_dm",6)

## 开放清单（2026-01-30）

### 已完成
- [x] 类型未选择时：后端不追加类型过滤；仅当 `leixing_list` 有值时拼接 `AND vjo."leixing" = ANY(%s)`（警情数据源）。
- [x] 所有数据源：在 SQL 的 `WHERE ...` 后追加 `AND 1=1`，保证后续动态条件统一以 `AND ...` 拼接。
- [x] 地区统计字段口径调整（汇总 + 明细）：
  - 警情：不变（`LEFT(vjo."cmdid", 6)`）
  - 行政/刑事：`LEFT(ajxx_cbdw_bh_dm, 6)`
  - 治拘：`LEFT(xzcfjds_cbdw_bh_dm, 6)`
  - 刑拘：`LEFT(jlz_cbdw_bh_dm, 6)`
  - 逮捕：`LEFT(dbz_cbdw_bh_dm, 6)`
  - 起诉：`LEFT(ajxx_cbdw_bh_dm, 6)`（通过关联案件表取地区口径）
  - 移送：`LEFT(ysajtzs_cbdw_bh_dm, 6)`

### 待你确认
- [ ] 以上新增/改用字段在当前库表/宽表里是否都存在：`ajxx_cbdw_bh_dm`、`xzcfjds_cbdw_bh_dm`、`jlz_cbdw_bh_dm`、`dbz_cbdw_bh_dm`、`ysajtzs_cbdw_bh_dm`。
- [ ] “起诉”地区口径：确认是否必须按案件 `ajxx_cbdw_bh_dm` 统计（而不是起诉表自身的 `qsryxx_cbqy_bh`）。

### 建议（可选）
- [ ] 前端多选框目前默认“全选”；如果你希望“初始化全不选但默认全量”，我可以把默认勾选改为不勾选（UI 文案保持“未选择(默认全量)”）。
