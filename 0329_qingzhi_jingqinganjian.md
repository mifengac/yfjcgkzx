# 任务:帮我在该系统新增一个模块,名为"警情案件分析",结构参考之前的模块,比如yfjcgkzx\jingqing_fenxi模块,另外你帮我分析下数据库查询是用视图还是直接写SQL好,如有任何问题或更好的建议则向我提出
## 数据源:
    - 1. 警情:"ywdata"."zq_kshddpt_dsjfx_jq"
    - 2. 案件:"ywdata"."zq_zfba_ajxx"
    - 3. 嫌疑人:"ywdata"."zq_zfba_xyrxx"
    - 4. 地区:"stdata"."b_dic_zzjgdm":{县市区:XXXXXX000000,派出所:XXXXXXXX0000}
### 数据源关联
    - 1. 警情-案件:通过警情编号关联:{警情:caseno,案件:ajxx_jqbh},关联关系,1比1,但不是严格的1:1,部分警情编号在案件中是匹配不到的
    - 2. 案件-嫌疑人:通过案件编号关联:{案件:ajxx_ajbh,嫌疑人:ajxx_join_ajxx_ajbh},关联关系1对多
## 初始化页面显示字段(初始化只显示计数值,且计数值可点击,点击后弹出详细信息(新页面)):
    - 1. 及时立案方面(平均)
    - 2. 及时研判抓人方面(平均)
    - 3. 及时破案方面(平均)
    - 4. 及时结案方面(平均)
## 计算逻辑:
    - 1. 及时立案方面:有关联警情的案件(即通过警情的警情编号与案件的警情编号关联,能关联到的案件(部分案件关联不到,关联不到的过滤掉)中,立案时间("ywdata"."zq_zfba_ajxx".ajxx_lasj)—接警时间("ywdata"."zq_kshddpt_dsjfx_jq".calltime),显示单位为小时,最后通过县市区/派出所分组取平均值
    - 2. 及时研判抓人方面:
        + 1. 有关联警情的案件,嫌疑人到案时间—接警时间(此处有关联判断条件与1不同,此处通过"ywdata"."zq_zfba_ajxx".ajxx_jqbh的开头第一个字符判断,如果是'4'则为有关联警情,如果不是则为无关联警情案件)嫌疑人到案时间(通过"ywdata"."zq_zfba_xyrxx".ajxx_join_ajxx_ajbh分组按照录入时间顺序排列(xyrxx_lrsj),即第一个录入的嫌疑人)—接警时间("ywdata"."zq_kshddpt_dsjfx_jq".calltime),,显示单位为小时,最后通过县市区/派出所分组取平均值
        + 2. 无关联警情案件,嫌疑人到案时间("ywdata"."zq_zfba_xyrxx".xyrxx_lrsj)—立案时间("ywdata"."zq_zfba_ajxx".ajxx_lasj),,显示单位为小时,最后通过县市区/派出所分组取平均值
    - 3. 及时破(结)案方面:
        + 1. (破案)刑事案件("ywdata"."zq_zfba_ajxx".ajxx_ajlx='刑事' AND "ajxx_pxjabs_dm" ='1')：破案时间("ywdata"."zq_zfba_ajxx"."ajxx_pxjarq")——立案时间("ywdata"."zq_zfba_ajxx".ajxx_lasj),显示单位为小时,最后通过县市区/派出所分组取平均值
        + 2. (结案)行政案件("ywdata"."zq_zfba_ajxx".ajxx_ajlx='行政' AND ajxx_ajzt_dm not in ('0101','0104','0112','0114')),处罚时间("ywdata"."zq_zfba_ajxx"."ajxx_cfsj")—立案时间("ywdata"."zq_zfba_ajxx".ajxx_lasj),,显示单位为小时,最后通过县市区/派出所分组取平均值
## 地区分组:
    - 1. 县市区,匹配通过"ssfjdm"匹配,显示通过"ssfj"显示:
        1. 及时立案方面:通过对"ywdata"."zq_kshddpt_dsjfx_jq".cmdid匹配分组
        2. 及时研判抓人方面:通过对LEFT("ywdata"."zq_zfba_ajxx".zza."ajxx_cbdw_bh_dm" ,6)||'000000'字段进行分组
        3. 及时破案方面:通过对LEFT("ywdata"."zq_zfba_ajxx".zza."ajxx_cbdw_bh_dm" ,6)||'000000'字段进行分组
    - 2. 派出所,匹配通过"ssfjdm"匹配,显示通过"ssfj"显示:
        1. 及时立案方面:通过对"ywdata"."zq_kshddpt_dsjfx_jq".dutydeptno匹配分组 
        2. 及时研判抓人方面:通过对LEFT("ywdata"."zq_zfba_ajxx".zza."ajxx_cbdw_bh_dm",8)||'0000'字段进行分组
        3. 及时破案方面:通过对LEFT("ywdata"."zq_zfba_ajxx".zza."ajxx_cbdw_bh_dm" ,8)||'0000'字段进行分组
## 页面布局
### 查询区:
    - 1. 立案时间:控件为日期时间范围控件,格式为'YYYY-MM-DD HH:MM:SS',默认为近7天的值,如今天是'2026-03-29',则默认时间为'2026-03-22 00:00:00'-'2026-03-29 00:00:00'
    - 2. 分局:数据源为:```SELECT DISTINCT  bdz."ssfj",bdz."ssfjdm"  FROM "stdata"."b_dic_zzjgdm" bdz ```,其中显示通过bdz."ssfj",实际过滤值使用bdz."ssfjdm"
    - 3. 类型:多选下拉框:数据源为```SELECT leixing FROM "ywdata"."case_type_config" ctc ```
        + 1. 初始化时默认不添加该过滤参数,即SQL后面不拼接该条件
        + 2. 当用户勾选类型时:
            - 1. 及时立案方面: ```AND "ywdata"."zq_kshddpt_dsjfx_jq"."neworicharasubclass" IN (SELECT UNNEST ( ctc."newcharasubclass_list") FROM "case_type_config" ctc WHERE ctc."leixing"= {类型})```
            - 2. 及时研判抓人方面:``` AND "ywdata"."zq_zfba_ajxx"."ajxx_aymc"  SIMILAR TO (SELECT ctc."ay_pattern" FROM "case_type_config" ctc WHERE ctc."leixing" ={类型})```
            - 3. 及时破案/结案方面(平均):``` AND "ywdata"."zq_zfba_ajxx"."ajxx_aymc"  SIMILAR TO (SELECT ctc."ay_pattern" FROM "case_type_config" ctc WHERE ctc."leixing" ={类型})```
    - 4. 查询:单击按钮,点击查询在数据展示区显示查询结果
    - 5. 导出:点击导出显示下拉框'xlsx','csv',点击对应的格式下载对应格式的文件,文件命名为
    - 6. 派出所:滑动按钮,打开按钮,统计维度改为"派出所"
### 数据展示区:
    1. 不管是县市区还是派出所分组,最后都要加一行汇总行,显示为"全市"
    2. 初始化显示的是分组平均值,单击分组值可弹出详细页面,显示详细数据,详细页面的数据同样支持导出为xlsx或csv
---

1. 数据展示区列名乱码了
2. 警情性质的控件有点窄,点击警情性质后,部分值右侧被挡住了,需要拖动
3. 修改查询及导出的数据源,现在改为不需要从数据库获取数据,而是从68.253.2.111/dsjfx/case/list获取
    - 1. 初始化警情性质口径为'原始'时:警情性质通过ywdata.case_type_config.newcharasubclass_list值与接口中`newOriCharaSubclassNo`匹配,其中newcharasubclass_list是{1xxx,2xxx}格式,`newOriCharaSubclassNo`是:1xxx,2xxx格式
    - 2. 初始化警情性质口径为'确认'时:警情性质通过ywdata.case_type_config.newcharasubclass_list值与接口中`newCharaSubclassNo`匹配,其中newcharasubclass_list是{1xxx,2xxx}格式,`newCharaSubclassNo`是:1xxx,2xxx格式
4. 当勾选未成年独立复选框时:接口参数`caseMarkNo`增加值'01020201,0102020101,0102020102,0102020103'4个值