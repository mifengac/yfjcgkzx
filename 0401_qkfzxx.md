# 任务1:帮我梳理数据逻辑,使用kingbase mcp,当前环境是互联网环境,实际运行环境是内网(无互联网),当前环境数据库都是结构没有数据,数据都在内网,各个维度要留好数据过滤接口,方便最后按照学校对齐,可以通过时间过滤
## 现在要做一个全市情况复杂学校的赋分排名,主要通过5个维度,下面你帮我梳理下SQL数据,你帮我分析下是做视图还是直接写SQL,是写5个SQL或视图还是写一个,可以根据任务2的业务需求不断优化SQL 
### 全市学校信息:
    - 1. ywdata.sh_gd_zxxxsxj_xx(中小学学生学籍信息),22W条数据
    - 2. ywdata.sh_yf_zzxj_xx(中职学籍信息),54W条
    - 3. 按照xxbsm(学校标识码),xxmc(学校名称),zgjyxzbmmc(主管教育局),将两个表合并,形成最终学校表
### 维度1:累计送生人数(送专门教育学校):通过ywdata.zq_zfba_wcnr_sfzxx表对`yxx`(学校名称)分组计数,计数值越高排名越高
### 维度2:涉校警情:参考SQL\0401_jingqingfuzaxuexiao.sql,最终按照计数值排名,计数值越高排名越高
    - 1. 现在警情查询到的学校部分是不标准的,比如有些警情查询到的警情学校名称是'XXXXX路-AB中学',其实该学校名称是'AB中学'你帮我分析下这个学校最终对齐是在SQL中还是在python代码中
### 维度3:案件团伙数,主要统计违法人数3人以上的学校占比,占比越高排名越高,参考SQL\0401_anjiantuanhuo_xx.sql,这里只统计3人及以上的团伙
### 维度4:辍学人数,通过ywdata.b_per_qscxwcnr(全市辍学未成年人)过滤,规则为:
    1. 通过ywdata.b_per_qscxwcnr的`zjhm`与ywdata.sh_gd_zxxxsxj_xx和 ywdata.sh_yf_zzxj_xx表的`sfzjh`字段匹配,如果两个表存在同一数据,则以ywdata.sh_yf_zzxj_xx表的数据为准,ywdata.sh_gd_zxxxsxj_xx表的数据不要
    2. 最后按照学校分组计数,计数值越高排名越高
### 维度5:夜不归宿学生人数:通过ywdata.t_spy_ryrlgj_xx(视频人脸轨迹信息)拿基础数据,参考SQL\0401_qkfzxx.sql,需要注意的问题
    - 1. 数据库有ywdata.str_to_ts,函数,主要是ywdata.t_spy_ryrlgj_xx.shot_time转换为时间的函数,用于优化查询,因为该函数有一个索引`CREATE INDEX idx_order_shot_time ON ywdata.t_spy_ryrlgj_xx USING btree (ywdata.str_to_ts((shot_time)::text)) TABLESPACE sys_default;`用来优化查询,你按照这个索引优化SQL,
    - 2. `shot_time`字段在数据库中实际值的格式为'YYYYMMDDMMHHSS',是字符串格式

# 任务2:按照任务1的数据,帮我新增一个模块,名为"学校赋分模块",新建一个文件夹名为,"xxffmk",同时模块内网同步新建对应的文件夹,
## 新增一个tab页面,名为"情况复杂学校赋分",以tab页的形式展示,页面样式使用static\css\water.css,布局参考jingqing_fenxi\templates\jingqing_fenxi_index.html即可
## 页面布局
### 1. 查询区域:
    - 1. 时间范围控件,参考jingqing_fenxi\中'警情分析'tab页的`开始时间`,`结束时间`格式
        + 1. 维度1使用`rx_time`字段过滤
        + 2. 维度2使用`calltime`字段过滤
        + 3. 维度3使用`xyrxx_lrsj`字段过滤
        + 4. 维度4不过滤时间,始终使用全量
        + 5. 维度5使用`shot_time`字段过滤
    - 2. 查询按钮,点击查询通过过滤参数和SQL查询数据展示
    - 3. 显示排名数,默认为10,只显示前十名,通过条件可以动态显示
### 2. 数据展示区
    - 1. 默认只显示学校及其分值
    - 2. 单击学校可以显示各个维度的分值
    - 3. 单击各个维度可以显示赋分的具体情况
### 3. 赋分规则:
    1. 维度1第一名20分,维度2第一名15分,维度3第一名15分,维度4第一名20分,维度5第一名10分
    2. 各维度按名次依次减1分,比如维度1第2名是19分,维度2第二名是14分