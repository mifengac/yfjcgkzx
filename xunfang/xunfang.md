# 任务:帮我在xunfang中新增一个tab页,名称是jiemiansanllei,html、service、routes、dao都是用"jiemiansanlei"开头
# 逻辑:根据前端条件"时间范围"和"警情类型"从人大金仓数据库读取数据"jingqings",然后使用训练好的模型"xunfang\5lei_dizhi_model"对"jingqings"的"occuraddress"列的值进行分类,将"分类结果"和"置信度"pred_prob写入到结果中,然后显示,同时支持xlsx和xls格式的下载
## 在"xunfang"模型中新增一个tab页名为"街面三类警情"
## 查询区:
### 下拉多选框名为"警情性质":从"ywdata"."case_type_config"表获取,显示用"leixing"字段,格式为text.值为"newcharasubclass_list"格式为数组,如{02011111，020222222}
### 时间范围控件:开始时间和结束时间,格式为'YYYY-MM-DD HH:MM:SS',默认为前7天的'00:00:00',如今天是'2026-01-16',那开始时间默认是'2026-01-09 00:00:00',结束时间默认为'2026-01-16 00:00:00'
### 下拉多选框,名为"yuanshiqueren",包含'原始','确认'两个选项;
    1.当选择'原始'时,SQL:为```SELECT jq.cmdname,jq.calltime,jq.occuraddress,jq.neworicharasubcategoryname FROM ywdata.zq_kshddpt_dsjfx_jq jq WHERE jq.neworicharasubclass IN(SELECT UNNEST(ctc.newcharasubclass_list)FROM ywdata.case_type_config ctc) WHERE ctc.leixing in ({警情性质}) AND jq.calltime BETWEEN {开始时间} AND {结束时间}```
    2.当选择'确认'时,SQL:为```SELECT jq.cmdname,jq.calltime,jq.occuraddress,jq.newcharasubcategoryname FROM ywdata.zq_kshddpt_dsjfx_jq jq WHERE jq.newcharasubclass IN(SELECT UNNEST(ctc.newcharasubclass_list)FROM ywdata.case_type_config ctc )WHERE ctc.leixing in ({警情性质}) AND jq.calltime BETWEEN {开始时间} AND {结束时间}```
### 单击按钮"导出":点击导出时下拉出现'xls','xlsx'两个选项,点击对应选项,现在对应格式文件,下载名称格式为"街面三类警情地址分类"+{时间戳}.xlsx
    1. 当警情性质多选时,每个警情性质一个sheet,每个sheet命名规则为{yuanshiqueren}{警情性质}+"地址分类"
    2. 当"yuanshiqueren"多选时,也分别对应一个sheet,比如用户警情性质选了'人身伤害类','扰乱秩序类',"yuanshiqueren"选了'原始','确认',那最终导出4个sheet,分别名为"原始人身伤害类地址分类","原始扰乱秩序类地址分类","确认人身伤害类地址分类","确认扰乱秩序类地址分类"
## 数据显示区:用来显示数据库查询分类后的数据