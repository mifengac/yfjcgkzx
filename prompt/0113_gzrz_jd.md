# 帮我将以下逻辑生成开发清单,如有疑问则向我提问,待我确认后按照开发清单开发任务
1. 在当前项目新增一个模块,名称为"gzrzdd",即再gzrzdd文件夹新增模块,数据处理逻辑和prompt\0113_tf-idf_rz.py逻辑一样,不过数据源改为了从数据库获取数据
2. 在项目主页新增一个按钮,名称为"工作日志督导",单击按钮跳转到"gzrzdd"页面
    1. 页面布局
        1. 条件设置区
            1. 文本输入框,只能输入数字,默认为5,名为count,设置在"统计近"后面,即"统计近"{count}"次工作日志
            2. 文本输入框,只能输入数字,默认80,名为chongfudu,后面有一个"%",即{chongfudu}%
            3. 单击按钮"统计"单击该按钮按照逻辑统计显示数据
        2. 数据展示区
            1. 页面展示数据最终通过2次计算得到,计算逻辑如下
                1. 从人大金仓数据库根据SQL获取数据,SQL由用户提供,查询SQL得到"gzrzs"
                2. 在python中统计
                    1. 先根据前端{count}值,按照"gzrzs"中的"证件号码"字段分组,"xxxxx"倒序排列,获取最新的{count}条值
                    2. 对1过滤后的数据按照,XXX,XXX分组,最终显示如下,其中表格中的数字可以点击,单击后弹出新页面显示对应的详细数据
                        |        | A区 | B区 | C市 | D县 | E县 |
                        | ------ | --- | --- | --- | --- | --- |
                        | XXX所  |     |     |     |     |     |
                        | XX所   |     |     |     |     |     |
        3. 数据展示区和单击数据弹出新页面的详细数据都可以下载,在数据展示右上角设置"导出"按钮,单击按钮下拉显示"xlsx,csv"两个值,单击对应的值下载对应格式的文件,其中数据展示区文件名格式为"各地最近{count}条工作日志重复度"+{时间戳},新页面详细数据的文件名格式为"最近{count}条重复工作日志详情"+{时间戳}
===
1. 删除C:\Users\So\Desktop\yfjcgkzx0111\gzrzdd\templates\gzrzdd.html中的SQL控件,这个SQL放在后端代码中,值为```SELECT a.xm as 姓名,a.zjhm as 证件号码,CASE WHEN substring(a.lgdw,1,6)='445302' THEN '云城' WHEN  substring(a.lgdw,1,6)='445303' THEN '云安' WHEN substring(a.lgdw,1,6)='445321' THEN '新兴' WHEN substring(a.lgdw,1,6)='445322' THEN '郁南' WHEN substring(a.lgdw,1,6)='445381' THEN '罗定' ELSE a.lgdw END AS 分局名称,b.sspcs AS 所属派出所,a.lgsj AS 列管时间,c.kzgzsj AS 工作日志开展工作时间,d.detail AS 工作日志工作类型,c.gzqksm AS 工作日志工作情况说明,c.djsj AS 工作日志系统登记时间 FROM (SELECT * FROM stdata.b_per_mdjffxrygl WHERE "deleteflag"='0' AND gkzt='01')a LEFT JOIN stdata.b_dic_zzjgdm b ON a.lgdw =b.sspcsdm LEFT JOIN (SELECT * FROM stdata.b_zdry_ryxx_gzrz WHERE deleteflag='0') c ON a.systemid =c.zdryid LEFT JOIN (SELECT * FROM stdata.s_sg_dict WHERE  kind_code ='ZAZDRY_GZRZ_GZLX') d ON c.gzlx=d.code WHERE c.kzgzsj >='2025-1-1'
2. 初始化时计算完重复度后先按照"证件号码"分组,且将"工作日志开展工作时间"列使用逗号分隔进行拼接,然后再按照"分局名称"和"所属派出所"分组计数,比如A分局和b派出所发现证件号码为x的人最近2次(2025-01-01,2025-01-09)日志重复度超过80%,那最终按照x分组,将工作日志开展工作时间按逗号分隔拼接为'2025-01-01,2025-01-09',那A分局和b派出所分组计数后得到的值为1,点击'1'弹出的详细数据也是1条,即按照证件号码拼接好的数据