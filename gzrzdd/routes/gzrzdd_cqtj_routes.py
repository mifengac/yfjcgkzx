"""
# 任务:帮我在"gzrzdd"模块中新增一个tab页,名为"工作日志超期统计",原来的页面C:\Users\So\Desktop\yfjcgkzx0111\gzrzdd\templates\gzrzdd.html名为"工作日志重复度统计"
# 逻辑:
1. 从"人员表"读取人员信息,其中包含一个"fxdj"(风险等级)字段
2. 从"日志表"根据人员身份证号码获取最新的日志时间,并与现在时间进行计算,然后根据"fxdj"判断是否超期
3. 
# 表结构
1. 人员表:stdata.b_per_mdjffxrygl,初始化SQL:```SELECT xm as 姓名, zjhm as 证件号码,lxdh as 联系电话,hjdz as 户籍地址 ,jzdz 居住地址,CASE WHEN substring(lgdw,1,6)='445302' then '云城分局' WHEN substring(lgdw,1,6)='445303' then '云安分局' WHEN substring(lgdw,1,6)='445321' then '新兴' WHEN substring(lgdw,1,6)='445322' then '郁南' WHEN substring(lgdw,1,6)='445381' then '罗定' ELSE lgdw end as 地区,lgsj AS 列管时间 FROM stdata.b_per_mdjffxrygl where deleteflag='0' AND gkzt='01'```
2. 日志表:stdata.b_zdry_ryxx_gzrz,初始化SQL:```SELECT * FROM stdata.b_zdry_ryxx_gzrz WHERE deleteflag='0'```
3. 派出所编码表:stdata.b_dic_zzjgdm,其中"sspcsdm"与人员表的"lgdwdm"匹配
    
"""