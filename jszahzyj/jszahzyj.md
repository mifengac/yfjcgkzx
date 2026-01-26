# 任务:帮我在jszahzyj新增一个模块,名为"精神障碍患者预警",文件都放在jszahzyj文件夹,对应功能的文件放在对应文件夹,如html放在templates,js放在static,路由文件放在routes,其他以此类推,文件夹我已创建好,逻辑是通过连接人大金仓数据库获取数据,SQL先空着,到时我拿到内网填
# 页面布局
    1. 标题"精神障碍患者预警"
    2. 过滤控件
        1. 列管时间(时间段),格式为'YYYY-MM-DD HH:MM:SS',结束时间默认为昨日,时间点格式为'00:00:00',开始时间默认为减7日,时间点格式为'00:00:00'
        2. 矛盾纠纷录入时间(时间段),格式为'YYYY-MM-DD HH:MM:SS',结束时间默认为昨日,时间点格式为'00:00:00',开始时间默认为减7日,时间点格式为'00:00:00'
        3. 分局(多选下拉框):'云城','云安','罗定','新兴','郁南'
        4. '查询'按钮
    3. 数据展示:展示SQL查询到的所有数据
    4. 数据展示右上角'导出'按钮,点击'导出'弹出下拉框,显示'xlsx','csv'两个选项,点击对应的选项下载对应格式的按钮,下载文件格式为'精神障碍患者预警'+{时间戳}.xlsx/csv

SELECT
DISTINCT on(a.systemid)	
a.systemid AS 系统编号,
	a.ywlsh AS 业务流水号,
    	a.jfmc AS 纠纷名称,
	c.detail AS 纠纷类型,
	a.jyqk AS 简要情况,
	a.fssj AS 发生时间,
	CASE
		WHEN a.sssj = '445300000000' THEN '云浮市公安局'
		ELSE a.sssj
	END AS "所属市局",
	 CASE
		WHEN substring(a.ssfj, 1, 6)= '445302' THEN '云城分局'
		WHEN substring(a.ssfj, 1, 6)= '445303' THEN '云安分局'
		WHEN substring(a.ssfj, 1, 6)= '445321' THEN '新兴县公安局'
		WHEN substring(a.ssfj, 1, 6)= '445381' THEN '罗定市公安局'
		WHEN substring(a.ssfj, 1, 6)= '445322' THEN '郁南县公安局'
		ELSE a.ssfj
	END AS 分局名称,
	 e.sspcs AS 所属派出所,
	d.detail AS 流转状态,
	a.djsj AS 纠纷登记时间,
	a.djdw_mc AS 纠纷登记单位名称,
	a.xgsj AS 纠纷修改时间,
	b.yjqqsj AS 移交请求时间,
	g.detail AS 粤平安反馈状态,
	CASE
		WHEN b.tczt = '1' THEN '已化解'
		WHEN b.tczt = '0' THEN '未化解'
		ELSE b.tczt
	END AS "调处状态",
	b.rksj AS 入库时间,
	CASE
		WHEN b.orderstate = '2' THEN '已登记:已分发待确认'
		WHEN b.orderstate = '5' THEN '处理中:其他'
		WHEN b.orderstate = '6' THEN '已结案'
		WHEN b.orderstate = '4' THEN '处理中:业务系统已受理'
		ELSE b.orderstate
	END AS "粤平安流程节点状态",
	b.processtime AS 粤平安流程节点时间,
	round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2) AS 粤平安移交时间差,
	case when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=12  and b.yjqqsj is null then '12小时内未移交' 
	 when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=24  and b.yjqqsj is null then '24小时内未移交'
	 when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=48  and b.yjqqsj is null then '48小时内未移交'
	 when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=72  and b.yjqqsj is null then '72小时内未移交'
	 when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)>72  and b.yjqqsj is null then '超出72小时仍未移交'
	 when round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2)<=48 and b.yjqqsj is not null then '48小时内移交'  
	 when round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2)<=72 and b.yjqqsj is not null  then '72小时内移交' else '超出72小时移交' end as "12-24-48-72小时内移交情况"
FROM
	(
		SELECT
			*
		FROM
			stdata.b_per_mdjfjfsjgl
		WHERE
			deleteflag = '0'
			AND sfgazzfw = '0'
			AND djsj >= '2026-01-01' 
	) a
LEFT JOIN (
		SELECT
			*
		FROM
			stdata.b_per_mdjfypafhsj
		WHERE
			deleteflag = '0' 
	) b ON
	a.systemid = b.systemid
LEFT JOIN (
		SELECT
			code ,
			detail
		FROM
			"stdata"."s_sg_dict"
		WHERE
			"kind_code" = 'SQRY_XGNMK_MDJF_JFLX'
	)c ON
	a.jflx = c.code
LEFT JOIN (
		SELECT
			code ,
			detail
		FROM
			"stdata"."s_sg_dict"
		WHERE
			"kind_code" = 'SQRY_XGNMK_MDJF_LCZT'
	)d ON
	a.lczt = d.code
LEFT JOIN (
		SELECT
			code ,
			detail
		FROM
			"stdata"."s_sg_dict"
		WHERE
			"kind_code" = 'SQRY_XGNMK_MDJF_YJFKZT'
	)g ON
	b.yjfkzt = g.code
LEFT JOIN stdata.b_dic_zzjgdm e ON
	a.sspcs = e.sspcsdm WHERE a.lczt<>'6'

SELECT
	a.xm AS 姓名,
	a.zjhm AS 证件号码,
	a.hjdz AS 户籍地址,
	a.lgsj AS 列管时间,
	CASE
		WHEN substring(a.sjgsdwdm, 1, 6)= '445321' THEN '新兴县公安局'
		WHEN substring(a.sjgsdwdm, 1, 6)= '445302' THEN '云城分局'
		WHEN substring(a.sjgsdwdm, 1, 6)= '445381' THEN '罗定市公安局'
		WHEN substring(a.sjgsdwdm, 1, 6)= '445322' THEN '郁南县公安局'
		WHEN substring(a.sjgsdwdm, 1, 6)= '445303' THEN '云安分局'
		ELSE a.sjgsdwdm
	END AS 列管分局名称,
	c.sspcs AS 列管派出所名称,
	b.jfmc AS 纠纷名称,
	b.jyqk AS 简要情况,
	b.mdjfdjsj AS 矛盾纠纷录入时间,
	CASE
		WHEN substring(b.mdjfsjgsdwdm, 1, 6)= '445321' THEN '新兴县公安局'
		WHEN substring(b.mdjfsjgsdwdm, 1, 6)= '445302' THEN '云城分局'
		WHEN substring(b.mdjfsjgsdwdm, 1, 6)= '445381' THEN '罗定市公安局'
		WHEN substring(b.mdjfsjgsdwdm, 1, 6)= '445322' THEN '郁南县公安局'
		WHEN substring(b.mdjfsjgsdwdm, 1, 6)= '445303' THEN '云安分局'
		ELSE b.mdjfsjgsdwdm
	END AS 矛盾纠纷录入分局名称,
	d.sspcs AS 矛盾纠纷录入派出所名称
FROM
	(
	SELECT
			*
	FROM
			stdata.b_per_jszahzryxxwh
	WHERE
			sflg = '1'
		AND "deleteflag" = '0'
) a
LEFT JOIN (
	SELECT
			* ，b1.djsj AS mdjfdjsj,
		b1.sjgsdwdm AS mdjfsjgsdwdm
	FROM
			(
			SELECT
				*
			FROM
				stdata.b_zdry_ryxx_mdjf
			WHERE
				deleteflag = '0'
		) a1
	LEFT JOIN (
			SELECT
					*
			FROM
					stdata.b_per_mdjfjfsjgl
			WHERE
					deleteflag = '0'
		) b1 ON
			a1.mdjflsh = b1.ywlsh
) b ON
	a.zjhm = b.zjhm
LEFT JOIN ywdata.b_zzjgdm c ON
	a.sjgsdwdm = c.sspcsdm
LEFT JOIN ywdata.b_zzjgdm d ON
	b.mdjfsjgsdwdm = d.sspcsdm
WHERE b.jfmc IS NOT NULL