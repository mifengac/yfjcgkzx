# 任务1:
    - 在"获取综查数据"的"警情案件统计"中新增几个字段
## 新增字段及逻辑
    - 案件数,同比案件数:
         - 数据来源:
            ```
                SELECT
                    ajxx_ajbh AS "案件编号",
                    ajxx_jqbh AS "警情编号",
                    ajxx_ajmc AS "案件名称",
                    ajxx_ajlx AS "案件类型",
                    ajxx_ajzt AS "案件状态",
                    ajxx_ay AS "案由",
                    ajxx_ay_dm AS "案由代码",
                    ajxx_fasj AS "发案时间",
                    ajxx_lasj AS "立案时间",
                    ajxx_sldw_mc AS "受理单位",
                    ajxx_cbdw_mc AS "承办单位",
                    LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                    ajxx_zbbj AS "在办标记",
                    ajxx_ajly AS "案件来源"
                FROM "ywdata"."zq_zfba_ajxx"
                WHERE ajxx_lasj BETWEEN %s AND %s
                AND 1=1
            ```
        - 过滤规则,不需要过滤ajxx_ajlx条件,其实就是'行政'和'刑事'相加
    - 治拘、同比治拘改为治安处罚、同比治安处罚，同时在过滤区新增一个"多选下拉框",名为治安处罚类型,有'警告','罚款','拘留'三个选项,通过对xz.xzcfjds_cfzl ~ 进行判断,参考hqzcsj\templates\zfba_wcnr_jqaj_tab.html模块:
    - 办结、同比办结:
        - 数据源：
            ```
                SELECT
                    ajxx_ajbh AS "案件编号",
                    ajxx_jqbh AS "警情编号",
                    ajxx_ajmc AS "案件名称",
                    ajxx_ajlx AS "案件类型",
                    ajxx_ajzt AS "案件状态",
                    ajxx_ay AS "案由",
                    ajxx_ay_dm AS "案由代码",
                    ajxx_fasj AS "发案时间",
                    ajxx_lasj AS "立案时间",
                    ajxx_sldw_mc AS "受理单位",
                    ajxx_cbdw_mc AS "承办单位",
                    LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                    ajxx_zbbj AS "在办标记",
                    ajxx_ajly AS "案件来源"
                FROM "ywdata"."zq_zfba_ajxx"
                WHERE ajxx_lasj BETWEEN  %s AND %s
                AND "ajxx_ajzt" IN ('已立案','已受案','已受理')
                AND 1=1
            ```
    - 高质量、同比高质量:
        - 数据源
            ```
            SELECT zza."ajxx_ajbh"案件编号 ,count(zzj."jlz_id")刑拘人数,zza."ajxx_lasj"立案时间 ,zza."ajxx_cbqy_jc" 承办区域,left("ajxx_cbdw_bh_dm",6)地区,zza."ajxx_cbdw_bh"办案单位 ,zza."ajxx_ajzt" 案件状态,zza."ajxx_fadd"发案地点 ,zza."ajxx_fasj" 发案时间, zza."ajxx_jyaq"简要案情 
            FROM "zq_zfba_ajxx" zza LEFT JOIN "zq_zfba_jlz" zzj ON zza."ajxx_ajbh" =zzj."ajxx_ajbh" 
            WHERE zza."ajxx_ajlx" ='刑事'  AND zza."ajxx_lasj"  BETWEEN '2026-01-01' AND '2026-02-04' GROUP BY 
            zza."ajxx_ajbh" ,zza."ajxx_lasj" ,zza."ajxx_cbqy_jc" ,zza."ajxx_cbdw_bh" ,zza."ajxx_jyaq" ,zza."ajxx_ajzt" ,zza."ajxx_fadd" ,zza."ajxx_fasj" ,left("ajxx_cbdw_bh_dm",6)
            ```
        - 过滤条件:
            - 开始时间、结束时间:zza."ajxx_lasj"
            - 类型: 同之前的zq_zfba_ajxx一样
# 任务2: 在hqzcsj的"未成年人统计"模块新增一个字段:
## 新增字段及逻辑
    - 案件数(被侵害)、同比案件数(被侵害)
        - 数据源:
            ```
                SELECT
                    ajxx_ajbh AS "案件编号",
                    ajxx_jqbh AS "警情编号",
                    ajxx_ajmc AS "案件名称",
                    ajxx_ajlx AS "案件类型",
                    ajxx_ajzt AS "案件状态",
                    ajxx_ay AS "案由",
                    ajxx_ay_dm AS "案由代码",
                    ajxx_fasj AS "发案时间",
                    ajxx_lasj AS "立案时间",
                    ajxx_sldw_mc AS "受理单位",
                    ajxx_cbdw_mc AS "承办单位",
                    LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                    ajxx_zbbj AS "在办标记",
                    ajxx_ajly AS "案件来源"
                FROM "ywdata"."zq_zfba_wcnr_shr_ajxx" 
                WHERE ajxx_lasj BETWEEN '2026-01-01' AND '2026-02-01'
                AND 1=1
            ```
        - 过滤条件:
            - 开始时间、结束时间:ajxx_lasj
            - 类型:和之前的一样

