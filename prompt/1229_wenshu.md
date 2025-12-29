# 任务:请你按照我下面的需求帮我生成开发清单
1. 在C:\Users\So\Desktop\project\yfjcgkzx1227\jingqing_anjian新增一个tab页名为'警情案件处罚查询与统计',写代码时都要新建文件进行开发,代码都写在C:\Users\So\Desktop\project\yfjcgkzx1227\jingqing_anjian中,命名规则为'jqaj_jqajcfcxytj_xxx',其中xxx代表对应的架构层,如routes,service,dao等
2. 逻辑为:页面初始化主要有'筛选区','数据显示区'两个模块,其中数据显示区的表格中都是数字,点击数字会弹出新页面,在另一个页面弹出,而不是当前页面,新页面只有一个'数据显示区',展示的数据是点击的数字对应的详细数据,其中两个页面的'数据显示区'都有一个有Hover样式的'导出'按钮,鼠标移上去后'导出'按钮下面下拉出'excel','csv'两个按钮,点击对应的按钮会下载对应格式的文件,文件数据就是当前'数据显示区'的数据
3. 页面布局和数据获取
    1. 筛选区:
        1. 下拉多选框'警情类型'leixing,通过对```SELECT ctc."leixing" FROM "case_type_config" ctc ```获取
        2. '开始时间'kssj,'结束时间'jssj,格式均为'YYYY-MM-DD HH:MM:SS'
        3. '查询'按钮
    2. 警情案件处罚查询与统计:数据显示区(jqajcfcxytjs):
        1. 最右侧有'导出'按钮,鼠标移上去显示'excel','csv'两个下拉按钮,点击对应按钮下载'数据显示区'中的数据
        2. 数据显示区:
            1. 字段有:地区,警情,同比警情,行政,同比行政,刑事,同比刑事,治拘,同比治拘,刑拘,同比刑拘,起诉,同比起诉,移送人员,同比移送人员,移送案件,同比移送案件
            2. 数据集jqajcfcxytjs:先通过SQL查询,之后在后端使用python进行二次统计,逻辑为:
                1. 查询前需要先通过前端页面的kssj,jssj计算同比的开始时间tbkssj,同比的结束时间tbjssj,如kssj为'2025-01-01 00:00:00',jssj为'2025-12-22 00:00:00',那么tbkssj值为'2024-01-01 00:00:00',tbjssj值为'2024-12-22 00:00:00'
                1. SQL
                    1. jingqings:
                        ```
                        SELECT *,LEFT(vjo."cmdid",6)AS "diqu" FROM "v_jq_optimized" vjo WHERE vjo."calltime" BETWEEN  {tbkssj}AND {jssj}AND "leixing" ={leixing}
                        ```
                    2. anjians:
                        ```
                        SELECT * FROM "mv_zfba_all_ajxx" mzaa WHERE mzaa."立案日期"  BETWEEN  {tbkssj}AND {jssj} AND mzaa."案由" SIMILAR TO (SELECT ay_pattern FROM "case_type_config" ctc WHERE ctc."leixing"={leixing})
                        ```
                    3. wenshus:
                        ```
                            WITH cfg AS (
                                    SELECT ay_pattern
                                    FROM ywdata.case_type_config
                            WHERE "leixing" ={leixing}
                                ),
                                ws_dedup AS (
                                    SELECT DISTINCT ON (ws.wsywxxid)
                                        ws.*
                                    FROM ywdata.mv_zfba_wenshu ws
                                    WHERE COALESCE(ws.spsj, ws.tfsj) >= {tbkssj}::timestamp
                                    AND COALESCE(ws.spsj, ws.tfsj) <= {jssj}::timestamp
                                    ORDER BY ws.wsywxxid, ws.tfsj DESC NULLS LAST
                                ),
                                aj_dedup AS (
                                    SELECT DISTINCT ON (aj.asjbh)
                                        aj.*
                                    FROM ywdata.zfba_aj_003 aj
                                    WHERE ('打架斗殴' IS NULL OR '打架斗殴' = '')
                                    OR EXISTS (
                                            SELECT 1
                                            FROM cfg c
                                            WHERE aj.aymc SIMILAR TO c.ay_pattern
                                        )
                                    ORDER BY aj.asjbh, aj.xgsj DESC NULLS LAST
                                )
                            ,base AS (
                                    SELECT
                                        LEFT(ws.badwdm, 6)AS region,
                                        ws.badwmc,
                                        ws.flws_dxbh,
                                        ws.flws_bt,
                                        ws.tfsj,
                                        ws.spsj,
                                        ws.asjbh,
                                        ws.asjmc,
                                        ws.wsywxxid,
                                        aj.aymc,
                                        p.sfjg,
                                        p.jlts,
                                        p.fk,
                                        ws.flws_zlmc,
                                        ws.flws_dxlxdm,
                                        ws.flws_dxbxm
                                    FROM ws_dedup ws
                                    LEFT JOIN aj_dedup aj ON ws.asjbh = aj.asjbh
                                    LEFT JOIN ywdata.zfba_aj_009 p ON ws.wsywxxid = p.wsywxxid
                                )
                                SELECT
                                    b.region::text AS region,
                                    b.badwmc::text AS badwmc,
                                    b.wsywxxid::TEXT AS wsywxxid,
                                    b.flws_dxlxdm::TEXT AS dxlxdm,
                                    b.flws_dxbh::text AS flws_dxbh,
                                    b.flws_dxbxm::text AS flws_dxbxm,
                                    b.flws_bt::text AS flws_bt,
                                    COALESCE(b.spsj,b.tfsj)AS spsj,
                                    b.asjbh::text AS asjbh,
                                    b.asjmc::text AS asjmc,
                                    COALESCE(b.sfjg,'0') AS jinggao,
                                    COALESCE(fk,'0') AS fakuan,
                                    COALESCE(jlts,'0') AS zhiju
                                FROM base b
                                WHERE  b.aymc IS NOT NULL
                        ```                    
                3. 1中查询到的数据进行二次计算,其中要提取时间段,带有'同比'的值时间段为{tbkssj}至{tbjssj},没带'同比的值时间段为{kssj}至{jssj}:
                    1. 地区:地区是固定值,通过对jingqings的"diqu"字段的值,anjians的"地区"字段的值,wenshus的"region"字段的值进行分组,然后映射,对应值的映射逻辑为:{'445302':'云城','445303':'云安','445381':'罗定','445321':'新兴','445322':'郁南','445300':'市局','其他'}
                    2. 警情/同比警情:jingqings中的"caseno"去重计数
                    3. 行政/同比行政:anjians中的"案件类型"值为'行政'的值的计数
                    4. 刑事/同比刑事:anjians中的"案件类型"值为'刑事'的值的计数
                    5. 治拘/同比治拘:wenshus中的"zhiju"值不为'0'的值的计数
                    6. 刑拘/同比刑拘:wenshus中的"flws_bt"包含'拘留证'的值对"wsywxxid"字段值去重计数
                    7. 起诉/同比起诉:wenshus中的"flws_bt"包含'起诉意见'的值对"wsywxxid"字段值去重计数
                    8. 移送人员/同比移送人员:wenshus中"dxlxdm"值为'01'且"flws_bt"包含'移送'的并对"wsywxxid"字段值去重计数
                    9. 移送案件/同比移送案件:wenshus中"dxlxdm"值为'04'且"flws_bt"包含'移送'的并对"wsywxxid"字段值去重计数
    3. 点击'警情案件处罚查询与统计'表格数字后新页面的:数据显示区(jqajcfcxytj_details):根据过滤条件直接显示SQL查询结果(jingqings,anjians,wenshus),不需要进行二次计算