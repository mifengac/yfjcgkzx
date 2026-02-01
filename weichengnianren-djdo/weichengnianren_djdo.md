# 任务:帮我在当前项目中新增一个模块,名为"未成年人(打架斗殴)",相关代码文件全部放在weichengnianren-djdo文件夹中
# 页面布局:
    1. 页面格式为可视化数据大屏格式,左上角有时间范围控件,格式为"YYYY-MM-DD HH:MM:SS",开始时间默认为当前减7,如今天是'2026-01-23'那么开始时间默认为'2026-01-16 00:00:00',结束时间默认为今天0点,如'2026-01-23 00:00:00'
    2. 标题为"未成年人打架斗殴六项指标监测"
    3. 共分为6个小板块,一行3个,共2行
    4. 每个板块名称分别为"警情转案率","采取矫治教育措施率","涉刑人员送学率","责令加强监护率","场所发案率","纳管人员再犯率"
    5. 每个小板块都包含
        1. 时间范围控件,格式为"YYYY-MM-DD HH:MM:SS",开始时间默认为当前减7,如今天是'2026-01-23'那么开始时间默认为'2026-01-16 00:00:00',结束时间默认为今天0点,如'2026-01-23 00:00:00'
        2. 六个"地区"的柱状图,每个地区三个"柱子",地区固定为"云城","云安","罗定","新兴","郁南","全市"
        3. 排序按钮,单击排序按钮后可以选择三个"柱子"其中的一个进行排序
        4. "下载"按钮,单击"下载"会下载xlsx格式的文件,文件名格式为{开始时间}+"-"+{结束时间}+{小版块标题}+{时间戳}.xlsx
        5. 柱状图中的柱子名称可以通过点击来"显示"或"隐藏"
    6. 页面右上角有3个按钮
        1. "导出总览",点击"导出总览"导出6个小模块的表格数据和柱状图数据,分别是"警情转案率柱状图""警情转案率表格","采取矫治教育措施率柱状图","采取矫治教育措施率表格","涉刑人员送学率柱状图","涉刑人员送学率表格","责令加强监护率柱状图","责令加强监护率表格","场所发案率柱状图","场所发案率表格","纳管人员再犯率柱状图","纳管人员再犯率表格",格式为{开始时间}+'-'+{结束时间}+'全市未成年人打架斗殴六项指标监测.pdf
        2. "导出详情",点击"导出详情"导出六个小版块的xlsx表格,分为6个sheet,每个sheet名称分别为"警情转案率","采取矫治教育措施率","涉刑人员送学率","责令加强监护率","场所发案率","纳管人员再犯率"
        3. "导入送校数据",点击该按钮弹出文件选择框,可以选择文件,用户只能选择xls格式文件,导入后进行判断,如果不是xls格式文件则弹出提醒框并停止后面的步骤,如果是xls文件判断"sheet"和"第三行"的列是否对的上,对不上则提醒用户并停止,如果正确则执行weichengnianren-djdo\0125_wcnr_sfzxx_import.py代码的逻辑导入数据,最后导入成功则弹出框提醒导入情况
    7. 柱状图使用weichengnianren-djdo\static\chart.min.js,版本是4.4.1
# 数据源:
    1.警情转案率:
        1. ```SELECT jq."caseno" 警情编号,jq."calltime" 报警时间,jq."cmdname" 分局,jq."dutydeptname" 管辖单位,jq."casecontents" 报警内容,jq."replies" 处警情况,mza."ajxx_ajbh" "案件编号" ,mza."ajxx_ajmc" "案件名称"
        ,LEFT(mza."ajxx_cbdw_bh_dm",6) "地区" ,mza."ajxx_cbdw_mc" "办案单位名称"
        FROM "ywdata"."zq_kshddpt_dsjfx_jq" jq LEFT JOIN "ywdata"."zq_zfba_wcnr_ajxx" mza ON jq."caseno" = mza."ajxx_jqbh"
        WHERE jq."newcharasubclass" IN (SELECT UNNEST (ctc."newcharasubclass_list") FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing"='打架斗殴')
        AND jq."calltime" BETWEEN {开始时间} AND {结束时间} AND jq."casemarkok" ~'未成年'```
        2. 其中"地区"通过:jq."cmdname" 分局,字段进行判断
            1. 所有数据为'全市'
            2. 包含'云城'的为云城
            3. 包含'云安'的为云安
            4. 包含'罗定'的为罗定
            5. 包含'新兴'的为新兴
            6. 包含'郁南'的为郁南
        3. 三个"柱子"分别为:
            1. "警情",通过jq."caseno" 警情编号计数
            2. "案件",通过mza."案件编号"计数(如果是"空"则不计数)
            3. "转案率",通过"案件"/"警情"*100%(取2位小数)
    2. 采取矫治教育措施率
        1. ```
        -- 更新：嫌疑人信息直接取 "ywdata"."zq_zfba_wcnr_xyr"（不再使用 mv_minor_person / mv_zfba_all_ajxx）
        -- 案件信息如需补充，可按 zzwx."ajxx_ajbhs" = aj."ajxx_ajbh" 关联 "ywdata"."zq_zfba_wcnr_ajxx"
        -- 文书命中仍按（案件编号 + 人员编号）与 zq_zfba_xjs / zq_zfba_zlwcnrzstdxwgftzs 做匹配（详见 dao.py）
        SELECT
            zzwx."ajxx_ajbhs" AS "案件编号",
            zzwx."ajxx_join_ajxx_ajmc" AS "案件名称",
            zzwx."ajxx_join_ajxx_lasj" AS "立案日期",
            zzwx."xyrxx_xm" AS "姓名",
            zzwx."xyrxx_sfzh" AS "证件号码",
            zzwx."xyrxx_nl" AS "年龄",
            zzwx."xyrxx_rybh" AS "人员编号",
            zzwx."xyrxx_ay_mc" AS "案由名称",
            LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) AS "地区代码"
        FROM "ywdata"."zq_zfba_wcnr_xyr" zzwx
        WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN {开始时间} AND {结束时间};
        ```
        2. 其中"地区"通过"地区"字段判断,所有数据即为"全市"
        3. 三个"柱子"分别为:
            1. "应采取矫治教育措施人数",对所有字段计数
            2. "已采取矫治教育措施人数",对"是否开具文书"的值为'是'进行计数
            3. "采取矫治教育措施率",通过"已采取矫治教育措施人数"/"应采取矫治教育措施人数"*100%,取2位小数
    3. 涉刑人员送学率
        1. ```
            -- 更新：嫌疑人信息直接取 "ywdata"."zq_zfba_wcnr_xyr"
            -- 是否送校：用 "ywdata"."zq_wcnr_sfzxx" 按 sfzhm=身份证号、且 rx_time > 立案时间 判定
            SELECT
                zzwx."xyrxx_rybh" AS "人员编号",
                zzwx."xyrxx_xm" AS "姓名",
                zzwx."xyrxx_sfzh" AS "证件号码",
                zzwx."xyrxx_hjdxz" AS "户籍地",
                zzwx."xyrxx_xzdxz" AS "现住地",
                zzwx."xyrxx_nl" AS "年龄",
                zzwx."ajxx_ajbhs" AS "案件编号",
                zzwx."ajxx_join_ajxx_ajmc" AS "案件名称",
                zzwx."ajxx_join_ajxx_lasj" AS "立案日期",
                LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) AS "地区代码"
            FROM "ywdata"."zq_zfba_wcnr_xyr" zzwx
            WHERE zzwx."ajxx_join_ajxx_ajlx" = '刑事'
              AND zzwx."ajxx_join_ajxx_lasj" BETWEEN {开始时间} AND {结束时间};
        ```
    2. 其中"地区"通过"地区"列分类,"全市"则为所有数据
    3. 三个"柱子"分别为:
        1. "符合涉刑人员送学人数",对所有人员计数
        2. "实际送学人身",对"是否送校"值为'是'的进行计数
        3. "涉刑人员送学率","实际送学人身"/"符合涉刑人员送学人数"*100%取2位小数
    4. 责令加强监护率
        1. ```
            -- 更新：嫌疑人信息直接取 "ywdata"."zq_zfba_wcnr_xyr"
            -- 文书命中按（案件编号 + 人员编号）与 "ywdata"."zq_zfba_jtjyzdtzs" 匹配（详见 dao.py）
            SELECT
                zzwx."ajxx_ajbhs" AS "案件编号",
                zzwx."xyrxx_rybh" AS "人员编号",
                zzwx."xyrxx_xm" AS "姓名",
                zzwx."xyrxx_sfzh" AS "证件号码",
                zzwx."ajxx_join_ajxx_ajmc" AS "案件名称",
                zzwx."ajxx_join_ajxx_lasj" AS "立案日期",
                LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) AS "地区代码"
            FROM "ywdata"."zq_zfba_wcnr_xyr" zzwx
            WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN {开始时间} AND {结束时间};
        ```
            2. 其中"地区"通过"地区"字段判断,所有数据即为"全市"
            3. 三个"柱子"分别为:
                1. "应责令加强监护人数",对所有字段计数
                2. "已责令加强监护人数",对"是否开具文书"的值为'是'进行计数
                3. "责令加强监护率",通过"已责令加强监护人数"/"应责令加强监护人数"*100%,取2位小数
    5. 场所发案率
        1. ```
            -- 更新：案源取 "ywdata"."zq_zfba_wcnr_ajxx"
            SELECT
                aj."ajxx_ajbh" AS "案件编号",
                aj."ajxx_lasj" AS "立案时间",
                LEFT(aj."ajxx_cbdw_bh_dm", 6) AS "地区",
                aj."ajxx_cbdw_mc" AS "办案单位",
                aj."ajxx_jyaq" AS "简要案情",
                aj."ajxx_fadd" AS "发案地点",
                aj."ajxx_fasj" AS "发案时间",
                aj."ajxx_ajzt" AS "案件状态"
            FROM "ywdata"."zq_zfba_wcnr_ajxx" aj
            WHERE aj."ajxx_lasj" BETWEEN {开始时间} AND {结束时间};
        ```
        2. 查询到的数据需要再次通过xungfang/5lei_dizhi_model中的模型对"案件发生地址名称"进行分类,该模型是通过macbert训练的,其中新增一列"分类结果"
        3. 其中"地区"通过"地区"列分类,"全市"则为所有数据
        4. 三个"柱子"分别为:
            1. "娱乐场所案件数",对"分类结果"值为'重点管控场所'的值进行过滤计数
            2. "案件数",对所有值进行计数
            3. "场所发案率",通过"娱乐场所案件数"/"案件数"*100%取2位小时
    6. 纳管人员再犯率
        1. ```
            -- 更新：比对基数（涉案嫌疑人）来自 "ywdata"."zq_zfba_wcnr_xyr"
            WITH fight_suspect AS (
                SELECT
                    zzwx."xyrxx_sfzh" AS zjhm,
                    zzwx."ajxx_join_ajxx_lasj" AS larq
                FROM "ywdata"."zq_zfba_wcnr_xyr" zzwx
                WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN {开始时间} AND {结束时间}
                  AND zzwx."xyrxx_sfzh" IS NOT NULL
            )
            SELECT
                bzr.*,
                CASE
                    WHEN bzr."ssfj_dm" ='445302000000' THEN '云城'
                    WHEN bzr."ssfj_dm" ='445303000000' THEN '云安'
                    WHEN bzr."ssfj_dm" ='445381000000' THEN '罗定'
                    WHEN bzr."ssfj_dm" ='445321000000' THEN '新兴'
                    WHEN bzr."ssfj_dm" ='445322000000' THEN '郁南'
                END AS "地区",
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM fight_suspect fs
                        WHERE fs.zjhm = bzr.zjhm
                          AND bzr.lgsj < fs.larq
                    ) THEN '是' ELSE '否' END AS "是否再犯"
            FROM "stdata"."b_zdry_ryxx" bzr
            WHERE bzr.sflg = '1'
              AND bzr."deleteflag" = '0';
        ```
        2. 其中"地区"通过"地区"列分类,"全市"则为所有数据
        3. 三个柱子分别为:
            1. "列管人数",对所有值计数
            2. "再犯人数",对"是否再犯"值为'是'的计数
            3. "再犯率","再犯人数"/"列管人数"*100%取两位小数
===
# 任务:帮我修改"发送短信"
1. 点击"发送短信"后在"发送短信"按钮下面弹出下拉框,有2个按钮,分别为"发送给领导","发送给责任人"点击其中任意一个按钮需先输入密码,密码校验成功后才能进行下一步,校验失败则提示'密码输入错误'
    1. 点击"发送给领导"按钮后,在当前页面弹出弹出框,有两个字段,分别为"发送号码"和"短信模板",都是可以编辑的,
        1. 发送号码默认从后端配置的数组获取,且用户可以修改,如果用户修改则按照用户修改的电话号码进行发送
        2. "短信模板"默认按照build_dashboard_sms_content方法组装好的显示,如果用户修改,则最后按照修改好的内容发送
    2. 点击"发送给责任人"按钮后,在当前页面弹出弹出框,弹出框构成为
        1. 三个模块
            1. 采取矫治教育措施率,该模块只显示"是否开具文书"中值为'否'的数据,同样只显示"短信模板"和"发送号码",模板为'2026年未成年人打架斗殴指标监测: 您办理的{案件名称}的{姓名}未开具《训诫书》/《责令未成年人遵守特定行为规范通知书》【基础管控中心】',其中案件名称中的姓名要脱敏,如'张小三殴打他人案'要脱敏为'张XX殴打他人案'."电话号码"为"联系电话_json"中的值,需要先判断是否是移动电话,如座机"0XXX-XXXXXXX"则过滤掉
            2. 涉刑人员送学率,该模块只显示"是否送校"中值为'否'的数据,同样只显示"短信模板"和"发送号码",模板为'2026年未成年人打架斗殴指标监测: 您办理的{案件名称}的{姓名}未送方正学校【基础管控中心】',其中案件名称中的姓名要脱敏,如'张小三殴打他人案'要脱敏为'张XX殴打他人案'."电话号码"为"联系电话_json"中的值,需要先判断是否是移动电话,如座机"0XXX-XXXXXXX"则过滤掉
            3. 责令加强监护率,该模块只显示"是否送校"中值为'否'的数据,同样只显示"短信模板"和"发送号码",模板为'2026年未成年人打架斗殴指标监测: 您办理的{案件名称}的{姓名}未开局《加强监督教育/责令接受家庭监督指导通知书》【基础管控中心】',其中案件名称中的姓名要脱敏,如'张小三殴打他人案'要脱敏为'张XX殴打他人案'."电话号码"为"联系电话_json"中的值,需要先判断是否是移动电话,如座机"0XXX-XXXXXXX"则过滤掉
        2. 其中"短信模板"和"发送号码"和"发送给领导"是一样可以编辑的
        3. 进行脱敏时可依据以下关键字过滤,'殴打','打架','滋事','故意伤害','斗殴'
    3. 点击"发送短信"后用户输入密码,输入的密码要显示'*'而不是明文
===
1. 在"刷新全部"后面新增一个"类型"控件,为下拉多选框,值通过```SELECT ctc.leixing from ywdata.case_type_config ctc```获取
2. 在weichengnianren-djdo\wcnr_djdo\dao.py的6个SQL中,修改```    WHERE ctc."leixing" = '打架斗殴'```为动态的,且支持多选
3. 当不选择"类型"时,6个SQL要删除这个过滤条件
    1. ```jq."newcharasubclass" IN (
        SELECT UNNEST(ctc."newcharasubclass_list")
        FROM ywdata."case_type_config" ctc
        WHERE ctc."leixing" = '打架斗殴'
    )```
    2. ```EXISTS (
            SELECT 1
            FROM ywdata."case_type_config" ctc
            WHERE ctc."leixing" = ANY({类型})
            AND COALESCE(字段值,'') SIMILAR TO ctc."ay_pattern"
        )```
3. 在"发送给责任人"功能的弹出框中新增"一键清除"功能,点击"一键清除"删除弹出框中所有可编辑的内容
4. query_ng_zf_details的SQL我已修改,请你在帮我加上start_time和end_time参数,参数放在AND mzaa."立案日期" BETWEEN '2026-01-01' AND NOW()
