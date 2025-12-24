1. 新增一个'未成年人'模块,代码放在weichengnianren文件夹中,主页中新增'未成年人'按钮,点击该按钮跳转到未成年人页面
2. 根据当前的用户权限查询是否有查看该模块的权限,查询'jcgkzx_permission'表查询用户名是否有'未成年人',如果有才显示'未成年人'按钮
3. 页面初始化通过SQL展示:
    SQL:
        ```
        WITH
        -- A. 涉及未成年人的警情（根据警情标注）
        juv_jq AS (
            SELECT
                j.caseno                       AS jqbh,
                j.calltime                     AS jq_calltime,
                j.neworicharasubclassname      AS jq_yssx,
                j.casemark                     AS jq_casemark,
                j.dutydeptno                   AS jq_dutydeptno,
                j.dutydeptname                 AS jq_dutydeptname,
                j.occuraddress                 AS jq_occuraddress,
                j.casecontents                 AS jq_casecontents,
                j.replies                      AS jq_replies
            FROM ywdata.zq_kshddpt_dsjfx_jq j
            WHERE j.casemark LIKE '%未成年%'
        ),

        -- B. 涉及未成年人的案件：先从案件+mv_minor_person 得到“未成年案件集合”
        minor_case AS (
            SELECT DISTINCT
                a."案件编号",
                a."地区",
                a."案件名称",
                a."警情编号",
                a."简要案情",
                a."办案单位名称",
                a."立案日期",
                a."案由",
                a."案件类型",
                a."案件状态名称"
            FROM ywdata.mv_zfba_all_ajxx a
            JOIN ywdata.mv_minor_person mp
            ON mp.asjbh = a."案件编号"
        ),

        -- C. 从 mv_minor_person 出发，按【案件编号 + 人员编号】聚合文书 + 行政处罚
        ws_xz_agg AS MATERIALIZED (
            SELECT
                mp.asjbh,
                mp.anjxgrybh,

                -- 文书聚合
                string_agg(
                    w.flws_zlmc,
                    ',' ORDER BY w.wsywxxid
                ) AS ws_name_list,        -- 文书种类名称列表

                string_agg(
                    w.flwslldz,
                    ',' ORDER BY w.wsywxxid
                ) AS ws_url_list,         -- 文书浏览地址列表

                json_agg(
                    json_build_object(
                        'name', w.flws_zlmc,
                        'url',  w.flwslldz
                    )
                    ORDER BY w.wsywxxid
                ) AS ws_json_list,        -- JSON数组 [{name,url},...]

                -- 行政处罚聚合
                MAX(x.jlts)    AS jlts,       -- 拘留天数
                MAX(x.fk)      AS fk,         -- 罚款金额
                MAX(x.sfjlbzx) AS sfjlbzx     -- 是否拘留不执行

            FROM ywdata.mv_minor_person mp
            LEFT JOIN ywdata.zfba_ws_001 w
            ON w.asjbh     = mp.asjbh
            AND w.flws_dxbh = mp.anjxgrybh
            -- 如需限定文书对象类型，可加：AND w.flws_dxlxmc = '违法行为人'

            LEFT JOIN ywdata.zfba_aj_009 x
            ON x.wsywxxid = w.wsywxxid

            GROUP BY
                mp.asjbh,
                mp.anjxgrybh
        ),

        -- D. 涉及未成年人的案件明细（按案件+人员 一行）
        juv_case AS (
            SELECT
                a."案件编号",
                a."地区",
                a."案件名称",
                a."警情编号",
                a."简要案情",
                a."办案单位名称",
                a."立案日期",
                a."案由",
                a."案件类型",
                a."案件状态名称",

                mp.xm,
                mp.xbmc,
                mp.zjhm,
                mp.hjd_xz,
                mp.xzd_xz,
                mp.zjzpckdz,
                mp.anjxgrybh,
                mp.role_names,

                wa.ws_name_list,
                wa.ws_url_list,
                wa.ws_json_list,
                wa.jlts,
                wa.fk,
                wa.sfjlbzx
            FROM minor_case a
            JOIN ywdata.mv_minor_person mp
            ON mp.asjbh = a."案件编号"
            LEFT JOIN ws_xz_agg wa
            ON wa.asjbh     = a."案件编号"
            AND wa.anjxgrybh = mp.anjxgrybh
        )

        -- E. 最终：FULL JOIN，把“未成年警情”与“未成年案件”合并
        SELECT
            -- 统一警情编号
            COALESCE(j.jqbh, c."警情编号")          AS "警情编号",

            -- 警情信息（如果案件无警情，这些字段为 NULL）
            j.jq_calltime                           AS "报警时间",
            j.jq_yssx                               AS "原始警情性质",
            j.jq_casemark                           AS "警情标注",
            j.jq_dutydeptno                         AS "管辖单位代码",
            j.jq_dutydeptname                       AS "管辖单位名称",
            j.jq_occuraddress                       AS "警情地址",
            j.jq_casecontents                       AS "报警内容",
            j.jq_replies                            AS "处警回复",

            -- 案件信息（如果是“只警情无案件”，这些字段为 NULL）
            c."案件编号",
            c."地区",
            c."案件名称",
            c."简要案情",
            c."办案单位名称",
            c."立案日期",
            c."案由",
            c."案件类型",
            c."案件状态名称",

            -- 案件人员（只对有案件的行有值）
            c.xm                                    AS "姓名",
            c.xbmc                                  AS "性别",
            c.zjhm                                  AS "证件号码",
            c.hjd_xz                                AS "户籍地",
            c.xzd_xz                                AS "现住址",
            c.zjzpckdz                              AS "证件照",
            c.anjxgrybh                             AS "人员编号",
            c.role_names                            AS "角色名称",

            -- 文书+处罚
            c.ws_name_list                          AS "法律文书种类名称",
            c.ws_url_list                           AS "法律文书浏览地址",
            c.ws_json_list                          AS "法律文书JSON列表",
            c.jlts                                  AS "拘留天数",
            c.fk                                    AS "罚款金额",
            c.sfjlbzx                               AS "是否拘留不执行",

            -- 关联类型：方便前端区分是“只有警情、只有案件、还是都有”
            CASE
                WHEN j.jqbh IS NOT NULL AND c."案件编号" IS NOT NULL THEN '警情+案件'
                WHEN j.jqbh IS NOT NULL AND c."案件编号" IS NULL     THEN '仅警情'
                WHEN j.jqbh IS NULL     AND c."案件编号" IS NOT NULL THEN '仅案件'
                ELSE '未知'
            END                                      AS "关联类型"

        FROM juv_jq j
        FULL JOIN juv_case c
        ON c."警情编号" = j.jqbh WHERE 1=1 ;
        ```
4. 页面有3个控件
    1. 数据类型:是一个多选下拉框,包含'警情+案件','仅警情','仅案件'三个值,选择对应的值在SQL后面拼接查询条件:'AND 关联类型 IN ({数据类型})
    2. 报警时间:时间范围控件,包含年月日时分秒,选择对应的时间范围在SQL后面拼接查询条件:'AND 报警时间 BETWEEN {开始时间} AND {结束时间}'
    3. 立案日期:时间范围控件,包含年月日时分秒,选择对应的时间范围在SQL后面拼接查询条件:'AND 立案日期 BETWEEN {开始时间} AND {结束时间}'
5. 在数据展示的列表右上方有一个'下载'按钮,点击'下载'会显示{'csv','excel'}两个按钮,点击对应的按钮可以将当前过滤的数据下载为对应格式的文件,文件命名为{时间戳}
6. 在查询的数据中'姓名'列根据'证件照'值是否为空可以点击,如果对应的'证件照'不为空则可以点击,否则不可以点击,点击姓名值可以下载证件照,后台实际执行的是'证件照'值,比如某条数据是:{姓名:张三,证件照:http://xxxxx.xxx},那么点击'张三'实际下载的链接是'http://xxxxx.xxx'
7. 在查询的数据中,有一列'法律文书json列表',该字段的值是一个json对象,格式是[{"name":"张三","url":"url1"},{"name":"李四","url":"url2"}],实际显示的时候显示的是'name',可以点击,点击'name'则通过'url'跳转到对应页面,如果'name'和'url'均为null则不显示任何内容,如[{"name":null,"url":null}]
8. 支持分页功能,默认显示20条数据,用户可以选择'50','100','全部'


1. 在weichengnianren目录中新增一个配置文件,可以配置前端页面显示哪些列
2. 数据类型控件是下拉框,点击后弹出'警情+案件','仅警情','仅案件',可以勾选一个或者多个,不勾选默认SQL语句不追加条件
3. 每页条数控件和'共XXX条记录,当前第X页'放在表格下方
4. 下载按钮放在查询按钮下方,查询按钮放在最右侧,4个控件'数据类型','报警时间范围','立案日期范围','查询'4个控件放在同一行