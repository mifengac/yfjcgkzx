# 任务5: hqzcsj模块"获取综查数据"的"获取数据"中新增一个数据源,名为"不予行政处罚决定书",表名为"zq_zfba_byxzcfjds"
    - 请求参数:
    {"paramArray":[{"conditions":[{"tabId":"1751872828124209177","tabCode":"byxzcfjds","fieldCode":"byxzcfjds_tfsj","tabType":"2","isPub":false,"operateSign":"10","values":["2026-01-30 00:00:00","2026-02-06 23:59:59"],"excludeDays":[],"rangeIncludeType":"0"},{"tabId":"1751872828124209177","tabCode":"byxzcfjds","fieldCode":"byxzcfjds_isdel","tabType":"2","isPub":false,"operateSign":"7","values":["0"],"isIncludeChilds":false,"dicCode":"00"},{"tabId":"1751872828124209177","tabCode":"byxzcfjds","fieldCode":"byxzcfjds_wszt","tabType":"2","isPub":false,"operateSign":"7","values":["03"],"isIncludeChilds":false,"dicCode":"ZD_CASE_WSZT"}],"tabId":"1751872828124209177","tabCode":"byxzcfjds","domainId":"11"}]}
    - 响应数据
    {
    "status": "success",
    "code": 0,
    "message": "操作成功！",
    "context": {
        "result": {
            "result": [
                {
                    "ajxx_ajbh": "A4453816500002026016009",
                    "byxzcfjds_ajmc": "-",
                    "byxzcfjds_cbqy_bh": "445381",
                    "byxzcfjds_tfsj": "2026-02-05 20:27:00",
                    "byxzcfjds_cflx": "1",
                    "byxzcfjds_dwbh": "-",
                    "byxzcfjds_dwmc": "-",
                    "byxzcfjds_wfss": "    违法行为人xxx于2025年12月23日15时许，在XXXX路段驾驶一辆白色电动车将若干铁皮和铁块盗窃走，因违法行为人XX是精神病人，无法辨认其行为",
                    "byxzcfjds_xgzj": "     违法行为人XX的陈述和申辩，报警人的陈述，现场监控视频      ",
                    "byxzcfjds_flyj": "《中华人民共和国治安管理处罚法》第十三条和第五十八条",
                    "byxzcfjds_sj": "-",
                    "byxzcfjds_zj": "-",
                    "byxzcfjds_fyjg": "XX市人民政府",
                    "byxzcfjds_rmfy": "XXXX人民法院",
                    "byxzcfjds_fj": "-",
                    "byxzcfjds_qzsj": "-",
                    "byxzcfjds_signname": "445381",
                    "byxzcfjds_cqr_sfzh": "-",
                    "byxzcfjds_cqr_xm": "-",
                    "byxzcfjds_cqyj": "-",
                    "byxzcfjds_cqsj": "-",
                    "byxzcfjds_psignname": "XX市公安局",
                    "byxzcfjds_splx": "-",
                    "byxzcfjds_fyjg_dz_lx": "-",
                    "byxzcfjds_zllx": "-",
                    "byxzcfjds_id": "F3343F199BC14C0FA4710FB2D1FAE887",
                    "byxzcfjds_isdel": "否",
                    "byxzcfjds_isdel_dm": "0",
                    "byxzcfjds_dataversion": "20260205201612",
                    "byxzcfjds_lrr_sfzh": "445381199810146610",
                    "byxzcfjds_lrsj": "2026-02-05 20:08:01",
                    "byxzcfjds_xgr_sfzh": "445381199810146610",
                    "byxzcfjds_xgsj": "2026-02-05 20:27:06",
                    "byxzcfjds_wszt": "审批通过",
                    "byxzcfjds_wszt_dm": "03",
                    "byxzcfjds_wsh": "316038",
                    "byxzcfjds_rybh": "R4453816500002026023004",
                    "byxzcfjds_rymc": "XX",
                    "byxzcfjds_cbr_sfzh": "445381199810146610",
                    "byxzcfjds_cbr_xm": "李业深",
                    "byxzcfjds_cbdw_bh": "XX市公安局XX派出所 ",
                    "byxzcfjds_cbdw_bh_dm": "445381650000",
                    "byxzcfjds_cbdw_mc": "XX市公安局XX派出所",
                    "byxzcfjds_cbdw_jc": "XX",
                    "byxzcfjds_ryxx": "        违法行为人：XX（曾用名：无），男，38岁，1987年12月27日生，民族：汉族，居民身份证：445381198712274518，文化程度：文盲，政治面貌：群众，户籍地：XX省XX市XX街道，现居地：XX省XX市XX街道，工作单位：无。",
                    "byxzcfjds_flyj1": "-",
                    "byxzcfjds_flyj2": "根据《中华人民共和国治安管理处罚法》第十三条和第五十八条之规定，现决定不予行政处罚。",
                    "byxzcfjds_cbryj": "因XX精神病人、智力残疾人在不能辨认或者不能控制自己行为的时候违反治安管理的，依据《中华人民共和国治安管理处罚法》第十三条的规定，决定不予行政处罚，当否，请领导审批。",
                    "byxzcfjds_sjly": "44BQ00",
                    "byxzcfjds_hjk_rksj": "2026-02-06 20:31:32",
                    "byxzcfjds_hjk_sclrsj": "2026-02-05 20:16:33"
                },
# 任务1: 修改hqzcsj模块"获取综查数据"的"获取提请文书"功能的"zq_zfba_tqzmjy",

    - 获取提请文书参数修改为如下(获取全部):
        access_token: e663a673-22c1-4a7c-80f5-f4e2c21e9b0d
        quickFilter: C75050E4832B4F5C882B7AE04B11FAC8
        modelId: 3014D8FB4791461998A87D794ED94077
        where: {"rules":[{"field":"AJBH","op":"like","value":"","type":"string","format":""},{"field":"WS_ID","op":"like","value":"E232953F755A49DF90F73295347FEECA,A00A6FF90C9E423C960D7FE4224970CD","type":"string","format":"","linkOp":"or"},{"field":"WSZH","op":"like","value":"","type":"string","format":""},{"field":"XGRY_XM","op":"like","value":"","type":"string","format":""},{"field":"DYCS","op":"between","value":"0|999","type":"number","format":""},{"field":"DYSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"CBDW_MC","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"CBDW_BH_1","op":"like","value":"","type":"string","format":""},{"field":"CBR_XM","op":"like","value":"","type":"string","format":""},{"field":"KJSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"SPSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"WSZT","op":"like","value":"03","type":"string","format":"","linkOp":"or"}],"op":"and"}
        mark: yshdws
        modelName: 已审核的文书SZ
        pkName: ID
        modelMark: yshdws
        resType: 02
        funcMark: yshdws
        funcId: 6CDD88E5222140A1B66E608814697B84
        resId: 17CF516331DE4DF6800761D9452BAAEF
        page: 1
        pagesize: 1000
        sortname: KJSJ,LRSJ,SPSJ,DYSJ
        sortorder: desc,desc,asc,asc
    - 获取文书的数据源改为"下拉多选"且放到"获取提请文书"按钮旁边,参考"获取数据"
# 任务2: 修改hqzcsj模块"警情案件统计"tab页的"高质量"列的显示,
    - 计数时也计数'刑拘数'大于2的,现在是计数的时候计数了所有,点击{计数值}进入详情中只有'刑拘数'大于2的
# 任务3: 修改hqzcsj模块"矫治情况统计"tab页,添加列
    - 在"违法人数"后新增一列"符合送生（行政）":通过对数据源"案件类型"值为'行政'且"是否符合送生"值为'是'的数据过滤并分组计数得到
    - 在"犯罪人数"人数"后新增一列"符合送生（刑事）":通过对数据源"案件类型"值为'刑事'且"是否符合送生"值为'是'的数据过滤并分组计数得到
    - 在"提请专门教育申请书数(行政)	"后新增一列"送生数（行政）":通过对数据源"案件类型"值为'行政'且"是否送校"值为'是'的数据过滤并分组计数得到
    - 在"提请专门教育申请书数(刑事)	"后新增一列"送生数（刑事）":通过对数据源"案件类型"值为'刑事'且"是否送校"值为'是'的数据过滤并分组计数得到

# 任务4: 修改hqzcsj中"获取综查数据"tab页的"获取数据"功能中的'未成年人(嫌疑人)'数据源的请求参数:其中
    ```
    json: {"paramArray":[{"conditions":[{"tabId":"1764455104469049399","tabCode":"ragl","fieldCode":"ragl_fasnl","tabType":"2","isPub":false,"operateSign":"10","values":[1,18],"rangeIncludeType":"2"},{"tabId":"22","tabCode":"xyr","fieldCode":"xyrxx_nl","tabType":"1","isPub":false,"operateSign":"10","values":[1,18],"rangeIncludeType":"2"},{"tabId":"22","tabCode":"xyr","fieldCode":"xyrxx_ryzt","tabType":"1","isPub":false,"operateSign":"7","values":["01","04"],"isIncludeChilds":false,"dicCode":"ZD_CASE_RYZT_BH"},{"tabId":"16","tabCode":"ajxx_join","fieldCode":"ajxx_ajlx","tabType":"1","isPub":false,"operateSign":"7","values":["01"],"isIncludeChilds":false,"dicCode":"ZD_CASE_AJLX"},{"tabId":"22","tabCode":"xyr","fieldCode":"xyrxx_lrsj","tabType":"1","isPub":false,"operateSign":"10","values":["2025-01-01 00:00:00","2025-08-17 23:59:59"],"excludeDays":[],"rangeIncludeType":"0"}],"tabId":"22","tabCode":"xyr","domainId":"11"}]}
    domainId: 11
    resultTabId: 22
    resultTabCode: xyr
    resultTableName: 嫌疑人信息
    tabId: 22
    pageSize: 
    pageNumber: 1
    sortColumns: 
    ```