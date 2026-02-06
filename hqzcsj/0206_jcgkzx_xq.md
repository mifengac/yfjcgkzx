# 任务1: 修改hqzcsj模块"获取综查数据"的"获取提请文书"功能的"zq_zfba_tqzmjy",

    - 获取提请文书参数修改为如下(获取全部):
        access_token: e663a673-22c1-4a7c-80f5-f4e2c21e9b0d
        quickFilter: C75050E4832B4F5C882B7AE04B11FAC3
        modelId: 3014B3FB4791461998A87D794ED94077
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