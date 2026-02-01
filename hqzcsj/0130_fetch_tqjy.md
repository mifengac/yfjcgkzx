# 任务:在hqzcsj模块的"获取综查数据"tab页中"获取数据"下面再增加一行"获取提请文书",点击后根据我提供的"请求参数"弹出对应编辑框(你帮我分析),弹出框中点击开始获取按钮将获取的数据导出数据库中,导入逻辑参考"获取数据"功能的逻辑,不需要jsonb
请求头:  
    请求网址:
    http://68.26.7.47:1999/com/api/v1/com/model/getQueryPageData
    请求方法:
    POST
    状态代码:
    200 OK
    远程地址:
    68.26.7.47:1999
    引荐来源网址政策:
    no-referrer-when-downgrade
    Access-Control-Allow-Origin:
    *
    Cache-Control:
    no-cache, no-store, max-age=0, must-revalidate
    Content-Type:
    application/json;charset=UTF-8
    Date:
    Fri, 30 Jan 2026 01:04:12 GMT
    Expires:
    0
    Pragma:
    no-cache
    Transfer-Encoding:
    chunked
    Vary:
    Origin
    Vary:
    Access-Control-Request-Method
    Vary:
    Access-Control-Request-Headers
    Vary:
    Origin
    Vary:
    Access-Control-Request-Method
    Vary:
    Access-Control-Request-Headers
    X-Content-Type-Options:
    nosniff
    X-Xss-Protection:
    1; mode=block
    Accept:
    application/json, text/javascript, */*; q=0.01
    Accept-Encoding:
    gzip, deflate
    Accept-Language:
    zh-CN,zh;q=0.9
    Authorization:
    Bearer <ACCESS_TOKEN>
    Connection:
    keep-alive
    Content-Length:
    2406
    Content-Type:
    application/x-www-form-urlencoded; charset=UTF-8
    Host:
    68.26.7.47:1999
    Origin:
    http://68.26.7.148:999
    Referer:
    http://68.26.7.148:999/com/datagrid/yshdws
    User-Agent:
    Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36

请求参数:
    access_token: <ACCESS_TOKEN>
    quickFilter: C75050E4832B4F5C882B7AE04B11FAC3
    modelId: 3014B3FB4791461998A87D794ED94077
    where: {"rules":[{"field":"AJBH","op":"like","value":"","type":"string","format":""},{"field":"WS_ID","op":"like","value":"E232953F755A49DF90F73295347FEECA,A00A6FF90C9E423C960D7FE4224970CD","type":"string","format":"","linkOp":"or"},{"field":"WSZH","op":"like","value":"","type":"string","format":""},{"field":"XGRY_XM","op":"like","value":"","type":"string","format":""},{"field":"DYCS","op":"between","value":"0|999","type":"number","format":""},{"field":"DYSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"CBDW_MC","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"CBDW_BH_1","op":"like","value":"","type":"string","format":""},{"field":"CBR_XM","op":"like","value":"","type":"string","format":""},{"field":"KJSJ","op":"between","value":"2025/12/01 00:00:00|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"SPSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"WSZT","op":"like","value":"03","type":"string","format":"","linkOp":"or"}],"op":"and"}
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

响应数据:
    {
        "msg": "查询成功！",
        "code": 200,
        "success": true,
        "Total": 580,
        "Data": {},
        "Rows": [
            {
                "AJMC": "抢夺他人财物案",
                "WSZH": "316019",
                "WSZTName": "审批通过",
                "DYCS": "1",
                "XGRY_XM": "",
                "CBDW_MC": "",
                "ZJZ": "1501DCF2B0C943E59F208A2FD448DB44",
                "DYSJ": "2026-01-29 22:48:09",
                "SPR_XM": "",
                "WS_ID": "7FD812735F8B48FAB3013E24F0BCABCB",
                "AJBH": "A4453815800002026016007",
                "SPSJ": "2026-01-29 22:47:24",
                "KJSJ": "2026-01-29 22:47:00",
                "WSMC": "提请专门教育告知书(xxx)",
                "BD_ID": "F2EC7652EE8E49779E4CB1A0E77DC519",
                "ID": "498852BE400E2A0AE06316E21D4441F3",
                "CBR_XM": "",
                "WSZT": "03",
                "LRSJ": "2026-01-29 22:36:40"
            },
# 任务:下面是我分析的2026年1月未成年人打架斗殴案件和嫌疑人数据,你帮我转换为两个json文件