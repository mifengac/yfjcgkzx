# 人案关联信息
## 请求
### 标头
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
    Wed, 04 Mar 2026 08:10:00 GMT
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
    Bearer 130853e5-da8b-4cc9-878a-791649743056
    Connection:
    keep-alive
    Content-Length:
    1902
    Content-Type:
    application/x-www-form-urlencoded; charset=UTF-8
    Host:
    68.26.7.47:1999
    Origin:
    http://68.26.7.148:999
    Referer:
    http://68.26.7.148:999/com/datagrid/raglxxcx
    User-Agent:
    Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36
### 参数
    access_token: 130853e5-da8b-4cc9-878a-791649743056
    quickFilter: 6CBE99BB63434C228BE7679066FFBB28
    modelId: 8CFB0D316ED0456E96115799D9081B16
    where: {"rules":[{"field":"AJBH","op":"like","value":"","type":"string","format":""},{"field":"JQBH","op":"like","value":"","type":"string","format":""},{"field":"AJMC","op":"like","value":"","type":"string","format":""},{"field":"AJLX","op":"like","value":"","type":"string","format":""},{"field":"AJLB","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"AJZT","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"SLSJ","op":"between","value":"|","type":"date","format":"yyyy-MM-dd HH:mm:ss"},{"field":"CBDW_BH","op":"like","value":"","type":"string","format":""},{"field":"CBQY_BH","op":"like","value":"","type":"string","format":""},{"field":"LRSJ","op":"between","value":"|","type":"date","format":"yyyy-MM-dd HH:mm:ss"}],"op":"and"}
    mark: raglxxcx
    modelName: 人案关联信息查询
    pkName: ID
    modelMark: raglxxcx
    resType: 02
    funcMark: raglxxcx
    funcId: 85232FDD2DCA478881292D4D7A78535B
    resId: 71EB8493E94C470E85798790633D507D
    page: 1
    pagesize: 30
    sortname: SLSJ
    sortorder: desc
### 响应
{
    "msg": "查询成功！",
    "code": 200,
    "success": true,
    "Total": 56008,
    "Data": {},
    "Rows": [
        {
            "HJDQH": "xx省XX县",
            "AJLB": "违规燃放烟花爆竹",
            "SSJQDMName": "",
            "ZJLXName": "居民身份证",
            "AJLXName": "行政",
            "XZDXZ": "xx省xx市XX县xx镇xx村委xx村悦梅坊33号",
            "XBDW": "XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所,XX县公安局xx派出所",
            "SLSJ": "2026-03-04 15:27:48",
            "ZJLX": "居民身份证",
            "HJDQHName": "xx省XX县",
            "ID": "4C27C30584A7D2C7E06316E21D4429FD",
            "FADD": "xx省xx市XX县xx镇州背悦梅坊饭堂楼顶",
            "XZDQH": "xx省XX县",
            "AJZTName": "已立案",
            "HJDXZ": "xx省xx市XX县xx镇xx村委xx村悦梅坊33号",
            "AJMC": "xx明违规燃放烟花案",
            "XBName": "男",
            "SSJQDM": "",
            "SFZH": "441228197612050319",
            "GZDW": "无",
            "RYZT": "其他",
            "XB": "男",
            "AJLX": "行政",
            "JASJ": "",
            "JQBH": "Z532151000026030415253048",
            "LASJ": "2026-03-04 15:27:48",
            "XZDQHName": "xx省XX县",
            "RYZTName": "其他",
            "AJBH": "Axxxx215100002026036005",
            "FASJ": "2026-03-04 15:27:53",
            "XM":XXX明",
            "SLDW_MC": "XX县公安局xx派出所",
            "CBDW_BHName": "XX县公安局xx派出所",
            "AJLBName": "违规燃放烟花爆竹",
            "LXFS": "18023359788",
            "CBDW_BH": "xxxx21510000",
            "AJZT": "已立案",
            "CBQY_BH": "xxxx21"
        }
    ]
}
# 非嫌疑人信息
## 请求
### 标头
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
    Wed, 04 Mar 2026 08:07:55 GMT
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
    Bearer 130853e5-da8b-4cc9-878a-791649743056
    Connection:
    keep-alive
    Content-Length:
    2225
    Content-Type:
    application/x-www-form-urlencoded; charset=UTF-8
    Host:
    68.26.7.47:1999
    Origin:
    http://68.26.7.148:999
    Referer:
    http://68.26.7.148:999/com/datagrid/sarjbxxcx
    User-Agent:
    Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36
### 参数
    access_token: 130853e5-da8b-4cc9-878a-791649743056
    modelId: 3E556BCBCCEE490E9A93EEF3EB814698
    where: {"rules":[{"field":"AJLX","op":"like","value":"","type":"string","format":""},{"field":"XM","op":"like","value":"","type":"string","format":""},{"field":"SFZH","op":"like","value":"","type":"string","format":""},{"field":"HJDXZ","op":"like","value":"","type":"string","format":""},{"field":"RYLX_MC","op":"like","value":"","type":"string","format":""},{"field":"AJMC","op":"like","value":"","type":"string","format":""},{"field":"AJBH","op":"like","value":"","type":"string","format":""},{"field":"ORG_CODE","op":"equal","value":"","type":"string","format":""},{"field":"REG_CODE","op":"equal","value":"","type":"string","format":""},{"field":"LRSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"LXFS","op":"like","value":"","type":"string","format":""},{"field":"AY_MC","op":"like","value":"","type":"string","format":""},{"field":"AY_BH","op":"like","value":"","type":"number","format":""}],"op":"and"}
    mark: sarjbxxcx
    modelName: 共享库涉案人信息查询SZ
    pkName: ID
    modelMark: sarjbxxcx
    resType: 01
    funcMark: sarjbxxcx
    funcId: 12A531A69A4D4F9883394201C90BF155
    resId: 6A8551F7074041228C8DFB7910150BC9
    quickFilter: E75DB25B0FF541BDBC4121FDBC363975
    page: 1
    pagesize: 10
### 响应
{
    "msg": "查询成功！",
    "code": 200,
    "success": true,
    "Total": 31833,
    "Data": {},
    "Rows": [
        {
            "AY_MC": "",
            "CSRQName": "2010-10-20 00:00:00",
            "XZDXZ": "xx省xx市xx街道美丽泷江",
            "REG_CODEName": "xx市公安局",
            "AY_BHName": "",
            "AY_BH": "",
            "WHCD": "",
            "ID": "FE129126BBBEE4EFE05316E21D448BF9",
            "HJDXZ": "xx省xx市罗播xxx村木屯156号",
            "AJMC": "xxx被强奸案",
            "WHCDName": "",
            "XBName": "女",
            "SHFDName": "",
            "MZName": "汉族",
            "SFZH": "450881201010205322",
            "GZDW": "无",
            "REG_CODE": "xxxx81",
            "XB": "2",
            "AJLX": "02",
            "MZ": "01",
            "CSRQ": "2010-10-20 00:00:00",
            "AJBH": "Axxxx815102002023066006",
            "LRR_SFZH": "xxxx8119981214781X",
            "RYLX_MC": "受害人",
            "SHFD": "",
            "XM":XXX婷",
            "ORG_CODE": "xxxx81510200",
            "LXFS": "18878599404",
            "AY_MCName": "",
            "ORG_CODEName": "xx市公安局xx派出所治安管理中队"
        }
    ]
}
# 嫌疑人信息
## 请求
### 标头
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
    Wed, 04 Mar 2026 08:05:48 GMT
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
    Bearer 130853e5-da8b-4cc9-878a-791649743056
    Connection:
    keep-alive
    Content-Length:
    2607
    Content-Type:
    application/x-www-form-urlencoded; charset=UTF-8
    Host:
    68.26.7.47:1999
    Origin:
    http://68.26.7.148:999
    Referer:
    http://68.26.7.148:999/com/datagrid/qsxyrxx
    User-Agent:
    Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36
### 参数
    access_token: 130853e5-da8b-4cc9-878a-791649743056
    quickFilter: 00FE321CFB9342A8B9C99FCF1D9EE2DC
    modelId: 9C8079F04C9E41DEB406058CCD2D0D20
    where: {"rules":[{"field":"JZLX_DM","op":"like","value":"","type":"string","format":""},{"field":"RYBH","op":"like","value":"","type":"string","format":""},{"field":"XM","op":"like","value":"","type":"string","format":""},{"field":"SFZH","op":"like","value":"","type":"string","format":""},{"field":"AJBH","op":"like","value":"","type":"string","format":""},{"field":"AJMC","op":"like","value":"","type":"string","format":""},{"field":"CBDW_BH","op":"like","value":"","type":"string","format":""},{"field":"CBQY_BH","op":"like","value":"","type":"string","format":""},{"field":"AJLX","op":"like","value":"","type":"string","format":""},{"field":"AY_BH","op":"equal","value":"","type":"string","format":"","linkOp":"or"},{"field":"LRSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"SFDY","op":"like","value":"","type":"string","format":""},{"field":"RDDB","op":"like","value":"","type":"string","format":""},{"field":"ZXWY","op":"like","value":"","type":"string","format":""},{"field":"WX","op":"like","value":"","type":"string","format":""},{"field":"LXFS","op":"like","value":"","type":"string","format":""}],"op":"and"}
    mark: qsxyrxx
    modelName: 全部嫌疑人信息SZ
    pkName: ID
    modelMark: qsxyrxx
    resType: 02
    funcMark: qsxyrxx
    funcId: C4AF374C680647F6A9FCB0DC1CE47D75
    resId: 0A236E69CE834AF8B28F0FEB2BC5EDE5
    page: 1
    pagesize: 10
### 响应
{
    "msg": "查询成功！",
    "code": 200,
    "success": true,
    "Total": 51880,
    "Data": {},
    "Rows": [
        {
            "AY_MC": "诈骗案",
            "XZD": "xx省xx市xx镇xx村委瓦屋11号",
            "SFDAName": "否",
            "AJLXName": "刑事",
            "ZXWY": "0",
            "ZZMM": "13",
            "CBQY_BHName": "xx市公安局",
            "SFDY": "0",
            "GASJ": "",
            "AYMC": "",
            "GJName": "中国",
            "RDDB": "0",
            "CRJ_ZJLX": "",
            "WHCD": "50",
            "ID": "57BCD5D9B9994EF5893613E89BD67D04",
            "HJDXZ": "xx省xx市xx镇xx村委瓦屋11号",
            "QQ": "",
            "JZLX_MC": "",
            "HYZK": "",
            "WX": "ZhangH998i",
            "AJMC": "xxx被诈骗案",
            "WHCDName": "高中",
            "GJ": "CHN",
            "XBName": "男",
            "MZName": "汉族",
            "SFZH": "xxxx81199812277518",
            "GZDW": "不详",
            "ZZMMName": "群众",
            "XB": "1",
            "AJLX": "02",
            "RYBH": "Rxxxx815103002023063008",
            "MZ": "01",
            "AJBH": "Axxxx815101002023066004",
            "XM":XXX贵",
            "SFDA": "0",
            "CBDW_BHName": "xx市公安局xx派出所社区警务中队",
            "LXFS": "不详",
            "CBDW_BH": "xxxx81510300",
            "LRSJ": "2023-06-14 09:26:49",
            "CBQY_BH": "xxxx81"
        }
    ]
}
# 开具文书
## 请求
### 标头
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
    Wed, 04 Mar 2026 08:04:05 GMT
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
    Bearer 130853e5-da8b-4cc9-878a-791649743056
    Connection:
    keep-alive
    Content-Length:
    2296
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
### 参数
    access_token: 130853e5-da8b-4cc9-878a-791649743056
    quickFilter: C75050E4832B4F5C882B7AE04B11FAC3
    modelId: 3014B3FB4791461998A87D794ED94077
    where: {"rules":[{"field":"AJBH","op":"like","value":"","type":"string","format":""},{"field":"WS_ID","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"WSZH","op":"like","value":"","type":"string","format":""},{"field":"XGRY_XM","op":"like","value":"","type":"string","format":""},{"field":"DYCS","op":"between","value":"0|999","type":"number","format":""},{"field":"DYSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"CBDW_MC","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"CBDW_BH_1","op":"like","value":"","type":"string","format":""},{"field":"CBR_XM","op":"like","value":"","type":"string","format":""},{"field":"KJSJ","op":"between","value":"2026/03/01 00:00:00|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"SPSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"WSZT","op":"like","value":"03","type":"string","format":"","linkOp":"or"}],"op":"and"}
    mark: yshdws
    modelName: 已审核的文书SZ
    pkName: ID
    modelMark: yshdws
    resType: 02
    funcMark: yshdws
    funcId: 6CDD88E5222140A1B66E608814697B84
    resId: 17CF516331DE4DF6800761D9452BAAEF
    page: 1
    pagesize: 10
    sortname: KJSJ,LRSJ,SPSJ,DYSJ
    sortorder: desc,desc,asc,asc
### 响应
{
    "msg": "查询成功！",
    "code": 200,
    "success": true,
    "Total": 1508,
    "Data": {},
    "Rows": [
        {
            "AJMC": "xxx被诈骗案",
            "WSZH": "（刑）搜查字〔20xx〕316129号\n",
            "WSZTName": "审批通过",
            "DYCS": "1",
            "XGRY_XM": "xx",
            "CBDW_MC": "XX县公安局刑事侦查大队四中队",
            "ZJZ": "4A86B0A1E3EA4ECCB980311171D761BD",
            "DYSJ": "2026-03-04 15:57:22",
            "SPR_XM": "xx",
            "WS_ID": "361D9E275C1B45ECA88DBF6D8B3443B9",
            "AJBH": "Axxxx215700002026016005",
            "SPSJ": "2026-03-04 15:56:05",
            "KJSJ": "2026-03-04 15:56:00",
            "WSMC": "搜查证(xxx)",
            "BD_ID": "AE2C8DADB23040FA8D7B3D239C9B091D",
            "ID": "4C2C567C4E28CC99E06316E21D4464B8",
            "CBR_XM": "xxx",
            "WSZT": "03",
            "LRSJ": "2026-03-04 15:30:56"
        }
    ]
}
# 刑事案件
## 请求
### 标头
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
    Wed, 04 Mar 2026 07:44:40 GMT
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
    Bearer 130853e5-da8b-4cc9-878a-791649743056
    Connection:
    keep-alive
    Content-Length:
    4539
    Content-Type:
    application/x-www-form-urlencoded; charset=UTF-8
    Host:
    68.26.7.47:1999
    Origin:
    http://68.26.7.148:999
    Referer:
    http://68.26.7.148:999/com/datagrid/gxkajjbxx(xs)
    User-Agent:
    Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36
### 参数
    access_token: 130853e5-da8b-4cc9-878a-791649743056
    modelId: D2B884737AFA432EBA3638DFE596FF11
    where: {"rules":[{"field":"AJBH","op":"like","value":"","type":"string","format":""},{"field":"JQBH","op":"like","value":"","type":"string","format":""},{"field":"AJMC","op":"like","value":"","type":"string","format":""},{"field":"AJZT","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"AYBH","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"XBR_XM","op":"like","value":"","type":"string","format":""},{"field":"CBDW_BH","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"CBQY_BH","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"SARY_XM","op":"like","value":"","type":"string","format":""},{"field":"FADD","op":"like","value":"","type":"string","format":""},{"field":"FASJ","op":"between","value":"|","type":"date","format":"yyyy-MM-dd HH:mm:ss"},{"field":"SLSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"LASJ","op":"between","value":"2026/03/01 00:00:00|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"YSSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm"},{"field":"CASJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"BYLASJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"JASJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"SSSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"SSJQDM","op":"like","value":"","type":"string","format":""},{"field":"PASJ","op":"between","value":"|","type":"date","format":"yyyy-MM-dd HH:mm:ss"},{"field":"XZSQBM","op":"like","value":"","type":"string","format":""},{"field":"LADM_MC","op":"like","value":"","type":"string","format":""},{"field":"JYAQ","op":"like","value":"","type":"string","format":""},{"field":"ZABS_KZXX_PD","op":"like","value":"","type":"string","format":""},{"field":"ZABS_KZXX","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"ZDGZ_LX","op":"like","value":"","type":"string","format":""},{"field":"ZDGZ_TXQK","op":"like","value":"","type":"string","format":"","linkOp":"or"}],"op":"and"}
    mark: gxkajjbxx(xs)
    modelName: 共享库案件基本信息(刑事)SZ
    pkName: ID
    modelMark: gxkajjbxxxs
    resType: 01
    funcMark: gxkajjbxx(xs)
    funcId: 2D9116F315764527A9954745F38C7522
    resId: 58C6239EB25E439B9F5C70E2C6E54A1B
    quickFilter: D7C9663A2AF540BA9F26D802EBA6A65D
    page: 1
    pagesize: 10
### 响应
{
    "msg": "查询成功！",
    "code": 200,
    "success": true,
    "Total": 32,
    "Data": {},
    "Rows": [
        {
            "ZABS_KZXX_PD": "1",
            "SSJQDMName": "",
            "ZABS_KZXX": "00",
            "ZDGZ_TXQKName": "",
            "ZABS_KZXX_PDName": "已填写",
            "AJLXName": "刑事",
            "BASJ": "2026-02-28 08:02:20",
            "CBDW_MC": "xx市公安局xx派出所",
            "SFSM": "0",
            "XYR_SFZH": "",
            "AYMC": "盗窃案",
            "LADM_MC": "xx市公安局xx派出所",
            "SLSJ": "2026-03-01 10:58:04",
            "YSSJ": "",
            "ID": "4BE9E68B5F935D25E06317E21D449221",
            "FADD": "xx市xx镇xx村委xx村白坟塝山山顶",
            "AJZTName": "已立案",
            "JYAQ": "2026年2月28日9时许，报警人xx在xx市粤澳华融能源科技有限公司运维时电脑后台发现到位于xx镇xx村委xx村光伏电站九号箱变11、14号逆变器电流中断，，xxx到xx镇xx村委xx村光伏电站进行检查，发现电站九号箱变11、14号逆变器的电缆被人剪断盗窃了，随后报警处理。报警人称现场被盗窃电线约3500米，估计价值约21000元。具体价值报警人暂未能提供发票等相关证据。",
            "XBR_XM": "xxx",
            "AJMC": "XX粤澳光伏电站被盗窃案",
            "SARY_XM": "xxx",
            "ZDGZ_LX": "0",
            "ZBR_SFZH": "xxxx81198811280032",
            "SSJQDM": "",
            "ZABS_KZXXName": "无",
            "AJLX": "02",
            "JASJ": "",
            "PASJ": "",
            "XZSQBM": "",
            "JQBH": "xxxx002026022808520300070",
            "LASJ": "2026-03-01 00:00:00",
            "ZDGZ_TXQK": "",
            "AJBH": "Axxxx816000002026036001",
            "FASJ": "2026-02-28 00:00:00",
            "ZDGZ_LXName": "未填写",
            "XYR_XM": "",
            "SLDW_MC": "xx市公安局xx派出所",
            "CBDW_BHName": "xx市公安局xx派出所",
            "CBDW_BH": "xxxx81600000",
            "XZSQBMName": "",
            "AJZT": "0202"
        }
    ]
}
# 行政案件
## 请求
### 标头
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
    Wed, 04 Mar 2026 07:41:33 GMT
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
    Bearer 130853e5-da8b-4cc9-878a-791649743056
    Connection:
    keep-alive
    Content-Length:
    4018
    Content-Type:
    application/x-www-form-urlencoded; charset=UTF-8
    Host:
    68.26.7.47:1999
    Origin:
    http://68.26.7.148:999
    Referer:
    http://68.26.7.148:999/com/datagrid/gxkajjbxx(xz)
    User-Agent:
    Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36
### 参数
    access_token: 130853e5-da8b-4cc9-878a-791649743056
    modelId: D2B884737AFA432EBA3638DFE596FF22
    where: {"rules":[{"field":"AJBH","op":"like","value":"","type":"string","format":""},{"field":"JQBH","op":"like","value":"","type":"string","format":""},{"field":"AJMC","op":"like","value":"","type":"string","format":""},{"field":"AJZT","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"AYBH","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"ZBR_XM","op":"like","value":"","type":"string","format":""},{"field":"CBDW_BH","op":"like","value":"","type":"string","format":""},{"field":"CBQY_BH","op":"like","value":"","type":"string","format":""},{"field":"SARY_XM","op":"like","value":"","type":"string","format":""},{"field":"FADD","op":"like","value":"","type":"string","format":""},{"field":"BASJ","op":"between","value":"|","type":"date","format":"yyyy-MM-dd HH:mm:ss"},{"field":"SLSJ","op":"between","value":"2026/03/01 00:00:00|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"YSSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"BYLASJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"JASJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"CFSJ","op":"between","value":"|","type":"date","format":"yyyy/MM/dd HH:mm:ss"},{"field":"TJSJ","op":"between","value":"|","type":"date","format":"yyyy-MM-dd"},{"field":"SSJQDM","op":"like","value":"","type":"string","format":""},{"field":"XZSQBM","op":"like","value":"","type":"string","format":""},{"field":"JYAQ","op":"like","value":"","type":"string","format":""},{"field":"ZABS_KZXX_PD","op":"like","value":"","type":"string","format":""},{"field":"ZABS_KZXX","op":"like","value":"","type":"string","format":"","linkOp":"or"},{"field":"ZDGZ_LX","op":"like","value":"","type":"string","format":""},{"field":"ZDGZ_TXQK","op":"like","value":"","type":"string","format":"","linkOp":"or"}],"op":"and"}
    mark: gxkajjbxx(xz)
    modelName: 共享库案件基本信息(行政)SZ
    pkName: ID
    modelMark: gxkajjbxxxz
    resType: 01
    funcMark: gxkajjbxx(xz)
    funcId: 2D9116F315764527A9954745F38C7511
    resId: 58C6239EB25E439B9F5C70E2C6E54A1B
    quickFilter: 69F915AB0D81416F8F2FD983BC85EE19
    page: 1
    pagesize: 10
### 响应
{
    "msg": "查询成功！",
    "code": 200,
    "success": true,
    "Total": 61,
    "Data": {},
    "Rows": [
        {
            "ZABS_KZXX_PD": "0",
            "SSJQDMName": "",
            "ZABS_KZXX": "",
            "ZDGZ_TXQKName": "",
            "ZABS_KZXX_PDName": "未填写",
            "AJLXName": "行政",
            "BASJ": "2026-03-01 00:44:37",
            "SFSM": "0",
            "CBDW_MC": "xx市公安局xx派出所",
            "CBQY_BHName": "xx市公安局",
            "AYMC": "赌博",
            "SLSJ": "2026-03-01 00:44:07",
            "ID": "4BD7AEE5713C2AB4E06317E21D443D72",
            "FADD": "",
            "AJZTName": "已处罚",
            "JYAQ": "2026年3月1日0时许，xx派出所巡逻到xx镇xx村委xx小学背后垌心祠堂旁边发现有xxx等人正在用打扑克玩“三公”大吃小的方式进行赌博。",
            "XBR_XM": "xxx",
            "AJMC": "xxx等人赌博案",
            "SARY_XM": "",
            "ZDGZ_LX": "0",
            "ZBR_SFZH": "xxxx81198904090414",
            "CFSJ": "2026-03-01 01:37:00",
            "SSJQDM": "",
            "ZABS_KZXXName": "",
            "TJSJ": "",
            "AJLX": "01",
            "JASJ": "",
            "XZSQBM": "",
            "JQBH": "Z538161000026030100403654",
            "ZDGZ_TXQK": "",
            "AJBH": "Axxxx816100002026036001",
            "ZDGZ_LXName": "未填写",
            "XYR_XM": "xxx",
            "ZBR_XM": "xxx",
            "CBDW_BHName": "xx市公安局xx派出所",
            "CBDW_BH": "xxxx81610000",
            "XZSQBMName": "",
            "AJZT": "0108",
            "CBQY_BH": "xxxx81"
        }
    ]
}