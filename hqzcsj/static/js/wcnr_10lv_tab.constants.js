(function() {
    const METRICS = [
        { key: "jq", label: "警情", type: "count", unit: "起" },
        { key: "za_rate", label: "转案率", type: "ratio" },
        { key: "xingzheng", label: "行政", type: "count", unit: "起" },
        { key: "xingshi", label: "刑事", type: "count", unit: "起" },
        { key: "bqh_case", label: "案件(被侵害)", type: "count", unit: "起" },
        { key: "wfzf_people", label: "违法犯罪人员", type: "count", unit: "人" },
        {
            key: "zmy_reoff",
            label: "专门教育学生结业后犯罪数",
            type: "composite",
            rateLabel: "专门教育学生结业后再犯率",
        },
        {
            key: "zmjz_reoff",
            label: "专门(矫治)教育学生结业后再犯数",
            type: "composite",
            rateLabel: "专门(矫治)教育学生结业后再犯率",
        },
        { key: "cs_bqh_case", label: "案件(场所被侵害)", type: "count", unit: "起" },
        {
            key: "xingshi_ratio",
            label: "刑事占比",
            type: "composite",
            rateLabel: "刑事占比率",
        },
        {
            key: "yzbl_ratio",
            label: "严重不良未成年人矫治教育占比",
            type: "composite",
            rateLabel: "严重不良未成年人矫治教育占比率",
        },
        {
            key: "sx_songjiao_ratio",
            label: "涉刑人员送生占比",
            type: "composite",
            rateLabel: "涉刑人员送矫率",
        },
        {
            key: "zmjz_ratio",
            label: "专门(矫治)教育占比",
            type: "composite",
            rateLabel: "专门(矫治)教育占比率",
        },
        {
            key: "naguan_ratio",
            label: "纳管人员再犯占比",
            type: "composite",
            rateLabel: "纳管人员再犯率",
        },
        {
            key: "zljiaqjh",
            label: "责令加强监护数",
            type: "composite",
            rateLabel: "责令加强监护率",
        },
    ];

    const METRIC_BY_KEY = {};
    for (const m of METRICS) {
        METRIC_BY_KEY[m.key] = m;
    }

    const METRIC_LOGIC_BY_LABEL = {
        "警情": "1. 确认警情性质:刑事治安\n2. 警情标注:未成年人",
        "转案率": "查询到的警情中最终受立为刑事/行政案件比例(有案件编号)",
        "行政": "1. 发案时年龄1-18岁(不包含18)\n2. 时间:立案时间\n3. 案件状态不包含:已合并、已撤销、已移交、不予立案\n4. 办案单位不包含:交通",
        "刑事": "1. 发案时年龄1-18岁(不包含18)\n2. 时间:立案时间\n3. 案件状态不包含:已合并、已撤销、已移交、不予立案\n4. 办案单位不包含:交通",
        "违法犯罪人员": "1. 行政刑事案件中违法犯罪嫌疑人案发时年龄小于18岁的未成年人",
        "专门教育学生结业后犯罪数": "方正学校矫治<6个月；统计期内离校后再次犯罪人数/离校人数。",
        "专门(矫治)教育学生结业后再犯数": "统计期内方正学校离校学生再次违法犯罪人数/离校人数。",
        "案件(场所被侵害)": "按 ajxx_jyaq 关键词过滤：KTV、酒吧、夜总会、迪厅、网吧、清吧、台球、桌球、俱乐部、棋牌、麻将、打牌、打扑克。",
        "刑事占比": "未成年嫌疑人刑事案件数/全量刑事案件数(不含交通)。",
        "严重不良未成年人矫治教育占比": "采取矫治教育措施人数/违法犯罪未成年人数。\n分母：发案时<18岁，且满足不予行政处罚、行政拘留不执行/不送、或刑事未开拘留证之一。\n分子：以上人员中已采取矫治教育措施人数。",
        "涉刑人员送生占比": "提请专门教育人数/因不满责任年龄不予刑事处罚人数。\n分母：发案时<16岁，且案件含不予立案、撤销案件、终止侦查或不予行政处罚文书。\n分子：以上案件和人员另含“提请专门教育”文书。",
        "专门(矫治)教育占比": "已送人数/符合专门(矫治)教育人数。\n分母：年龄>12，且满足二次违法首犯有训诫、三次及以上违法、治拘>4天、或刑事未刑拘之一。\n分子：送方正学校、刑拘、或治拘>4天且执行拘留。",
        "纳管人员再犯占比": "列管严重不良行为未成年人在列管后再次违反犯罪人数/列管严重不良未成年人人数",
        "责令加强监护数": "开具《加强监督教育/责令接受家庭监督指导通知书》数量/违法犯罪未成年人数。\n分母：案件中发案时<18岁的嫌疑人。\n分子：按案件编号统计文书数量；同案2名未成年人且开具2份文书，则计2。",
    };

    window.Wcnr10lvTab = window.Wcnr10lvTab || {};
    window.Wcnr10lvTab.constants = {
        REGION_COL: "地区",
        METRICS,
        METRIC_BY_KEY,
        METRIC_LOGIC_BY_LABEL,
    };
})();
