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
            label: "专门教育学生结业后再犯数",
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
    ];

    const METRIC_BY_KEY = {};
    for (const m of METRICS) {
        METRIC_BY_KEY[m.key] = m;
    }

    window.Wcnr10lvTab = window.Wcnr10lvTab || {};
    window.Wcnr10lvTab.constants = {
        REGION_COL: "地区",
        METRICS,
        METRIC_BY_KEY,
    };
})();
