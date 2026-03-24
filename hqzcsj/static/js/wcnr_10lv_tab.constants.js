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
        "专门教育学生结业后犯罪数": "方正学校矫治<6个月；离校人数取 ywdata.zq_zfba_wcnr_sfzxx_lxxx，统计期内离校后再次犯罪人数/离校人数。",
        "专门(矫治)教育学生结业后再犯数": "方正学校离校人数取 ywdata.zq_zfba_wcnr_sfzxx_lxxx，统计期内离校后再次违法犯罪人数/离校人数。",
        "案件(场所被侵害)": "按 ajxx_jyaq 关键词过滤：KTV、酒吧、夜总会、迪厅、网吧、清吧、台球、桌球、俱乐部、棋牌、麻将、打牌、打扑克。",
        "刑事占比": "未成年嫌疑人刑事案件数/全量刑事案件数(不含交通)。",
        "严重不良未成年人矫治教育占比": "采取矫治教育措施人数/违法犯罪未成年人数。\n分母：发案时<18岁，且满足不予行政处罚、行政拘留不执行/不送、或刑事未开拘留证之一。\n分子：以上人员中已采取矫治教育措施人数。",
        "涉刑人员送生占比": "提请专门教育人数/因不满责任年龄不予刑事处罚人数。\n分母：刑事案件中发案时<16岁的嫌疑人，且满足以下任一条件：案件含不予立案/撤销案件文书（仅匹配案件编号）；或案件和人员含不予行政处罚文书（匹配案件编号+姓名）。\n分子：以上人员中，案件和人员另含【提请专门教育】文书（匹配案件编号+姓名）。",
        "专门(矫治)教育占比": "提请专门(矫治)教育人数/符合专门(矫治)教育人数。\n分母：结果集中“是否符合专门(矫治)教育”为“是”的人数；行政口径包括拘留不执行、2次违法且案由相同且第一次开具训诫书/责令遵守特定行为规范通知书、3次及以上违法；刑事口径为未刑拘且未开具《终止侦查决定书》。\n分子：在分母范围内，结果集中“是否开具专门(矫治)教育申请书”为“是”的人数。\n地区：按 ajxx_join_ajxx_cbdw_bh_dm 前六位；类型：按 ajxx_join_ajxx_ay 匹配；时间：按 xyrxx_lrsj 过滤。",
        "纳管人员再犯占比": "列管严重不良行为未成年人在列管后再次违反犯罪人数/列管严重不良未成年人人数",
        "责令加强监护数": "已责令加强监护数/应责令加强监护数。\n分母（应责令加强监护数）：基于v_wcnr_wfry_jbxx_base视图，按案件编号分组，统计每案件中发案时<18岁的未成年嫌疑人数（去重）。\n分子（已责令加强监护数）：按案件编号统计zq_zfba_jtjyzdtzs2（家庭教育指导通知书）记录数。",
    };

    window.Wcnr10lvTab = window.Wcnr10lvTab || {};
    window.Wcnr10lvTab.constants = {
        REGION_COL: "地区",
        METRICS,
        METRIC_BY_KEY,
        METRIC_LOGIC_BY_LABEL,
    };
})();
