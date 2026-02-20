(function() {
    const CURRENT_METRIC_COLS = [
        "警情",
        "转案数",
        "案件数",
        "行政",
        "刑事",
        "治安处罚",
        "刑拘",
        "逮捕",
        "起诉",
        "移送案件",
        "办结",
        "破案",
        "高质量",
    ];

    const YOY_META_BY_COL = {
        "同比警情": { currentCol: "警情", unit: "起" },
        "同比案件数": { currentCol: "案件数", unit: "起" },
        "同比行政": { currentCol: "行政", unit: "起" },
        "同比刑事": { currentCol: "刑事", unit: "起" },
        "同比治安处罚": { currentCol: "治安处罚", unit: "人次" },
        "同比刑拘": { currentCol: "刑拘", unit: "人次" },
        "同比逮捕": { currentCol: "逮捕", unit: "人次" },
        "同比起诉": { currentCol: "起诉", unit: "人次" },
        "同比移送案件": { currentCol: "移送案件", unit: "起" },
        "同比办结": { currentCol: "办结", unit: "起" },
        "同比破案": { currentCol: "破案", unit: "起" },
        "同比高质量": { currentCol: "高质量", unit: "起" },
    };

    const HB_META_BY_COL = {};
    Object.values(YOY_META_BY_COL).forEach((meta) => {
        HB_META_BY_COL[`环比${meta.currentCol}`] = {
            currentCol: meta.currentCol,
            unit: meta.unit,
        };
    });

    const RATIO_DEF_BY_COL = {};
    Object.entries(YOY_META_BY_COL).forEach(([compareCol, meta]) => {
        RATIO_DEF_BY_COL[`${compareCol}比例`] = {
            compareCol,
            currentCol: meta.currentCol,
            unit: meta.unit,
        };
    });
    Object.entries(HB_META_BY_COL).forEach(([compareCol, meta]) => {
        RATIO_DEF_BY_COL[`${compareCol}比例`] = {
            compareCol,
            currentCol: meta.currentCol,
            unit: meta.unit,
        };
    });

    const METRIC_BY_COL = {};
    for (const metric of CURRENT_METRIC_COLS) {
        METRIC_BY_COL[metric] = metric;
        METRIC_BY_COL[`同比${metric}`] = metric;
        METRIC_BY_COL[`环比${metric}`] = metric;
    }

    window.ZfbaJqAjTab = window.ZfbaJqAjTab || {};
    window.ZfbaJqAjTab.constants = {
        REGION_COL: "地区",
        CURRENT_METRIC_COLS,
        YOY_META_BY_COL,
        HB_META_BY_COL,
        RATIO_DEF_BY_COL,
        METRIC_BY_COL,
    };
})();
