(function() {
    const CURRENT_METRIC_COLS = [
        "警情",
        "转案数",
        "案件数(被侵害)",
        "场所案件(被侵害)",
        "行政",
        "刑事",
        "场所案件",
        "治安处罚",
        "治安处罚(不执行)",
        "刑拘",
        "矫治文书(行政)",
        "矫治文书(刑事)",
        "加强监督教育(行政)",
        "加强监督教育(刑事)",
        "符合送校",
        "送校",
    ];

    const YOY_META_BY_COL = {
        "同比警情": { currentCol: "警情", unit: "起" },
        "同比转案数": { currentCol: "转案数", unit: "起" },
        "同比案件数(被侵害)": { currentCol: "案件数(被侵害)", unit: "起" },
        "同比场所案件(被侵害)": { currentCol: "场所案件(被侵害)", unit: "起" },
        "同比行政": { currentCol: "行政", unit: "起" },
        "同比刑事": { currentCol: "刑事", unit: "起" },
        "同比场所案件": { currentCol: "场所案件", unit: "起" },
        "同比治安处罚": { currentCol: "治安处罚", unit: "人次" },
        "同比治安处罚(不执行)": { currentCol: "治安处罚(不执行)", unit: "人次" },
        "同比刑拘": { currentCol: "刑拘", unit: "人次" },
        "同比矫治文书(行政)": { currentCol: "矫治文书(行政)", unit: "人次" },
        "同比矫治文书(刑事)": { currentCol: "矫治文书(刑事)", unit: "人次" },
        "同比加强监督教育(行政)": { currentCol: "加强监督教育(行政)", unit: "人次" },
        "同比加强监督教育(刑事)": { currentCol: "加强监督教育(刑事)", unit: "人次" },
        "同比送校": { currentCol: "送校", unit: "人次" },
    };

    const HB_META_BY_COL = {};
    Object.values(YOY_META_BY_COL).forEach((meta) => {
        HB_META_BY_COL[`环比${meta.currentCol}`] = {
            currentCol: meta.currentCol,
            unit: meta.unit,
        };
    });

    const METRIC_BY_COL = {};
    for (const metric of CURRENT_METRIC_COLS) {
        METRIC_BY_COL[metric] = metric;
        if (YOY_META_BY_COL[`同比${metric}`]) {
            METRIC_BY_COL[`同比${metric}`] = metric;
            METRIC_BY_COL[`环比${metric}`] = metric;
        }
    }

    window.WcnrJqAjTab = window.WcnrJqAjTab || {};
    window.WcnrJqAjTab.constants = {
        REGION_COL: "地区",
        CURRENT_METRIC_COLS,
        YOY_META_BY_COL,
        HB_META_BY_COL,
        METRIC_BY_COL,
    };
})();
