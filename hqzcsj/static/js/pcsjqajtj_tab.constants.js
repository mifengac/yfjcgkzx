(function() {
    const ALWAYS_COLS = [
        "所属分局",
        "派出所名称",
        "警情",
        "同比警情",
        "转案率",
        "同比转案率",
        "行政",
        "同比行政",
        "刑事",
        "同比刑事",
        "办结率",
        "破案率",
        "高质量",
        "同比高质量",
        "治拘",
        "同比治拘",
        "刑拘",
        "同比刑拘",
        "逮捕",
        "同比逮捕",
        "起诉",
        "同比起诉",
    ];

    const RATIO_COLS = [
        { after: "同比警情", col: "同比警情比例" },
        { after: "同比行政", col: "同比行政比例" },
        { after: "同比刑事", col: "同比刑事比例" },
        { after: "同比高质量", col: "同比高质量比例" },
        { after: "同比治拘", col: "同比治拘比例" },
        { after: "同比刑拘", col: "同比刑拘比例" },
        { after: "同比逮捕", col: "同比逮捕比例" },
        { after: "同比起诉", col: "同比起诉比例" },
    ];

    const CLICKABLE_COLS = new Set([
        "警情",
        "同比警情",
        "行政",
        "同比行政",
        "刑事",
        "同比刑事",
        "高质量",
        "同比高质量",
        "治拘",
        "同比治拘",
        "刑拘",
        "同比刑拘",
        "逮捕",
        "同比逮捕",
        "起诉",
        "同比起诉",
    ]);

    const METRIC_BY_COL = {
        "警情": "警情",
        "同比警情": "警情",
        "行政": "行政",
        "同比行政": "行政",
        "刑事": "刑事",
        "同比刑事": "刑事",
        "高质量": "高质量",
        "同比高质量": "高质量",
        "治拘": "治拘",
        "同比治拘": "治拘",
        "刑拘": "刑拘",
        "同比刑拘": "刑拘",
        "逮捕": "逮捕",
        "同比逮捕": "逮捕",
        "起诉": "起诉",
        "同比起诉": "起诉",
    };

    function getDisplayColumns(showRatio) {
        if (!showRatio) {
            return ALWAYS_COLS.slice();
        }
        const cols = [];
        for (const col of ALWAYS_COLS) {
            cols.push(col);
            for (const ratio of RATIO_COLS) {
                if (ratio.after === col) {
                    cols.push(ratio.col);
                }
            }
        }
        return cols;
    }

    window.PcsJqAjTjTab = window.PcsJqAjTjTab || {};
    window.PcsJqAjTjTab.constants = {
        ALWAYS_COLS,
        RATIO_COLS,
        CLICKABLE_COLS,
        METRIC_BY_COL,
        getDisplayColumns,
    };
})();
