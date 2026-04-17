(function() {
    const IDENTITY_COLS = ["所属分局", "派出所名称"];

    const METRIC_GROUPS = [
        {
            currentCol: "警情",
            yoyCol: "同比警情",
            hbCol: "环比警情",
            ratioCols: ["同比警情比例", "环比警情比例"],
            metric: "警情",
            clickable: true,
        },
        {
            currentCol: "转案率",
            yoyCol: "同比转案率",
            hbCol: "环比转案率",
            metric: "",
            clickable: false,
        },
        {
            currentCol: "行政",
            yoyCol: "同比行政",
            hbCol: "环比行政",
            ratioCols: ["同比行政比例", "环比行政比例"],
            metric: "行政",
            clickable: true,
        },
        {
            currentCol: "刑事",
            yoyCol: "同比刑事",
            hbCol: "环比刑事",
            ratioCols: ["同比刑事比例", "环比刑事比例"],
            metric: "刑事",
            clickable: true,
        },
        {
            currentCol: "办结率",
            hbCol: "环比办结率",
            metric: "",
            clickable: false,
        },
        {
            currentCol: "破案率",
            hbCol: "环比破案率",
            metric: "",
            clickable: false,
        },
        {
            currentCol: "高质量",
            yoyCol: "同比高质量",
            hbCol: "环比高质量",
            ratioCols: ["同比高质量比例", "环比高质量比例"],
            metric: "高质量",
            clickable: true,
        },
        {
            currentCol: "治拘",
            yoyCol: "同比治拘",
            hbCol: "环比治拘",
            ratioCols: ["同比治拘比例", "环比治拘比例"],
            metric: "治拘",
            clickable: true,
        },
        {
            currentCol: "刑拘",
            yoyCol: "同比刑拘",
            hbCol: "环比刑拘",
            ratioCols: ["同比刑拘比例", "环比刑拘比例"],
            metric: "刑拘",
            clickable: true,
        },
        {
            currentCol: "逮捕",
            yoyCol: "同比逮捕",
            hbCol: "环比逮捕",
            ratioCols: ["同比逮捕比例", "环比逮捕比例"],
            metric: "逮捕",
            clickable: true,
        },
        {
            currentCol: "起诉",
            yoyCol: "同比起诉",
            hbCol: "环比起诉",
            ratioCols: ["同比起诉比例", "环比起诉比例"],
            metric: "起诉",
            clickable: true,
        },
    ];

    const CLICKABLE_COLS = new Set();
    const METRIC_BY_COL = {};
    for (const group of METRIC_GROUPS) {
        if (!group.metric || !group.clickable) continue;
        for (const col of [group.currentCol, group.yoyCol, group.hbCol]) {
            if (!col) continue;
            CLICKABLE_COLS.add(col);
            METRIC_BY_COL[col] = group.metric;
        }
    }

    function getDisplayColumns(showRatio, showHb) {
        const cols = IDENTITY_COLS.slice();
        for (const group of METRIC_GROUPS) {
            cols.push(group.currentCol);
            if (group.yoyCol) {
                cols.push(group.yoyCol);
            }
            if (showRatio && Array.isArray(group.ratioCols) && group.ratioCols[0]) {
                cols.push(group.ratioCols[0]);
            }
            if (showHb && group.hbCol) {
                cols.push(group.hbCol);
                if (showRatio && Array.isArray(group.ratioCols) && group.ratioCols[1]) {
                    cols.push(group.ratioCols[1]);
                }
            }
        }
        return cols;
    }

    window.PcsJqAjTjTab = window.PcsJqAjTjTab || {};
    window.PcsJqAjTjTab.constants = {
        IDENTITY_COLS,
        METRIC_GROUPS,
        CLICKABLE_COLS,
        METRIC_BY_COL,
        getDisplayColumns,
    };
})();
