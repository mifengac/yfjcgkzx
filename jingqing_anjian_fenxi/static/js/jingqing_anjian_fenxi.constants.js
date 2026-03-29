(function() {
    const SUMMARY_COLUMNS = [
        "分局",
        "当前分组名称",
        "及时立案平均小时",
        "及时研判抓人平均小时",
        "及时破案平均小时",
        "及时结案平均小时",
    ];

    const CLICKABLE_COLS = new Set([
        "及时立案平均小时",
        "及时研判抓人平均小时",
        "及时破案平均小时",
        "及时结案平均小时",
    ]);

    const METRIC_BY_COL = {
        "及时立案平均小时": "timely_filing",
        "及时研判抓人平均小时": "timely_arrest",
        "及时破案平均小时": "timely_solve",
        "及时结案平均小时": "timely_close",
    };

    window.JingqingAnjianFenxi = window.JingqingAnjianFenxi || {};
    window.JingqingAnjianFenxi.constants = {
        SUMMARY_COLUMNS,
        CLICKABLE_COLS,
        METRIC_BY_COL,
    };
})();
