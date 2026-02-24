(function() {
    function formatDateTimeLocal(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, "0");
        const day = String(date.getDate()).padStart(2, "0");
        const hours = String(date.getHours()).padStart(2, "0");
        const minutes = String(date.getMinutes()).padStart(2, "0");
        const seconds = String(date.getSeconds()).padStart(2, "0");
        return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
    }

    function formatDateTime(v) {
        const date = new Date(v);
        if (isNaN(date.getTime())) return "";
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, "0");
        const day = String(date.getDate()).padStart(2, "0");
        const hours = String(date.getHours()).padStart(2, "0");
        const minutes = String(date.getMinutes()).padStart(2, "0");
        const seconds = String(date.getSeconds()).padStart(2, "0");
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    }

    function getDisplayCols({ regionCol, currentMetricCols, yoyMetaByCol, showRatio, showHb }) {
        const cols = [regionCol];
        for (const metric of currentMetricCols) {
            const yoyCol = `同比${metric}`;
            const hbCol = `环比${metric}`;
            const hasCompare = !!yoyMetaByCol[yoyCol];
            cols.push(metric);
            if (!hasCompare) continue;

            cols.push(yoyCol);
            if (showRatio) {
                if (metric === "转案数") {
                    cols.push("转案率");
                    cols.push("同比转案率");
                } else {
                    cols.push(`${yoyCol}比例`);
                }
            }

            if (showHb) {
                cols.push(hbCol);
                if (showRatio) {
                    if (metric === "转案数") {
                        cols.push("环比转案率");
                    } else {
                        cols.push(`${hbCol}比例`);
                    }
                }
            }
        }
        return cols;
    }

    function isRatioColumn(colName) {
        const c = String(colName || "");
        return c.endsWith("比例") || c === "转案率" || c === "同比转案率" || c === "环比转案率";
    }

    function setDefaultTimeRange({ startEl, endEl, hbStartEl, hbEndEl }) {
        const now = new Date();
        const today0 = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);

        const start = new Date(today0);
        start.setDate(start.getDate() - 7);
        const end = new Date(today0);

        const hbStart = new Date(today0);
        hbStart.setDate(hbStart.getDate() - 14);
        const hbEnd = new Date(today0);
        hbEnd.setDate(hbEnd.getDate() - 7);
        hbEnd.setSeconds(hbEnd.getSeconds() - 1);

        startEl.value = formatDateTimeLocal(start);
        endEl.value = formatDateTimeLocal(end);
        hbStartEl.value = formatDateTimeLocal(hbStart);
        hbEndEl.value = formatDateTimeLocal(hbEnd);
    }

    window.WcnrJqAjTab = window.WcnrJqAjTab || {};
    window.WcnrJqAjTab.helpers = {
        formatDateTime,
        getDisplayCols,
        isRatioColumn,
        setDefaultTimeRange,
    };
})();
