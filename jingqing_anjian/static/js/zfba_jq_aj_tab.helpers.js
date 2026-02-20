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

    function formatDateTime(dateStr) {
        const date = new Date(dateStr);
        if (isNaN(date.getTime())) return "";
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, "0");
        const day = String(date.getDate()).padStart(2, "0");
        const hours = String(date.getHours()).padStart(2, "0");
        const minutes = String(date.getMinutes()).padStart(2, "0");
        const seconds = String(date.getSeconds()).padStart(2, "0");
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    }

    function getDisplayCols({
        regionCol,
        currentMetricCols,
        showRatio,
        showHb,
    }) {
        const cols = [regionCol];
        for (const metric of currentMetricCols) {
            const yoyCol = `同比${metric}`;
            const hbCol = `环比${metric}`;
            cols.push(metric);
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
                if (showRatio && metric !== "转案数") cols.push(`${hbCol}比例`);
            }
        }
        return cols;
    }

    function fmtPlainNumber(v) {
        const n = Number(v || 0);
        if (Number.isNaN(n)) return "0";
        if (Number.isInteger(n)) return String(n);
        return n.toFixed(2).replace(/\.?0+$/, "");
    }

    function calcRatioText(currentValue, compareValue, unit) {
        const currentNum = Number(currentValue || 0);
        const compareNum = Number(compareValue || 0);
        if (currentNum === compareNum) return "持平";
        if (currentNum === 0 && compareNum !== 0) return `下降${fmtPlainNumber(compareNum)}${unit}`;
        if (currentNum !== 0 && compareNum === 0) return `上升${fmtPlainNumber(currentNum)}${unit}`;
        if (compareNum === 0) return "持平";
        const ratio = ((currentNum - compareNum) / compareNum) * 100;
        return `${ratio.toFixed(2)}%`;
    }

    function calcPercentText(numerator, denominator) {
        const num = Number(numerator || 0);
        const den = Number(denominator || 0);
        if (!Number.isFinite(num) || !Number.isFinite(den) || den <= 0) return "0.00%";
        return `${((num / den) * 100).toFixed(2)}%`;
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

    window.ZfbaJqAjTab = window.ZfbaJqAjTab || {};
    window.ZfbaJqAjTab.helpers = {
        formatDateTimeLocal,
        formatDateTime,
        getDisplayCols,
        fmtPlainNumber,
        calcRatioText,
        calcPercentText,
        setDefaultTimeRange,
    };
})();
