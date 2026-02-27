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

    function setDefaultTimeRange({ startEl, endEl, hbStartEl, hbEndEl }) {
        const now = new Date();
        const today0 = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);

        const start = new Date(today0);
        start.setDate(start.getDate() - 7);

        const hbStart = new Date(today0);
        hbStart.setDate(hbStart.getDate() - 14);

        const hbEnd = new Date(today0);
        hbEnd.setDate(hbEnd.getDate() - 7);

        startEl.value = formatDateTimeLocal(start);
        endEl.value = formatDateTimeLocal(today0);
        hbStartEl.value = formatDateTimeLocal(hbStart);
        hbEndEl.value = formatDateTimeLocal(hbEnd);
    }

    function getDisplayColumns(metrics, showHb, showRatio) {
        const cols = ["地区"];

        const addCount = (label) => {
            cols.push(label);
            cols.push(`同比${label}`);
            if (showRatio) cols.push(`同比${label}比例`);
            if (showHb) {
                cols.push(`环比${label}`);
                if (showRatio) cols.push(`环比${label}比例`);
            }
        };

        const addRatio = (label) => {
            cols.push(label);
            cols.push(`同比${label}`);
            if (showHb) cols.push(`环比${label}`);
        };

        const addComposite = (label, rateLabel) => {
            cols.push(label);
            cols.push(`同比${label}`);
            if (showRatio) {
                cols.push(rateLabel);
                cols.push(`同比${rateLabel}`);
            }
            if (showHb) {
                cols.push(`环比${label}`);
                if (showRatio) cols.push(`环比${rateLabel}`);
            }
        };

        for (const m of metrics || []) {
            if (m.type === "count") {
                addCount(m.label);
            } else if (m.type === "ratio") {
                addRatio(m.label);
            } else if (m.type === "composite") {
                addComposite(m.label, m.rateLabel);
            }
        }

        return cols;
    }

    function resolveDetailTarget(metrics, colName) {
        const col = String(colName || "");

        for (const m of metrics || []) {
            if (m.type === "count") {
                if (col === m.label) return { metric: m.key, part: "value", period: "current" };
                if (col === `同比${m.label}`) return { metric: m.key, part: "value", period: "yoy" };
                if (col === `环比${m.label}`) return { metric: m.key, part: "value", period: "hb" };
                if (col === `同比${m.label}比例` || col === `环比${m.label}比例`) {
                    return { metric: m.key, part: "value", period: "current" };
                }
            }

            if (m.type === "ratio") {
                if (col === m.label) return { metric: m.key, part: "numerator", period: "current" };
                if (col === `同比${m.label}`) return { metric: m.key, part: "numerator", period: "yoy" };
                if (col === `环比${m.label}`) return { metric: m.key, part: "numerator", period: "hb" };
            }

            if (m.type === "composite") {
                if (col === m.label) return { metric: m.key, part: "numerator", period: "current" };
                if (col === `同比${m.label}`) return { metric: m.key, part: "numerator", period: "yoy" };
                if (col === `环比${m.label}`) return { metric: m.key, part: "numerator", period: "hb" };
                if (col === m.rateLabel) return { metric: m.key, part: "numerator", period: "current" };
                if (col === `同比${m.rateLabel}`) return { metric: m.key, part: "numerator", period: "yoy" };
                if (col === `环比${m.rateLabel}`) return { metric: m.key, part: "numerator", period: "hb" };
            }
        }

        return null;
    }

    function isCompositeValue(text) {
        return /^\d+\/\d+$/.test(String(text || "").trim());
    }

    window.Wcnr10lvTab = window.Wcnr10lvTab || {};
    window.Wcnr10lvTab.helpers = {
        formatDateTime,
        getDisplayColumns,
        resolveDetailTarget,
        setDefaultTimeRange,
        isCompositeValue,
    };
})();
