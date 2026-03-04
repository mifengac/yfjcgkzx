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

    function formatDateTime(text) {
        const date = new Date(text);
        if (Number.isNaN(date.getTime())) return "";
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, "0");
        const day = String(date.getDate()).padStart(2, "0");
        const hours = String(date.getHours()).padStart(2, "0");
        const minutes = String(date.getMinutes()).padStart(2, "0");
        const seconds = String(date.getSeconds()).padStart(2, "0");
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    }

    function setDefaultTimeRange(startEl, endEl) {
        const now = new Date();
        const today0 = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
        const start = new Date(today0);
        start.setDate(start.getDate() - 7);
        if (startEl) startEl.value = formatDateTimeLocal(start);
        if (endEl) endEl.value = formatDateTimeLocal(today0);
    }

    function escapeHtml(text) {
        return String(text || "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function parseContentDispositionFilename(resp, fallbackName) {
        const cd = resp.headers.get("content-disposition") || "";
        const matched = cd.match(/filename\*=UTF-8''([^;]+)/i) || cd.match(/filename="?([^;"]+)"?/i);
        if (!matched || !matched[1]) return fallbackName;
        try {
            return decodeURIComponent(matched[1]);
        } catch (_e) {
            return matched[1];
        }
    }

    window.PcsJqAjTjTab = window.PcsJqAjTjTab || {};
    window.PcsJqAjTjTab.helpers = {
        formatDateTimeLocal,
        formatDateTime,
        setDefaultTimeRange,
        escapeHtml,
        parseContentDispositionFilename,
    };
})();
