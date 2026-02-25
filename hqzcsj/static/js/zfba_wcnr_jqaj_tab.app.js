(function() {
    const NS = window.WcnrJqAjTab || {};
    const C = NS.constants || {};
    const H = NS.helpers || {};
    const endpoints = window.WCNR_JQAJ_ENDPOINTS || {};

    const API_LEIXING_URL = endpoints.apiLeixing || "/hqzcsj/zfba_wcnr_jqaj/api/leixing";
    const API_SUMMARY_URL = endpoints.apiSummary || "/hqzcsj/zfba_wcnr_jqaj/api/summary";
    const DETAIL_PAGE_URL = endpoints.detailPage || "/hqzcsj/zfba_wcnr_jqaj/detail";
    const EXPORT_SUMMARY_URL = endpoints.exportSummary || "/hqzcsj/zfba_wcnr_jqaj/export";
    const REPORT_EXPORT_URL = endpoints.reportExport || "/hqzcsj/zfba_wcnr_jqaj/report_export";
    const EXPORT_DETAIL_ALL_URL = endpoints.exportDetailAll || "/hqzcsj/zfba_wcnr_jqaj/detail/export_all";

    const startEl = document.getElementById("wcnrStart");
    const endEl = document.getElementById("wcnrEnd");
    const hbStartEl = document.getElementById("wcnrHbStart");
    const hbEndEl = document.getElementById("wcnrHbEnd");
    const statusEl = document.getElementById("wcnrStatus");
    const errEl = document.getElementById("wcnrErr");
    const tbl = document.getElementById("wcnrTbl");
    const queryBtn = document.getElementById("wcnrQueryBtn");
    const exportAllBtn = document.getElementById("wcnrExportAllBtn");
    const showHbEl = document.getElementById("wcnrShowHb");
    const showRatioEl = document.getElementById("wcnrShowRatio");

    if (!startEl || !endEl || !hbStartEl || !hbEndEl || !statusEl || !errEl || !tbl || !queryBtn || !exportAllBtn) {
        return;
    }

    let lastMeta = null;
    let lastRows = [];

    const dd = document.getElementById("wcnrExportDd");
    const exportBtn = document.getElementById("wcnrExportBtn");
    if (!dd || !exportBtn) {
        return;
    }

    exportBtn.addEventListener("click", (e) => {
        e.preventDefault();
        dd.classList.toggle("open");
    });
    dd.querySelectorAll(".dropdown-menu a").forEach((a) => {
        a.addEventListener("click", (e) => {
            e.preventDefault();
            const fmt = a.getAttribute("data-fmt") || "xlsx";
            doExport(fmt);
            dd.classList.remove("open");
        });
    });
    document.addEventListener("click", (e) => {
        if (!dd.contains(e.target)) dd.classList.remove("open");
    });

    const reportDd = document.getElementById("wcnrReportDd");
    const reportBtn = document.getElementById("wcnrReportBtn");
    if (reportDd && reportBtn) {
        reportBtn.addEventListener("click", (e) => {
            e.preventDefault();
            reportDd.classList.toggle("open");
        });
        reportDd.querySelectorAll(".dropdown-menu a").forEach((a) => {
            a.addEventListener("click", (e) => {
                e.preventDefault();
                const fmt = a.getAttribute("data-fmt") || "xlsx";
                doReportExport(fmt);
                reportDd.classList.remove("open");
            });
        });
        document.addEventListener("click", (e) => {
            if (!reportDd.contains(e.target)) reportDd.classList.remove("open");
        });
    }

    const msDisplay = document.getElementById("wcnrTypesDisplay");
    const msDropdown = document.getElementById("wcnrTypesDropdown");

    function selectedTypes() {
        const boxes = Array.from(msDropdown.querySelectorAll('input[type="checkbox"]'));
        return boxes.filter((b) => b.value !== "_all" && b.checked).map((b) => b.value);
    }
    function syncAllBox() {
        const allBox = msDropdown.querySelector('input[value="_all"]');
        if (!allBox) return;
        const boxes = Array.from(msDropdown.querySelectorAll('input[type="checkbox"]')).filter((b) => b.value !== "_all");
        const allChecked = boxes.length > 0 && boxes.every((b) => b.checked);
        const noneChecked = boxes.length > 0 && boxes.every((b) => !b.checked);
        allBox.checked = allChecked;
        allBox.indeterminate = !allChecked && !noneChecked;
    }
    function renderMsLabel() {
        const boxes = Array.from(msDropdown.querySelectorAll('input[type="checkbox"]')).filter((b) => b.value !== "_all");
        const sel = selectedTypes();
        if (boxes.length === 0) {
            msDisplay.textContent = "无类型";
            return;
        }
        if (sel.length === 0) {
            msDisplay.textContent = "未选择(默认全量)";
            return;
        }
        if (sel.length === boxes.length) {
            msDisplay.textContent = "全部";
            return;
        }
        msDisplay.textContent = `已选 ${sel.length} 项`;
    }
    function openMs() {
        msDropdown.classList.add("open");
    }
    function closeMs() {
        msDropdown.classList.remove("open");
    }
    msDisplay.addEventListener("click", (e) => {
        e.stopPropagation();
        if (msDropdown.classList.contains("open")) closeMs(); else openMs();
    });
    msDropdown.addEventListener("click", (e) => e.stopPropagation());
    document.addEventListener("click", () => closeMs());
    msDropdown.addEventListener("change", (e) => {
        const t = e.target;
        if (!t || t.tagName !== "INPUT") return;
        if (t.value === "_all") {
            const boxes = Array.from(msDropdown.querySelectorAll('input[type="checkbox"]')).filter((b) => b.value !== "_all");
            boxes.forEach((b) => { b.checked = t.checked; });
        } else {
            syncAllBox();
        }
        renderMsLabel();
    });

    async function loadTypes() {
        msDropdown.innerHTML = '<div class="muted" style="padding:8px;">加载中...</div>';
        const resp = await fetch(API_LEIXING_URL);
        const data = await resp.json();
        if (!data.success) throw new Error(data.message || "加载类型失败");
        const items = data.data || [];
        const html = [];
        html.push('<label class="multi-select-option"><input type="checkbox" value="_all"><span>全选</span></label>');
        for (const x of items) {
            const s = String(x || "").trim();
            if (!s) continue;
            html.push(`<label class="multi-select-option"><input type="checkbox" value="${s.replace(/\"/g, "&quot;")}"><span>${s}</span></label>`);
        }
        msDropdown.innerHTML = html.join("");
        syncAllBox();
        renderMsLabel();
    }

    const zaDisplay = document.getElementById("wcnrZaDisplay");
    const zaDropdown = document.getElementById("wcnrZaDropdown");
    const ZA_OPTS = ["警告", "罚款", "拘留"];

    function selectedZaTypes() {
        const boxes = Array.from(zaDropdown.querySelectorAll('input[type="checkbox"]'));
        return boxes.filter((b) => b.value !== "_all" && b.checked).map((b) => b.value);
    }
    function syncZaAllBox() {
        const allBox = zaDropdown.querySelector('input[value="_all"]');
        if (!allBox) return;
        const boxes = Array.from(zaDropdown.querySelectorAll('input[type="checkbox"]')).filter((b) => b.value !== "_all");
        const allChecked = boxes.length > 0 && boxes.every((b) => b.checked);
        const noneChecked = boxes.length > 0 && boxes.every((b) => !b.checked);
        allBox.checked = allChecked;
        allBox.indeterminate = !allChecked && !noneChecked;
    }
    function renderZaLabel() {
        const boxes = Array.from(zaDropdown.querySelectorAll('input[type="checkbox"]')).filter((b) => b.value !== "_all");
        const sel = selectedZaTypes();
        if (boxes.length === 0) {
            zaDisplay.textContent = "无选项";
            return;
        }
        if (sel.length === 0) {
            zaDisplay.textContent = "未选择(默认全量)";
            return;
        }
        if (sel.length === boxes.length) {
            zaDisplay.textContent = "全部";
            return;
        }
        zaDisplay.textContent = `已选 ${sel.length} 项`;
    }
    function openZa() {
        zaDropdown.classList.add("open");
    }
    function closeZa() {
        zaDropdown.classList.remove("open");
    }
    zaDisplay.addEventListener("click", (e) => {
        e.stopPropagation();
        if (zaDropdown.classList.contains("open")) closeZa(); else openZa();
    });
    zaDropdown.addEventListener("click", (e) => e.stopPropagation());
    document.addEventListener("click", () => closeZa());
    zaDropdown.addEventListener("change", (e) => {
        const t = e.target;
        if (!t || t.tagName !== "INPUT") return;
        if (t.value === "_all") {
            const boxes = Array.from(zaDropdown.querySelectorAll('input[type="checkbox"]')).filter((b) => b.value !== "_all");
            boxes.forEach((b) => { b.checked = t.checked; });
        } else {
            syncZaAllBox();
        }
        renderZaLabel();
    });

    function initZaOptions() {
        const html = [];
        html.push('<label class="multi-select-option"><input type="checkbox" value="_all"><span>全选</span></label>');
        for (const s of ZA_OPTS) {
            html.push(`<label class="multi-select-option"><input type="checkbox" value="${s}"><span>${s}</span></label>`);
        }
        zaDropdown.innerHTML = html.join("");
        syncZaAllBox();
        renderZaLabel();
    }

    function buildBaseQueryParams(metaOverride) {
        const st = (metaOverride && metaOverride.start_time) ? metaOverride.start_time : H.formatDateTime(startEl.value);
        const et = (metaOverride && metaOverride.end_time) ? metaOverride.end_time : H.formatDateTime(endEl.value);
        const hbst = (metaOverride && metaOverride.hb_start_time) ? metaOverride.hb_start_time : H.formatDateTime(hbStartEl.value);
        const hbet = (metaOverride && metaOverride.hb_end_time) ? metaOverride.hb_end_time : H.formatDateTime(hbEndEl.value);
        const types = selectedTypes();
        const zaTypes = selectedZaTypes();
        const usp = new URLSearchParams();
        if (st) usp.set("start_time", st);
        if (et) usp.set("end_time", et);
        if (hbst) usp.set("hb_start_time", hbst);
        if (hbet) usp.set("hb_end_time", hbet);
        for (const t of types) usp.append("leixing", t);
        for (const z of zaTypes) usp.append("za_type", z);
        return usp;
    }

    function buildReportQueryParams() {
        const st = H.formatDateTime(startEl.value);
        const et = H.formatDateTime(endEl.value);
        const types = selectedTypes();
        const usp = new URLSearchParams();
        if (st) usp.set("start_time", st);
        if (et) usp.set("end_time", et);
        for (const t of types) usp.append("leixing", t);
        return usp;
    }

    function applyDisplayFlags(usp, showRatio, showHb) {
        usp.set("show_ratio", showRatio ? "1" : "0");
        usp.set("show_hb", showHb ? "1" : "0");
        return usp;
    }

    function renderTable(rows, meta) {
        const showRatio = !!(showRatioEl && showRatioEl.checked);
        const showHb = !!(showHbEl && showHbEl.checked);
        const cols = H.getDisplayCols({
            regionCol: C.REGION_COL,
            currentMetricCols: C.CURRENT_METRIC_COLS,
            yoyMetaByCol: C.YOY_META_BY_COL,
            showRatio,
            showHb,
        });

        const thead = `<thead><tr>${cols.map((c) => `<th>${c}</th>`).join("")}</tr></thead>`;
        const tbodyRows = (rows || []).map((r) => {
            const diquName = r["地区"] || "";
            const diquCode = r["地区代码"] || "__ALL__";
            const rowClass = (diquName === "全市" || diquCode === "__ALL__") ? "total-row" : "";
            const tds = cols.map((c) => {
                if (c === C.REGION_COL) return `<td class="rowh">${diquName}</td>`;

                if (H.isRatioColumn(c)) {
                    const ratioVal = (r[c] == null) ? "" : r[c];
                    return `<td class="num-cell ratio-cell">${ratioVal}</td>`;
                }

                const raw = (r[c] == null) ? 0 : r[c];
                if (!raw || Number(raw) <= 0) return `<td class="num-cell">${raw || 0}</td>`;

                const metric = C.METRIC_BY_COL[c] || "";
                if (!metric) return `<td class="num-cell">${raw}</td>`;

                const isYoy = String(c).startsWith("同比");
                const isHb = String(c).startsWith("环比");
                const st = isYoy ? (meta.yoy_start_time || "") : (isHb ? (meta.hb_start_time || "") : (meta.start_time || ""));
                const et = isYoy ? (meta.yoy_end_time || "") : (isHb ? (meta.hb_end_time || "") : (meta.end_time || ""));

                const usp = buildBaseQueryParams({ start_time: st, end_time: et });
                usp.set("metric", metric);
                usp.set("diqu", diquCode);
                const href = `${DETAIL_PAGE_URL}?${usp.toString()}`;
                return `<td class="num-cell clickable-cell" data-href="${href}" style="cursor:pointer; color:#1976d2; font-weight:800;">${raw}</td>`;
            });
            return `<tr class="${rowClass}">${tds.join("")}</tr>`;
        }).join("");

        tbl.innerHTML = thead + `<tbody>${tbodyRows || `<tr><td colspan="${cols.length}" class="muted">无数据</td></tr>`}</tbody>`;

        tbl.querySelectorAll("td.clickable-cell").forEach((td) => {
            td.addEventListener("click", () => {
                const href = td.getAttribute("data-href");
                if (!href) return;
                try {
                    const openedInModal = typeof window.wcnrOpenDetail === "function" ? window.wcnrOpenDetail(href) : false;
                    if (openedInModal === false) {
                        window.open(href, "_blank");
                    }
                } catch (e) {
                    errEl.textContent = `打开明细失败：${e && e.message ? e.message : String(e)}`;
                    window.open(href, "_blank");
                }
            });
        });
    }

    async function query() {
        errEl.textContent = "";
        statusEl.textContent = "查询中...";
        queryBtn.disabled = true;
        try {
            const usp = applyDisplayFlags(buildBaseQueryParams(), true, true);
            const resp = await fetch(`${API_SUMMARY_URL}?${usp.toString()}`);
            const data = await resp.json();
            if (!data.success) throw new Error(data.message || "查询失败");
            statusEl.textContent = `当前：${data.meta.start_time} ~ ${data.meta.end_time}；同比：${data.meta.yoy_start_time} ~ ${data.meta.yoy_end_time}；环比：${data.meta.hb_start_time} ~ ${data.meta.hb_end_time}`;
            lastMeta = data.meta || null;
            lastRows = data.rows || [];
            renderTable(lastRows, data.meta || {});
        } catch (e) {
            errEl.textContent = e.message || String(e);
            statusEl.textContent = "";
            lastRows = [];
        } finally {
            queryBtn.disabled = false;
        }
    }

    function doExport(fmt) {
        const showRatio = !!(showRatioEl && showRatioEl.checked);
        const showHb = !!(showHbEl && showHbEl.checked);
        const usp = applyDisplayFlags(buildBaseQueryParams(), showRatio, showHb);
        usp.set("fmt", fmt);
        const href = `${EXPORT_SUMMARY_URL}?${usp.toString()}`;
        window.location.href = href;
    }

    function doReportExport(fmt) {
        const usp = buildReportQueryParams();
        usp.set("fmt", fmt);
        const href = `${REPORT_EXPORT_URL}?${usp.toString()}`;
        window.location.href = href;
    }

    function exportAllDetail() {
        errEl.textContent = "";
        const usp = buildBaseQueryParams();
        const href = `${EXPORT_DETAIL_ALL_URL}?${usp.toString()}`;
        window.location.href = href;
    }

    queryBtn.addEventListener("click", (e) => {
        e.preventDefault();
        query();
    });
    exportAllBtn.addEventListener("click", (e) => {
        e.preventDefault();
        exportAllDetail();
    });

    if (showRatioEl) {
        showRatioEl.addEventListener("change", () => {
            renderTable(lastRows, lastMeta || {});
        });
    }
    if (showHbEl) {
        showHbEl.addEventListener("change", () => {
            renderTable(lastRows, lastMeta || {});
        });
    }

    window.wcnrOpenDetail = function(href) {
        const modal = document.getElementById("wcnrDetailModal");
        const frame = document.getElementById("wcnrDetailFrame");
        if (!href) return false;
        if (!modal || !frame) return false;
        frame.src = href;
        modal.style.display = "flex";
        return true;
    };
    window.wcnrCloseModal = function(event) {
        if (event && event.target && event.target.id !== "wcnrDetailModal") return;
        const modal = document.getElementById("wcnrDetailModal");
        const frame = document.getElementById("wcnrDetailFrame");
        modal.style.display = "none";
        frame.src = "about:blank";
    };

    H.setDefaultTimeRange({ startEl, endEl, hbStartEl, hbEndEl });
    initZaOptions();

    let hasAutoQueried = false;
    const typesLoaded = loadTypes().catch((e) => {
        errEl.textContent = e.message || String(e);
        msDisplay.textContent = "加载失败";
        throw e;
    });

    async function maybeAutoQuery() {
        if (hasAutoQueried) return;
        hasAutoQueried = true;
        await typesLoaded;
        await query();
    }

    const wcnrTabBtn = document.querySelector('#hqzcsjTabs .tab-btn[data-tab="wcnr"]');
    if (wcnrTabBtn) {
        wcnrTabBtn.addEventListener("click", () => maybeAutoQuery().catch(() => {}));
    }

    const wcnrPanel = document.getElementById("tab-wcnr");
    if (wcnrPanel && wcnrPanel.classList.contains("active")) {
        maybeAutoQuery().catch(() => {});
    }
})();
