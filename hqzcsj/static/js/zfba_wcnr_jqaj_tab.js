(function() {
    const endpoints = window.WCNR_JQAJ_ENDPOINTS || {};
    const API_LEIXING_URL = endpoints.apiLeixing || "/hqzcsj/zfba_wcnr_jqaj/api/leixing";
    const API_SUMMARY_URL = endpoints.apiSummary || "/hqzcsj/zfba_wcnr_jqaj/api/summary";
    const DETAIL_PAGE_URL = endpoints.detailPage || "/hqzcsj/zfba_wcnr_jqaj/detail";
    const EXPORT_SUMMARY_URL = endpoints.exportSummary || "/hqzcsj/zfba_wcnr_jqaj/export";
    const EXPORT_DETAIL_ALL_URL = endpoints.exportDetailAll || "/hqzcsj/zfba_wcnr_jqaj/detail/export_all";

    const REGION_COL = "地区";
    const BASE_COLS = [
        "地区",
        "警情", "同比警情",
        "转案数", "同比转案数",
        "案件数(被侵害)", "同比案件数(被侵害)",
        "场所案件(被侵害)", "同比场所案件(被侵害)",
        "行政", "同比行政",
        "刑事", "同比刑事",
        "场所案件", "同比场所案件",
        "治安处罚", "同比治安处罚",
        "治安处罚(不执行)", "同比治安处罚(不执行)",
        "刑拘", "同比刑拘",
        "矫治文书(行政)", "同比矫治文书(行政)",
        "矫治文书(刑事)", "同比矫治文书(刑事)",
        "加强监督教育(行政)", "同比加强监督教育(行政)",
        "加强监督教育(刑事)", "同比加强监督教育(刑事)",
        "符合送校",
        "送校", "同比送校"
    ];
    const YOY_META_BY_COL = {
        "同比警情": { currentCol: "警情", unit: "起" },
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
    const RATIO_DEF_BY_COL = {};
    Object.entries(YOY_META_BY_COL).forEach(([compareCol, meta]) => {
        RATIO_DEF_BY_COL[`${compareCol}比例`] = { compareCol, currentCol: meta.currentCol, unit: meta.unit };
    });
    const METRIC_BY_COL = {
        "警情": "警情",
        "同比警情": "警情",
        "转案数": "转案数",
        "同比转案数": "转案数",
        "案件数(被侵害)": "案件数(被侵害)",
        "同比案件数(被侵害)": "案件数(被侵害)",
        "场所案件(被侵害)": "场所案件(被侵害)",
        "同比场所案件(被侵害)": "场所案件(被侵害)",
        "行政": "行政",
        "同比行政": "行政",
        "刑事": "刑事",
        "同比刑事": "刑事",
        "场所案件": "场所案件",
        "同比场所案件": "场所案件",
        "治安处罚": "治安处罚",
        "同比治安处罚": "治安处罚",
        "治安处罚(不执行)": "治安处罚(不执行)",
        "同比治安处罚(不执行)": "治安处罚(不执行)",
        "刑拘": "刑拘",
        "同比刑拘": "刑拘",
        "矫治文书(行政)": "矫治文书(行政)",
        "同比矫治文书(行政)": "矫治文书(行政)",
        "矫治文书(刑事)": "矫治文书(刑事)",
        "同比矫治文书(刑事)": "矫治文书(刑事)",
        "加强监督教育(行政)": "加强监督教育(行政)",
        "同比加强监督教育(行政)": "加强监督教育(行政)",
        "加强监督教育(刑事)": "加强监督教育(刑事)",
        "同比加强监督教育(刑事)": "加强监督教育(刑事)",
        "符合送校": "符合送校",
        "送校": "送校",
        "同比送校": "送校",
    };

    const startEl = document.getElementById("wcnrStart");
    const endEl = document.getElementById("wcnrEnd");
    const statusEl = document.getElementById("wcnrStatus");
    const errEl = document.getElementById("wcnrErr");
    const tbl = document.getElementById("wcnrTbl");
    const queryBtn = document.getElementById("wcnrQueryBtn");
    const exportAllBtn = document.getElementById("wcnrExportAllBtn");
    const showRatioEl = document.getElementById("wcnrShowRatio");

    if (!startEl || !endEl || !statusEl || !errEl || !tbl || !queryBtn || !exportAllBtn) {
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

    const ms = document.getElementById("wcnrTypesMs");
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
        if (boxes.length === 0) { msDisplay.textContent = "无类型"; return; }
        if (sel.length === 0) { msDisplay.textContent = "未选择(默认全量)"; return; }
        if (sel.length === boxes.length) { msDisplay.textContent = "全部"; return; }
        msDisplay.textContent = `已选 ${sel.length} 项`;
    }
    function openMs() { msDropdown.classList.add("open"); }
    function closeMs() { msDropdown.classList.remove("open"); }
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
        if (boxes.length === 0) { zaDisplay.textContent = "无选项"; return; }
        if (sel.length === 0) { zaDisplay.textContent = "未选择(默认全量)"; return; }
        if (sel.length === boxes.length) { zaDisplay.textContent = "全部"; return; }
        zaDisplay.textContent = `已选 ${sel.length} 项`;
    }
    function openZa() { zaDropdown.classList.add("open"); }
    function closeZa() { zaDropdown.classList.remove("open"); }
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
        if (!v) return "";
        const raw = String(v).trim();
        let s = raw.includes("T") ? raw.replace("T", " ") : raw;
        if (s.length === 16) s += ":00";
        if (s.length > 19) s = s.slice(0, 19);
        return s;
    }
    function setDefaultTimeRange() {
        const now = new Date();
        const today0 = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
        const start = new Date(today0);
        start.setDate(start.getDate() - 7);
        const end = new Date(today0);
        startEl.value = formatDateTimeLocal(start);
        endEl.value = formatDateTimeLocal(end);
    }

    function buildQueryParams(metaOverride) {
        const st = (metaOverride && metaOverride.start_time) ? metaOverride.start_time : formatDateTime(startEl.value);
        const et = (metaOverride && metaOverride.end_time) ? metaOverride.end_time : formatDateTime(endEl.value);
        const types = selectedTypes();
        const zaTypes = selectedZaTypes();
        const usp = new URLSearchParams();
        if (st) usp.set("start_time", st);
        if (et) usp.set("end_time", et);
        for (const t of types) usp.append("leixing", t);
        for (const z of zaTypes) usp.append("za_type", z);
        return usp;
    }

    function getDisplayCols() {
        const showRatio = !!(showRatioEl && showRatioEl.checked);
        if (!showRatio) return [...BASE_COLS];

        const cols = [];
        for (const c of BASE_COLS) {
            cols.push(c);
            if (!String(c).startsWith("同比")) continue;
            if (c === "同比转案数") {
                cols.push("转案率");
                cols.push("同比转案率");
                continue;
            }
            if (YOY_META_BY_COL[c]) cols.push(`${c}比例`);
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

    function renderTable(rows, meta) {
        const cols = getDisplayCols();
        const thead = `<thead><tr>${cols.map((c) => `<th>${c}</th>`).join("")}</tr></thead>`;
        const tbodyRows = (rows || []).map((r) => {
            const diquName = r["地区"] || "";
            const diquCode = r["地区代码"] || "__ALL__";
            const rowClass = (diquName === "全市" || diquCode === "__ALL__") ? "total-row" : "";
            const tds = cols.map((c) => {
                if (c === REGION_COL) return `<td class="rowh">${diquName}</td>`;
                if (c === "转案率") {
                    return `<td class="num-cell ratio-cell">${calcPercentText(r["转案数"], r["警情"])}</td>`;
                }
                if (c === "同比转案率") {
                    return `<td class="num-cell ratio-cell">${calcPercentText(r["同比转案数"], r["同比警情"])}</td>`;
                }
                const ratioDef = RATIO_DEF_BY_COL[c];
                if (ratioDef) {
                    const ratioText = calcRatioText(r[ratioDef.currentCol], r[ratioDef.compareCol], ratioDef.unit);
                    return `<td class="num-cell ratio-cell">${ratioText}</td>`;
                }
                const v = (r[c] == null) ? 0 : r[c];
                if (!v || Number(v) <= 0) return `<td class="num-cell">${v || 0}</td>`;
                const metric = METRIC_BY_COL[c] || "";
                const isYoy = String(c).startsWith("同比");
                const st = isYoy ? (meta.yoy_start_time || "") : (meta.start_time || "");
                const et = isYoy ? (meta.yoy_end_time || "") : (meta.end_time || "");
                const usp = buildQueryParams({ start_time: st, end_time: et });
                usp.set("metric", metric);
                usp.set("diqu", diquCode);
                const href = `${DETAIL_PAGE_URL}?${usp.toString()}`;
                return `<td class="num-cell clickable-cell" data-href="${href}" style="cursor:pointer; color:#1976d2; font-weight:800;">${v}</td>`;
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
            const usp = buildQueryParams();
            const resp = await fetch(`${API_SUMMARY_URL}?${usp.toString()}`);
            const data = await resp.json();
            if (!data.success) throw new Error(data.message || "查询失败");
            statusEl.textContent = `当前：${data.meta.start_time} ~ ${data.meta.end_time}；同比：${data.meta.yoy_start_time} ~ ${data.meta.yoy_end_time}`;
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
        const usp = buildQueryParams();
        usp.set("fmt", fmt);
        const href = `${EXPORT_SUMMARY_URL}?${usp.toString()}`;
        window.location.href = href;
    }

    function exportAllDetail() {
        errEl.textContent = "";
        const usp = buildQueryParams();
        const href = `${EXPORT_DETAIL_ALL_URL}?${usp.toString()}`;
        window.location.href = href;
    }

    queryBtn.addEventListener("click", (e) => { e.preventDefault(); query(); });
    exportAllBtn.addEventListener("click", (e) => { e.preventDefault(); exportAllDetail(); });
    if (showRatioEl) {
        showRatioEl.addEventListener("change", () => {
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

    setDefaultTimeRange();
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
