(function() {
    const NS = window.Wcnr10lvTab || {};
    const C = NS.constants || {};
    const H = NS.helpers || {};
    const endpoints = window.WCNR_10LV_ENDPOINTS || {};

    const startEl = document.getElementById("wcnr10lvStart");
    const endEl = document.getElementById("wcnr10lvEnd");
    const hbStartEl = document.getElementById("wcnr10lvHbStart");
    const hbEndEl = document.getElementById("wcnr10lvHbEnd");
    const statusEl = document.getElementById("wcnr10lvStatus");
    const errEl = document.getElementById("wcnr10lvErr");
    const tbl = document.getElementById("wcnr10lvTbl");
    const queryBtn = document.getElementById("wcnr10lvQueryBtn");
    const showHbEl = document.getElementById("wcnr10lvShowHb");
    const showRatioEl = document.getElementById("wcnr10lvShowRatio");

    const exportDd = document.getElementById("wcnr10lvExportDd");
    const exportBtn = document.getElementById("wcnr10lvExportBtn");
    const exportDetailDd = document.getElementById("wcnr10lvExportDetailDd");
    const exportDetailBtn = document.getElementById("wcnr10lvExportDetailBtn");

    const msDisplay = document.getElementById("wcnr10lvTypesDisplay");
    const msDropdown = document.getElementById("wcnr10lvTypesDropdown");

    if (!startEl || !endEl || !hbStartEl || !hbEndEl || !statusEl || !errEl || !tbl || !queryBtn) {
        return;
    }

    const API_LEIXING = endpoints.apiLeixing || "/hqzcsj/wcnr_10lv/api/leixing";
    const API_SUMMARY = endpoints.apiSummary || "/hqzcsj/wcnr_10lv/api/summary";
    const DETAIL_PAGE = endpoints.detailPage || "/hqzcsj/wcnr_10lv/detail";
    const EXPORT_SUMMARY = endpoints.exportSummary || "/hqzcsj/wcnr_10lv/export";
    const EXPORT_DETAIL = endpoints.exportDetail || "/hqzcsj/wcnr_10lv/export_detail";

    let lastMeta = null;
    let lastRows = [];

    function toResolvedUrl(pathOrUrl) {
        const s = String(pathOrUrl || "").trim();
        if (!s) return s;
        if (/^https?:\/\//i.test(s) || s.startsWith("/")) return s;
        return new URL(s, window.location.href).toString();
    }

    function fallbackUrlIfNeeded(url) {
        const s = String(url || "");
        if (s.includes("/wcnr_10lv/")) return s.replace("/wcnr_10lv/", "/wcnr10lv/");
        if (s.includes("wcnr_10lv/")) return s.replace("wcnr_10lv/", "wcnr10lv/");
        return "";
    }

    async function fetchJsonStrict(url) {
        const REQUEST_TIMEOUT_MS = 45000;
        const finalUrl = toResolvedUrl(url);
        const tryFetch = async (u) => {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
            let resp;
            try {
                resp = await fetch(u, { signal: controller.signal });
            } catch (err) {
                if (err && err.name === "AbortError") {
                    throw new Error(`请求超时（>${REQUEST_TIMEOUT_MS / 1000}s）: ${u}`);
                }
                throw err;
            } finally {
                clearTimeout(timer);
            }
            const text = await resp.text();
            let data = null;
            try {
                data = JSON.parse(text);
            } catch (_) {
                throw new Error(`接口返回非JSON（HTTP ${resp.status}）: ${u}`);
            }
            if (!resp.ok || !data || !data.success) {
                throw new Error((data && data.message) || `请求失败（HTTP ${resp.status}）: ${u}`);
            }
            return data;
        };

        try {
            return await tryFetch(finalUrl);
        } catch (e) {
            const fb = fallbackUrlIfNeeded(finalUrl);
            if (!fb || fb === finalUrl) throw e;
            return await tryFetch(fb);
        }
    }

    function selectedTypes() {
        if (!msDropdown) return [];
        const boxes = Array.from(msDropdown.querySelectorAll('input[type="checkbox"]'));
        return boxes.filter((b) => b.value !== "_all" && b.checked).map((b) => b.value);
    }

    function syncAllBox() {
        if (!msDropdown) return;
        const allBox = msDropdown.querySelector('input[value="_all"]');
        if (!allBox) return;
        const boxes = Array.from(msDropdown.querySelectorAll('input[type="checkbox"]')).filter((b) => b.value !== "_all");
        const allChecked = boxes.length > 0 && boxes.every((b) => b.checked);
        const noneChecked = boxes.length > 0 && boxes.every((b) => !b.checked);
        allBox.checked = allChecked;
        allBox.indeterminate = !allChecked && !noneChecked;
    }

    function renderMsLabel() {
        if (!msDisplay || !msDropdown) return;
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
        if (msDropdown) msDropdown.classList.add("open");
    }
    function closeMs() {
        if (msDropdown) msDropdown.classList.remove("open");
    }

    if (msDisplay && msDropdown) {
        msDisplay.addEventListener("click", (e) => {
            e.stopPropagation();
            if (msDropdown.classList.contains("open")) closeMs();
            else openMs();
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
    }

    async function loadTypes() {
        if (!msDropdown) return;
        msDropdown.innerHTML = '<div class="muted" style="padding:8px;">加载中...</div>';
        const data = await fetchJsonStrict(API_LEIXING);
        const items = data.data || [];
        const html = [];
        html.push('<label class="multi-select-option"><input type="checkbox" value="_all" checked><span>全选</span></label>');
        for (const x of items) {
            const s = String(x || "").trim();
            if (!s) continue;
            html.push(`<label class="multi-select-option"><input type="checkbox" value="${s.replace(/"/g, "&quot;")}" checked><span>${s}</span></label>`);
        }
        msDropdown.innerHTML = html.join("");
        syncAllBox();
        renderMsLabel();
    }

    function buildBaseQueryParams() {
        const usp = new URLSearchParams();
        const st = H.formatDateTime(startEl.value);
        const et = H.formatDateTime(endEl.value);
        const hbst = H.formatDateTime(hbStartEl.value);
        const hbet = H.formatDateTime(hbEndEl.value);
        if (st) usp.set("start_time", st);
        if (et) usp.set("end_time", et);
        if (hbst) usp.set("hb_start_time", hbst);
        if (hbet) usp.set("hb_end_time", hbet);
        for (const t of selectedTypes()) usp.append("leixing", t);
        return usp;
    }

    function openDetail(href) {
        const modal = document.getElementById("wcnr10lvDetailModal");
        const frame = document.getElementById("wcnr10lvDetailFrame");
        if (!modal || !frame) {
            window.open(href, "_blank");
            return;
        }
        frame.src = href;
        modal.style.display = "flex";
    }

    window.wcnr10lvCloseModal = function(event) {
        if (event && event.target && event.target.id !== "wcnr10lvDetailModal") return;
        const modal = document.getElementById("wcnr10lvDetailModal");
        const frame = document.getElementById("wcnr10lvDetailFrame");
        if (modal) modal.style.display = "none";
        if (frame) frame.src = "about:blank";
    };

    function buildDetailHref(target, diquCode) {
        const usp = buildBaseQueryParams();
        usp.set("metric", target.metric);
        usp.set("part", target.part || "value");
        usp.set("period", target.period || "current");
        usp.set("diqu", diquCode || "__ALL__");
        return `${DETAIL_PAGE}?${usp.toString()}`;
    }

    function renderTable(rows, meta) {
        const showHb = !!(showHbEl && showHbEl.checked);
        const showRatio = !!(showRatioEl && showRatioEl.checked);
        const cols = H.getDisplayColumns(C.METRICS || [], showHb, showRatio);

        const head = `<thead><tr>${cols.map((c) => `<th>${c}</th>`).join("")}</tr></thead>`;
        const body = (rows || []).map((r) => {
            const diquName = r["地区"] || "";
            const diquCode = r["地区代码"] || "__ALL__";
            const rowClass = (diquCode === "__ALL__" || diquName === "全市") ? "total-row" : "";
            const tds = cols.map((c) => {
                if (c === "地区") return `<td class="rowh">${diquName}</td>`;

                const target = H.resolveDetailTarget(C.METRICS || [], c);
                const raw = (r[c] == null) ? "" : r[c];
                const cellClass = String(c).includes("率") || String(c).includes("比例") ? "num-cell ratio-cell" : "num-cell";
                if (!target) return `<td class="${cellClass}">${raw}</td>`;

                const href = buildDetailHref(target, diquCode);
                return `<td class="${cellClass} clickable-cell" data-href="${href}" style="cursor:pointer; color:#1976d2; font-weight:800;">${raw}</td>`;
            });
            return `<tr class="${rowClass}">${tds.join("")}</tr>`;
        }).join("");

        tbl.innerHTML = head + `<tbody>${body || `<tr><td colspan="${cols.length}" class="muted">无数据</td></tr>`}</tbody>`;

        tbl.querySelectorAll("td.clickable-cell").forEach((td) => {
            td.addEventListener("click", () => {
                const href = td.getAttribute("data-href");
                if (href) openDetail(href);
            });
        });

        if (meta && meta.flags && meta.flags.addr_model_degraded_current) {
            statusEl.textContent += "；地址分类模型异常，场所被侵害指标已降级";
        }
    }

    async function query() {
        errEl.textContent = "";
        statusEl.textContent = "查询中...";
        queryBtn.disabled = true;
        try {
            const showHb = !!(showHbEl && showHbEl.checked);
            const showRatio = !!(showRatioEl && showRatioEl.checked);
            const usp = buildBaseQueryParams();
            usp.set("show_hb", showHb ? "1" : "0");
            usp.set("show_ratio", showRatio ? "1" : "0");
            const data = await fetchJsonStrict(`${API_SUMMARY}?${usp.toString()}`);

            lastMeta = data.meta || null;
            lastRows = data.rows || [];
            statusEl.textContent = `当前：${data.meta.start_time} ~ ${data.meta.end_time}；同比：${data.meta.yoy_start_time} ~ ${data.meta.yoy_end_time}；环比：${data.meta.hb_start_time} ~ ${data.meta.hb_end_time}`;
            renderTable(lastRows, data.meta || {});
        } catch (e) {
            errEl.textContent = e.message || String(e);
            statusEl.textContent = "";
            lastRows = [];
            tbl.innerHTML = "";
        } finally {
            queryBtn.disabled = false;
        }
    }

    function doExport(fmt) {
        const usp = buildBaseQueryParams();
        usp.set("fmt", fmt);
        usp.set("show_hb", (showHbEl && showHbEl.checked) ? "1" : "0");
        usp.set("show_ratio", (showRatioEl && showRatioEl.checked) ? "1" : "0");
        window.location.href = `${EXPORT_SUMMARY}?${usp.toString()}`;
    }

    function doExportDetail(fmt) {
        const usp = buildBaseQueryParams();
        usp.set("fmt", fmt);
        usp.set("show_hb", (showHbEl && showHbEl.checked) ? "1" : "0");
        usp.set("show_ratio", (showRatioEl && showRatioEl.checked) ? "1" : "0");
        window.location.href = `${EXPORT_DETAIL}?${usp.toString()}`;
    }

    if (exportBtn && exportDd) {
        exportBtn.addEventListener("click", (e) => {
            e.preventDefault();
            exportDd.classList.toggle("open");
        });
        exportDd.querySelectorAll(".dropdown-menu a").forEach((a) => {
            a.addEventListener("click", (e) => {
                e.preventDefault();
                const fmt = a.getAttribute("data-fmt") || "xlsx";
                doExport(fmt);
                exportDd.classList.remove("open");
            });
        });
        document.addEventListener("click", (e) => { if (!exportDd.contains(e.target)) exportDd.classList.remove("open"); });
    }

    if (exportDetailBtn && exportDetailDd) {
        exportDetailBtn.addEventListener("click", (e) => {
            e.preventDefault();
            exportDetailDd.classList.toggle("open");
        });
        exportDetailDd.querySelectorAll(".dropdown-menu a").forEach((a) => {
            a.addEventListener("click", (e) => {
                e.preventDefault();
                const fmt = a.getAttribute("data-fmt") || "xlsx";
                doExportDetail(fmt);
                exportDetailDd.classList.remove("open");
            });
        });
        document.addEventListener("click", (e) => { if (!exportDetailDd.contains(e.target)) exportDetailDd.classList.remove("open"); });
    }

    if (showHbEl) {
        showHbEl.addEventListener("change", () => {
            const on = !!showHbEl.checked;
            const hbLoaded = !!(lastMeta && lastMeta.hb_loaded);
            if (on && !hbLoaded) {
                query();
                return;
            }
            renderTable(lastRows, lastMeta || {});
        });
    }

    if (showRatioEl) {
        showRatioEl.addEventListener("change", () => {
            renderTable(lastRows, lastMeta || {});
        });
    }

    queryBtn.addEventListener("click", (e) => {
        e.preventDefault();
        query();
    });

    H.setDefaultTimeRange({ startEl, endEl, hbStartEl, hbEndEl });

    let hasAutoQueried = false;
    const typesLoaded = loadTypes().catch((e) => {
        errEl.textContent = e.message || String(e);
        if (msDisplay) msDisplay.textContent = "加载失败";
        throw e;
    });

    async function maybeAutoQuery() {
        if (hasAutoQueried) return;
        hasAutoQueried = true;
        await typesLoaded;
        await query();
    }

    const tabBtn = document.querySelector('#hqzcsjTabs .tab-btn[data-tab="wcnr10lv"]');
    if (tabBtn) {
        tabBtn.addEventListener("click", () => maybeAutoQuery().catch(() => {}));
    }

    const panel = document.getElementById("tab-wcnr10lv");
    if (panel && panel.classList.contains("active")) {
        maybeAutoQuery().catch(() => {});
    }
})();
