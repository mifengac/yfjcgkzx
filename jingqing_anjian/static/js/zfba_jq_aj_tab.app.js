(function() {
    const NS = window.ZfbaJqAjTab || {};
    const C = NS.constants || {};
    const H = NS.helpers || {};
    const endpoints = window.ZFBA_JQ_AJ_ENDPOINTS || {};

    const startEl = document.getElementById("jqajStart");
    const endEl = document.getElementById("jqajEnd");
    const hbStartEl = document.getElementById("jqajHbStart");
    const hbEndEl = document.getElementById("jqajHbEnd");
    const statusEl = document.getElementById("jqajStatus");
    const errEl = document.getElementById("jqajErr");
    const tbl = document.getElementById("jqajTbl");
    const queryBtn = document.getElementById("jqajQueryBtn");
    const reportBtn = document.getElementById("jqajReportBtn");
    const dailyReportDd = document.getElementById("jqajDailyReportDd");
    const dailyReportBtn = document.getElementById("jqajDailyReportBtn");
    const busyMask = document.getElementById("jqajBusyMask");
    const showHbEl = document.getElementById("jqajShowHb");
    const showRatioEl = document.getElementById("jqajShowRatio");
    const canExportDailyReport = !!endpoints.canExportDailyReport;

    if (!startEl || !endEl || !hbStartEl || !hbEndEl || !statusEl || !errEl || !tbl || !queryBtn || !reportBtn) {
        return;
    }

    let lastMeta = null;
    let lastRows = [];

    function setDailyReportBusy(isBusy) {
        if (!busyMask) return;
        if (isBusy) {
            busyMask.classList.add("active");
            document.body.style.overflow = "hidden";
            return;
        }
        busyMask.classList.remove("active");
        document.body.style.overflow = "";
    }

    function initDailyReportPermission() {
        if (!dailyReportBtn) return;
        if (canExportDailyReport) {
            if (dailyReportDd) dailyReportDd.style.display = "";
            dailyReportBtn.style.display = "";
            return;
        }
        if (dailyReportDd) dailyReportDd.style.display = "none";
        dailyReportBtn.style.display = "none";
        dailyReportBtn.disabled = true;
    }

    const dd = document.getElementById("jqajExportDd");
    const exportBtn = document.getElementById("jqajExportBtn");
    if (!dd || !exportBtn) return;
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

    if (dailyReportDd && dailyReportBtn) {
        dailyReportBtn.addEventListener("click", (e) => {
            e.preventDefault();
            dailyReportDd.classList.toggle("open");
        });
        dailyReportDd.querySelectorAll(".dropdown-menu a").forEach((a) => {
            a.addEventListener("click", (e) => {
                e.preventDefault();
                const fmt = (a.getAttribute("data-fmt") || "html").toLowerCase();
                exportDailyReport(fmt);
                dailyReportDd.classList.remove("open");
            });
        });
        document.addEventListener("click", (e) => {
            if (!dailyReportDd.contains(e.target)) dailyReportDd.classList.remove("open");
        });
    }

    const ms = document.getElementById("jqajTypesMs");
    const msDisplay = document.getElementById("jqajTypesDisplay");
    const msDropdown = document.getElementById("jqajTypesDropdown");

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
        const resp = await fetch(endpoints.apiLeixing);
        const data = await resp.json();
        if (!data.success) throw new Error(data.message || "加载类型失败");
        const items = data.data || [];
        const html = [];
        html.push('<label class="multi-select-option"><input type="checkbox" value="_all" checked><span>全选</span></label>');
        for (const x of items) {
            const s = String(x || "").trim();
            if (!s) continue;
            html.push(`<label class="multi-select-option"><input type="checkbox" value="${s.replace(/\"/g, "&quot;")}" checked><span>${s}</span></label>`);
        }
        msDropdown.innerHTML = html.join("");
        syncAllBox();
        renderMsLabel();
    }

    const zaDisplay = document.getElementById("jqajZaDisplay");
    const zaDropdown = document.getElementById("jqajZaDropdown");
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

    function buildQueryParams(metaOverride) {
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

    function renderTable(rows, meta) {
        const cols = H.getDisplayCols({
            regionCol: C.REGION_COL,
            currentMetricCols: C.CURRENT_METRIC_COLS,
            showRatio: !!(showRatioEl && showRatioEl.checked),
            showHb: !!(showHbEl && showHbEl.checked),
        });
        const thead = `<thead><tr>${cols.map((c) => `<th>${c}</th>`).join("")}</tr></thead>`;
        const tbodyRows = (rows || []).map((r) => {
            const diquName = r["地区"] || "";
            const diquCode = r["地区代码"] || "__ALL__";
            const tds = cols.map((c) => {
                if (c === C.REGION_COL) return `<td class="rowh">${diquName}</td>`;
                if (c === "转案率") {
                    return `<td class="num-cell ratio-cell">${H.calcPercentText(r["转案数"], r["警情"])}</td>`;
                }
                if (c === "同比转案率") {
                    return `<td class="num-cell ratio-cell">${H.calcPercentText(r["同比转案数"], r["同比警情"])}</td>`;
                }
                const ratioDef = C.RATIO_DEF_BY_COL[c];
                if (ratioDef) {
                    const ratioText = H.calcRatioText(r[ratioDef.currentCol], r[ratioDef.compareCol], ratioDef.unit);
                    return `<td class="num-cell ratio-cell">${ratioText}</td>`;
                }
                const v = (r[c] == null) ? 0 : r[c];
                if (!v || Number(v) <= 0) return `<td class="num-cell">${v || 0}</td>`;
                const metric = C.METRIC_BY_COL[c] || "";
                const isYoy = String(c).startsWith("同比");
                const isHb = String(c).startsWith("环比");
                const st = isYoy ? (meta.yoy_start_time || "") : (isHb ? (meta.hb_start_time || "") : (meta.start_time || ""));
                const et = isYoy ? (meta.yoy_end_time || "") : (isHb ? (meta.hb_end_time || "") : (meta.end_time || ""));
                const usp = buildQueryParams({ start_time: st, end_time: et });
                usp.set("metric", metric);
                usp.set("diqu", diquCode);
                const href = `${endpoints.detailPage}?${usp.toString()}`;
                return `<td class="num-cell clickable-cell" data-href="${href}" style="cursor:pointer; color:#1976d2; font-weight:800;">${v}</td>`;
            });
            return `<tr>${tds.join("")}</tr>`;
        }).join("");
        tbl.innerHTML = thead + `<tbody>${tbodyRows || `<tr><td colspan="${cols.length}" class="muted">无数据</td></tr>`}</tbody>`;

        tbl.querySelectorAll("td.clickable-cell").forEach((td) => {
            td.addEventListener("click", () => {
                const href = td.getAttribute("data-href");
                if (href) window.jqajOpenDetail(href);
            });
        });
    }

    async function query() {
        errEl.textContent = "";
        statusEl.textContent = "查询中...";
        queryBtn.disabled = true;
        try {
            const usp = buildQueryParams();
            usp.set("show_hb", "1");
            const resp = await fetch(`${endpoints.apiSummary}?${usp.toString()}`);
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
        const usp = buildQueryParams();
        usp.set("fmt", fmt);
        if (showRatioEl && showRatioEl.checked) usp.set("show_ratio", "1");
        usp.set("show_hb", (showHbEl && showHbEl.checked) ? "1" : "0");
        const href = `${endpoints.exportSummary}?${usp.toString()}`;
        window.location.href = href;
    }

    async function exportReport() {
        errEl.textContent = "";
        const kssj = H.formatDateTime(startEl.value);
        const jssj = H.formatDateTime(endEl.value);
        const hbkssj = H.formatDateTime(hbStartEl.value);
        const hbjssj = H.formatDateTime(hbEndEl.value);
        const zaTypes = selectedZaTypes();
        if (!kssj || !jssj || !hbkssj || !hbjssj) {
            errEl.textContent = "请填写开始/结束/环比开始/环比结束时间";
            return;
        }
        reportBtn.disabled = true;
        statusEl.textContent = "正在导出报表...";
        try {
            const resp = await fetch(endpoints.reportExport, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ kssj, jssj, hbkssj, hbjssj, za_types: zaTypes }),
            });
            if (!resp.ok) {
                let msg = "导出报表失败";
                try {
                    const js = await resp.json();
                    msg = (js && js.message) ? js.message : msg;
                } catch (_e) {
                    // ignore
                }
                throw new Error(msg);
            }
            const blob = await resp.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            const cd = resp.headers.get("content-disposition") || "";
            const m = cd.match(/filename\*=UTF-8''([^;]+)/i) || cd.match(/filename="?([^;"]+)"?/i);
            a.download = m ? decodeURIComponent(m[1]) : `警情案件统计报表_${new Date().getTime()}.xls`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            statusEl.textContent = "导出成功";
        } catch (e) {
            errEl.textContent = e.message || String(e);
            statusEl.textContent = "";
        } finally {
            reportBtn.disabled = false;
        }
    }

    async function exportDailyReport(fmt) {
        errEl.textContent = "";
        if (!canExportDailyReport) {
            errEl.textContent = "当前用户无权导出警情日报";
            return;
        }
        const outFmt = (fmt || "html").toLowerCase();
        if (!["html", "docx", "pdf"].includes(outFmt)) {
            errEl.textContent = "不支持的导出格式";
            return;
        }
        const startTime = H.formatDateTime(startEl.value);
        const endTime = H.formatDateTime(endEl.value);
        if (!startTime || !endTime) {
            errEl.textContent = "请填写开始时间和结束时间";
            return;
        }
        if (!endpoints.dailyReportExport) {
            errEl.textContent = "未配置警情日报导出接口";
            return;
        }

        if (dailyReportBtn) dailyReportBtn.disabled = true;
        setDailyReportBusy(true);
        statusEl.textContent = `日报生成中，请等待...（${outFmt.toUpperCase()}）`;
        try {
            const resp = await fetch(endpoints.dailyReportExport, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ start_time: startTime, end_time: endTime, fmt: outFmt }),
            });

            const contentType = resp.headers.get("content-type") || "";
            if (!resp.ok || contentType.includes("application/json")) {
                let msg = "导出警情日报失败";
                try {
                    const js = await resp.json();
                    msg = (js && js.message) ? js.message : msg;
                } catch (_e) {
                    // ignore
                }
                throw new Error(msg);
            }

            const blob = await resp.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            const cd = resp.headers.get("content-disposition") || "";
            const m = cd.match(/filename\*=UTF-8''([^;]+)/i) || cd.match(/filename="?([^;"]+)"?/i);
            a.download = m ? decodeURIComponent(m[1]) : `警情日报_${new Date().getTime()}.${outFmt}`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            statusEl.textContent = `导出警情日报成功（${outFmt.toUpperCase()}）`;
        } catch (e) {
            errEl.textContent = e.message || String(e);
            statusEl.textContent = "";
        } finally {
            setDailyReportBusy(false);
            if (dailyReportBtn) dailyReportBtn.disabled = false;
        }
    }

    reportBtn.addEventListener("click", (e) => { e.preventDefault(); exportReport(); });
    queryBtn.addEventListener("click", (e) => { e.preventDefault(); query(); });

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

    window.jqajOpenDetail = function(href) {
        const modal = document.getElementById("jqajDetailModal");
        const frame = document.getElementById("jqajDetailFrame");
        frame.src = href;
        modal.style.display = "flex";
    };

    window.jqajCloseModal = function(event) {
        if (event && event.target && event.target.id !== "jqajDetailModal") return;
        const modal = document.getElementById("jqajDetailModal");
        const frame = document.getElementById("jqajDetailFrame");
        modal.style.display = "none";
        frame.src = "about:blank";
    };

    H.setDefaultTimeRange({ startEl, endEl, hbStartEl, hbEndEl });
    initZaOptions();
    initDailyReportPermission();

    loadTypes()
        .then(() => query())
        .catch((e) => {
            errEl.textContent = e.message || String(e);
            msDisplay.textContent = "加载失败";
        });
})();
