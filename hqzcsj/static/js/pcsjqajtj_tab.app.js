(function() {
    const NS = window.PcsJqAjTjTab || {};
    const C = NS.constants || {};
    const H = NS.helpers || {};
    const endpoints = window.PCSJQAJTJ_ENDPOINTS || {};

    const startEl = document.getElementById("pcsjqajtjStart");
    const endEl = document.getElementById("pcsjqajtjEnd");
    const hbStartEl = document.getElementById("pcsjqajtjHbStart");
    const hbEndEl = document.getElementById("pcsjqajtjHbEnd");
    const fenjuDisplay = document.getElementById("pcsjqajtjFenjuDisplay");
    const fenjuDropdown = document.getElementById("pcsjqajtjFenjuDropdown");
    const typesDisplay = document.getElementById("pcsjqajtjTypesDisplay");
    const typesDropdown = document.getElementById("pcsjqajtjTypesDropdown");
    const showHbEl = document.getElementById("pcsjqajtjShowHb");
    const showRatioEl = document.getElementById("pcsjqajtjShowRatio");
    const queryBtn = document.getElementById("pcsjqajtjQueryBtn");
    const exportBtn = document.getElementById("pcsjqajtjExportBtn");
    const exportDd = document.getElementById("pcsjqajtjExportDd");
    const statusEl = document.getElementById("pcsjqajtjStatus");
    const errEl = document.getElementById("pcsjqajtjErr");
    const tbl = document.getElementById("pcsjqajtjTbl");
    const busyMask = document.getElementById("pcsjqajtjBusyMask");
    const busyTextEl = document.getElementById("pcsjqajtjBusyText");

    if (
        !startEl || !endEl || !hbStartEl || !hbEndEl || !fenjuDisplay || !fenjuDropdown ||
        !typesDisplay || !typesDropdown || !showHbEl || !showRatioEl || !queryBtn || !exportBtn ||
        !exportDd || !statusEl || !errEl || !tbl
    ) {
        return;
    }

    let lastMeta = null;
    let lastRows = [];
    let lastSelectedFenjuNames = [];
    let lastAllFenjuSelected = true;

    function setBusy(isBusy, text) {
        if (!busyMask) return;
        if (isBusy) {
            if (busyTextEl && text) busyTextEl.textContent = text;
            busyMask.classList.add("active");
            document.body.style.overflow = "hidden";
            return;
        }
        busyMask.classList.remove("active");
        document.body.style.overflow = "";
    }

    function getCheckBoxes(dropdownEl) {
        return Array.from(dropdownEl.querySelectorAll('input[type="checkbox"]'));
    }

    function getDataBoxes(dropdownEl) {
        return getCheckBoxes(dropdownEl).filter((box) => box.value !== "_all");
    }

    function syncAllBox(dropdownEl) {
        const allBox = dropdownEl.querySelector('input[value="_all"]');
        if (!allBox) return;
        const dataBoxes = getDataBoxes(dropdownEl);
        const allChecked = dataBoxes.length > 0 && dataBoxes.every((box) => box.checked);
        const noneChecked = dataBoxes.length > 0 && dataBoxes.every((box) => !box.checked);
        allBox.checked = allChecked;
        allBox.indeterminate = !allChecked && !noneChecked;
    }

    function selectedValues(dropdownEl) {
        return getDataBoxes(dropdownEl).filter((box) => box.checked).map((box) => box.value);
    }

    function selectedLabels(dropdownEl) {
        return getDataBoxes(dropdownEl)
            .filter((box) => box.checked)
            .map((box) => box.getAttribute("data-label") || box.value);
    }

    function renderMultiLabel(displayEl, dropdownEl, emptyText) {
        const dataBoxes = getDataBoxes(dropdownEl);
        const selected = selectedValues(dropdownEl);
        if (dataBoxes.length === 0) {
            displayEl.textContent = "无选项";
            return;
        }
        if (selected.length === 0) {
            displayEl.textContent = emptyText;
            return;
        }
        if (selected.length === dataBoxes.length) {
            displayEl.textContent = "全部";
            return;
        }
        displayEl.textContent = `已选 ${selected.length} 项`;
    }

    function bindDropdown(displayEl, dropdownEl, onChange) {
        displayEl.addEventListener("click", (e) => {
            e.stopPropagation();
            dropdownEl.classList.toggle("open");
        });
        dropdownEl.addEventListener("click", (e) => e.stopPropagation());
        document.addEventListener("click", () => dropdownEl.classList.remove("open"));
        dropdownEl.addEventListener("change", (e) => {
            const target = e.target;
            if (!target || target.tagName !== "INPUT") return;
            if (target.value === "_all") {
                getDataBoxes(dropdownEl).forEach((box) => {
                    box.checked = target.checked;
                });
            } else {
                syncAllBox(dropdownEl);
            }
            onChange();
        });
    }

    function fillMultiOptions(dropdownEl, items) {
        const html = ['<label class="multi-select-option"><input type="checkbox" value="_all" checked><span>全选</span></label>'];
        for (const item of items) {
            const value = H.escapeHtml(item.value);
            const label = H.escapeHtml(item.label);
            html.push(
                `<label class="multi-select-option"><input type="checkbox" value="${value}" data-label="${label}" checked><span>${label}</span></label>`
            );
        }
        dropdownEl.innerHTML = html.join("");
        syncAllBox(dropdownEl);
    }

    async function loadTypes() {
        typesDropdown.innerHTML = '<div class="muted" style="padding:8px;">加载中...</div>';
        const resp = await fetch(endpoints.apiLeixing);
        const data = await resp.json();
        if (!data.success) throw new Error(data.message || "加载类型失败");
        fillMultiOptions(typesDropdown, data.data || []);
        renderMultiLabel(typesDisplay, typesDropdown, "未选择(默认全量)");
    }

    async function loadFenju() {
        fenjuDropdown.innerHTML = '<div class="muted" style="padding:8px;">加载中...</div>';
        const resp = await fetch(endpoints.apiFenju);
        const data = await resp.json();
        if (!data.success) throw new Error(data.message || "加载分局失败");
        fillMultiOptions(fenjuDropdown, data.data || []);
        renderMultiLabel(fenjuDisplay, fenjuDropdown, "未选择(默认全量)");
    }

    function currentFenjuMeta() {
        const selectedCodes = selectedValues(fenjuDropdown);
        const selectedNames = selectedLabels(fenjuDropdown);
        const total = getDataBoxes(fenjuDropdown).length;
        const allSelected = selectedCodes.length === 0 || (total > 0 && selectedCodes.length === total);
        return { selectedCodes, selectedNames, allSelected };
    }

    function buildQueryParams(timeOverride) {
        const usp = new URLSearchParams();
        const startTime = timeOverride && timeOverride.start_time
            ? timeOverride.start_time
            : H.formatDateTime(startEl.value);
        const endTime = timeOverride && timeOverride.end_time
            ? timeOverride.end_time
            : H.formatDateTime(endEl.value);
        const hbStartTime = timeOverride && timeOverride.hb_start_time
            ? timeOverride.hb_start_time
            : H.formatDateTime(hbStartEl.value);
        const hbEndTime = timeOverride && timeOverride.hb_end_time
            ? timeOverride.hb_end_time
            : H.formatDateTime(hbEndEl.value);
        if (startTime) usp.set("start_time", startTime);
        if (endTime) usp.set("end_time", endTime);
        if (hbStartTime) usp.set("hb_start_time", hbStartTime);
        if (hbEndTime) usp.set("hb_end_time", hbEndTime);
        for (const type of selectedValues(typesDropdown)) usp.append("leixing", type);
        for (const code of selectedValues(fenjuDropdown)) usp.append("ssfjdm", code);
        return usp;
    }

    function getDisplayColumns() {
        return C.getDisplayColumns(!!showRatioEl.checked, !!showHbEl.checked);
    }

    function isClickableCol(col) {
        return !!(C.CLICKABLE_COLS && C.CLICKABLE_COLS.has(col));
    }

    function renderTable(rows, meta) {
        const cols = getDisplayColumns();
        const thead = `<thead><tr>${cols.map((col) => `<th>${col}</th>`).join("")}</tr></thead>`;
        const bodyRows = (rows || []).map((row) => {
            const cells = cols.map((col) => {
                if (col === "所属分局" || col === "派出所名称") {
                    return `<td class="rowh">${row[col] == null ? "" : row[col]}</td>`;
                }

                const value = row[col] == null ? "" : row[col];
                if (isClickableCol(col) && Number(value) > 0) {
                    const metric = C.METRIC_BY_COL[col] || "";
                    const isYoy = String(col).startsWith("同比");
                    const isHb = String(col).startsWith("环比");
                    const st = isYoy ? (meta.yoy_start_time || "") : (isHb ? (meta.hb_start_time || "") : (meta.start_time || ""));
                    const et = isYoy ? (meta.yoy_end_time || "") : (isHb ? (meta.hb_end_time || "") : (meta.end_time || ""));
                    const usp = buildQueryParams({ start_time: st, end_time: et });
                    usp.set("metric", metric);
                    usp.set("pcsdm", row["派出所代码"] || "");
                    usp.set("pcs_name", row["派出所名称"] || "");
                    const href = `${endpoints.detailPage}?${usp.toString()}`;
                    return `<td class="num-cell clickable-cell" data-href="${href}" style="cursor:pointer; color:#1976d2; font-weight:800;">${value}</td>`;
                }

                const cls = typeof value === "string" && value.includes("%") ? "num-cell rate-cell" : "num-cell";
                return `<td class="${cls}">${value}</td>`;
            });
            return `<tr>${cells.join("")}</tr>`;
        }).join("");

        tbl.innerHTML = thead + `<tbody>${bodyRows || `<tr><td colspan="${cols.length}" class="muted">无数据</td></tr>`}</tbody>`;
        tbl.querySelectorAll("td.clickable-cell").forEach((cell) => {
            cell.addEventListener("click", () => {
                const href = cell.getAttribute("data-href");
                if (href) {
                    window.pcsjqajtjOpenDetail(href);
                }
            });
        });
    }

    async function querySummary() {
        errEl.textContent = "";
        statusEl.textContent = "";
        queryBtn.disabled = true;
        setBusy(true, "正在查询中，请稍候...");
        try {
            const usp = buildQueryParams();
            const resp = await fetch(`${endpoints.apiSummary}?${usp.toString()}`);
            const data = await resp.json();
            if (!data.success) {
                throw new Error(data.message || "查询失败");
            }
            lastMeta = data.meta || null;
            lastRows = data.rows || [];
            const fenjuMeta = currentFenjuMeta();
            lastSelectedFenjuNames = fenjuMeta.selectedNames;
            lastAllFenjuSelected = fenjuMeta.allSelected;

            statusEl.textContent = `当前：${data.meta.start_time} ~ ${data.meta.end_time}；同比：${data.meta.yoy_start_time} ~ ${data.meta.yoy_end_time}；环比：${data.meta.hb_start_time} ~ ${data.meta.hb_end_time}`;
            renderTable(lastRows, data.meta || {});
        } catch (e) {
            errEl.textContent = e && e.message ? e.message : String(e);
            lastMeta = null;
            lastRows = [];
            tbl.innerHTML = "";
        } finally {
            setBusy(false);
            queryBtn.disabled = false;
        }
    }

    async function doExport(fmt) {
        errEl.textContent = "";
        setBusy(true, "正在导出中，请稍候...");
        try {
            const columns = getDisplayColumns();
            const payload = {
                fmt,
                rows: lastRows || [],
                columns,
                start_time: (lastMeta && lastMeta.start_time) || H.formatDateTime(startEl.value),
                end_time: (lastMeta && lastMeta.end_time) || H.formatDateTime(endEl.value),
                selected_fenjv_names: lastSelectedFenjuNames || [],
                all_fenju_selected: !!lastAllFenjuSelected,
            };
            const resp = await fetch(endpoints.exportSummary, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            const contentType = (resp.headers.get("content-type") || "").toLowerCase();
            if (!resp.ok || contentType.includes("application/json")) {
                let message = "导出失败";
                try {
                    const js = await resp.json();
                    message = (js && js.message) ? js.message : message;
                } catch (_e) {
                    // ignore
                }
                throw new Error(message);
            }
            const blob = await resp.blob();
            const downloadUrl = URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.href = downloadUrl;
            link.download = H.parseContentDispositionFilename(resp, `派出所警情案件统计_${Date.now()}.${fmt}`);
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(downloadUrl);
        } catch (e) {
            errEl.textContent = e && e.message ? e.message : String(e);
        } finally {
            setBusy(false);
        }
    }

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

    queryBtn.addEventListener("click", (e) => {
        e.preventDefault();
        querySummary();
    });

    exportBtn.addEventListener("click", (e) => {
        e.preventDefault();
        exportDd.classList.toggle("open");
    });
    exportDd.querySelectorAll(".dropdown-menu a").forEach((a) => {
        a.addEventListener("click", (e) => {
            e.preventDefault();
            const fmt = a.getAttribute("data-fmt") || "xlsx";
            exportDd.classList.remove("open");
            doExport(fmt);
        });
    });
    document.addEventListener("click", (e) => {
        if (!exportDd.contains(e.target)) exportDd.classList.remove("open");
    });

    window.pcsjqajtjOpenDetail = function(href) {
        const modal = document.getElementById("pcsjqajtjDetailModal");
        const frame = document.getElementById("pcsjqajtjDetailFrame");
        if (!modal || !frame) {
            window.open(href, "_blank");
            return;
        }
        frame.src = href;
        modal.style.display = "flex";
    };

    window.pcsjqajtjCloseModal = function(event) {
        if (event && event.target && event.target.id !== "pcsjqajtjDetailModal") return;
        const modal = document.getElementById("pcsjqajtjDetailModal");
        const frame = document.getElementById("pcsjqajtjDetailFrame");
        if (!modal || !frame) return;
        modal.style.display = "none";
        frame.src = "about:blank";
    };

    bindDropdown(typesDisplay, typesDropdown, () => {
        renderMultiLabel(typesDisplay, typesDropdown, "未选择(默认全量)");
    });
    bindDropdown(fenjuDisplay, fenjuDropdown, () => {
        renderMultiLabel(fenjuDisplay, fenjuDropdown, "未选择(默认全量)");
    });

    H.setDefaultTimeRange({ startEl, endEl, hbStartEl, hbEndEl });

    Promise.all([loadTypes(), loadFenju()])
        .catch((e) => {
            errEl.textContent = e && e.message ? e.message : String(e);
            if (!typesDisplay.textContent || typesDisplay.textContent === "加载中...") {
                typesDisplay.textContent = "加载失败";
            }
            if (!fenjuDisplay.textContent || fenjuDisplay.textContent === "加载中...") {
                fenjuDisplay.textContent = "加载失败";
            }
        });
})();
