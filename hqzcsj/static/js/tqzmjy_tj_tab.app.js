(function() {
    const NS = window.TqzmjyTjTab || {};
    const C = NS.constants || {};
    const H = NS.helpers || {};
    const endpoints = window.TQZMJY_TJ_ENDPOINTS || {};

    const startEl = document.getElementById("tqzmjyStart");
    const endEl = document.getElementById("tqzmjyEnd");
    const fenjuDisplay = document.getElementById("tqzmjyFenjuDisplay");
    const fenjuDropdown = document.getElementById("tqzmjyFenjuDropdown");
    const typesDisplay = document.getElementById("tqzmjyTypesDisplay");
    const typesDropdown = document.getElementById("tqzmjyTypesDropdown");
    const queryBtn = document.getElementById("tqzmjyQueryBtn");
    const exportBtn = document.getElementById("tqzmjyExportBtn");
    const exportDd = document.getElementById("tqzmjyExportDd");
    const statusEl = document.getElementById("tqzmjyStatus");
    const errEl = document.getElementById("tqzmjyErr");
    const tbl = document.getElementById("tqzmjyTbl");
    const busyMask = document.getElementById("tqzmjyBusyMask");
    const busyTextEl = document.getElementById("tqzmjyBusyText");

    if (
        !startEl || !endEl || !fenjuDisplay || !fenjuDropdown || !typesDisplay || !typesDropdown ||
        !queryBtn || !exportBtn || !exportDd || !statusEl || !errEl || !tbl
    ) {
        return;
    }

    let lastMeta = null;

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

    function selectedFilterValues(dropdownEl) {
        const dataBoxes = getDataBoxes(dropdownEl);
        const selected = selectedValues(dropdownEl);
        if (selected.length === 0 || selected.length === dataBoxes.length) {
            return [];
        }
        return selected;
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

    function fillMultiOptions(dropdownEl, items, selectAllByDefault) {
        const checkedAttr = selectAllByDefault ? " checked" : "";
        const html = [`<label class="multi-select-option"><input type="checkbox" value="_all"${checkedAttr}><span>全选</span></label>`];
        for (const item of items) {
            const value = H.escapeHtml(item.value);
            const label = H.escapeHtml(item.label);
            html.push(
                `<label class="multi-select-option"><input type="checkbox" value="${value}"${checkedAttr}><span>${label}</span></label>`
            );
        }
        dropdownEl.innerHTML = html.join("");
        syncAllBox(dropdownEl);
    }

    async function fetchJson(url, defaultMessage) {
        const resp = await fetch(url);
        const data = await resp.json();
        if (!resp.ok || !data.success) {
            throw new Error((data && data.message) || defaultMessage);
        }
        return data;
    }

    async function loadTypes() {
        typesDropdown.innerHTML = '<div class="muted" style="padding:8px;">加载中...</div>';
        const data = await fetchJson(endpoints.apiLeixing, "加载类型失败");
        fillMultiOptions(typesDropdown, data.data || [], false);
        renderMultiLabel(typesDisplay, typesDropdown, "全量");
    }

    async function loadFenju() {
        fenjuDropdown.innerHTML = '<div class="muted" style="padding:8px;">加载中...</div>';
        const data = await fetchJson(endpoints.apiFenju, "加载分局失败");
        fillMultiOptions(fenjuDropdown, data.data || [], false);
        renderMultiLabel(fenjuDisplay, fenjuDropdown, "全量");
    }

    function buildQueryParams(fmt) {
        const usp = new URLSearchParams();
        const startTime = H.formatDateTime(startEl.value);
        const endTime = H.formatDateTime(endEl.value);
        if (startTime) usp.set("start_time", startTime);
        if (endTime) usp.set("end_time", endTime);
        for (const type of selectedFilterValues(typesDropdown)) usp.append("leixing", type);
        for (const code of selectedFilterValues(fenjuDropdown)) usp.append("ssfjdm", code);
        if (fmt) usp.set("fmt", fmt);
        return usp;
    }

    function renderTable(rows) {
        const columns = C.DISPLAY_COLUMNS || [];
        const thead = `<thead><tr>${columns.map((col) => `<th>${H.escapeHtml(col)}</th>`).join("")}</tr></thead>`;
        const body = (rows || []).map((row) => {
            const cells = columns.map((col) => {
                const value = row[col] == null ? "" : String(row[col]);
                return `<td class="${col === "审批时间" ? "num-cell" : ""}">${H.escapeHtml(value)}</td>`;
            });
            return `<tr>${cells.join("")}</tr>`;
        }).join("");
        tbl.innerHTML = thead + `<tbody>${body || `<tr><td colspan="${columns.length}" class="muted">无数据</td></tr>`}</tbody>`;
    }

    async function queryRows() {
        errEl.textContent = "";
        queryBtn.disabled = true;
        setBusy(true, "正在查询中，请稍候...");
        try {
            const usp = buildQueryParams();
            const data = await fetchJson(`${endpoints.apiQuery}?${usp.toString()}`, "查询失败");
            lastMeta = data.meta || null;
            renderTable(data.rows || []);
            const meta = data.meta || {};
            statusEl.textContent = `审批时间：${meta.start_time || ""} ~ ${meta.end_time || ""}；共 ${(data.rows || []).length} 条`;
        } catch (e) {
            errEl.textContent = e && e.message ? e.message : String(e);
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
            const usp = buildQueryParams(fmt);
            const resp = await fetch(`${endpoints.exportData}?${usp.toString()}`);
            const contentType = (resp.headers.get("content-type") || "").toLowerCase();
            if (!resp.ok || contentType.includes("application/json")) {
                let message = "导出失败";
                try {
                    const data = await resp.json();
                    message = (data && data.message) ? data.message : message;
                } catch (_e) {
                    // ignore
                }
                throw new Error(message);
            }
            const blob = await resp.blob();
            const downloadUrl = URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.href = downloadUrl;
            link.download = H.parseContentDispositionFilename(resp, `提请专门教育申请书_${Date.now()}.${fmt}`);
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

    queryBtn.addEventListener("click", (e) => {
        e.preventDefault();
        queryRows();
    });

    exportBtn.addEventListener("click", (e) => {
        e.preventDefault();
        exportDd.classList.toggle("open");
    });
    exportDd.querySelectorAll(".dropdown-menu a").forEach((anchor) => {
        anchor.addEventListener("click", (e) => {
            e.preventDefault();
            exportDd.classList.remove("open");
            doExport(anchor.getAttribute("data-fmt") || "xlsx");
        });
    });
    document.addEventListener("click", (e) => {
        if (!exportDd.contains(e.target)) exportDd.classList.remove("open");
    });

    bindDropdown(typesDisplay, typesDropdown, () => {
        renderMultiLabel(typesDisplay, typesDropdown, "全量");
    });
    bindDropdown(fenjuDisplay, fenjuDropdown, () => {
        renderMultiLabel(fenjuDisplay, fenjuDropdown, "全量");
    });

    H.setDefaultTimeRange(startEl, endEl);

    Promise.all([loadTypes(), loadFenju()])
        .then(() => queryRows())
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
