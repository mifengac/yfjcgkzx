(function() {
    const NS = window.JingqingAnjianFenxi || {};
    const C = NS.constants || {};
    const H = NS.helpers || {};
    const endpoints = window.JQAJFX_ENDPOINTS || {};

    const startEl = document.getElementById("jqajfxStart");
    const endEl = document.getElementById("jqajfxEnd");
    const fenjuDisplay = document.getElementById("jqajfxFenjuDisplay");
    const fenjuDropdown = document.getElementById("jqajfxFenjuDropdown");
    const typeDisplay = document.getElementById("jqajfxTypeDisplay");
    const typeDropdown = document.getElementById("jqajfxTypeDropdown");
    const modeToggle = document.getElementById("jqajfxModeToggle");
    const modeText = document.getElementById("jqajfxModeText");
    const queryBtn = document.getElementById("jqajfxQueryBtn");
    const exportBtn = document.getElementById("jqajfxExportBtn");
    const exportDd = document.getElementById("jqajfxExportDd");
    const statusEl = document.getElementById("jqajfxStatus");
    const errEl = document.getElementById("jqajfxErr");
    const tableEl = document.getElementById("jqajfxTable");
    const busyMask = document.getElementById("jqajfxBusyMask");
    const busyTextEl = document.getElementById("jqajfxBusyText");

    if (
        !startEl || !endEl || !fenjuDisplay || !fenjuDropdown || !typeDisplay || !typeDropdown ||
        !modeToggle || !modeText || !queryBtn || !exportBtn || !exportDd || !statusEl || !errEl || !tableEl
    ) {
        return;
    }

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
        return getCheckBoxes(dropdownEl).filter(function(box) { return box.value !== "_all"; });
    }

    function syncAllBox(dropdownEl) {
        const allBox = dropdownEl.querySelector('input[value="_all"]');
        if (!allBox) return;
        const dataBoxes = getDataBoxes(dropdownEl);
        const allChecked = dataBoxes.length > 0 && dataBoxes.every(function(box) { return box.checked; });
        const noneChecked = dataBoxes.length > 0 && dataBoxes.every(function(box) { return !box.checked; });
        allBox.checked = allChecked;
        allBox.indeterminate = !allChecked && !noneChecked;
    }

    function selectedValues(dropdownEl) {
        return getDataBoxes(dropdownEl)
            .filter(function(box) { return box.checked; })
            .map(function(box) { return box.value; });
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
        displayEl.addEventListener("click", function(e) {
            e.stopPropagation();
            dropdownEl.classList.toggle("open");
        });
        dropdownEl.addEventListener("click", function(e) { e.stopPropagation(); });
        document.addEventListener("click", function() { dropdownEl.classList.remove("open"); });
        dropdownEl.addEventListener("change", function(e) {
            const target = e.target;
            if (!target || target.tagName !== "INPUT") return;
            if (target.value === "_all") {
                getDataBoxes(dropdownEl).forEach(function(box) {
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
        (items || []).forEach(function(item) {
            const value = H.escapeHtml(item.value);
            const label = H.escapeHtml(item.label);
            html.push(
                `<label class="multi-select-option"><input type="checkbox" value="${value}" data-label="${label}" checked><span>${label}</span></label>`
            );
        });
        dropdownEl.innerHTML = html.join("");
        syncAllBox(dropdownEl);
    }

    async function loadOptions(url, dropdownEl, displayEl, emptyText) {
        dropdownEl.innerHTML = '<div class="muted" style="padding:8px;">加载中...</div>';
        const response = await fetch(url);
        const data = await response.json();
        if (!data.success) {
            throw new Error(data.message || "加载失败");
        }
        fillMultiOptions(dropdownEl, data.data || []);
        renderMultiLabel(displayEl, dropdownEl, emptyText);
    }

    function currentGroupMode() {
        return modeToggle.checked ? "station" : "county";
    }

    function refreshModeText() {
        modeText.textContent = `统计维度：${modeToggle.checked ? "派出所" : "县市区"}`;
    }

    function buildQueryParams() {
        const usp = new URLSearchParams();
        const startTime = H.formatDateTime(startEl.value);
        const endTime = H.formatDateTime(endEl.value);
        if (startTime) usp.set("start_time", startTime);
        if (endTime) usp.set("end_time", endTime);
        usp.set("group_mode", currentGroupMode());
        selectedValues(typeDropdown).forEach(function(value) {
            usp.append("leixing", value);
        });
        selectedValues(fenjuDropdown).forEach(function(value) {
            usp.append("ssfjdm", value);
        });
        return usp;
    }

    function isClickableCol(col) {
        return !!(C.CLICKABLE_COLS && C.CLICKABLE_COLS.has(col));
    }

    function renderTable(rows) {
        const columns = C.SUMMARY_COLUMNS || [];
        const thead = `<thead><tr>${columns.map(function(col) { return `<th>${col}</th>`; }).join("")}</tr></thead>`;
        const bodyRows = (rows || []).map(function(row) {
            const cells = columns.map(function(col) {
                const value = row[col] == null ? "" : row[col];
                if (col === "分局" || col === "当前分组名称") {
                    return `<td class="text-left">${value}</td>`;
                }
                if (isClickableCol(col) && String(value).trim() !== "") {
                    const usp = buildQueryParams();
                    usp.set("metric", C.METRIC_BY_COL[col] || "");
                    usp.set("group_code", row.group_code || "__ALL__");
                    usp.set("group_name", row["当前分组名称"] || "");
                    const href = `${endpoints.detailPage}?${usp.toString()}`;
                    return `<td class="clickable-cell" data-href="${href}">${value}</td>`;
                }
                return `<td>${value}</td>`;
            });
            return `<tr>${cells.join("")}</tr>`;
        }).join("");

        tableEl.innerHTML = thead + `<tbody>${bodyRows || `<tr><td colspan="${columns.length}" class="muted">无数据</td></tr>`}</tbody>`;
        tableEl.querySelectorAll("td.clickable-cell").forEach(function(cell) {
            cell.addEventListener("click", function() {
                const href = cell.getAttribute("data-href");
                if (href) window.jingqingAnjianFenxiOpenDetail(href);
            });
        });
    }

    async function querySummary() {
        errEl.textContent = "";
        statusEl.textContent = "";
        queryBtn.disabled = true;
        setBusy(true, "正在查询中，请稍候...");
        try {
            const response = await fetch(`${endpoints.apiSummary}?${buildQueryParams().toString()}`);
            const data = await response.json();
            if (!data.success) {
                throw new Error(data.message || "查询失败");
            }
            statusEl.textContent = `当前时间：${data.meta.start_time} ~ ${data.meta.end_time}；统计维度：${data.meta.group_mode_label}`;
            renderTable(data.rows || []);
        } catch (error) {
            errEl.textContent = error && error.message ? error.message : String(error);
            tableEl.innerHTML = "";
        } finally {
            setBusy(false);
            queryBtn.disabled = false;
        }
    }

    function doExport(fmt) {
        const usp = buildQueryParams();
        usp.set("fmt", fmt);
        window.location.href = `${endpoints.exportSummary}?${usp.toString()}`;
    }

    window.jingqingAnjianFenxiOpenDetail = function(href) {
        const modal = document.getElementById("jqajfxDetailModal");
        const frame = document.getElementById("jqajfxDetailFrame");
        if (!modal || !frame) {
            window.open(href, "_blank");
            return;
        }
        frame.src = href;
        modal.style.display = "flex";
    };

    window.jingqingAnjianFenxiCloseModal = function(event) {
        if (event && event.target && event.target.id !== "jqajfxDetailModal") return;
        const modal = document.getElementById("jqajfxDetailModal");
        const frame = document.getElementById("jqajfxDetailFrame");
        if (!modal || !frame) return;
        modal.style.display = "none";
        frame.src = "about:blank";
    };

    queryBtn.addEventListener("click", function(e) {
        e.preventDefault();
        querySummary();
    });

    exportBtn.addEventListener("click", function(e) {
        e.preventDefault();
        exportDd.classList.toggle("open");
    });
    exportDd.querySelectorAll(".dropdown-menu a").forEach(function(link) {
        link.addEventListener("click", function(e) {
            e.preventDefault();
            exportDd.classList.remove("open");
            doExport(link.getAttribute("data-fmt") || "xlsx");
        });
    });
    document.addEventListener("click", function(e) {
        if (!exportDd.contains(e.target)) exportDd.classList.remove("open");
    });

    modeToggle.addEventListener("change", refreshModeText);
    bindDropdown(typeDisplay, typeDropdown, function() {
        renderMultiLabel(typeDisplay, typeDropdown, "未选择（默认全量）");
    });
    bindDropdown(fenjuDisplay, fenjuDropdown, function() {
        renderMultiLabel(fenjuDisplay, fenjuDropdown, "未选择（默认全量）");
    });

    H.setDefaultTimeRange(startEl, endEl);
    refreshModeText();

    Promise.all([
        loadOptions(endpoints.apiLeixing, typeDropdown, typeDisplay, "未选择（默认全量）"),
        loadOptions(endpoints.apiFenju, fenjuDropdown, fenjuDisplay, "未选择（默认全量）"),
    ]).catch(function(error) {
        errEl.textContent = error && error.message ? error.message : String(error);
    });
})();
