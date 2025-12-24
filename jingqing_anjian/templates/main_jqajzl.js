(() => {
    function formatDateTimeLocal(date) {
        const pad = (num) => String(num).padStart(2, "0");
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
            `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
    }

    function toQueryDate(value) {
        if (!value) return "";
        return value.replace("T", " ");
    }

    function showStatus(el, message, isSuccess) {
        if (!el) return;
        el.textContent = message;
        el.className = `status-message ${isSuccess ? "success" : "error"}`;
        el.style.display = "block";
    }

    function getSelectedCaseTypes(select) {
        const dropdown = document.getElementById("jqajzlCaseTypesDropdown");
        if (!dropdown) return [];
        return Array.from(dropdown.querySelectorAll("input[type=\"checkbox\"]:checked"))
            .map(input => input.value)
            .filter(value => value);
    }

    function updateCaseTypesDisplay() {
        const display = document.getElementById("jqajzlCaseTypesDisplay");
        const dropdown = document.getElementById("jqajzlCaseTypesDropdown");
        if (!display || !dropdown) return;
        const selected = Array.from(dropdown.querySelectorAll("input[type=\"checkbox\"]:checked"))
            .map(input => input.dataset.label || input.value);
        display.textContent = selected.length ? selected.join(", ") : "全部类型";
    }

    function buildParams(baseParams) {
        const params = new URLSearchParams();
        (baseParams.caseTypes || []).forEach(type => params.append("case_types", type));
        if (baseParams.startTime) params.append("start_time", baseParams.startTime);
        if (baseParams.endTime) params.append("end_time", baseParams.endTime);
        if (baseParams.region) params.append("region", baseParams.region);
        if (baseParams.statusName) params.append("status_name", baseParams.statusName);
        if (baseParams.metric) params.append("metric", baseParams.metric);
        if (baseParams.format) params.append("format", baseParams.format);
        return params;
    }

    function getFilenameFromDisposition(disposition, fallback) {
        if (!disposition) return fallback;
        const match = disposition.match(/filename\*?=(?:UTF-8'')?\"?([^\";]+)\"?/i);
        if (match && match[1]) {
            try {
                return decodeURIComponent(match[1]);
            } catch (err) {
                return match[1];
            }
        }
        return fallback;
    }

    function initOverviewPage() {
        const page = document.getElementById("jqajzl-page");
        if (!page) return;

        const caseTypeContainer = document.getElementById("jqajzlCaseTypes");
        const caseTypeDisplay = document.getElementById("jqajzlCaseTypesDisplay");
        const caseTypeDropdown = document.getElementById("jqajzlCaseTypesDropdown");
        const startInput = document.getElementById("jqajzlStartTime");
        const endInput = document.getElementById("jqajzlEndTime");

        if (startInput && endInput) {
            const now = new Date();
            const end = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
            const start = new Date(end.getTime() - 3 * 24 * 60 * 60 * 1000);
            startInput.value = formatDateTimeLocal(start);
            endInput.value = formatDateTimeLocal(end);
        }

        if (caseTypeDisplay && caseTypeDropdown) {
            caseTypeDisplay.addEventListener("click", (event) => {
                event.stopPropagation();
                caseTypeDropdown.classList.toggle("open");
            });
            document.addEventListener("click", (event) => {
                if (!caseTypeContainer) return;
                if (!caseTypeContainer.contains(event.target)) {
                    caseTypeDropdown.classList.remove("open");
                }
            });
        }

        fetch("/jingqing_anjian/api/case_types")
            .then(response => response.json())
            .then(data => {
                if (!data.success) return;
                const options = data.data || [];
                options.forEach(item => {
                    const value = item.leixing || item["leixing"] || "";
                    if (!value) return;
                    const wrapper = document.createElement("label");
                    wrapper.className = "multi-select-option";

                    const checkbox = document.createElement("input");
                    checkbox.type = "checkbox";
                    checkbox.value = value;
                    checkbox.dataset.label = value;
                    checkbox.addEventListener("change", updateCaseTypesDisplay);

                    const text = document.createElement("span");
                    text.textContent = value;

                    wrapper.appendChild(checkbox);
                    wrapper.appendChild(text);
                    if (caseTypeDropdown) {
                        caseTypeDropdown.appendChild(wrapper);
                    }
                });
                updateCaseTypesDisplay();
            })
            .catch(() => {});
    }

    window.searchJqajzl = function searchJqajzl() {
        const statusEl = document.getElementById("jqajzlStatus");
        const resultsEl = document.getElementById("jqajzlResults");
        const caseTypeSelect = document.getElementById("jqajzlCaseTypes");
        const startInput = document.getElementById("jqajzlStartTime");
        const endInput = document.getElementById("jqajzlEndTime");

        const startValue = startInput ? startInput.value : "";
        const endValue = endInput ? endInput.value : "";
        if (!startValue || !endValue) {
            showStatus(statusEl, "请先选择开始时间和结束时间。", false);
            return;
        }
        if (new Date(startValue) > new Date(endValue)) {
            showStatus(statusEl, "开始时间不能晚于结束时间。", false);
            return;
        }

        const caseTypes = getSelectedCaseTypes(caseTypeSelect);
        const params = buildParams({
            caseTypes,
            startTime: toQueryDate(startValue),
            endTime: toQueryDate(endValue)
        });

        fetch(`/jingqing_anjian/api/jqajzl/summary?${params.toString()}`)
            .then(response => response.json())
            .then(data => {
                if (!data.success) {
                    showStatus(statusEl, data.message || "查询失败。", false);
                    return;
                }
                window.jqajzlQuery = {
                    caseTypes,
                    startTime: toQueryDate(startValue),
                    endTime: toQueryDate(endValue)
                };
                showStatus(statusEl, "查询成功。", true);
                renderSummaryTable(resultsEl, data.columns || [], data.data || []);
            })
            .catch(err => {
                showStatus(statusEl, `查询失败: ${err}`, false);
            });
    };

    function renderSummaryTable(container, columns, rows) {
        if (!container) return;
        if (!rows.length) {
            container.innerHTML = `
                <p style="text-align: center; color: #666; padding: 30px;">
                    未找到相关记录
                </p>
            `;
            return;
        }

        let html = `
            <div class="results-table-container">
                <table class="results-table">
                    <thead>
                        <tr>
        `;
        columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += `
                        </tr>
                    </thead>
                    <tbody>
        `;

        rows.forEach(row => {
            const isTotal = row["地区"] === "合计";
            html += `<tr${isTotal ? ' class="total-row"' : ""}>`;
            columns.forEach((col, index) => {
                const value = row[col] ?? "";
                if (index === 0) {
                    html += `<td>${value}</td>`;
                } else {
                    const region = isTotal ? "" : row["地区"];
                    const metric = col === "案件数" || col === "警情数" ? col : "";
                    const statusName = col !== "案件数" && col !== "警情数" ? col : "";
                    html += `<td><a class="jqajzl-link" href="#" data-region="${region}" data-metric="${metric}" data-status="${statusName}">${value}</a></td>`;
                }
            });
            html += `</tr>`;
        });

        html += `
                    </tbody>
                </table>
            </div>
        `;
        container.innerHTML = html;

        container.querySelectorAll(".jqajzl-link").forEach(link => {
            link.addEventListener("click", (event) => {
                event.preventDefault();
                openJqajzlDetail(link.dataset);
            });
        });
    }

    function openJqajzlDetail(dataset) {
        const modal = document.getElementById("jqajzlModal");
        const frame = document.getElementById("jqajzlDetailFrame");
        if (!modal || !frame) return;

        const query = window.jqajzlQuery || {};
        const params = buildParams({
            caseTypes: query.caseTypes || [],
            startTime: query.startTime || "",
            endTime: query.endTime || "",
            region: dataset.region || "",
            statusName: dataset.status || "",
            metric: dataset.metric || ""
        });

        frame.src = `/jingqing_anjian/jqajzl_detail?${params.toString()}`;
        modal.style.display = "flex";
    }

    window.closeJqajzlModal = function closeJqajzlModal() {
        const modal = document.getElementById("jqajzlModal");
        const frame = document.getElementById("jqajzlDetailFrame");
        if (modal) modal.style.display = "none";
        if (frame) frame.src = "";
    };

    window.exportJqajzl = function exportJqajzl(format) {
        const query = window.jqajzlQuery || {};
        if (!query.startTime || !query.endTime) {
            alert("请先查询后再导出。");
            return;
        }
        const params = buildParams({
            caseTypes: query.caseTypes || [],
            startTime: query.startTime,
            endTime: query.endTime,
            format
        });

        fetch(`/jingqing_anjian/api/jqajzl/export?${params.toString()}`)
            .then(response => {
                if (!response.ok) throw new Error("导出失败");
                const disposition = response.headers.get("Content-Disposition") || "";
                return response.blob().then(blob => ({ blob, disposition }));
            })
            .then(({ blob, disposition }) => {
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = getFilenameFromDisposition(disposition, "警情案件总览导出");
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);
            })
            .catch(() => alert("导出失败，请稍后重试。"));
    };

    function initDetailPage() {
        const detailPage = document.getElementById("jqajzl-detail-page");
        if (!detailPage) return;

        const statusEl = document.getElementById("jqajzlDetailStatus");
        const resultsEl = document.getElementById("jqajzlDetailResults");
        const exportBtn = document.getElementById("jqajzlDetailExportBtn");
        const exportMenu = document.getElementById("jqajzlDetailExportMenu");
        const params = new URLSearchParams(window.location.search);

        if (exportBtn && exportMenu) {
            exportBtn.addEventListener("click", (event) => {
                event.stopPropagation();
                exportMenu.classList.toggle("open");
            });
            document.addEventListener("click", (event) => {
                if (!exportMenu.contains(event.target) && event.target !== exportBtn) {
                    exportMenu.classList.remove("open");
                }
            });
        }

        fetch(`/jingqing_anjian/api/jqajzl/detail?${params.toString()}`)
            .then(response => response.json())
            .then(data => {
                if (!data.success) {
                    showStatus(statusEl, data.message || "查询失败。", false);
                    return;
                }
                window.jqajzlDetailQuery = Object.fromEntries(params.entries());
                renderDetailTable(resultsEl, data.columns || [], data.data || []);
            })
            .catch(err => {
                showStatus(statusEl, `查询失败: ${err}`, false);
            });
    }

    function renderDetailTable(container, columns, rows) {
        if (!container) return;
        if (!rows.length) {
            container.innerHTML = `
                <p style="text-align: center; color: #666; padding: 30px;">
                    未找到相关记录
                </p>
            `;
            return;
        }

        let html = `
            <div class="results-table-container">
                <table class="results-table">
                    <thead>
                        <tr>
        `;
        columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += `
                        </tr>
                    </thead>
                    <tbody>
        `;
        rows.forEach(row => {
            html += `<tr>`;
            columns.forEach(col => {
                let value = row[col] ?? "";
                if (col === "文书列表") {
                    value = renderDocumentLinks(value);
                    html += `<td>${value}</td>`;
                } else if (col === "处警情况") {
                    html += `<td>${cleanChujingStatus(value)}</td>`;
                } else {
                    html += `<td>${value}</td>`;
                }
            });
            html += `</tr>`;
        });

        html += `
                    </tbody>
                </table>
                <p style="text-align: center; color: #4caf50; padding: 10px;">
                    查询成功！找到 ${rows.length} 条相关记录
                </p>
            </div>
        `;
        container.innerHTML = html;
        setupDetailHorizontalScroll();
    }

    window.exportJqajzlDetail = function exportJqajzlDetail(format) {
        const query = window.jqajzlDetailQuery || {};
        const params = new URLSearchParams(query);
        params.set("format", format);

        fetch(`/jingqing_anjian/api/jqajzl/detail_export?${params.toString()}`)
            .then(response => {
                if (!response.ok) throw new Error("导出失败");
                const disposition = response.headers.get("Content-Disposition") || "";
                return response.blob().then(blob => ({ blob, disposition }));
            })
            .then(({ blob, disposition }) => {
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = getFilenameFromDisposition(disposition, "警情案件总览明细导出");
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);
            })
            .catch(() => alert("导出失败，请稍后重试。"));
    };

    document.addEventListener("DOMContentLoaded", () => {
        initOverviewPage();
        initDetailPage();
        initModalResize();
    });
})();

function renderDocumentLinks(value) {
    let list = value;
    if (!list) return "";
    if (typeof list === "string") {
        try {
            list = JSON.parse(list);
        } catch (err) {
            return "";
        }
    }
    if (!Array.isArray(list)) return "";
    return list
        .map(item => {
            const name = (item && item.name) ? String(item.name) : "";
            const url = (item && item.url) ? String(item.url) : "";
            if (!name || !url) return "";
            return `<a href="${url}" target="_blank" rel="noopener">${name}</a>`;
        })
        .filter(Boolean)
        .join("<br>");
}

function cleanChujingStatus(value) {
    if (!value) return "未反馈";
    const text = String(value);
    const rules = [
        { key: "【结警反馈】", label: "【结警反馈】" },
        { key: "【过程反馈】", label: "【过程反馈】" },
        { key: "处理结果说明", label: "处理结果说明" },
    ];
    for (const rule of rules) {
        const idx = text.indexOf(rule.key);
        if (idx !== -1) {
            const after = text.slice(idx);
            const endIndex = after.search(/\r?\n\s*\r?\n/);
            if (endIndex === -1) return after.trim();
            return after.slice(0, endIndex).trim();
        }
    }
    if (text.includes("关联重复报警")) return "重复报警";
    return "未反馈";
}

function initModalResize() {
    const bar = document.getElementById("jqajzlResizeBar");
    const modalContent = document.querySelector(".modal-content");
    if (!bar || !modalContent) return;

    let resizing = false;
    let startX = 0;
    let startWidth = 0;

    bar.addEventListener("mousedown", (event) => {
        resizing = true;
        startX = event.clientX;
        startWidth = modalContent.offsetWidth;
        event.preventDefault();
    });

    document.addEventListener("mousemove", (event) => {
        if (!resizing) return;
        const delta = event.clientX - startX;
        const nextWidth = Math.max(600, startWidth + delta);
        modalContent.style.width = `${nextWidth}px`;
    });

    document.addEventListener("mouseup", () => {
        resizing = false;
    });
}

function setupDetailHorizontalScroll() {
    const scrollBar = document.getElementById("jqajzlDetailScroll");
    const scrollInner = document.getElementById("jqajzlDetailScrollInner");
    const tableContainer = document.querySelector("#jqajzlDetailResults .results-table-container");
    if (!scrollBar || !scrollInner || !tableContainer) return;

    const syncWidth = () => {
        scrollInner.style.width = `${tableContainer.scrollWidth}px`;
    };
    syncWidth();

    if (!scrollBar.dataset.bound) {
        let syncing = false;
        scrollBar.onscroll = () => {
            if (syncing) return;
            syncing = true;
            tableContainer.scrollLeft = scrollBar.scrollLeft;
            syncing = false;
        };
        tableContainer.onscroll = () => {
            if (syncing) return;
            syncing = true;
            scrollBar.scrollLeft = tableContainer.scrollLeft;
            syncing = false;
        };

        window.addEventListener("resize", syncWidth);
        scrollBar.dataset.bound = "1";
    }
}
