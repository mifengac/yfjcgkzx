(function() {
    var initialized = false;
    var columns = [
        "派出所名称",
        "派出所代码",
        "风险等级",
        "周期类型",
        "最新周期",
        "上期数量",
        "最新数量",
        "增量",
        "当前连续上升周期数",
        "当前连续上升次数",
        "趋势序列",
        "涉及周期范围"
    ];
    var numericColumns = {
        "上期数量": true,
        "最新数量": true,
        "增量": true,
        "当前连续上升周期数": true,
        "当前连续上升次数": true
    };
    var riskOrder = { "高风险": 3, "中风险": 2, "低风险": 1 };
    var state = {
        rows: [],
        filteredRows: [],
        pageNum: 1,
        pageSize: 20,
        sortKey: "",
        sortDir: "desc",
        keyword: ""
    };

    function $(id) { return document.getElementById(id); }

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function toNumber(value) {
        var num = Number(value);
        return Number.isNaN(num) ? 0 : num;
    }

    function riskClass(level) {
        if (level === "高风险") return "rising-risk-high";
        if (level === "中风险") return "rising-risk-medium";
        return "rising-risk-low";
    }

    function rowRiskClass(level) {
        if (level === "高风险") return "rising-risk-row-high";
        if (level === "中风险") return "rising-risk-row-medium";
        return "";
    }

    function sortValue(row, key) {
        if (key === "风险等级") return riskOrder[row[key]] || 0;
        if (numericColumns[key]) return toNumber(row[key]);
        return String(row[key] == null ? "" : row[key]);
    }

    function compareRows(a, b) {
        if (!state.sortKey) return 0;
        var left = sortValue(a, state.sortKey);
        var right = sortValue(b, state.sortKey);
        var result;
        if (typeof left === "number" && typeof right === "number") {
            result = left - right;
        } else {
            result = String(left).localeCompare(String(right), "zh-CN");
        }
        return state.sortDir === "asc" ? result : -result;
    }

    function applyFilters() {
        var keyword = state.keyword.trim().toLowerCase();
        state.filteredRows = state.rows.filter(function(row) {
            if (!keyword) return true;
            return String(row["派出所名称"] || "").toLowerCase().indexOf(keyword) >= 0;
        });
        if (state.sortKey) state.filteredRows.sort(compareRows);
        var totalPages = getTotalPages();
        if (state.pageNum > totalPages) state.pageNum = totalPages;
        if (state.pageNum < 1) state.pageNum = 1;
    }

    function getTotalPages() {
        return Math.max(1, Math.ceil(state.filteredRows.length / state.pageSize));
    }

    function renderCell(row, col) {
        var value = row[col] == null ? "" : row[col];
        if (col === "风险等级") {
            return '<span class="rising-risk-badge ' + riskClass(value) + '">' + escapeHtml(value || "低风险") + "</span>";
        }
        return escapeHtml(value);
    }

    function renderTable() {
        var table = $("risingIncidentTable");
        if (!table) return;
        var headerHtml = columns.map(function(col) {
            var cls = "rising-incident-sortable";
            if (state.sortKey === col) cls += state.sortDir === "asc" ? " sort-asc" : " sort-desc";
            return '<th class="' + cls + '" data-sort-key="' + escapeHtml(col) + '">' + escapeHtml(col) + "</th>";
        }).join("");
        var html = "<thead><tr>" + headerHtml + "</tr></thead><tbody>";
        if (!state.filteredRows.length) {
            html += '<tr><td class="muted" colspan="' + columns.length + '" style="padding:18px;">无符合条件数据</td></tr>';
        } else {
            var start = (state.pageNum - 1) * state.pageSize;
            var pageRows = state.filteredRows.slice(start, start + state.pageSize);
            pageRows.forEach(function(row) {
                html += '<tr class="' + rowRiskClass(row["风险等级"]) + '">' + columns.map(function(col) {
                    var numeric = numericColumns[col] ? " num-cell" : "";
                    return '<td class="rowh' + numeric + '">' + renderCell(row, col) + "</td>";
                }).join("") + "</tr>";
            });
        }
        html += "</tbody>";
        table.innerHTML = html;
    }

    function renderPagination() {
        var container = $("risingIncidentPagination");
        if (!container) return;
        var totalPages = getTotalPages();
        container.innerHTML =
            "<div class='pagination-meta'>共 " + state.rows.length + " 条，筛选后 " + state.filteredRows.length +
            " 条，第 " + state.pageNum + "/" + totalPages + " 页</div>" +
            "<div class='pagination-controls'>" +
            "<button type='button' data-page-action='first'" + (state.pageNum <= 1 ? " disabled" : "") + ">首页</button>" +
            "<button type='button' data-page-action='prev'" + (state.pageNum <= 1 ? " disabled" : "") + ">上一页</button>" +
            "<button type='button' data-page-action='next'" + (state.pageNum >= totalPages ? " disabled" : "") + ">下一页</button>" +
            "<button type='button' data-page-action='last'" + (state.pageNum >= totalPages ? " disabled" : "") + ">末页</button>" +
            "</div>";
    }

    function render() {
        applyFilters();
        renderTable();
        renderPagination();
    }

    function setData(rows) {
        state.rows = Array.isArray(rows) ? rows.slice() : [];
        state.pageNum = 1;
        render();
    }

    function renderEmpty(message) {
        state.rows = [];
        state.filteredRows = [];
        var table = $("risingIncidentTable");
        if (table) {
            table.innerHTML = '<tbody><tr><td class="muted" style="padding:18px;">' + escapeHtml(message) + "</td></tr></tbody>";
        }
        var container = $("risingIncidentPagination");
        if (container) container.innerHTML = "";
    }

    function handleSort(target) {
        var key = target.getAttribute("data-sort-key");
        if (!key) return;
        if (state.sortKey === key) {
            state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
        } else {
            state.sortKey = key;
            state.sortDir = numericColumns[key] || key === "风险等级" ? "desc" : "asc";
        }
        state.pageNum = 1;
        render();
    }

    function handlePageAction(action) {
        var totalPages = getTotalPages();
        if (action === "first") state.pageNum = 1;
        else if (action === "prev") state.pageNum = Math.max(1, state.pageNum - 1);
        else if (action === "next") state.pageNum = Math.min(totalPages, state.pageNum + 1);
        else if (action === "last") state.pageNum = totalPages;
        render();
    }

    function init() {
        if (initialized) return;
        initialized = true;
        var searchInput = $("risingIncidentDeptSearch");
        var pageSizeSelect = $("risingIncidentPageSize");
        var table = $("risingIncidentTable");
        var pagination = $("risingIncidentPagination");
        if (searchInput) {
            searchInput.addEventListener("input", function() {
                state.keyword = searchInput.value || "";
                state.pageNum = 1;
                render();
            });
        }
        if (pageSizeSelect) {
            state.pageSize = Number(pageSizeSelect.value) || state.pageSize;
            pageSizeSelect.addEventListener("change", function() {
                state.pageSize = Number(pageSizeSelect.value) || 20;
                state.pageNum = 1;
                render();
            });
        }
        if (table) {
            table.addEventListener("click", function(event) {
                var target = event.target;
                while (target && target !== table && target.tagName !== "TH") target = target.parentNode;
                if (target && target.tagName === "TH") handleSort(target);
            });
        }
        if (pagination) {
            pagination.addEventListener("click", function(event) {
                var target = event.target;
                if (!target || target.tagName !== "BUTTON" || target.disabled) return;
                handlePageAction(target.getAttribute("data-page-action"));
            });
        }
    }

    window.RisingIncidentGrid = {
        init: init,
        setData: setData,
        renderEmpty: renderEmpty
    };
})();
