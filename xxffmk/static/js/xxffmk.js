(function() {
    const state = {
        filters: null,
        rankingRows: [],
        selectedSchool: null,
        selectedDimension: null,
        dimensionPage: 1,
        dimensionPageSize: 20
    };

    function byId(id) {
        return document.getElementById(id);
    }

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function setStatus(text) {
        byId("statusText").textContent = text;
    }

    function readFilters() {
        return {
            beginDate: byId("beginDate").value,
            endDate: byId("endDate").value,
            limit: Number(byId("limit").value || 10)
        };
    }

    function buildQuery(params) {
        const search = new URLSearchParams();
        Object.keys(params).forEach(function(key) {
            const value = params[key];
            if (value !== undefined && value !== null && value !== "") {
                search.set(key, value);
            }
        });
        return search.toString();
    }

    function renderEmpty(targetId, text) {
        byId(targetId).innerHTML = '<div class="empty-state">' + escapeHtml(text) + '</div>';
    }

    function renderRankingTable(rows) {
        if (!rows.length) {
            renderEmpty("rankingTableShell", "当前条件下暂无学校数据。");
            return;
        }
        const html = [
            '<table>',
            '<thead><tr><th>排名</th><th>学校</th><th>主管教育局</th><th>总分</th></tr></thead>',
            '<tbody>'
        ];
        rows.forEach(function(row) {
            html.push(
                "<tr>",
                "<td>" + escapeHtml(row.rank) + "</td>",
                '<td><button type="button" class="link-button" data-school-code="' + escapeHtml(row.xxbsm) + '">' + escapeHtml(row.xxmc || row.xxbsm) + "</button></td>",
                "<td>" + escapeHtml(row.zgjyxzbmmc || "") + "</td>",
                '<td><span class="score-badge">' + escapeHtml(row.total_score) + "</span></td>",
                "</tr>"
            );
        });
        html.push("</tbody></table>");
        byId("rankingTableShell").innerHTML = html.join("");

        byId("rankingTableShell").querySelectorAll("[data-school-code]").forEach(function(button) {
            button.addEventListener("click", function() {
                loadSchoolDetail(button.getAttribute("data-school-code"));
            });
        });
    }

    function renderUnmatchedSummary(summary) {
        const items = [];
        Object.keys(summary || {}).forEach(function(key) {
            const rows = summary[key] || [];
            if (!rows.length) {
                return;
            }
            rows.forEach(function(row) {
                items.push('<span class="pill">' + escapeHtml(key) + " / " + escapeHtml(row.raw_school_name) + " / " + escapeHtml(row.count) + "</span>");
            });
        });
        byId("unmatchedSummary").innerHTML = items.length ? '<div class="pill-list">' + items.join("") + "</div>" : '<div class="empty-state">暂无未匹配数据</div>';
    }

    function renderSchoolDetailTable(payload) {
        if (!payload || !payload.school) {
            renderEmpty("schoolDetailTableShell", "点击左侧学校后显示各维度原始值、名次和得分。");
            return;
        }
        const school = payload.school;
        byId("schoolDetailSubtitle").textContent = (school.xxmc || school.xxbsm) + " 的维度得分明细";
        const html = [
            '<table>',
            '<thead><tr><th>维度</th><th>原始值</th><th>名次</th><th>得分</th><th>操作</th></tr></thead>',
            '<tbody>'
        ];
        (payload.dimension_order || []).forEach(function(config) {
            const score = school.dimension_scores[config.key] || {};
            html.push(
                "<tr>",
                "<td>" + escapeHtml(config.label) + "</td>",
                "<td>" + escapeHtml(score.value || 0) + "</td>",
                "<td>" + escapeHtml(score.rank || "-") + "</td>",
                "<td>" + escapeHtml(score.score || 0) + "</td>",
                '<td><button type="button" class="link-button" data-dimension-key="' + escapeHtml(config.key) + '">' + "查看明细" + "</button></td>",
                "</tr>"
            );
        });
        html.push("</tbody></table>");
        byId("schoolDetailTableShell").innerHTML = html.join("");
        byId("schoolDetailTableShell").querySelectorAll("[data-dimension-key]").forEach(function(button) {
            button.addEventListener("click", function() {
                state.selectedDimension = button.getAttribute("data-dimension-key");
                state.dimensionPage = 1;
                loadDimensionDetail();
            });
        });
    }

    function renderDimensionDetail(payload) {
        if (!payload || !(payload.rows || []).length) {
            renderEmpty("dimensionDetailTableShell", "当前维度暂无明细。");
            byId("dimensionPagination").style.display = "none";
            return;
        }
        const columns = payload.columns || [];
        const html = ['<table><thead><tr>'];
        columns.forEach(function(column) {
            html.push("<th>" + escapeHtml(column) + "</th>");
        });
        html.push("</tr></thead><tbody>");
        payload.rows.forEach(function(row) {
            html.push("<tr>");
            columns.forEach(function(column) {
                html.push("<td>" + escapeHtml(row[column]) + "</td>");
            });
            html.push("</tr>");
        });
        html.push("</tbody></table>");
        byId("dimensionDetailTableShell").innerHTML = html.join("");

        if ((payload.unmatched_summary || []).length) {
            const summary = payload.unmatched_summary.map(function(item) {
                return escapeHtml(item.raw_school_name) + " / " + escapeHtml(item.count);
            }).join("；");
            byId("dimensionDetailSubtitle").textContent = payload.dimension + " 明细（未匹配摘要：" + summary + "）";
        }

        byId("dimensionPagination").style.display = "flex";
        byId("dimensionPaginationMeta").textContent = "第 " + payload.page + " 页 / 共 " + Math.max(1, Math.ceil(payload.total / payload.page_size)) + " 页，合计 " + payload.total + " 条";
        byId("prevPageBtn").disabled = payload.page <= 1;
        byId("nextPageBtn").disabled = payload.page * payload.page_size >= payload.total;
    }

    async function requestJson(url, options) {
        const response = await fetch(url, options);
        const payload = await response.json();
        if (!response.ok || !payload.success) {
            throw new Error(payload.message || "请求失败");
        }
        return payload.data;
    }

    async function loadRanking() {
        state.filters = readFilters();
        state.selectedSchool = null;
        state.selectedDimension = null;
        setStatus("正在查询...");
        renderEmpty("schoolDetailTableShell", "点击左侧学校后显示各维度原始值、名次和得分。");
        renderEmpty("dimensionDetailTableShell", "点击“学校维度得分”中的维度查看具体明细。");
        byId("schoolDetailSubtitle").textContent = "点击左侧学校后显示各维度原始值、名次和得分。";
        byId("dimensionDetailSubtitle").textContent = "点击“学校维度得分”中的维度查看具体明细。";
        byId("dimensionPagination").style.display = "none";
        try {
            const data = await requestJson("/xxffmk/api/rank", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify(state.filters)
            });
            state.filters = data.filters;
            state.rankingRows = data.rows || [];
            renderRankingTable(state.rankingRows);
            renderUnmatchedSummary(data.unmatched_summary || {});
            setStatus("查询完成，共 " + (data.total || 0) + " 所学校。");
        } catch (error) {
            renderEmpty("rankingTableShell", error.message);
            renderUnmatchedSummary({});
            setStatus(error.message);
        }
    }

    async function loadSchoolDetail(schoolCode) {
        if (!schoolCode || !state.filters) {
            return;
        }
        state.selectedSchool = schoolCode;
        state.selectedDimension = null;
        state.dimensionPage = 1;
        byId("dimensionDetailSubtitle").textContent = "点击“学校维度得分”中的维度查看具体明细。";
        renderEmpty("dimensionDetailTableShell", "点击“学校维度得分”中的维度查看具体明细。");
        byId("dimensionPagination").style.display = "none";
        setStatus("正在加载学校维度得分...");
        try {
            const query = buildQuery({
                xxbsm: schoolCode,
                beginDate: state.filters.beginDate,
                endDate: state.filters.endDate
            });
            const data = await requestJson("/xxffmk/api/school_detail?" + query, {method: "GET"});
            renderSchoolDetailTable(data);
            setStatus("学校维度得分已加载。");
        } catch (error) {
            renderEmpty("schoolDetailTableShell", error.message);
            setStatus(error.message);
        }
    }

    async function loadDimensionDetail() {
        if (!state.selectedSchool || !state.selectedDimension || !state.filters) {
            return;
        }
        setStatus("正在加载维度明细...");
        try {
            const query = buildQuery({
                xxbsm: state.selectedSchool,
                dimension: state.selectedDimension,
                beginDate: state.filters.beginDate,
                endDate: state.filters.endDate,
                page: state.dimensionPage,
                page_size: state.dimensionPageSize
            });
            const data = await requestJson("/xxffmk/api/dimension_detail?" + query, {method: "GET"});
            byId("dimensionDetailSubtitle").textContent = data.dimension + " 明细";
            renderDimensionDetail(data);
            setStatus("维度明细已加载。");
        } catch (error) {
            renderEmpty("dimensionDetailTableShell", error.message);
            byId("dimensionPagination").style.display = "none";
            setStatus(error.message);
        }
    }

    function resetFilters() {
        byId("beginDate").value = window.XXFFMK_DEFAULTS.beginDate || "";
        byId("endDate").value = window.XXFFMK_DEFAULTS.endDate || "";
        byId("limit").value = "10";
        loadRanking();
    }

    async function refreshData() {
        const refreshBtn = byId("refreshBtn");
        const originalText = refreshBtn ? refreshBtn.textContent : "";
        if (refreshBtn) {
            refreshBtn.disabled = true;
            refreshBtn.textContent = "刷新中...";
        }
        setStatus("正在刷新物化视图...");
        try {
            const data = await requestJson("/xxffmk/api/refresh", {
                method: "POST"
            });
            setStatus((data.message || "刷新完成") + "，正在重新查询...");
            await loadRanking();
        } catch (error) {
            setStatus(error.message);
        } finally {
            if (refreshBtn) {
                refreshBtn.disabled = false;
                refreshBtn.textContent = originalText || "刷新数据";
            }
        }
    }

    function initPagination() {
        byId("prevPageBtn").addEventListener("click", function() {
            if (state.dimensionPage <= 1) {
                return;
            }
            state.dimensionPage -= 1;
            loadDimensionDetail();
        });
        byId("nextPageBtn").addEventListener("click", function() {
            state.dimensionPage += 1;
            loadDimensionDetail();
        });
    }

    function init() {
        byId("queryBtn").addEventListener("click", loadRanking);
        byId("resetBtn").addEventListener("click", resetFilters);
        byId("refreshBtn").addEventListener("click", refreshData);
        initPagination();
        loadRanking();
    }

    document.addEventListener("DOMContentLoaded", init);
})();
