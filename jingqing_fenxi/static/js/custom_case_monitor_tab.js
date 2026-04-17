(function() {
    var QUERY_POLL_INTERVAL_MS = 500;
    var QUERY_TIMEOUT_MS = 5 * 60 * 1000;
    var state = {
        branches: [],
        schemes: [],
        allSchemes: [],
        fieldOptions: [],
        operatorOptions: [],
        pageNum: 1,
        pageSize: 15,
        total: 0,
        selectedSchemeId: "",
        initialized: false,
        busy: false,
        queryToken: 0,
        queryStartedAt: 0,
        pollTimer: 0,
        activeJobId: ""
    };

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function formatDateTimeLocal(value) {
        return value ? String(value).replace(" ", "T") : "";
    }

    function formatRequestTime(value) {
        return value ? String(value).replace("T", " ") : "";
    }

    function requestJson(url, options) {
        return fetch(url, options).then(function(response) {
            return response.json().then(function(data) {
                if (!response.ok || !data.success) {
                    throw new Error((data && data.message) || ("请求失败（HTTP " + response.status + "）"));
                }
                return data;
            });
        });
    }

    function getQueryButton() {
        return document.querySelector("[data-special-case-type='custom-case-monitor'] [data-action='query']");
    }

    function getExportButton() {
        return document.querySelector("#customMonitorDownloadMenu [data-action='toggle-export']");
    }

    function getSelectedBranches() {
        var boxes = document.querySelectorAll("#customMonitorBranchDropdown input[type='checkbox']");
        var values = [];
        Array.prototype.forEach.call(boxes, function(box) {
            if (box.checked) values.push(box.value);
        });
        return values;
    }

    function renderBranchLabel() {
        var labelNode = document.querySelector("#customMonitorBranchDisplay span");
        if (!labelNode) return;
        var values = getSelectedBranches();
        if (!values.length) labelNode.textContent = "全部";
        else if (values.length === state.branches.length) labelNode.textContent = "全部分局";
        else labelNode.textContent = values.join("、");
    }

    function renderBranchOptions(branches) {
        state.branches = branches || [];
        var dropdown = document.getElementById("customMonitorBranchDropdown");
        if (!dropdown) return;
        dropdown.innerHTML = "";
        state.branches.forEach(function(branch) {
            var label = document.createElement("label");
            var input = document.createElement("input");
            input.type = "checkbox";
            input.value = branch.value;
            input.addEventListener("change", renderBranchLabel);
            label.appendChild(input);
            label.appendChild(document.createTextNode(branch.label));
            dropdown.appendChild(label);
        });
        renderBranchLabel();
    }

    function setMessage(boxId, message) {
        var box = document.getElementById(boxId);
        if (!box) return;
        if (message) {
            box.textContent = message;
            box.classList.remove("special-case-hidden");
        } else {
            box.textContent = "";
            box.classList.add("special-case-hidden");
        }
    }

    function setError(message) {
        setMessage("customMonitorErr", message);
    }

    function setFormError(message) {
        setMessage("customMonitorFormErr", message);
    }

    function setEmptyState(message) {
        setMessage("customMonitorEmptyState", message);
    }

    function setBusy(isBusy, text, detail) {
        var mask = document.getElementById("customMonitorBusyMask");
        var textNode = document.getElementById("customMonitorBusyText");
        var detailNode = document.getElementById("customMonitorBusyStats");
        state.busy = !!isBusy;
        if (textNode) textNode.textContent = text || "正在查询，请稍候...";
        if (detailNode) detailNode.textContent = detail || "已拉取 0 条；规则扫描 0 条，命中 0 条；分局保留 0 条";
        if (mask) {
            if (state.busy) mask.classList.add("active");
            else mask.classList.remove("active");
        }
        updateActionState();
    }

    function clearPollTimer() {
        if (state.pollTimer) {
            window.clearTimeout(state.pollTimer);
            state.pollTimer = 0;
        }
    }

    function cancelPendingQuery() {
        clearPollTimer();
        state.activeJobId = "";
    }

    function renderSchemeOptions(enabledSchemes) {
        var select = document.getElementById("customMonitorScheme");
        if (!select) return;
        select.innerHTML = "";
        enabledSchemes.forEach(function(scheme) {
            var option = document.createElement("option");
            option.value = String(scheme.id);
            option.textContent = scheme.scheme_name;
            select.appendChild(option);
        });
        if (!enabledSchemes.length) {
            var emptyOption = document.createElement("option");
            emptyOption.value = "";
            emptyOption.textContent = "暂无启用方案";
            select.appendChild(emptyOption);
            select.disabled = true;
            state.selectedSchemeId = "";
            return;
        }
        select.disabled = false;
        if (!state.selectedSchemeId || !enabledSchemes.some(function(item) { return String(item.id) === String(state.selectedSchemeId); })) {
            state.selectedSchemeId = String(enabledSchemes[0].id);
        }
        select.value = state.selectedSchemeId;
    }

    function getSelectedScheme() {
        var schemeId = String(state.selectedSchemeId || "");
        for (var i = 0; i < state.allSchemes.length; i++) {
            if (String(state.allSchemes[i].id) === schemeId) return state.allSchemes[i];
        }
        return null;
    }

    function findSchemeById(schemeId) {
        var normalized = String(schemeId || "");
        for (var i = 0; i < state.allSchemes.length; i++) {
            if (String(state.allSchemes[i].id) === normalized) return state.allSchemes[i];
        }
        return null;
    }

    function renderTable(rows) {
        var table = document.getElementById("customMonitorTable");
        if (!table) return;
        var html = "<thead><tr>" +
            "<th>接警号</th><th>报警时间</th><th>分局编码</th><th>管辖单位</th><th>警情级别</th><th>涉案地址</th><th>报警人</th><th>报警人电话</th><th>简要案情</th><th>反馈内容</th><th>命中关键字</th>" +
            "</tr></thead><tbody>";
        if (!rows || !rows.length) {
            html += "<tr><td colspan='11' style='text-align:center;color:#64748b;'>无符合条件数据</td></tr>";
        } else {
            rows.forEach(function(row) {
                html += "<tr>" +
                    "<td>" + escapeHtml(row.caseNo || "") + "</td>" +
                    "<td>" + escapeHtml(row.callTime || "") + "</td>" +
                    "<td>" + escapeHtml(row.cmdId || "") + "</td>" +
                    "<td>" + escapeHtml(row.dutyDeptName || "") + "</td>" +
                    "<td>" + escapeHtml(row.caseLevelName || "") + "</td>" +
                    "<td>" + escapeHtml(row.occurAddress || "") + "</td>" +
                    "<td>" + escapeHtml(row.callerName || "") + "</td>" +
                    "<td>" + escapeHtml(row.callerPhone || "") + "</td>" +
                    "<td>" + escapeHtml(row.caseContents || "") + "</td>" +
                    "<td>" + escapeHtml(row.replies || "") + "</td>" +
                    "<td>" + escapeHtml(row.hitKeywordDetails || "") + "</td>" +
                    "</tr>";
            });
        }
        html += "</tbody>";
        table.innerHTML = html;
    }

    function renderPagination() {
        var container = document.getElementById("customMonitorPagination");
        if (!container) return;
        var totalPages = Math.max(1, Math.ceil(state.total / state.pageSize));
        container.innerHTML =
            "<div class='pagination-meta'>共 " + state.total + " 条，第 " + state.pageNum + "/" + totalPages + " 页</div>" +
            "<div class='pagination-controls'>" +
            "<button type='button' id='customMonitorPrevPage' " + (state.pageNum <= 1 ? "disabled" : "") + ">上一页</button>" +
            "<button type='button' id='customMonitorNextPage' " + (state.pageNum >= totalPages ? "disabled" : "") + ">下一页</button>" +
            "</div>";
        document.getElementById("customMonitorPrevPage").addEventListener("click", function() {
            if (state.pageNum > 1) query(state.pageNum - 1);
        });
        document.getElementById("customMonitorNextPage").addEventListener("click", function() {
            if (state.pageNum < totalPages) query(state.pageNum + 1);
        });
    }

    function renderStatus(result) {
        var node = document.getElementById("customMonitorStatus");
        if (!node) return;
        node.textContent = "方案：" + (result.scheme_name || "") + "；时间范围：" + result.start_time + " 至 " + result.end_time + "；命中 " + result.total + " 条";
    }

    function buildPayload(pageNum) {
        return {
            scheme_id: state.selectedSchemeId,
            start_time: formatRequestTime(document.getElementById("customMonitorStartTime").value),
            end_time: formatRequestTime(document.getElementById("customMonitorEndTime").value),
            branches: getSelectedBranches(),
            page_num: pageNum || state.pageNum,
            page_size: state.pageSize
        };
    }

    function updateActionState() {
        var hasEnabledScheme = state.schemes.length > 0;
        var queryBtn = getQueryButton();
        var exportBtn = getExportButton();
        if (queryBtn) queryBtn.disabled = !hasEnabledScheme || state.busy;
        if (exportBtn) exportBtn.disabled = !hasEnabledScheme || state.busy;
        if (!hasEnabledScheme) {
            setEmptyState("暂无启用的监测方案，请先在“方案管理”中新增或启用方案。");
            renderTable([]);
            state.total = 0;
            renderPagination();
            var status = document.getElementById("customMonitorStatus");
            if (status) status.textContent = "";
            if (state.busy) setBusy(false);
        } else {
            setEmptyState("");
        }
    }

    function formatBusyDetail(stats) {
        var data = stats || {};
        return "已拉取 " + (data.upstream_row_count || 0) +
            " 条；规则扫描 " + (data.rule_scanned_count || 0) +
            " 条，命中 " + (data.rule_match_count || 0) +
            " 条；分局保留 " + (data.branch_filtered_count || 0) + " 条";
    }

    function updateBusyByStatus(status) {
        var message = (status && status.message) || "正在查询，请稍候...";
        var stats = (status && status.stats) || {};
        setBusy(true, message, formatBusyDetail(stats));
    }

    function finishQuery(result, token) {
        if (token !== state.queryToken) return;
        cancelPendingQuery();
        setBusy(false);
        state.pageNum = result.page_num;
        state.pageSize = result.page_size;
        state.total = result.total;
        setError("");
        renderStatus(result);
        renderTable(result.rows || []);
        renderPagination();
    }

    function handleQueryError(error, token) {
        if (token !== state.queryToken) return;
        cancelPendingQuery();
        setBusy(false);
        setError((error && error.message) || "查询失败");
        renderTable([]);
        state.total = 0;
        renderPagination();
    }

    function scheduleJobPoll(jobId, token) {
        clearPollTimer();
        state.pollTimer = window.setTimeout(function() {
            pollQueryJob(jobId, token);
        }, QUERY_POLL_INTERVAL_MS);
    }

    function pollQueryJob(jobId, token) {
        if (token !== state.queryToken) return;
        if (Date.now() - state.queryStartedAt > QUERY_TIMEOUT_MS) {
            handleQueryError(new Error("查询超时，请稍后重试"), token);
            return;
        }
        requestJson("/jingqing_fenxi/api/custom-case-monitor/query-jobs/" + encodeURIComponent(jobId))
            .then(function(response) {
                if (token !== state.queryToken) return;
                var status = response.data || {};
                updateBusyByStatus(status);
                if (status.state === "success") {
                    finishQuery(status.result || {}, token);
                    return;
                }
                if (status.state === "failed") {
                    handleQueryError(new Error(status.message || "查询失败"), token);
                    return;
                }
                scheduleJobPoll(jobId, token);
            })
            .catch(function(error) {
                handleQueryError(error, token);
            });
    }

    function query(pageNum) {
        if (!state.selectedSchemeId) {
            setError("请先选择监测方案");
            return;
        }
        cancelPendingQuery();
        var payload = buildPayload(pageNum);
        var token = state.queryToken + 1;
        state.queryToken = token;
        state.queryStartedAt = Date.now();
        setError("");
        setBusy(true, "正在创建查询任务...", "已拉取 0 条；规则扫描 0 条，命中 0 条；分局保留 0 条");
        requestJson("/jingqing_fenxi/api/custom-case-monitor/query-jobs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        })
            .then(function(result) {
                if (token !== state.queryToken) return;
                state.activeJobId = result.job_id;
                updateBusyByStatus({
                    stage: "fetching",
                    message: "正在拉取警情...",
                    stats: {
                        upstream_row_count: 0,
                        rule_scanned_count: 0,
                        rule_match_count: 0,
                        branch_filtered_count: 0
                    }
                });
                pollQueryJob(result.job_id, token);
            })
            .catch(function(error) {
                handleQueryError(error, token);
            });
    }

    function exportData(format) {
        if (!state.selectedSchemeId) {
            setError("请先选择监测方案");
            return;
        }
        var payload = buildPayload(1);
        var params = new URLSearchParams({
            format: format,
            scheme_id: payload.scheme_id,
            start_time: payload.start_time,
            end_time: payload.end_time,
            branches: payload.branches.join(",")
        });
        window.location.href = "/jingqing_fenxi/download/custom-case-monitor?" + params.toString();
    }

    function getFieldLabel(value) {
        for (var i = 0; i < state.fieldOptions.length; i++) {
            if (state.fieldOptions[i].value === value) return state.fieldOptions[i].label;
        }
        return value || "";
    }

    function getOperatorLabel(value) {
        for (var i = 0; i < state.operatorOptions.length; i++) {
            if (state.operatorOptions[i].value === value) return state.operatorOptions[i].label;
        }
        return value || "";
    }

    function getRuleGroupNo(rule) {
        var value = parseInt(rule && rule.group_no, 10);
        return value > 0 ? value : 1;
    }

    function getLastRuleGroupNo(rules) {
        var items = rules || [];
        if (!items.length) return 1;
        return getRuleGroupNo(items[items.length - 1]);
    }

    function getNextRuleGroupNo(rules) {
        var maxGroupNo = 0;
        (rules || []).forEach(function(rule) {
            maxGroupNo = Math.max(maxGroupNo, getRuleGroupNo(rule));
        });
        return maxGroupNo + 1 || 1;
    }

    function buildRuleSummary(rules) {
        var grouped = {};
        (rules || []).forEach(function(rule) {
            var groupNo = getRuleGroupNo(rule);
            if (!grouped[groupNo]) grouped[groupNo] = [];
            grouped[groupNo].push("G" + groupNo + " " + getFieldLabel(rule.field_name) + " / " + getOperatorLabel(rule.operator));
        });
        return Object.keys(grouped)
            .sort(function(a, b) { return Number(a) - Number(b); })
            .map(function(groupNo) { return grouped[groupNo].join(" + "); })
            .join(" | ");
    }

    function createSelectOptions(options, selectedValue) {
        return options.map(function(option) {
            var selected = option.value === selectedValue ? " selected" : "";
            return "<option value='" + escapeHtml(option.value) + "'" + selected + ">" + escapeHtml(option.label) + "</option>";
        }).join("");
    }

    function ruleValuePlaceholder(operator) {
        if (operator === "regex_any" || operator === "regex_all") {
            return "正则模式请一行一条，不要用逗号分隔；例如：(持|拿).{0,6}(刀|械)";
        }
        return "一行一个值，或用逗号分隔";
    }

    function normalizeRuleValueText(ruleValues) {
        if (Array.isArray(ruleValues)) return ruleValues.join("\n");
        return ruleValues || "";
    }

    function renderSchemeList() {
        var table = document.getElementById("customMonitorSchemeTable");
        if (!table) return;
        var html = "<thead><tr><th>方案名称</th><th>启用</th><th>更新时间</th><th>规则概览</th><th>操作</th></tr></thead><tbody>";
        if (!state.allSchemes.length) {
            html += "<tr><td colspan='5' style='text-align:center;color:#64748b;'>暂无方案，请先新增方案</td></tr>";
        } else {
            state.allSchemes.forEach(function(scheme) {
                html += "<tr>" +
                    "<td>" + escapeHtml(scheme.scheme_name || "") + "</td>" +
                    "<td>" + (scheme.is_enabled ? "是" : "否") + "</td>" +
                    "<td>" + escapeHtml(scheme.updated_at || "") + "</td>" +
                    "<td>" + escapeHtml(buildRuleSummary(scheme.rules || []) || "无规则") + "</td>" +
                    "<td class='custom-monitor-op-cell'>" +
                    "<button type='button' class='module-button custom-monitor-mini-btn' data-action='edit' data-id='" + scheme.id + "'>编辑</button>" +
                    "<button type='button' class='module-button custom-monitor-mini-btn custom-monitor-secondary-btn' data-action='toggle' data-id='" + scheme.id + "'>" + (scheme.is_enabled ? "禁用" : "启用") + "</button>" +
                    "<button type='button' class='module-button custom-monitor-mini-btn custom-monitor-danger-btn' data-action='delete' data-id='" + scheme.id + "'>删除</button>" +
                    "</td>" +
                    "</tr>";
            });
        }
        html += "</tbody>";
        table.innerHTML = html;
        Array.prototype.forEach.call(table.querySelectorAll("[data-action='edit']"), function(button) {
            button.addEventListener("click", function() {
                loadSchemeIntoForm(findSchemeById(button.getAttribute("data-id")));
            });
        });
        Array.prototype.forEach.call(table.querySelectorAll("[data-action='toggle']"), function(button) {
            button.addEventListener("click", function() {
                toggleScheme(button.getAttribute("data-id"));
            });
        });
        Array.prototype.forEach.call(table.querySelectorAll("[data-action='delete']"), function(button) {
            button.addEventListener("click", function() {
                deleteScheme(button.getAttribute("data-id"));
            });
        });
    }

    function renderRuleList(rules) {
        var container = document.getElementById("customMonitorRuleList");
        if (!container) return;
        var items = (rules || []).map(function(rule) {
            return {
                field_name: rule.field_name || (state.fieldOptions[0] && state.fieldOptions[0].value) || "combined_text",
                operator: rule.operator || (state.operatorOptions[0] && state.operatorOptions[0].value) || "contains_any",
                rule_values: rule.rule_values || [],
                group_no: getRuleGroupNo(rule),
                is_enabled: rule.is_enabled !== false
            };
        });
        if (!items.length) {
            container.innerHTML = "<div class='custom-monitor-rule-empty'>暂无规则，请先新增一条规则。</div>";
            return;
        }

        var html = "<div class='custom-monitor-rule-empty' style='text-align:left;'>同组规则需同时命中（AND），不同组命中任一组即可（OR）。</div>";
        items.forEach(function(rule, index) {
            html += "<div class='custom-monitor-rule-row' data-rule-index='" + index + "'>" +
                "<div class='custom-monitor-rule-header'>" +
                "<div class='custom-monitor-rule-title'>规则 " + (index + 1) + "</div>" +
                "<div class='custom-monitor-rule-tools'>" +
                "<label class='custom-monitor-toggle'><input type='checkbox' data-role='enabled' " + (rule.is_enabled ? "checked" : "") + "><span>启用</span></label>" +
                "<button type='button' class='module-button custom-monitor-mini-btn custom-monitor-danger-btn' data-role='remove'>删除规则</button>" +
                "</div>" +
                "</div>" +
                "<div class='custom-monitor-rule-meta'>" +
                "<label class='custom-monitor-rule-field'>规则组<input data-role='groupNo' type='number' min='1' value='" + escapeHtml(String(getRuleGroupNo(rule))) + "'></label>" +
                "<label class='custom-monitor-rule-field'>字段<select data-role='field'>" + createSelectOptions(state.fieldOptions, rule.field_name) + "</select></label>" +
                "<label class='custom-monitor-rule-field'>操作符<select data-role='operator'>" + createSelectOptions(state.operatorOptions, rule.operator) + "</select></label>" +
                "</div>" +
                "<label class='custom-monitor-rule-values'>值列表<textarea data-role='values' rows='5' placeholder='" + escapeHtml(ruleValuePlaceholder(rule.operator)) + "'>" + escapeHtml(normalizeRuleValueText(rule.rule_values)) + "</textarea></label>" +
                "</div>";
        });
        container.innerHTML = html;
        Array.prototype.forEach.call(container.querySelectorAll("[data-role='remove']"), function(button) {
            button.addEventListener("click", function() {
                var row = button.closest(".custom-monitor-rule-row");
                if (!row || !row.parentNode) return;
                row.parentNode.removeChild(row);
                if (!container.querySelector(".custom-monitor-rule-row")) renderRuleList([]);
            });
        });
        Array.prototype.forEach.call(container.querySelectorAll("[data-role='operator']"), function(select) {
            select.addEventListener("change", function() {
                var row = select.closest(".custom-monitor-rule-row");
                if (!row) return;
                var textarea = row.querySelector("[data-role='values']");
                if (!textarea) return;
                textarea.setAttribute("placeholder", ruleValuePlaceholder(select.value));
            });
        });
    }

    function blankRule(groupNo) {
        return {
            field_name: state.fieldOptions.length ? state.fieldOptions[0].value : "combined_text",
            operator: state.operatorOptions.length ? state.operatorOptions[0].value : "contains_any",
            rule_values: [],
            group_no: groupNo > 0 ? groupNo : 1,
            is_enabled: true
        };
    }

    function resetForm() {
        document.getElementById("customMonitorEditingId").value = "";
        document.getElementById("customMonitorSchemeNameInput").value = "";
        document.getElementById("customMonitorSchemeDescInput").value = "";
        document.getElementById("customMonitorSchemeEnabled").checked = true;
        setFormError("");
        renderRuleList([blankRule(1)]);
    }

    function loadSchemeIntoForm(scheme) {
        if (!scheme) {
            resetForm();
            return;
        }
        document.getElementById("customMonitorEditingId").value = scheme.id;
        document.getElementById("customMonitorSchemeNameInput").value = scheme.scheme_name || "";
        document.getElementById("customMonitorSchemeDescInput").value = scheme.description || "";
        document.getElementById("customMonitorSchemeEnabled").checked = !!scheme.is_enabled;
        setFormError("");
        renderRuleList((scheme.rules || []).length ? scheme.rules : [blankRule(1)]);
    }

    function collectRulesFromForm() {
        var rows = document.querySelectorAll("#customMonitorRuleList .custom-monitor-rule-row");
        var rules = [];
        Array.prototype.forEach.call(rows, function(row, index) {
            var field = row.querySelector("[data-role='field']");
            var operator = row.querySelector("[data-role='operator']");
            var values = row.querySelector("[data-role='values']");
            var groupNo = row.querySelector("[data-role='groupNo']");
            var enabled = row.querySelector("[data-role='enabled']");
            rules.push({
                field_name: field ? field.value : "",
                operator: operator ? operator.value : "",
                rule_values: values ? values.value : "",
                group_no: groupNo ? groupNo.value : 1,
                is_enabled: enabled ? enabled.checked : true,
                sort_order: index + 1
            });
        });
        return rules;
    }

    function collectFormPayload() {
        return {
            scheme_name: document.getElementById("customMonitorSchemeNameInput").value,
            description: document.getElementById("customMonitorSchemeDescInput").value,
            is_enabled: document.getElementById("customMonitorSchemeEnabled").checked,
            rules: collectRulesFromForm()
        };
    }

    function openModal() {
        var drawer = document.getElementById("customMonitorSchemeDrawer");
        if (!drawer) return;
        drawer.classList.add("open");
        document.body.style.overflow = "hidden";
        setFormError("");
        if (!document.getElementById("customMonitorEditingId").value) {
            loadSchemeIntoForm(getSelectedScheme() || null);
        }
    }

    function closeModal() {
        var drawer = document.getElementById("customMonitorSchemeDrawer");
        if (!drawer) return;
        drawer.classList.remove("open");
        document.body.style.overflow = "";
    }

    function refreshSchemes(options) {
        var preserveSelected = options && options.preserveSelected;
        return requestJson("/jingqing_fenxi/api/custom-case-monitor/schemes")
            .then(function(result) {
                var previousSelected = preserveSelected ? state.selectedSchemeId : "";
                state.allSchemes = result.schemes || [];
                state.schemes = state.allSchemes.filter(function(item) { return !!item.is_enabled; });
                if (previousSelected) state.selectedSchemeId = previousSelected;
                renderSchemeOptions(state.schemes);
                renderSchemeList();
                updateActionState();
            });
    }

    function toggleScheme(schemeId) {
        var scheme = findSchemeById(schemeId);
        if (!scheme) return;
        requestJson("/jingqing_fenxi/api/custom-case-monitor/schemes/" + scheme.id, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                scheme_name: scheme.scheme_name,
                description: scheme.description || "",
                is_enabled: !scheme.is_enabled,
                rules: scheme.rules || []
            })
        })
            .then(function() {
                return refreshSchemes({ preserveSelected: true });
            })
            .then(function() {
                if (state.selectedSchemeId) {
                    setError("");
                    query(1);
                }
            })
            .catch(function(error) {
                setFormError(error.message || "切换方案状态失败");
            });
    }

    function deleteScheme(schemeId) {
        var scheme = findSchemeById(schemeId);
        if (!scheme) return;
        if (!window.confirm("确定删除方案“" + scheme.scheme_name + "”吗？")) return;
        requestJson("/jingqing_fenxi/api/custom-case-monitor/schemes/" + scheme.id, {
            method: "DELETE"
        })
            .then(function() {
                resetForm();
                return refreshSchemes({ preserveSelected: true });
            })
            .then(function() {
                setError("");
                if (state.selectedSchemeId) query(1);
            })
            .catch(function(error) {
                setFormError(error.message || "删除方案失败");
            });
    }

    function saveCurrentScheme() {
        var schemeId = document.getElementById("customMonitorEditingId").value;
        var payload = collectFormPayload();
        var url = "/jingqing_fenxi/api/custom-case-monitor/schemes" + (schemeId ? ("/" + schemeId) : "");
        var method = schemeId ? "PUT" : "POST";
        requestJson(url, {
            method: method,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        })
            .then(function(result) {
                state.selectedSchemeId = String((result.scheme && result.scheme.id) || state.selectedSchemeId || "");
                return refreshSchemes({ preserveSelected: true });
            })
            .then(function() {
                loadSchemeIntoForm(getSelectedScheme() || null);
                closeModal();
                if (state.selectedSchemeId) {
                    setError("");
                    query(1);
                }
            })
            .catch(function(error) {
                setFormError(error.message || "保存方案失败");
            });
    }

    function bindEvents() {
        var display = document.getElementById("customMonitorBranchDisplay");
        var dropdown = document.getElementById("customMonitorBranchDropdown");
        var menu = document.querySelector("#customMonitorDownloadMenu .download-menu-content");
        if (display) {
            display.addEventListener("click", function(event) {
                event.stopPropagation();
                if (dropdown) dropdown.classList.toggle("show");
            });
        }
        document.addEventListener("click", function() {
            if (dropdown) dropdown.classList.remove("show");
            if (menu) menu.classList.remove("show");
        });
        if (dropdown) dropdown.addEventListener("click", function(event) {
            event.stopPropagation();
        });
        getQueryButton().addEventListener("click", function() {
            query(1);
        });
        getExportButton().addEventListener("click", function(event) {
            event.stopPropagation();
            if (menu) menu.classList.toggle("show");
        });
        Array.prototype.forEach.call(document.querySelectorAll("#customMonitorDownloadMenu [data-action='export']"), function(button) {
            button.addEventListener("click", function(event) {
                event.stopPropagation();
                if (menu) menu.classList.remove("show");
                exportData(button.getAttribute("data-format"));
            });
        });
        document.getElementById("customMonitorScheme").addEventListener("change", function(event) {
            state.selectedSchemeId = event.target.value;
            query(1);
        });
        document.getElementById("customMonitorManageBtn").addEventListener("click", function() {
            if (!state.allSchemes.length) resetForm();
            openModal();
        });
        document.getElementById("customMonitorNewSchemeBtn").addEventListener("click", function() {
            resetForm();
        });
        document.getElementById("customMonitorAddRuleBtn").addEventListener("click", function() {
            var rules = collectRulesFromForm();
            if (!rules.length) rules = [blankRule(1)];
            rules.push(blankRule(getLastRuleGroupNo(rules)));
            renderRuleList(rules);
        });
        document.getElementById("customMonitorAddGroupBtn").addEventListener("click", function() {
            var rules = collectRulesFromForm();
            if (!rules.length) rules = [blankRule(1)];
            rules.push(blankRule(getNextRuleGroupNo(rules)));
            renderRuleList(rules);
        });
        document.getElementById("customMonitorSaveSchemeBtn").addEventListener("click", saveCurrentScheme);
        document.getElementById("customMonitorCancelBtn").addEventListener("click", closeModal);
        document.getElementById("customMonitorCloseModal").addEventListener("click", closeModal);
        document.getElementById("customMonitorModalBackdrop").addEventListener("click", closeModal);
        document.addEventListener("keydown", function(event) {
            var drawer = document.getElementById("customMonitorSchemeDrawer");
            if (event.key === "Escape" && drawer && drawer.classList.contains("open")) closeModal();
        });
    }

    function initDefaults() {
        requestJson("/jingqing_fenxi/api/custom-case-monitor/defaults")
            .then(function(result) {
                state.fieldOptions = result.field_options || [];
                state.operatorOptions = result.operator_options || [];
                state.schemes = result.schemes || [];
                state.selectedSchemeId = result.selected_scheme_id ? String(result.selected_scheme_id) : "";
                document.getElementById("customMonitorStartTime").value = formatDateTimeLocal(result.start_time);
                document.getElementById("customMonitorEndTime").value = formatDateTimeLocal(result.end_time);
                renderBranchOptions(result.branches || []);
                return refreshSchemes({ preserveSelected: true });
            })
            .then(function() {
                if (state.schemes.length) {
                    loadSchemeIntoForm(getSelectedScheme() || null);
                    query(1);
                } else {
                    resetForm();
                }
            })
            .catch(function(error) {
                setError(error.message || "初始化失败");
                updateActionState();
            });
    }

    function init() {
        if (state.initialized || !document.getElementById("customMonitorStartTime")) return;
        state.initialized = true;
        bindEvents();
        initDefaults();
    }

    window.CustomCaseMonitorTabPage = { init: init };
})();
