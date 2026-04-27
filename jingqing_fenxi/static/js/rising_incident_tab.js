(function() {
    var initialized = false;
    var typeTreeRaw = [];
    var caseTypeSource = "nature";
    var caseTypeDataLoaded = false;
    var caseTypeRequestSeq = 0;

    function $(id) { return document.getElementById(id); }

    function pad(value) { return String(value).padStart(2, "0"); }

    function formatLocal(date) {
        return date.getFullYear() + "-" + pad(date.getMonth() + 1) + "-" + pad(date.getDate()) +
            "T" + pad(date.getHours()) + ":" + pad(date.getMinutes()) + ":" + pad(date.getSeconds());
    }

    function formatApiDate(value) { return value ? String(value).replace("T", " ") : ""; }

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function startOfBusinessWeek(date) {
        var anchor = new Date(2000, 0, 7, 0, 0, 0, 0);
        var today = new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
        var days = Math.floor((today.getTime() - anchor.getTime()) / 86400000);
        var offset = Math.floor(days / 7) * 7;
        return new Date(anchor.getTime() + offset * 86400000);
    }

    function initDates() {
        var now = new Date();
        var end = startOfBusinessWeek(now);
        var start = new Date(end.getTime() - 8 * 7 * 86400000);
        $("risingIncidentBeginDate").value = formatLocal(start);
        $("risingIncidentEndDate").value = formatLocal(end);
    }

    function setBusy(isBusy) {
        var loading = $("risingIncidentLoading");
        var btn = $("risingIncidentAnalyzeBtn");
        var exportBtn = $("risingIncidentExportBtn");
        if (loading) loading.style.display = isBusy ? "inline-block" : "none";
        if (btn) btn.disabled = isBusy;
        if (exportBtn) exportBtn.disabled = isBusy;
    }

    function setError(message) {
        var box = $("risingIncidentError");
        if (!box) return;
        box.textContent = message || "";
        box.classList.toggle("special-case-hidden", !message);
    }

    function renderCaseTypeSourceSwitch(dropdown) {
        var sourceApi = window.AnalysisCaseTypeSource;
        if (!dropdown || !sourceApi) return;
        var wrap = document.createElement("div");
        wrap.className = "case-type-source-switch";
        ["nature", "plan"].forEach(function(source) {
            var btn = document.createElement("button");
            btn.type = "button";
            btn.className = "case-type-source-btn" + (caseTypeSource === source ? " active" : "");
            btn.setAttribute("data-rising-case-type-source", source);
            btn.appendChild(document.createTextNode(sourceApi.getSourceLabel(source)));
            wrap.appendChild(btn);
        });
        dropdown.appendChild(wrap);
    }

    function renderCaseTypeLabel() {
        var sourceApi = window.AnalysisCaseTypeSource;
        var display = $("risingIncidentCaseTypeDisplay");
        var dropdown = $("risingIncidentCaseTypeDropdown");
        if (!display || !dropdown) return;
        var sourceLabel = sourceApi ? sourceApi.getSourceLabel(caseTypeSource) : "警情类型";
        var boxes = dropdown.querySelectorAll('input[type="checkbox"]');
        var total = 0;
        var checked = 0;
        for (var i = 0; i < boxes.length; i++) {
            if (boxes[i].value === "_all") continue;
            total++;
            if (boxes[i].checked) checked++;
        }
        var allBox = dropdown.querySelector('input[value="_all"]');
        if (allBox) {
            allBox.checked = total > 0 && checked === total;
            allBox.indeterminate = checked > 0 && checked < total;
        }
        if (total === 0) {
            display.innerHTML = sourceLabel + "：" + (caseTypeDataLoaded ? "无可选项" : "加载中...");
        } else if (checked === 0) {
            display.innerHTML = sourceLabel + "：未选择";
        } else if (checked === total) {
            display.innerHTML = sourceLabel + "：全部";
        } else {
            display.innerHTML = sourceLabel + "：已选" + checked + "项";
        }
    }

    function initCaseTypeMultiSelect() {
        var display = $("risingIncidentCaseTypeDisplay");
        var dropdown = $("risingIncidentCaseTypeDropdown");
        if (!display || !dropdown) return;

        display.onclick = function(e) {
            e = e || window.event;
            if (e.stopPropagation) e.stopPropagation(); else e.cancelBubble = true;
            if (dropdown.className.indexOf("open") >= 0) {
                dropdown.className = dropdown.className.replace(/\s*open/g, "");
            } else {
                dropdown.className += " open";
            }
        };

        dropdown.onclick = function(e) {
            e = e || window.event;
            if (e.stopPropagation) e.stopPropagation(); else e.cancelBubble = true;
            var target = e.target || e.srcElement;
            if (target && target.getAttribute && target.getAttribute("data-rising-case-type-source")) {
                loadCaseTypes(target.getAttribute("data-rising-case-type-source"));
            }
        };

        dropdown.onchange = function(e) {
            e = e || window.event;
            var target = e.target || e.srcElement;
            if (!target || target.tagName !== "INPUT") return;

            if (target.value === "_all") {
                var boxes = dropdown.querySelectorAll('input[type="checkbox"]');
                for (var i = 0; i < boxes.length; i++) {
                    if (boxes[i].value !== "_all") boxes[i].checked = target.checked;
                }
            }
            renderCaseTypeLabel();
        };

        document.addEventListener("click", function() {
            dropdown.className = dropdown.className.replace(/\s*open/g, "");
        });
    }

    function renderCaseTypeOptions() {
        var sourceApi = window.AnalysisCaseTypeSource;
        var dropdown = $("risingIncidentCaseTypeDropdown");
        if (!dropdown || !sourceApi) return;
        dropdown.innerHTML = "";
        renderCaseTypeSourceSwitch(dropdown);

        var allLabel = document.createElement("label");
        allLabel.className = "multi-select-option";
        allLabel.style.cssText = "border-bottom:1px solid #eee;font-weight:bold;";
        var allCb = document.createElement("input");
        allCb.type = "checkbox";
        allCb.value = "_all";
        allLabel.appendChild(allCb);
        allLabel.appendChild(document.createTextNode(" 全选"));
        dropdown.appendChild(allLabel);

        var parents = sourceApi.getVisibleNodes(typeTreeRaw);
        parents.forEach(function(parent) {
            var label = document.createElement("label");
            label.className = "multi-select-option";
            var cb = document.createElement("input");
            cb.type = "checkbox";
            cb.value = parent.id;
            label.appendChild(cb);
            label.appendChild(document.createTextNode(" " + parent.name));
            dropdown.appendChild(label);
        });

        renderCaseTypeLabel();
    }

    function renderCaseTypeLoading() {
        var sourceApi = window.AnalysisCaseTypeSource;
        var dropdown = $("risingIncidentCaseTypeDropdown");
        var display = $("risingIncidentCaseTypeDisplay");
        if (!dropdown || !sourceApi) return;
        dropdown.innerHTML = "";
        renderCaseTypeSourceSwitch(dropdown);
        dropdown.appendChild(document.createTextNode("加载中..."));
        if (display) display.innerHTML = sourceApi.getSourceLabel(caseTypeSource) + "：加载中...";
    }

    function loadCaseTypes(source) {
        var sourceApi = window.AnalysisCaseTypeSource;
        if (!sourceApi) {
            var display = $("risingIncidentCaseTypeDisplay");
            if (display) display.innerHTML = "警情类型：加载失败";
            return;
        }
        caseTypeSource = sourceApi.normalizeSource(source || caseTypeSource);
        caseTypeDataLoaded = false;
        typeTreeRaw = [];
        var requestSeq = ++caseTypeRequestSeq;
        renderCaseTypeLoading();

        fetch(sourceApi.getSourceEndpoint(caseTypeSource))
            .then(function(response) { return response.json(); })
            .then(function(data) {
                if (requestSeq !== caseTypeRequestSeq) return;
                typeTreeRaw = data || [];
                caseTypeDataLoaded = true;
                renderCaseTypeOptions();
            })
            .catch(function(error) {
                if (requestSeq !== caseTypeRequestSeq) return;
                console.error(error);
                caseTypeDataLoaded = true;
                var dropdown = $("risingIncidentCaseTypeDropdown");
                if (dropdown) {
                    dropdown.innerHTML = "";
                    renderCaseTypeSourceSwitch(dropdown);
                    var message = document.createElement("div");
                    message.style.padding = "5px";
                    message.appendChild(document.createTextNode("加载失败"));
                    dropdown.appendChild(message);
                }
                renderCaseTypeLabel();
            });
    }

    function collectCaseTypePayload() {
        var sourceApi = window.AnalysisCaseTypeSource;
        if (!sourceApi) {
            return { ids: [], charaNo: "", chara: "", source: caseTypeSource };
        }
        var boxes = document.querySelectorAll('#risingIncidentCaseTypeDropdown input[type="checkbox"]');
        var selectedParentIds = [];
        for (var i = 0; i < boxes.length; i++) {
            if (boxes[i].value === "_all" || !boxes[i].checked) continue;
            selectedParentIds.push(boxes[i].value);
        }
        var payload = sourceApi.collectSelectionPayload(typeTreeRaw, selectedParentIds, caseTypeSource);
        return {
            ids: selectedParentIds,
            charaNo: (payload.codes || []).join(","),
            chara: (payload.names || []).join(","),
            source: caseTypeSource
        };
    }

    function collectParams() {
        var caseType = collectCaseTypePayload();
        return {
            beginDate: formatApiDate($("risingIncidentBeginDate").value),
            endDate: formatApiDate($("risingIncidentEndDate").value),
            caseTypeSource: caseType.source,
            caseTypeIds: caseType.ids,
            newOriCharaSubclassNo: caseType.charaNo,
            newOriCharaSubclass: caseType.chara,
            periodType: $("risingIncidentPeriodType").value || "business_week",
            minPeriods: $("risingIncidentMinPeriods").value || "3",
            currentOnly: $("risingIncidentCurrentOnly").checked ? "1" : "0"
        };
    }

    function toFormData(params) {
        var form = new FormData();
        Object.keys(params).forEach(function(key) {
            var value = params[key];
            if (Array.isArray(value)) {
                value.forEach(function(item) { form.append(key + "[]", item); });
            } else {
                form.append(key, value);
            }
        });
        return form;
    }

    function appendSearchParam(search, key, value) {
        if (Array.isArray(value)) value.forEach(function(item) { search.append(key + "[]", item); });
        else search.append(key, value);
    }

    function updateStatus(meta, rows, periods) {
        var status = $("risingIncidentStatus");
        if (!status) return;
        if (!meta) {
            status.textContent = "";
            return;
        }
        status.textContent = [
            "统计范围：" + (meta.beginDate || "") + " 至 " + (meta.endDate || ""),
            "类型：" + (meta.chara || "全部"),
            "周期：" + (meta.periodTypeLabel || ""),
            "周期数：" + ((periods || []).length),
            "命中派出所：" + ((rows || []).length)
        ].join("；");
    }

    function renderEmpty(message) {
        if (window.RisingIncidentGrid) {
            window.RisingIncidentGrid.renderEmpty(message);
            return;
        }
        var table = $("risingIncidentTable");
        if (table) table.innerHTML = '<tbody><tr><td class="muted" style="padding:18px;">' + escapeHtml(message) + "</td></tr></tbody>";
    }

    function renderTable(rows) {
        if (window.RisingIncidentGrid) window.RisingIncidentGrid.setData(rows || []);
    }

    function handleResult(result) {
        if (!result || result.code !== 0) {
            throw new Error((result && result.message) || "统计失败");
        }
        var payload = result.data || {};
        renderTable(payload.rows || []);
        updateStatus(payload.meta || {}, payload.rows || [], payload.periods || []);
        setError("");
    }

    function analyze() {
        var params = collectParams();
        if (!params.newOriCharaSubclassNo) {
            setError("请选择警情类型");
            alert("请选择警情类型");
            return;
        }
        setBusy(true);
        setError("");
        fetch("/jingqing_fenxi/api/rising-incident/analyze", {
            method: "POST",
            body: toFormData(params)
        })
            .then(function(response) { return response.json(); })
            .then(handleResult)
            .catch(function(error) {
                updateStatus(null, [], []);
                renderEmpty("无数据");
                setError(error.message || "统计失败");
            })
            .finally(function() {
                setBusy(false);
            });
    }

    function doExport() {
        var params = collectParams();
        if (!params.newOriCharaSubclassNo) {
            setError("请选择警情类型");
            alert("请选择警情类型");
            return;
        }
        var search = new URLSearchParams();
        Object.keys(params).forEach(function(key) {
            appendSearchParam(search, key, params[key]);
        });
        window.location.href = "/jingqing_fenxi/download/rising-incident?" + search.toString();
    }

    function init() {
        if (initialized || !$("risingIncidentAnalyzeBtn")) return;
        initialized = true;
        initDates();
        if (window.RisingIncidentGrid) window.RisingIncidentGrid.init();
        initCaseTypeMultiSelect();
        loadCaseTypes("nature");
        renderEmpty("请设置条件后点击统计");
        $("risingIncidentAnalyzeBtn").addEventListener("click", analyze);
        $("risingIncidentExportBtn").addEventListener("click", doExport);
    }

    window.RisingIncidentTabPage = { init: init };
})();
