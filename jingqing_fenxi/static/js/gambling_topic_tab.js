(function() {
    var chartsMap = {};
    var TIME_BUCKET_CHOICES = [1, 2, 3, 4, 6, 8, 12];
    var initialized = false;
    var clusterRefreshTimer = null;
    var lastAnalyzeState = {
        data: null,
        base: null,
        dims: []
    };

    function setError(message) {
        var box = document.getElementById("gamblingTopicError");
        if (!box) return;
        if (message) {
            box.textContent = message;
            box.classList.remove("special-case-hidden");
        } else {
            box.textContent = "";
            box.classList.add("special-case-hidden");
        }
    }

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function getSelectedDimensions() {
        var boxes = document.querySelectorAll("#gamblingDimMsDropdown input[type='checkbox']");
        var dims = [];
        for (var i = 0; i < boxes.length; i++) {
            if (boxes[i].value !== "_all" && boxes[i].checked) dims.push(boxes[i].value);
        }
        return dims;
    }

    function initDimensionMultiSelect() {
        var display = document.getElementById("gamblingDimMsDisplay");
        var dropdown = document.getElementById("gamblingDimMsDropdown");
        if (!display || !dropdown) return;

        function syncAllBox() {
            var allBox = dropdown.querySelector("input[value='_all']");
            if (!allBox) return;
            var boxes = dropdown.querySelectorAll("input[type='checkbox']");
            var total = 0;
            var checked = 0;
            for (var i = 0; i < boxes.length; i++) {
                if (boxes[i].value === "_all") continue;
                total++;
                if (boxes[i].checked) checked++;
            }
            allBox.checked = total > 0 && checked === total;
            allBox.indeterminate = checked > 0 && checked < total;
        }

        function renderLabel() {
            var dims = getSelectedDimensions();
            if (dims.length === 0) {
                display.innerHTML = "请选择分析维度";
                return;
            }
            var boxes = dropdown.querySelectorAll("input[type='checkbox']");
            var total = 0;
            for (var i = 0; i < boxes.length; i++) {
                if (boxes[i].value !== "_all") total++;
            }
            display.innerHTML = dims.length === total ? "全部" : ("已选" + dims.length + "项");
        }

        display.onclick = function(event) {
            event = event || window.event;
            if (event.stopPropagation) event.stopPropagation(); else event.cancelBubble = true;
            dropdown.classList.toggle("open");
        };

        dropdown.onclick = function(event) {
            event = event || window.event;
            if (event.stopPropagation) event.stopPropagation(); else event.cancelBubble = true;
        };

        dropdown.onchange = function(event) {
            event = event || window.event;
            var target = event.target || event.srcElement;
            if (!target || target.tagName !== "INPUT") return;
            if (target.value === "_all") {
                var boxes = dropdown.querySelectorAll("input[type='checkbox']");
                for (var i = 0; i < boxes.length; i++) {
                    if (boxes[i].value !== "_all") boxes[i].checked = target.checked;
                }
            } else {
                syncAllBox();
            }
            renderLabel();
            updateDimensionOptionVisibility();
        };

        document.addEventListener("click", function() {
            dropdown.classList.remove("open");
        });

        syncAllBox();
        renderLabel();
    }

    function initDates() {
        function format(date) {
            var d = new Date(date);
            var yyyy = d.getFullYear();
            var mm = String(d.getMonth() + 1).padStart(2, "0");
            var dd = String(d.getDate()).padStart(2, "0");
            var hh = String(d.getHours()).padStart(2, "0");
            var min = String(d.getMinutes()).padStart(2, "0");
            var ss = String(d.getSeconds()).padStart(2, "0");
            return yyyy + "-" + mm + "-" + dd + "T" + hh + ":" + min + ":" + ss;
        }

        function parse(value) {
            if (!value) return null;
            var normalized = String(value).trim().replace(" ", "T");
            var match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/);
            if (!match) return null;
            return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]), Number(match[4]), Number(match[5]), Number(match[6] || 0), 0);
        }

        function syncCompareRangeByMainRange() {
            var beginInput = document.getElementById("gamblingBeginDate");
            var endInput = document.getElementById("gamblingEndDate");
            var m2mStartInput = document.getElementById("gamblingM2mStartTime");
            var m2mEndInput = document.getElementById("gamblingM2mEndTime");
            if (!beginInput || !endInput || !m2mStartInput || !m2mEndInput) return;

            var begin = parse(beginInput.value);
            var end = parse(endInput.value);
            if (!begin || !end) return;

            var durationMs = end.getTime() - begin.getTime();
            var m2mEnd = new Date(begin.getTime());
            var m2mStart = new Date(m2mEnd.getTime() - durationMs);

            m2mEndInput.value = format(m2mEnd);
            m2mStartInput.value = format(m2mStart);
        }

        var now = new Date();
        var todayBegin = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
        var start = new Date(todayBegin.getTime() - 7 * 24 * 3600 * 1000);
        document.getElementById("gamblingBeginDate").value = format(start);
        document.getElementById("gamblingEndDate").value = format(todayBegin);
        syncCompareRangeByMainRange();

        document.getElementById("gamblingBeginDate").addEventListener("change", syncCompareRangeByMainRange);
        document.getElementById("gamblingEndDate").addEventListener("change", syncCompareRangeByMainRange);
    }

    function updateTimeBucketLabel() {
        var idxInput = document.getElementById("gamblingOptTimeBucketIndex");
        var label = document.getElementById("gamblingOptTimeBucketLabel");
        if (!idxInput || !label) return;
        var idx = Number(idxInput.value);
        if (Number.isNaN(idx) || idx < 0 || idx >= TIME_BUCKET_CHOICES.length) idx = 2;
        label.innerHTML = TIME_BUCKET_CHOICES[idx] + "小时";
    }

    function updateDeptTopNLabel() {
        var allCheckbox = document.getElementById("gamblingOptDeptAll");
        var range = document.getElementById("gamblingOptDeptTopN");
        var label = document.getElementById("gamblingOptDeptTopNLabel");
        if (!allCheckbox || !range || !label) return;
        range.disabled = allCheckbox.checked;
        label.innerHTML = allCheckbox.checked ? "全部" : ("Top " + range.value);
    }

    function syncDeptTopNRangeFromData(deptAllData) {
        var range = document.getElementById("gamblingOptDeptTopN");
        if (!range) return;
        var total = Array.isArray(deptAllData) ? deptAllData.length : 0;
        var max = total > 0 ? total : 1;
        var current = Number(range.value);
        range.max = String(max);
        if (Number.isNaN(current) || current < 1) current = 1;
        if (current > max) current = max;
        range.value = String(current);
    }

    function updatePhoneMinCountLabel() {
        var range = document.getElementById("gamblingOptPhoneMinCount");
        var label = document.getElementById("gamblingOptPhoneMinCountLabel");
        if (!range || !label) return;
        label.innerHTML = ">=" + range.value + "次";
    }

    function updateClusterRadiusLabel() {
        var range = document.getElementById("gamblingOptClusterRadius");
        var label = document.getElementById("gamblingOptClusterRadiusLabel");
        if (!range || !label) return;
        label.innerHTML = range.value + "米";
    }

    function getAnalysisOptionValues() {
        var timeBucketIdx = Number(document.getElementById("gamblingOptTimeBucketIndex").value);
        if (Number.isNaN(timeBucketIdx) || timeBucketIdx < 0 || timeBucketIdx >= TIME_BUCKET_CHOICES.length) {
            timeBucketIdx = 2;
        }
        var deptAll = document.getElementById("gamblingOptDeptAll").checked;
        var deptTopN = deptAll ? null : Number(document.getElementById("gamblingOptDeptTopN").value);
        return {
            timeBucketHours: TIME_BUCKET_CHOICES[timeBucketIdx],
            deptTopN: deptTopN,
            repeatPhoneMinCount: Number(document.getElementById("gamblingOptPhoneMinCount").value),
            repeatAddrRadiusMeters: Number(document.getElementById("gamblingOptClusterRadius").value)
        };
    }

    function updateDimensionOptionVisibility() {
        var dims = getSelectedDimensions();
        document.getElementById("gambling-opt-time").style.display = dims.indexOf("time") >= 0 ? "flex" : "none";
        document.getElementById("gambling-opt-dept").style.display = dims.indexOf("dept") >= 0 ? "flex" : "none";
        document.getElementById("gambling-opt-phone").style.display = dims.indexOf("phone") >= 0 ? "flex" : "none";
        document.getElementById("gambling-opt-cluster").style.display = dims.indexOf("cluster") >= 0 ? "flex" : "none";
    }

    function initAnalysisOptions() {
        document.getElementById("gamblingOptTimeBucketIndex").addEventListener("input", function() {
            updateTimeBucketLabel();
            onOptionChangedRealtime("time");
        });
        document.getElementById("gamblingOptDeptAll").addEventListener("change", function() {
            updateDeptTopNLabel();
            onOptionChangedRealtime("dept");
        });
        document.getElementById("gamblingOptDeptTopN").addEventListener("input", function() {
            updateDeptTopNLabel();
            onOptionChangedRealtime("dept");
        });
        document.getElementById("gamblingOptPhoneMinCount").addEventListener("input", function() {
            updatePhoneMinCountLabel();
            onOptionChangedRealtime("phone");
        });
        document.getElementById("gamblingOptClusterRadius").addEventListener("input", function() {
            updateClusterRadiusLabel();
            onOptionChangedRealtime("cluster");
        });
        updateTimeBucketLabel();
        updateDeptTopNLabel();
        updatePhoneMinCountLabel();
        updateClusterRadiusLabel();
        updateDimensionOptionVisibility();
    }

    function buildTimeDataFromHourly(hourly, bucketHours) {
        if (!hourly || hourly.length !== 24) return [];
        var pairs = [];
        for (var start = 0; start < 24; start += bucketHours) {
            var end = start + bucketHours;
            var count = 0;
            for (var h = start; h < end; h++) count += (hourly[h] || 0);
            pairs.push([start + "-" + end + "时", count]);
        }
        pairs.sort(function(a, b) { return b[1] - a[1]; });
        return pairs;
    }

    function buildDeptDataFromAll(allData, topN) {
        if (!allData) return [];
        if (!topN) return allData;
        return allData.slice(0, topN);
    }

    function buildPhoneDataFromAll(allData, minCount) {
        if (!allData) return [];
        return allData.filter(function(item) { return item[1] >= minCount; });
    }

    function renderSrrTable(rows) {
        var box = document.getElementById("gambling-box-srr");
        var container = document.getElementById("gambling-table-srr");
        box.classList.add("active");
        var html = '<h3 style="margin:0 0 10px 0;">各地同比环比</h3>';
        html += '<table class="special-case-table"><thead><tr>';
        html += "<th>单位名称</th><th>本期数</th><th>同比上期</th><th>同比比例</th><th>环比上期</th><th>环比比例</th>";
        html += "</tr></thead><tbody>";
        (rows || []).forEach(function(row) {
            html += "<tr><td>" + escapeHtml(row.name || "") + "</td>";
            html += "<td>" + escapeHtml(row.presentCycle == null ? "" : row.presentCycle) + "</td>";
            html += "<td>" + escapeHtml(row.upperY2yCycle == null ? "" : row.upperY2yCycle) + "</td>";
            html += "<td>" + escapeHtml(row.y2yProportion || "") + "</td>";
            html += "<td>" + escapeHtml(row.upperM2mCycle == null ? "" : row.upperM2mCycle) + "</td>";
            html += "<td>" + escapeHtml(row.m2mProportion || "") + "</td></tr>";
        });
        html += "</tbody></table>";
        container.innerHTML = html;
    }

    function renderSrrState(message, isError) {
        var box = document.getElementById("gambling-box-srr");
        var container = document.getElementById("gambling-table-srr");
        box.classList.add("active");
        container.innerHTML =
            '<h3 style="margin:0 0 10px 0;">各地同比环比</h3>' +
            '<div style="padding:14px 16px;border:1px ' + (isError ? "solid #f5c2c7;background:#fff5f5;color:#842029;" : "dashed #d0d7de;background:#fafcff;color:#64748b;") + 'border-radius:6px;">' +
            escapeHtml(message) +
            "</div>";
    }

    function renderGamblingWayTable(result) {
        var box = document.getElementById("gambling-box-way");
        var container = document.getElementById("gambling-table-way");
        var columns = (result && result.columns) || [];
        var rows = (result && result.rows) || [];
        box.classList.add("active");
        if (!rows.length) {
            container.innerHTML = '<div class="monitor-empty-state">无符合条件数据</div>';
            return;
        }
        var html = '<div class="results-table-container"><table class="special-case-table"><thead><tr><th>地区</th>';
        columns.forEach(function(column) { html += "<th>" + escapeHtml(column) + "</th>"; });
        html += "<th>合计</th></tr></thead><tbody>";
        rows.forEach(function(row) {
            var counts = row.counts || {};
            html += "<tr><td>" + escapeHtml(row.cmdName || "") + "</td>";
            columns.forEach(function(column) { html += "<td>" + escapeHtml(counts[column] || 0) + "</td>"; });
            html += "<td>" + escapeHtml(row.total || 0) + "</td></tr>";
        });
        html += "</tbody></table></div>";
        container.innerHTML = html;
    }

    function renderWildernessTable(result) {
        var box = document.getElementById("gambling-box-wilderness");
        var container = document.getElementById("gambling-table-wilderness");
        var rows = (result && result.rows) || [];
        box.classList.add("active");
        if (!rows.length) {
            container.innerHTML = '<div class="monitor-empty-state">无符合条件数据</div>';
            return;
        }
        var html = '<div class="results-table-container"><table class="special-case-table"><thead><tr><th>地区</th><th>数量</th></tr></thead><tbody>';
        rows.forEach(function(row) {
            html += "<tr><td>" + escapeHtml(row.cmdName || "") + "</td><td>" + escapeHtml(row.total || 0) + "</td></tr>";
        });
        html += "</tbody></table></div>";
        container.innerHTML = html;
    }

    function toggleChartState(chartId, stateId, message, isError) {
        var canvas = document.getElementById(chartId);
        var state = document.getElementById(stateId);
        if (!canvas || !state) return;
        if (message) {
            canvas.style.display = "none";
            state.textContent = message;
            state.classList.remove("special-case-hidden");
            if (isError) state.classList.add("error"); else state.classList.remove("error");
            return;
        }
        canvas.style.display = "block";
        state.textContent = "";
        state.classList.add("special-case-hidden");
        state.classList.remove("error");
    }

    function destroyChart(chartId) {
        var canvas = document.getElementById(chartId);
        if (!canvas) return;
        var existing = Chart.getChart ? Chart.getChart(canvas) : chartsMap[chartId];
        if (existing) existing.destroy();
        chartsMap[chartId] = null;
    }

    function renderChart(boxId, chartId, stateId, label, dataPairs, reverse, type) {
        if (typeof reverse === "undefined") reverse = false;
        if (typeof type === "undefined") type = "bar";
        var box = document.getElementById(boxId);
        var canvas = document.getElementById(chartId);
        if (!box || !canvas) return;
        box.classList.add("active");
        destroyChart(chartId);

        var pairs = (dataPairs || []).slice();
        if (reverse) pairs.reverse();
        if (!pairs.length) {
            toggleChartState(chartId, stateId, "无符合条件数据", false);
            return;
        }

        toggleChartState(chartId, stateId, "", false);
        var ctx = canvas.getContext("2d");
        var labels = pairs.map(function(item) { return item[0]; });
        var values = pairs.map(function(item) { return item[1]; });
        var isHorizontal = type === "horizontalBar";
        var chartType = isHorizontal ? "bar" : type;
        var extraOptions = isHorizontal ? { indexAxis: "y" } : {};
        chartsMap[chartId] = new Chart(ctx, {
            type: chartType,
            data: {
                labels: labels,
                datasets: [{
                    label: label,
                    data: values,
                    backgroundColor: "rgba(54, 162, 235, 0.6)",
                    borderColor: "rgba(54, 162, 235, 1)",
                    borderWidth: 1
                }]
            },
            options: Object.assign({
                responsive: true,
                maintainAspectRatio: false,
                scales: { x: { beginAtZero: true }, y: { beginAtZero: true } },
                plugins: { legend: { display: false } }
            }, extraOptions)
        });
    }

    function renderChartError(boxId, chartId, stateId, message) {
        var box = document.getElementById(boxId);
        if (!box) return;
        box.classList.add("active");
        destroyChart(chartId);
        toggleChartState(chartId, stateId, message, true);
    }

    function renderFromCurrentState() {
        if (!lastAnalyzeState.data) return;
        var data = lastAnalyzeState.data || {};
        var base = lastAnalyzeState.base || {};
        var dims = lastAnalyzeState.dims || [];
        var opts = getAnalysisOptionValues();
        document.querySelectorAll("[id^='gambling-box-']").forEach(function(box) {
            box.classList.remove("active");
        });

        if (dims.indexOf("srr") >= 0) {
            if (data.srr_error) renderSrrState("统计失败：" + (data.srr_error.message || "上游接口异常"), true);
            else if (data.srr && data.srr.length > 0) renderSrrTable(data.srr);
            else renderSrrState("无符合条件数据", false);
        }
        if (dims.indexOf("gambling_way") >= 0) renderGamblingWayTable(data.gambling_way || {});
        if (dims.indexOf("wilderness") >= 0) renderWildernessTable(data.wilderness || {});
        if (dims.indexOf("time") >= 0) {
            var timeData = base.timeHourly ? buildTimeDataFromHourly(base.timeHourly, opts.timeBucketHours) : (data.time || []);
            renderChart("gambling-box-time", "gambling-chart-time", "gambling-state-time", "时段报警数（每" + opts.timeBucketHours + "小时）", timeData, false, "bar");
        }
        if (dims.indexOf("dept") >= 0) {
            syncDeptTopNRangeFromData(base.deptAll || data.dept || []);
            updateDeptTopNLabel();
            var deptData = base.deptAll ? buildDeptDataFromAll(base.deptAll, opts.deptTopN) : (data.dept || []);
            renderChart("gambling-box-dept", "gambling-chart-dept", "gambling-state-dept", "派出所报警数", deptData, false, "horizontalBar");
        }
        if (dims.indexOf("phone") >= 0) {
            var phoneData = base.phoneAll ? buildPhoneDataFromAll(base.phoneAll, opts.repeatPhoneMinCount) : (data.phone || []);
            renderChart("gambling-box-phone", "gambling-chart-phone", "gambling-state-phone", "重复报警电话（>=" + opts.repeatPhoneMinCount + "次）", phoneData, false, "bar");
        }
        if (dims.indexOf("cluster") >= 0) {
            renderChart("gambling-box-cluster", "gambling-chart-cluster", "gambling-state-cluster", "重复报警地址（半径" + opts.repeatAddrRadiusMeters + "米）", data.cluster || [], false, "horizontalBar");
        }
        if (dims.indexOf("addr") >= 0) {
            if (data.addr_error) renderChartError("gambling-box-addr", "gambling-chart-addr", "gambling-state-addr", "统计失败：" + (data.addr_error.message || "地址分类模型不可用"));
            else renderChart("gambling-box-addr", "gambling-chart-addr", "gambling-state-addr", "警情地址统计", data.addr || [], false, "horizontalBar");
        }
    }

    function clearRenderedState() {
        document.querySelectorAll("[id^='gambling-box-']").forEach(function(box) {
            box.classList.remove("active");
        });
        ["gambling-chart-time", "gambling-chart-dept", "gambling-chart-phone", "gambling-chart-cluster", "gambling-chart-addr"].forEach(function(chartId) {
            destroyChart(chartId);
        });
    }

    function triggerClusterRefreshIfNeeded() {
        if ((lastAnalyzeState.dims || []).indexOf("cluster") < 0) return;
        if (clusterRefreshTimer) clearTimeout(clusterRefreshTimer);
        clusterRefreshTimer = setTimeout(function() { doAnalyze(); }, 400);
    }

    function onOptionChangedRealtime(optionType) {
        renderFromCurrentState();
        if (optionType === "cluster") triggerClusterRefreshIfNeeded();
    }

    function collectFormData() {
        var form = new FormData();
        ["gamblingBeginDate", "gamblingEndDate", "gamblingM2mStartTime", "gamblingM2mEndTime"].forEach(function(id) {
            var value = document.getElementById(id).value;
            if (value) value = value.replace("T", " ");
            if (id === "gamblingBeginDate") form.append("beginDate", value);
            if (id === "gamblingEndDate") form.append("endDate", value);
            if (id === "gamblingM2mStartTime") form.append("m2mStartTime", value);
            if (id === "gamblingM2mEndTime") form.append("m2mEndTime", value);
        });

        getSelectedDimensions().forEach(function(dim) {
            form.append("dimensions[]", dim);
        });

        var options = getAnalysisOptionValues();
        form.append("timeBucketHours", options.timeBucketHours);
        form.append("deptTopN", options.deptTopN == null ? "all" : String(options.deptTopN));
        form.append("repeatPhoneMinCount", String(options.repeatPhoneMinCount));
        form.append("repeatAddrRadiusMeters", String(options.repeatAddrRadiusMeters));
        return form;
    }

    function doAnalyze() {
        var formData = collectFormData();
        var selectedDims = formData.getAll("dimensions[]");
        if (!selectedDims.length) {
            alert("请至少选择一个分析维度");
            return;
        }

        setError("");
        document.getElementById("gamblingTopicLoading").style.display = "inline-block";
        fetch("/jingqing_fenxi/api/gambling-topic/analyze", {
            method: "POST",
            body: formData
        })
            .then(function(response) { return response.json(); })
            .then(function(result) {
                document.getElementById("gamblingTopicLoading").style.display = "none";
                if (result.code !== 0) throw new Error(result.message || "统计失败");
                lastAnalyzeState.data = result.data || {};
                lastAnalyzeState.base = result.analysisBase || {};
                lastAnalyzeState.dims = selectedDims;
                setError("");
                renderFromCurrentState();
            })
            .catch(function(error) {
                document.getElementById("gamblingTopicLoading").style.display = "none";
                clearRenderedState();
                setError(error.message || "统计失败");
            });
    }

    function doExport() {
        var formData = collectFormData();
        var selectedDims = formData.getAll("dimensions[]");
        if (!selectedDims.length) {
            alert("请至少选择一个分析维度");
            return;
        }

        var params = new URLSearchParams();
        formData.forEach(function(value, key) {
            params.append(key, value);
        });
        window.location.href = "/jingqing_fenxi/download/gambling-topic?" + params.toString();
    }

    function init() {
        if (initialized || !document.getElementById("gamblingTopicAnalyzeBtn")) return;
        initialized = true;
        initDimensionMultiSelect();
        initDates();
        initAnalysisOptions();
        document.getElementById("gamblingTopicAnalyzeBtn").addEventListener("click", doAnalyze);
        document.getElementById("gamblingTopicExportBtn").addEventListener("click", doExport);
    }

    window.GamblingTopicTabPage = { init: init };
})();
