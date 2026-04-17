(function() {
    var treeDataRaw = [];
    var chartsMap = {};
    var TIME_BUCKET_CHOICES = [1, 2, 3, 4, 6, 8, 12];
    var lastAnalyzeState = {
        data: null,
        base: null,
        dims: []
    };
    var clusterRefreshTimer = null;

    function getSelectedDimensions() {
        var boxes = document.querySelectorAll('#dimMsDropdown input[type="checkbox"]');
        var dims = [];
        for (var i = 0; i < boxes.length; i++) {
            if (boxes[i].value !== '_all' && boxes[i].checked) dims.push(boxes[i].value);
        }
        return dims;
    }

    function initCaseTypeMultiSelect() {
        var display = document.getElementById('caseTypeMsDisplay');
        var dropdown = document.getElementById('caseTypeMsDropdown');
        if (!display || !dropdown) return;

        function syncAllBox() {
            var allBox = dropdown.querySelector('input[value="_all"]');
            if (!allBox) return;
            var boxes = dropdown.querySelectorAll('input[type="checkbox"]');
            var total = 0;
            var checked = 0;
            for (var i = 0; i < boxes.length; i++) {
                if (boxes[i].value === '_all') continue;
                total++;
                if (boxes[i].checked) checked++;
            }
            allBox.checked = total > 0 && checked === total;
            allBox.indeterminate = checked > 0 && checked < total;
        }

        function renderLabel() {
            var boxes = dropdown.querySelectorAll('input[type="checkbox"]');
            var total = 0;
            var checked = 0;
            for (var i = 0; i < boxes.length; i++) {
                if (boxes[i].value === '_all') continue;
                total++;
                if (boxes[i].checked) checked++;
            }
            if (total === 0) {
                display.innerHTML = '加载中...';
                return;
            }
            if (checked === 0) {
                display.innerHTML = '未选择(默认全量)';
                return;
            }
            if (checked === total) {
                display.innerHTML = '全部';
                return;
            }
            display.innerHTML = '已选 ' + checked + ' 项';
        }

        display.onclick = function(e) {
            e = e || window.event;
            if (e.stopPropagation) e.stopPropagation(); else e.cancelBubble = true;
            if (dropdown.className.indexOf('open') >= 0) {
                dropdown.className = dropdown.className.replace(/\s*open/g, '');
            } else {
                dropdown.className += ' open';
            }
        };

        dropdown.onclick = function(e) {
            e = e || window.event;
            if (e.stopPropagation) e.stopPropagation(); else e.cancelBubble = true;
        };

        dropdown.onchange = function(e) {
            e = e || window.event;
            var t = e.target || e.srcElement;
            if (!t || t.tagName !== 'INPUT') return;

            if (t.value === '_all') {
                var boxes = dropdown.querySelectorAll('input[type="checkbox"]');
                for (var i = 0; i < boxes.length; i++) {
                    if (boxes[i].value !== '_all') boxes[i].checked = t.checked;
                }
            } else {
                syncAllBox();
            }
            renderLabel();
        };

        document.addEventListener('click', function() {
            dropdown.className = dropdown.className.replace(/\s*open/g, '');
        });

        window._caseTypeSyncLabel = renderLabel;
    }

    function initDimensionMultiSelect() {
        var display = document.getElementById('dimMsDisplay');
        var dropdown = document.getElementById('dimMsDropdown');
        if (!display || !dropdown) return;

        function syncAllBox() {
            var allBox = dropdown.querySelector('input[value="_all"]');
            if (!allBox) return;
            var boxes = dropdown.querySelectorAll('input[type="checkbox"]');
            var total = 0;
            var checked = 0;
            for (var i = 0; i < boxes.length; i++) {
                if (boxes[i].value === '_all') continue;
                total++;
                if (boxes[i].checked) checked++;
            }
            allBox.checked = total > 0 && checked === total;
            allBox.indeterminate = checked > 0 && checked < total;
        }

        function renderLabel() {
            var dims = getSelectedDimensions();
            if (dims.length === 0) {
                display.innerHTML = '请选择维度';
                return;
            }

            var boxes = dropdown.querySelectorAll('input[type="checkbox"]');
            var total = 0;
            for (var i = 0; i < boxes.length; i++) {
                if (boxes[i].value !== '_all') total++;
            }

            if (dims.length === total) {
                display.innerHTML = '全部';
                return;
            }
            display.innerHTML = '已选 ' + dims.length + ' 项';
        }

        display.onclick = function(e) {
            e = e || window.event;
            if (e.stopPropagation) e.stopPropagation(); else e.cancelBubble = true;
            if (dropdown.className.indexOf('open') >= 0) {
                dropdown.className = dropdown.className.replace(/\s*open/g, '');
            } else {
                dropdown.className += ' open';
            }
        };

        dropdown.onclick = function(e) {
            e = e || window.event;
            if (e.stopPropagation) e.stopPropagation(); else e.cancelBubble = true;
        };

        dropdown.onchange = function(e) {
            e = e || window.event;
            var t = e.target || e.srcElement;
            if (!t || t.tagName !== 'INPUT') return;

            if (t.value === '_all') {
                var boxes = dropdown.querySelectorAll('input[type="checkbox"]');
                for (var i = 0; i < boxes.length; i++) {
                    if (boxes[i].value !== '_all') boxes[i].checked = t.checked;
                }
            } else {
                syncAllBox();
            }

            renderLabel();
            updateDimensionOptionVisibility();
        };

        document.addEventListener('click', function() {
            dropdown.className = dropdown.className.replace(/\s*open/g, '');
        });

        renderLabel();
    }

    function initDates() {
        var format = function(date) {
            var d = new Date(date);
            var yyyy = d.getFullYear();
            var mm = String(d.getMonth() + 1).padStart(2, '0');
            var dd = String(d.getDate()).padStart(2, '0');
            var hh = String(d.getHours()).padStart(2, '0');
            var min = String(d.getMinutes()).padStart(2, '0');
            var ss = String(d.getSeconds()).padStart(2, '0');
            return yyyy + '-' + mm + '-' + dd + 'T' + hh + ':' + min + ':' + ss;
        };

        var parse = function(value) {
            if (!value) return null;
            var normalized = String(value).trim().replace(' ', 'T');
            var match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/);
            if (!match) return null;
            return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]), Number(match[4]), Number(match[5]), Number(match[6] || 0), 0);
        };

        var shiftYearSafe = function(date, years) {
            var d = new Date(date.getTime());
            var month = d.getMonth();
            d.setFullYear(d.getFullYear() + years);
            if (d.getMonth() !== month) d.setDate(0);
            return d;
        };

        var syncCompareRangeByMainRange = function() {
            var beginInput = document.getElementById('beginDate');
            var endInput = document.getElementById('endDate');
            var m2mStartInput = document.getElementById('m2mStartTime');
            var m2mEndInput = document.getElementById('m2mEndTime');
            var y2yStartInput = document.getElementById('y2yStartTime');
            var y2yEndInput = document.getElementById('y2yEndTime');
            if (!beginInput || !endInput || !m2mStartInput || !m2mEndInput || !y2yStartInput || !y2yEndInput) return;

            var begin = parse(beginInput.value);
            var end = parse(endInput.value);
            if (!begin || !end) return;

            var durationMs = end.getTime() - begin.getTime();
            var m2mEnd = new Date(begin.getTime());
            var m2mStart = new Date(m2mEnd.getTime() - durationMs);
            var y2yStart = shiftYearSafe(begin, -1);
            var y2yEnd = shiftYearSafe(end, -1);

            m2mEndInput.value = format(m2mEnd);
            m2mStartInput.value = format(m2mStart);
            y2yStartInput.value = format(y2yStart);
            y2yEndInput.value = format(y2yEnd);
        };

        var now = new Date();
        var todayBegin = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
        var start = new Date(todayBegin.getTime() - 7 * 24 * 3600 * 1000);
        var end = todayBegin;

        document.getElementById('beginDate').value = format(start);
        document.getElementById('endDate').value = format(end);

        syncCompareRangeByMainRange();
        document.getElementById('beginDate').addEventListener('change', syncCompareRangeByMainRange);
        document.getElementById('endDate').addEventListener('change', syncCompareRangeByMainRange);
    }

    function updateTimeBucketLabel() {
        var idxInput = document.getElementById('optTimeBucketIndex');
        var label = document.getElementById('optTimeBucketLabel');
        if (!idxInput || !label) return;
        var idx = Number(idxInput.value);
        if (Number.isNaN(idx) || idx < 0 || idx >= TIME_BUCKET_CHOICES.length) idx = 2;
        label.innerHTML = TIME_BUCKET_CHOICES[idx] + '小时';
    }

    function updateDeptTopNLabel() {
        var allCheckbox = document.getElementById('optDeptAll');
        var range = document.getElementById('optDeptTopN');
        var label = document.getElementById('optDeptTopNLabel');
        if (!allCheckbox || !range || !label) return;
        range.disabled = allCheckbox.checked;
        label.innerHTML = allCheckbox.checked ? '全部' : ('Top ' + range.value);
    }

    function syncDeptTopNRangeFromData(deptAllData) {
        var range = document.getElementById('optDeptTopN');
        if (!range) return;
        var total = Array.isArray(deptAllData) ? deptAllData.length : 0;
        var max = total > 0 ? total : 1;
        range.max = String(max);
        var current = Number(range.value);
        if (Number.isNaN(current) || current < 1) current = 1;
        if (current > max) current = max;
        range.value = String(current);
    }

    function updatePhoneMinCountLabel() {
        var range = document.getElementById('optPhoneMinCount');
        var label = document.getElementById('optPhoneMinCountLabel');
        if (!range || !label) return;
        label.innerHTML = '>=' + range.value + '次';
    }

    function updateClusterRadiusLabel() {
        var range = document.getElementById('optClusterRadius');
        var label = document.getElementById('optClusterRadiusLabel');
        if (!range || !label) return;
        label.innerHTML = range.value + '米';
    }

    function getAnalysisOptionValues() {
        var timeBucketIdx = Number(document.getElementById('optTimeBucketIndex').value);
        if (Number.isNaN(timeBucketIdx) || timeBucketIdx < 0 || timeBucketIdx >= TIME_BUCKET_CHOICES.length) {
            timeBucketIdx = 2;
        }
        var deptAll = document.getElementById('optDeptAll').checked;
        var deptTopN = deptAll ? null : Number(document.getElementById('optDeptTopN').value);
        return {
            timeBucketHours: TIME_BUCKET_CHOICES[timeBucketIdx],
            deptTopN: deptTopN,
            repeatPhoneMinCount: Number(document.getElementById('optPhoneMinCount').value),
            repeatAddrRadiusMeters: Number(document.getElementById('optClusterRadius').value)
        };
    }

    function updateDimensionOptionVisibility() {
        var dims = getSelectedDimensions();
        var visibleMap = {
            time: dims.indexOf('time') >= 0,
            dept: dims.indexOf('dept') >= 0,
            phone: dims.indexOf('phone') >= 0,
            cluster: dims.indexOf('cluster') >= 0
        };
        document.getElementById('opt-time').style.display = visibleMap.time ? 'flex' : 'none';
        document.getElementById('opt-dept').style.display = visibleMap.dept ? 'flex' : 'none';
        document.getElementById('opt-phone').style.display = visibleMap.phone ? 'flex' : 'none';
        document.getElementById('opt-cluster').style.display = visibleMap.cluster ? 'flex' : 'none';
    }

    function buildTimeDataFromHourly(hourly, bucketHours) {
        if (!hourly || hourly.length !== 24) return [];
        var pairs = [];
        for (var start = 0; start < 24; start += bucketHours) {
            var end = start + bucketHours;
            var count = 0;
            for (var h = start; h < end; h++) count += (hourly[h] || 0);
            pairs.push([start + '-' + end + '时', count]);
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

    function renderSrrEmptyState() {
        var box = document.getElementById('box-srr');
        box.classList.add('active');
        var container = document.getElementById('table-srr');
        container.innerHTML = '<h3 style="margin:0 0 10px 0;">各地同环比</h3>' +
            '<div style="padding:16px;border:1px dashed #d0d7de;border-radius:6px;color:#666;">无符合条件数据</div>';
    }

    function renderSrrErrorState(errInfo) {
        var box = document.getElementById('box-srr');
        box.classList.add('active');
        var container = document.getElementById('table-srr');
        var message = (errInfo && errInfo.message) ? errInfo.message : '上游接口异常';
        var upstreamCode = (errInfo && errInfo.upstream_code !== undefined && errInfo.upstream_code !== null) ? errInfo.upstream_code : '';
        var codeText = upstreamCode === '' ? '' : ('code=' + upstreamCode);

        container.innerHTML = '<h3 style="margin:0 0 10px 0;">各地同环比</h3>' +
            '<div style="padding:14px 16px;border:1px solid #f5c2c7;border-radius:6px;background:#fff5f5;color:#842029;">' +
            '<div style="font-weight:bold;margin-bottom:4px;">同环比取数失败：' + message + '</div>' +
            (codeText ? ('<div style="font-size:12px;opacity:0.85;">' + codeText + '</div>') : '') +
            '</div>';
    }

    function renderSrrTable(rows) {
        var box = document.getElementById('box-srr');
        box.classList.add('active');
        var container = document.getElementById('table-srr');
        var html = '<h3 style="margin:0 0 10px 0;">各地同环比</h3>';
        html += '<table style="width:100%;border-collapse:collapse;font-size:13px;">';
        html += '<thead><tr style="background:#1976d2;color:#fff;">';
        html += '<th style="padding:6px 10px;text-align:left;">单位名称</th>';
        html += '<th style="padding:6px 10px;text-align:right;">本期数</th>';
        html += '<th style="padding:6px 10px;text-align:right;">同比上期</th>';
        html += '<th style="padding:6px 10px;text-align:right;">同比比例</th>';
        html += '<th style="padding:6px 10px;text-align:right;">环比上期</th>';
        html += '<th style="padding:6px 10px;text-align:right;">环比比例</th>';
        html += '</tr></thead><tbody>';
        rows.forEach(function(r, idx) {
            var isTotal = !r.code;
            var bg = isTotal ? '#e3f2fd' : (idx % 2 === 0 ? '#fff' : '#f9f9f9');
            var fw = isTotal ? 'font-weight:bold;' : '';
            html += '<tr style="background:' + bg + ';' + fw + '">';
            html += '<td style="padding:5px 10px;">' + (r.name || '') + '</td>';
            html += '<td style="padding:5px 10px;text-align:right;">' + (r.presentCycle == null ? '' : r.presentCycle) + '</td>';
            html += '<td style="padding:5px 10px;text-align:right;">' + (r.upperY2yCycle == null ? '' : r.upperY2yCycle) + '</td>';
            html += '<td style="padding:5px 10px;text-align:right;">' + (r.y2yProportion || '') + '</td>';
            html += '<td style="padding:5px 10px;text-align:right;">' + (r.upperM2mCycle == null ? '' : r.upperM2mCycle) + '</td>';
            html += '<td style="padding:5px 10px;text-align:right;">' + (r.m2mProportion || '') + '</td>';
            html += '</tr>';
        });
        html += '</tbody></table>';
        container.innerHTML = html;
    }

    function renderChart(boxId, chartId, label, dataPairs, reverse, type) {
        if (typeof reverse === 'undefined') reverse = false;
        if (typeof type === 'undefined') type = 'bar';
        var box = document.getElementById(boxId);
        box.classList.add('active');
        var canvas = document.getElementById(chartId);
        var ctx = canvas.getContext('2d');
        var existing = Chart.getChart ? Chart.getChart(canvas) : chartsMap[chartId];
        if (existing) existing.destroy();
        chartsMap[chartId] = null;
        var pairs = (dataPairs || []).slice();
        if (reverse) pairs.reverse();
        var labels = pairs.map(function(x) { return x[0]; });
        var values = pairs.map(function(x) { return x[1]; });
        var isHorizontal = type === 'horizontalBar';
        var chartType = isHorizontal ? 'bar' : type;
        var extraOptions = isHorizontal ? { indexAxis: 'y' } : {};
        chartsMap[chartId] = new Chart(ctx, {
            type: chartType,
            data: {
                labels: labels,
                datasets: [{
                    label: label,
                    data: values,
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderColor: 'rgba(54, 162, 235, 1)',
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

    function renderFromCurrentState() {
        if (!lastAnalyzeState.data) return;
        var data = lastAnalyzeState.data || {};
        var base = lastAnalyzeState.base || {};
        var dims = lastAnalyzeState.dims || [];
        var opts = getAnalysisOptionValues();
        document.querySelectorAll('.chart-box').forEach(function(b) { b.classList.remove('active'); });

        if (dims.indexOf('srr') >= 0) {
            if (data.srr_error) renderSrrErrorState(data.srr_error);
            else if (data.srr && data.srr.length > 0) renderSrrTable(data.srr);
            else renderSrrEmptyState();
        }
        if (dims.indexOf('time') >= 0) {
            var timeData = base.timeHourly ? buildTimeDataFromHourly(base.timeHourly, opts.timeBucketHours) : (data.time || []);
            renderChart('box-time', 'chart-time', '时段报警数(每' + opts.timeBucketHours + '小时)', timeData, false, 'bar');
        }
        if (dims.indexOf('dept') >= 0) {
            syncDeptTopNRangeFromData(base.deptAll || data.dept || []);
            updateDeptTopNLabel();
            var deptData = base.deptAll ? buildDeptDataFromAll(base.deptAll, opts.deptTopN) : (data.dept || []);
            var deptLabel = opts.deptTopN ? ('派出所报警数(Top ' + opts.deptTopN + ')') : '派出所报警数(全部)';
            renderChart('box-dept', 'chart-dept', deptLabel, deptData, false, 'horizontalBar');
        }
        if (dims.indexOf('phone') >= 0) {
            var phoneData = base.phoneAll ? buildPhoneDataFromAll(base.phoneAll, opts.repeatPhoneMinCount) : (data.phone || []);
            renderChart('box-phone', 'chart-phone', '重复报警电话(>=' + opts.repeatPhoneMinCount + '次)', phoneData, false, 'bar');
        }
        if (dims.indexOf('cluster') >= 0) {
            renderChart('box-cluster', 'chart-cluster', '重复报警地址(半径' + opts.repeatAddrRadiusMeters + '米)', data.cluster || [], false, 'horizontalBar');
        }
    }

    function triggerClusterRefreshIfNeeded() {
        if ((lastAnalyzeState.dims || []).indexOf('cluster') < 0) return;
        if (clusterRefreshTimer) clearTimeout(clusterRefreshTimer);
        clusterRefreshTimer = setTimeout(function() { doAnalyze(); }, 400);
    }

    function onOptionChangedRealtime(optionType) {
        renderFromCurrentState();
        if (optionType === 'cluster') triggerClusterRefreshIfNeeded();
    }

    function initAnalysisOptions() {
        document.getElementById('optTimeBucketIndex').addEventListener('input', function() {
            updateTimeBucketLabel();
            onOptionChangedRealtime('time');
        });
        document.getElementById('optDeptAll').addEventListener('change', function() {
            updateDeptTopNLabel();
            onOptionChangedRealtime('dept');
        });
        document.getElementById('optDeptTopN').addEventListener('input', function() {
            updateDeptTopNLabel();
            onOptionChangedRealtime('dept');
        });
        document.getElementById('optPhoneMinCount').addEventListener('input', function() {
            updatePhoneMinCountLabel();
            onOptionChangedRealtime('phone');
        });
        document.getElementById('optClusterRadius').addEventListener('input', function() {
            updateClusterRadiusLabel();
            onOptionChangedRealtime('cluster');
        });
        updateTimeBucketLabel();
        updateDeptTopNLabel();
        updatePhoneMinCountLabel();
        updateClusterRadiusLabel();
        updateDimensionOptionVisibility();
    }

    function loadTreeData() {
        fetch('/jingqing_fenxi/treeData')
            .then(function(res) { return res.json(); })
            .then(function(data) {
                treeDataRaw = data || [];
                var dropdown = document.getElementById('caseTypeMsDropdown');
                if (!dropdown) return;
                dropdown.innerHTML = '';

                var allLabel = document.createElement('label');
                allLabel.className = 'multi-select-option';
                allLabel.style.cssText = 'border-bottom:1px solid #eee;font-weight:bold;';
                var allCb = document.createElement('input');
                allCb.type = 'checkbox';
                allCb.value = '_all';
                allLabel.appendChild(allCb);
                allLabel.appendChild(document.createTextNode(' 全选'));
                dropdown.appendChild(allLabel);

                var parents = treeDataRaw.filter(function(item) { return !item.pId; });
                parents.forEach(function(p) {
                    var label = document.createElement('label');
                    label.className = 'multi-select-option';
                    var cb = document.createElement('input');
                    cb.type = 'checkbox';
                    cb.value = p.id;
                    cb.dataset.name = p.name;
                    label.appendChild(cb);
                    label.appendChild(document.createTextNode(' ' + p.name));
                    dropdown.appendChild(label);
                });

                if (window._caseTypeSyncLabel) window._caseTypeSyncLabel();
            })
            .catch(function(err) {
                console.error(err);
                var dropdown = document.getElementById('caseTypeMsDropdown');
                if (dropdown) dropdown.innerHTML = '<div style="padding:5px;">加载失败</div>';
            });
    }

    function collectFormData() {
        var form = new FormData();
        var dateIds = ['beginDate', 'endDate', 'm2mStartTime', 'm2mEndTime', 'y2yStartTime', 'y2yEndTime'];
        for (var i = 0; i < dateIds.length; i++) {
            var v = document.getElementById(dateIds[i]).value;
            if (v) v = v.replace('T', ' ');
            form.append(dateIds[i], v);
        }

        var dimBoxes = document.querySelectorAll('#dimMsDropdown input[type="checkbox"]');
        for (var j = 0; j < dimBoxes.length; j++) {
            if (dimBoxes[j].value !== '_all' && dimBoxes[j].checked) form.append('dimensions[]', dimBoxes[j].value);
        }

        var typeBoxes = document.querySelectorAll('#caseTypeMsDropdown input[type="checkbox"]');
        var names = [];
        var tags = [];
        var selectedParentIds = [];
        var tagSeen = {};
        var nameSeen = {};
        for (var k = 0; k < typeBoxes.length; k++) {
            if (typeBoxes[k].value === '_all' || !typeBoxes[k].checked) continue;
            var pid = typeBoxes[k].value;
            selectedParentIds.push(pid);
            var children = treeDataRaw.filter(function(item) { return item.pId === pid; });
            children.forEach(function(c) {
                if (c.tag) {
                    if (!tagSeen[c.tag]) {
                        tags.push(c.tag);
                        tagSeen[c.tag] = true;
                    }
                    if (c.name && !nameSeen[c.name]) {
                        names.push(c.name);
                        nameSeen[c.name] = true;
                    }
                }
            });
        }

        form.append('newOriCharaSubclassNo', tags.join(','));
        form.append('newOriCharaSubclass', names.join(','));
        for (var x = 0; x < selectedParentIds.length; x++) form.append('caseTypeIds[]', selectedParentIds[x]);

        var options = getAnalysisOptionValues();
        form.append('timeBucketHours', options.timeBucketHours);
        form.append('deptTopN', options.deptTopN == null ? 'all' : String(options.deptTopN));
        form.append('repeatPhoneMinCount', String(options.repeatPhoneMinCount));
        form.append('repeatAddrRadiusMeters', String(options.repeatAddrRadiusMeters));
        return form;
    }

    function doAnalyze() {
        var formData = collectFormData();
        var selectedDims = formData.getAll('dimensions[]');
        if (!formData.has('dimensions[]')) {
            alert('请选择至少一个分析维度');
            return;
        }
        if (!formData.get('newOriCharaSubclassNo')) {
            alert('请选择警情类型（需存在可用子项）');
            return;
        }

        document.getElementById('loading').style.display = 'inline-block';
        fetch('/jingqing_fenxi/analyze', { method: 'POST', body: formData })
            .then(function(r) { return r.json(); })
            .then(function(res) {
                document.getElementById('loading').style.display = 'none';
                if (res.code !== 0) {
                    alert('分析失败');
                    return;
                }
                lastAnalyzeState.data = res.data || {};
                lastAnalyzeState.base = res.analysisBase || {};
                lastAnalyzeState.dims = selectedDims;
                renderFromCurrentState();
            })
            .catch(function(err) {
                console.error(err);
                document.getElementById('loading').style.display = 'none';
                alert('请求异常');
            });
    }

    function doExport() {
        var formData = collectFormData();
        if (!formData.has('dimensions[]')) {
            alert('请选择至少一个分析维度');
            return;
        }
        if (!formData.get('newOriCharaSubclassNo')) {
            alert('请先选择警情类型');
            return;
        }

        document.getElementById('loading').style.display = 'inline-block';
        fetch('/jingqing_fenxi/export', { method: 'POST', body: formData })
            .then(function(response) {
                document.getElementById('loading').style.display = 'none';
                if (!response.ok) throw new Error('Network response was not ok');
                return response.blob();
            })
            .then(function(blob) {
                var url = window.URL.createObjectURL(blob);
                var a = document.createElement('a');
                a.style.display = 'none';
                a.href = url;
                a.download = '警情分析报表.xlsx';
                document.body.appendChild(a);
                a.click();
                window.URL.revokeObjectURL(url);
            })
            .catch(function(err) {
                console.error(err);
                document.getElementById('loading').style.display = 'none';
                alert('导出失败');
            });
    }

    function init() {
        if (!document.getElementById('analysisDoAnalyzeBtn')) return;
        initCaseTypeMultiSelect();
        initDimensionMultiSelect();
        initDates();
        initAnalysisOptions();
        loadTreeData();
        document.getElementById('analysisDoAnalyzeBtn').addEventListener('click', doAnalyze);
        document.getElementById('analysisDoExportBtn').addEventListener('click', doExport);
    }

    window.AnalysisTabPage = { init: init };
})();
