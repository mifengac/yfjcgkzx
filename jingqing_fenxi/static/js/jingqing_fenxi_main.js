let treeDataRaw = [];
let chartsMap = {};

// Dropdown Logic
function toggleDropdown(event, menuId) {
    event.stopPropagation();
    const menu = document.getElementById(menuId);
    if (!menu) return;
    const wasShow = menu.classList.contains('show');
    
    document.querySelectorAll('.dropdown-menu').forEach(m => m.classList.remove('show'));
    
    if (!wasShow) {
        menu.classList.add('show');
    }
}

document.addEventListener('click', function() {
    document.querySelectorAll('.dropdown-menu').forEach(m => m.classList.remove('show'));
});

function updateTypeCount() {
    const checked = document.querySelectorAll('#caseTypeMenu input[type="checkbox"]:checked').length;
    document.getElementById('caseTypeBtn').innerText = "已选择 " + checked + " 项";
}

function updateDimCount() {
    const checked = document.querySelectorAll('#dimMenu input[type="checkbox"]:checked').length;
    document.getElementById('dimBtn').innerText = "已选择 " + checked + " 项";
}



function selectAllDims(masterCb) {
    document.querySelectorAll('#dimMenu input[type="checkbox"]:not(#dimSelectAll)').forEach(cb => {
        cb.checked = masterCb.checked;
    });
    updateDimCount();
}

function selectAllTypes(masterCb) {
    document.querySelectorAll('#caseTypeMenu input[type="checkbox"]:not(#typeSelectAll)').forEach(cb => {
        cb.checked = masterCb.checked;
    });
    updateTypeCount();
}
function initDates() {
    const format = (date) => {
        const d = new Date(date);
        const yyyy = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const dd = String(d.getDate()).padStart(2, '0');
        const hh = String(d.getHours()).padStart(2, '0');
        const min = String(d.getMinutes()).padStart(2, '0');
        const ss = String(d.getSeconds()).padStart(2, '0');
        return `${yyyy}-${mm}-${dd}T${hh}:${min}:${ss}`;
    };

    const now = new Date();
    const todayBegin = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
    
    // Begin Date: 7 days ago
    const start = new Date(todayBegin.getTime() - 7 * 24 * 3600 * 1000);
    const end = todayBegin;
    
    // MoM dates
    const m2mStart = new Date(todayBegin.getTime() - 14 * 24 * 3600 * 1000);
    const m2mEnd = new Date(todayBegin.getTime() - 7 * 24 * 3600 * 1000);

    document.getElementById('beginDate').value = format(start);
    document.getElementById('endDate').value = format(end);
    document.getElementById('m2mStartTime').value = format(m2mStart);
    document.getElementById('m2mEndTime').value = format(m2mEnd);
    
    // set Y2Y artificially
    const y2yStart = new Date(start);
    y2yStart.setFullYear(y2yStart.getFullYear() - 1);
    const y2yEnd = new Date(end);
    y2yEnd.setFullYear(y2yEnd.getFullYear() - 1);
    
    document.getElementById('y2yStartTime').value = format(y2yStart);
    document.getElementById('y2yEndTime').value = format(y2yEnd);
}

function loadTreeData() {
    fetch('/jingqing_fenxi/treeData')
        .then(res => res.json())
        .then(data => {
            treeDataRaw = data || [];
            const menu = document.getElementById('caseTypeMenu');
            if(!menu) return;
            menu.innerHTML = '';

            // 全选 item
            (function() {
                const allLabel = document.createElement('label');
                allLabel.className = 'dropdown-item';
                allLabel.style.cssText = 'border-bottom:1px solid #eee;font-weight:bold;';
                allLabel.onclick = (e) => e.stopPropagation();
                const allCb = document.createElement('input');
                allCb.type = 'checkbox';
                allCb.id = 'typeSelectAll';
                allCb.onchange = function() { selectAllTypes(this); };
                allLabel.appendChild(allCb);
                allLabel.appendChild(document.createTextNode(' 全选'));
                menu.appendChild(allLabel);
            })();

            // Only add nodes without pId
            const parents = treeDataRaw.filter(item => !item.pId);
            parents.forEach(p => {
                const label = document.createElement('label');
                label.className = 'dropdown-item';
                label.onclick = (e) => e.stopPropagation();

                const cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.value = p.id;
                cb.dataset.name = p.name;
                cb.onchange = updateTypeCount;

                label.appendChild(cb);
                label.appendChild(document.createTextNode(' ' + p.name));
                menu.appendChild(label);
            });
        }).catch(err => {
            console.error(err);
            const menu = document.getElementById('caseTypeMenu');
            if(menu) menu.innerHTML = '<div style="padding:5px;">加载失败</div>';
        });
}

function collectFormData() {
    const form = new FormData();
    // Replace T with space
    ['beginDate', 'endDate', 'm2mStartTime', 'm2mEndTime', 'y2yStartTime', 'y2yEndTime'].forEach(id => {
        let v = document.getElementById(id).value;
        if (v) v = v.replace('T', ' ');
        form.append(id, v);
    });

    // Dimensions
    const checkedDims = document.querySelectorAll('#dimMenu input[type="checkbox"]:checked');
    checkedDims.forEach(cb => {
        form.append('dimensions[]', cb.value);
    });

    // Selected Parent Node tags
    const checkedTypes = document.querySelectorAll('#caseTypeMenu input[type="checkbox"]:checked');
    let names = [];
    let tags = [];
    
    checkedTypes.forEach(cb => {
        const pid = cb.value;
        const children = treeDataRaw.filter(item => item.pId === pid);
        children.forEach(c => {
            if(c.tag) {
                tags.push(c.tag);
                names.push(c.name);
            }
        });
    });

    form.append('newOriCharaSubclassNo', tags.join(','));
    form.append('newOriCharaSubclass', names.join(','));
    
    return form;
}

function renderSrrTable(rows) {
    const box = document.getElementById('box-srr');
    box.classList.add('active');
    const container = document.getElementById('table-srr');

    let html = '<h3 style="margin:0 0 10px 0;">各地同环比</h3>';
    html += '<table style="width:100%;border-collapse:collapse;font-size:13px;">';
    html += '<thead><tr style="background:#1976d2;color:#fff;">';
    html += '<th style="padding:6px 10px;text-align:left;">单位名称</th>';
    html += '<th style="padding:6px 10px;text-align:right;">本期数</th>';
    html += '<th style="padding:6px 10px;text-align:right;">同比上期</th>';
    html += '<th style="padding:6px 10px;text-align:right;">同比比例</th>';
    html += '<th style="padding:6px 10px;text-align:right;">环比上期</th>';
    html += '<th style="padding:6px 10px;text-align:right;">环比比例</th>';
    html += '</tr></thead><tbody>';

    rows.forEach((r, idx) => {
        const isTotal = !r.code;
        const bg = isTotal ? '#e3f2fd' : (idx % 2 === 0 ? '#fff' : '#f9f9f9');
        const fw = isTotal ? 'font-weight:bold;' : '';
        const y2yColor = (r.y2yProportion || '').includes('↑') ? 'color:#e53935;' : ((r.y2yProportion || '').includes('↓') ? 'color:#43a047;' : '');
        const m2mColor = (r.m2mProportion || '').includes('↑') ? 'color:#e53935;' : ((r.m2mProportion || '').includes('↓') ? 'color:#43a047;' : '');
        html += `<tr style="background:${bg};${fw}">`;
        html += `<td style="padding:5px 10px;">${r.name || ''}</td>`;
        html += `<td style="padding:5px 10px;text-align:right;">${r.presentCycle ?? ''}</td>`;
        html += `<td style="padding:5px 10px;text-align:right;">${r.upperY2yCycle ?? ''}</td>`;
        html += `<td style="padding:5px 10px;text-align:right;${y2yColor}">${r.y2yProportion || ''}</td>`;
        html += `<td style="padding:5px 10px;text-align:right;">${r.upperM2mCycle ?? ''}</td>`;
        html += `<td style="padding:5px 10px;text-align:right;${m2mColor}">${r.m2mProportion || ''}</td>`;
        html += '</tr>';
    });

    html += '</tbody></table>';
    container.innerHTML = html;
}

function renderChart(boxId, chartId, label, dataPairs, reverse = false, type = 'bar') {
    const box = document.getElementById(boxId);
    box.classList.add('active');

    const canvas = document.getElementById(chartId);
    const ctx = canvas.getContext('2d');

    // Safely destroy any existing chart on this canvas (handles orphan instances)
    const existing = Chart.getChart ? Chart.getChart(canvas) : chartsMap[chartId];
    if (existing) {
        existing.destroy();
    }
    chartsMap[chartId] = null;

    // Sort logic passed from backend usually, but JS handles reverse if specified
    if(reverse) {
        dataPairs.reverse();
    }

    let labels = dataPairs.map(x => x[0]);
    let values = dataPairs.map(x => x[1]);

    // Chart.js v3: horizontalBar removed, use indexAxis:'y' instead
    let isHorizontal = (type === 'horizontalBar');
    let chartType = isHorizontal ? 'bar' : type;
    let extraOptions = isHorizontal ? { indexAxis: 'y' } : {};

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
            scales: {
                x: { beginAtZero: true },
                y: { beginAtZero: true }
            },
            plugins: {
                legend: { display: false },
                // inline datalabels drawn via afterDatasetsDraw plugin below
            }
        }, extraOptions),
        plugins: [{
            id: 'inlineDatalabels',
            afterDatasetsDraw(chart) {
                const ctx = chart.ctx;
                chart.data.datasets.forEach((dataset, i) => {
                    const meta = chart.getDatasetMeta(i);
                    meta.data.forEach((bar, index) => {
                        const value = dataset.data[index];
                        if (value === null || value === undefined || value === 0) return;
                        ctx.save();
                        ctx.font = 'bold 11px Arial';
                        ctx.fillStyle = '#333';
                        if (isHorizontal) {
                            // horizontal bar: label to the right of bar end
                            ctx.textAlign = 'left';
                            ctx.textBaseline = 'middle';
                            ctx.fillText(value, bar.x + 4, bar.y);
                        } else {
                            // vertical bar: label above bar
                            ctx.textAlign = 'center';
                            ctx.textBaseline = 'bottom';
                            ctx.fillText(value, bar.x, bar.y - 3);
                        }
                        ctx.restore();
                    });
                });
            }
        }]
    });
}

function doAnalyze() {
    const formData = collectFormData();
    if (!formData.has('dimensions[]')) {
        alert("请选择至少一个分析维度");
        return;
    }
    if (!formData.get('newOriCharaSubclassNo')) {
        alert("请选警情类型(对应无子节点或者为空时不拉取数据)");
    }

    // hide all boxes first
    document.querySelectorAll('.chart-box').forEach(b => b.classList.remove('active'));
    document.getElementById('loading').style.display = 'inline-block';

    fetch('/jingqing_fenxi/analyze', {
        method: 'POST',
        body: formData
    })
    .then(r => r.json())
    .then(res => {
        document.getElementById('loading').style.display = 'none';
        if (res.code !== 0) {
            alert("分析失败");
            return;
        }
        
        const data = res.data;
        
        if (data.srr && data.srr.length > 0) {
            renderSrrTable(data.srr);
        }
        if (data.time) {
            // Backend returns descending by count, requirement: "倒序显示", we can keep backend sort or reverse here
            renderChart('box-time', 'chart-time', '时段报警数', data.time, false, 'bar');
        }
        if (data.dept) {
            renderChart('box-dept', 'chart-dept', '派出所报警数', data.dept, false, 'horizontalBar');
        }
        if (data.phone) {
            renderChart('box-phone', 'chart-phone', '重复手机号报警数', data.phone, false, 'bar');
        }
        if (data.cluster) {
            renderChart('box-cluster', 'chart-cluster', '半径50米内重复地址报警数', data.cluster, false, 'horizontalBar');
        }
        
    }).catch(err => {
        console.error(err);
        document.getElementById('loading').style.display = 'none';
        alert("请求异常");
    });
}

function doExport() {
    const formData = collectFormData();
    if (!formData.has('dimensions[]')) {
        alert("请选择至少一个分析维度");
        return;
    }
    
    document.getElementById('loading').style.display = 'inline-block';
    
    fetch('/jingqing_fenxi/export', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        document.getElementById('loading').style.display = 'none';
        if (!response.ok) throw new Error('Network response was not ok');
        return response.blob();
    })
    .then(blob => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        a.download = '警情分析报表.xlsx';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
    })
    .catch(err => {
        console.error(err);
        document.getElementById('loading').style.display = 'none';
        alert("导出失败");
    });
}

window.onload = function() {
    initDates();
    loadTreeData();
};
