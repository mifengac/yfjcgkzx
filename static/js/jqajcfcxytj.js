// 警情案件处罚查询与统计 - 主页面 JavaScript

// 全局状态
const state = {
    caseTypes: [],
    selectedTypes: [],
    summaryData: []
};

// 初始化页面
document.addEventListener('DOMContentLoaded', function() {
    initCaseTypesSelect();
    setDefaultTimeRange();
    setupEventListeners();
});

// 初始化警情类型多选框
async function initCaseTypesSelect() {
    try {
        const response = await fetch('/jingqing_anjian/api/jqajcfcxytj/types');
        const result = await response.json();

        if (result.success && result.data) {
            state.caseTypes = result.data.map(item => item.leixing || item.leixing);
            renderCaseTypesOptions();
        }
    } catch (error) {
        console.error('加载警情类型失败:', error);
        showStatus('加载警情类型失败', 'error');
    }
}

// 渲染警情类型选项
function renderCaseTypesOptions() {
    const dropdown = document.getElementById('caseTypesDropdown');
    if (!dropdown) return;

    let html = `
        <label class="multi-select-option">
            <input type="checkbox" value="_all" onchange="toggleAllTypes(this)">
            <span>全选</span>
        </label>
    `;

    state.caseTypes.forEach(type => {
        html += `
            <label class="multi-select-option">
                <input type="checkbox" value="${type}" onchange="updateSelectedTypes()">
                <span>${type}</span>
            </label>
        `;
    });

    dropdown.innerHTML = html;
}

// 切换多选下拉框显示
document.addEventListener('click', function(e) {
    const multiSelect = document.getElementById('caseTypes');
    const display = document.getElementById('caseTypesDisplay');
    const dropdown = document.getElementById('caseTypesDropdown');

    if (multiSelect && (multiSelect.contains(e.target) || display === e.target)) {
        dropdown.classList.toggle('open');
    } else if (dropdown && !dropdown.contains(e.target)) {
        dropdown.classList.remove('open');
    }
});

// 全选/取消全选
function toggleAllTypes(checkbox) {
    const checkboxes = document.querySelectorAll('#caseTypesDropdown input[type="checkbox"]:not([value="_all"])');
    checkboxes.forEach(cb => {
        cb.checked = checkbox.checked;
    });
    updateSelectedTypes();
}

// 更新选中的类型
function updateSelectedTypes() {
    const checkboxes = document.querySelectorAll('#caseTypesDropdown input[type="checkbox"]:checked:not([value="_all"])');
    state.selectedTypes = Array.from(checkboxes).map(cb => cb.value);

    const display = document.getElementById('caseTypesDisplay');
    const allCheckbox = document.querySelector('#caseTypesDropdown input[value="_all"]');

    if (state.selectedTypes.length === 0) {
        display.textContent = '请选择警情类型';
    } else if (state.selectedTypes.length === state.caseTypes.length) {
        display.textContent = '全部类型';
        allCheckbox.checked = true;
    } else {
        display.textContent = `已选 ${state.selectedTypes.length} 项`;
        allCheckbox.checked = false;
    }
}

// 设置默认时间范围（最近一个月）
function setDefaultTimeRange() {
    const now = new Date();

    // 本期：开始=今日向前7天 00:00:00；结束=前一日 23:59:59
    const startDate = new Date(now);
    startDate.setDate(startDate.getDate() - 7);
    const endDate = new Date(now);
    endDate.setDate(endDate.getDate() - 1);

    // 环比：开始=今日向前14天 00:00:00；结束=今日向前8天 23:59:59
    const hbStartDate = new Date(now);
    hbStartDate.setDate(hbStartDate.getDate() - 14);
    const hbEndDate = new Date(now);
    hbEndDate.setDate(hbEndDate.getDate() - 8);

    document.getElementById('startTime').value = formatDateTimeLocal(new Date(startDate.getFullYear(), startDate.getMonth(), startDate.getDate(), 0, 0, 0));
    document.getElementById('endTime').value = formatDateTimeLocal(new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate(), 23, 59, 59));

    const hbStartEl = document.getElementById('hbStartTime');
    const hbEndEl = document.getElementById('hbEndTime');
    if (hbStartEl) {
        hbStartEl.value = formatDateTimeLocal(new Date(hbStartDate.getFullYear(), hbStartDate.getMonth(), hbStartDate.getDate(), 0, 0, 0));
    }
    if (hbEndEl) {
        hbEndEl.value = formatDateTimeLocal(new Date(hbEndDate.getFullYear(), hbEndDate.getMonth(), hbEndDate.getDate(), 23, 59, 59));
    }
}

// 格式化日期时间为 datetime-local 输入格式
function formatDateTimeLocal(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
}

// 格式化日期时间为标准格式
function formatDateTime(dateStr) {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return '';
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

// 设置事件监听
function setupEventListeners() {
    // 可以在这里添加其他事件监听
}

// 查询数据
async function searchData() {
    // 允许不选择类型，查询全部数据
    const kssj = formatDateTime(document.getElementById('startTime').value);
    const jssj = formatDateTime(document.getElementById('endTime').value);

    if (!kssj || !jssj) {
        showStatus('请选择开始时间和结束时间', 'error');
        return;
    }

    showStatus('正在查询...', 'success');

    try {
        const response = await fetch('/jingqing_anjian/api/jqajcfcxytj/summary', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                leixing: state.selectedTypes,
                kssj: kssj,
                jssj: jssj
            })
        });

        const result = await response.json();

        if (result.success) {
            state.summaryData = result.data;
            renderSummaryTable(result.columns, result.data);
            showStatus('查询成功', 'success');
        } else {
            showStatus(result.message || '查询失败', 'error');
        }
    } catch (error) {
        console.error('查询失败:', error);
        showStatus('查询失败: ' + error.message, 'error');
    }
}

// 计算合计行
function calculateTotals(data, columns) {
    const totals = {};
    columns.forEach(col => {
        if (col === '地区') {
            totals[col] = '合计';
        } else {
            let sum = 0;
            data.forEach(row => {
                const val = row[col];
                if (typeof val === 'number') {
                    sum += val;
                } else if (typeof val === 'string' && val !== '') {
                    const num = parseInt(val, 10);
                    if (!isNaN(num)) {
                        sum += num;
                    }
                }
            });
            totals[col] = sum;
        }
    });
    return totals;
}

// 渲染汇总表格
function renderSummaryTable(columns, data) {
    const container = document.getElementById('resultsContainer');

    if (!data || data.length === 0) {
        container.innerHTML = '<p style="text-align: center; color: #666; padding: 30px;">暂无数据</p>';
        return;
    }

    // 过滤掉"地区代码"列，不显示
    const displayColumns = columns.filter(col => col !== '地区代码');

    // 计算合计行
    const totals = calculateTotals(data, displayColumns);

    let html = '<div class="results-table-container"><table class="results-table"><thead><tr>';

    // 表头
    displayColumns.forEach(col => {
        html += `<th>${col}</th>`;
    });
    html += '</tr></thead><tbody>';

    // 表体
    data.forEach(row => {
        html += '<tr>';
        displayColumns.forEach(col => {
            const value = row[col];
            if (col === '地区') {
                html += `<td>${value || ''}</td>`;
            } else if (value && value > 0) {
                html += `<td class="clickable-cell" onclick="openDetail('${col}', '${row['地区代码']}', ${value})">${value}</td>`;
            } else {
                html += `<td>${value || 0}</td>`;
            }
        });
        html += '</tr>';
    });

    // 合计行
    html += '<tr class="total-row">';
    displayColumns.forEach(col => {
        const value = totals[col];
        if (col === '地区') {
            html += `<td>${value}</td>`;
        } else if (value && value > 0) {
            html += `<td class="clickable-cell" onclick="openDetail('${col}', 'all', ${value})">${value}</td>`;
        } else {
            html += `<td>${value || 0}</td>`;
        }
    });
    html += '</tr>';

    html += '</tbody></table></div>';
    container.innerHTML = html;
}

// 打开明细弹窗
function openDetail(field, regionCode, value) {
    if (value === 0 || value === '-' || value === '') return;

    const kssj = formatDateTime(document.getElementById('startTime').value);
    const jssj = formatDateTime(document.getElementById('endTime').value);

    const params = new URLSearchParams({
        leixing: state.selectedTypes.join(','),
        kssj: kssj,
        jssj: jssj,
        click_field: field,
        region: regionCode
    });

    const modal = document.getElementById('detailModal');
    const frame = document.getElementById('detailFrame');

    frame.src = `/jingqing_anjian/jqajcfcxytj/detail?${params.toString()}`;
    modal.style.display = 'flex';
}

// 关闭弹窗
function closeModal(event) {
    // 如果点击的是弹窗背景，关闭弹窗
    if (event && event.target.id !== 'detailModal') return;
    document.getElementById('detailModal').style.display = 'none';
    document.getElementById('detailFrame').src = 'about:blank';
}

// 导出数据
async function exportData(format) {
    if (state.summaryData.length === 0) {
        showStatus('没有数据可导出', 'error');
        return;
    }

    const kssj = formatDateTime(document.getElementById('startTime').value);
    const jssj = formatDateTime(document.getElementById('endTime').value);

    try {
        const response = await fetch('/jingqing_anjian/api/jqajcfcxytj/export', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                leixing: state.selectedTypes,
                kssj: kssj,
                jssj: jssj,
                format: format,
                data_type: 'summary'
            })
        });

        if (response.ok) {
            const blob = await response.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `警情案件处罚统计_${new Date().getTime()}.${format === 'excel' ? 'xlsx' : 'csv'}`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            showStatus('导出成功', 'success');
        } else {
            showStatus('导出失败', 'error');
        }
    } catch (error) {
        console.error('导出失败:', error);
        showStatus('导出失败: ' + error.message, 'error');
    }
}

// 导出报表（写入 xls 模板；固定类型，不受多选框影响）
async function exportReport() {
    const kssj = formatDateTime(document.getElementById('startTime').value);
    const jssj = formatDateTime(document.getElementById('endTime').value);
    const hbkssj = formatDateTime((document.getElementById('hbStartTime') || {}).value || '');
    const hbjssj = formatDateTime((document.getElementById('hbEndTime') || {}).value || '');

    if (!kssj || !jssj || !hbkssj || !hbjssj) {
        showStatus('请填写开始/结束/环比开始/环比结束时间', 'error');
        return;
    }

    showStatus('正在导出报表...', 'success');
    try {
        const response = await fetch('/jingqing_anjian/api/jqajcfcxytj/report_export', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                kssj: kssj,
                jssj: jssj,
                hbkssj: hbkssj,
                hbjssj: hbjssj
            })
        });

        if (!response.ok) {
            let msg = '导出失败';
            try {
                const js = await response.json();
                msg = (js && js.message) ? js.message : msg;
            } catch (e) {}
            showStatus(msg, 'error');
            return;
        }

        const blob = await response.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;

        const cd = response.headers.get('content-disposition') || '';
        const m = cd.match(/filename\\*=UTF-8''([^;]+)/i) || cd.match(/filename=\"?([^;\"]+)\"?/i);
        a.download = m ? decodeURIComponent(m[1]) : `警情案件处罚统计报表_${new Date().getTime()}.xls`;

        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        showStatus('导出成功', 'success');
    } catch (error) {
        console.error('导出报表失败:', error);
        showStatus('导出报表失败: ' + error.message, 'error');
    }
}

// 显示状态消息
function showStatus(message, type) {
    const statusEl = document.getElementById('statusMessage');
    statusEl.textContent = message;
    statusEl.className = `status-message ${type}`;
    statusEl.style.display = 'block';

    // 3秒后自动隐藏成功消息
    if (type === 'success') {
        setTimeout(() => {
            statusEl.style.display = 'none';
        }, 3000);
    }
}
