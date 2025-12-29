// 警情案件处罚查询与统计 - 明细页面 JavaScript

// 全局状态
const detailState = {
    columns: [],
    data: [],
    params: {}
};

// 初始化页面
document.addEventListener('DOMContentLoaded', function() {
    loadDetailData();
    setupExportButton();
});

// 从 URL 参数中获取查询条件并加载数据
function loadDetailData() {
    const urlParams = new URLSearchParams(window.location.search);

    detailState.params = {
        leixing: urlParams.get('leixing') ? urlParams.get('leixing').split(',') : [],
        kssj: urlParams.get('kssj') || '',
        jssj: urlParams.get('jssj') || '',
        click_field: urlParams.get('click_field') || '',
        region: urlParams.get('region') || ''
    };

    // 更新标题
    const titleEl = document.getElementById('detailTitle');
    if (titleEl && detailState.params.click_field) {
        titleEl.textContent = `${detailState.params.click_field} - 明细数据`;
    }

    fetchDetailData();
}

// 获取明细数据
async function fetchDetailData() {
    showStatus('正在加载明细数据...', 'success');

    try {
        const response = await fetch('/jingqing_anjian/api/jqajcfcxytj/detail', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(detailState.params)
        });

        const result = await response.json();

        if (result.success) {
            detailState.columns = result.columns;
            detailState.data = result.data;
            renderDetailTable(result.columns, result.data);
            showStatus('加载成功', 'success');
            // 3秒后隐藏消息
            setTimeout(() => hideStatus(), 3000);
        } else {
            showStatus(result.message || '加载失败', 'error');
        }
    } catch (error) {
        console.error('加载明细数据失败:', error);
        showStatus('加载失败: ' + error.message, 'error');
    }
}

// 渲染明细表格
function renderDetailTable(columns, data) {
    const container = document.getElementById('detailResults');

    if (!data || data.length === 0) {
        container.innerHTML = '<p style="text-align: center; color: #666; padding: 30px;">暂无数据</p>';
        return;
    }

    let html = '<div class="results-table-container"><table class="results-table"><thead><tr>';

    // 表头
    columns.forEach(col => {
        html += `<th>${col}</th>`;
    });
    html += '</tr></thead><tbody>';

    // 表体
    data.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            const value = row[col] !== null && row[col] !== undefined ? row[col] : '';
            html += `<td>${value}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;

    // 更新水平滚动条
    updateHorizontalScroll();
}

// 更新水平滚动条
function updateHorizontalScroll() {
    const table = document.querySelector('.results-table');
    const scrollInner = document.getElementById('detailScrollInner');

    if (table && scrollInner) {
        scrollInner.style.width = table.offsetWidth + 'px';
    }

    // 同步滚动
    const tableContainer = document.querySelector('.results-table-container');
    const horizontalScroll = document.getElementById('detailScroll');

    if (tableContainer && horizontalScroll) {
        tableContainer.addEventListener('scroll', function() {
            horizontalScroll.scrollLeft = this.scrollLeft;
        });

        horizontalScroll.addEventListener('scroll', function() {
            tableContainer.scrollLeft = this.scrollLeft;
        });
    }
}

// 设置导出按钮
function setupExportButton() {
    const exportBtn = document.getElementById('exportBtn');
    const exportMenu = document.getElementById('exportMenu');

    if (exportBtn && exportMenu) {
        exportBtn.addEventListener('click', function(e) {
            e.stopPropagation();
            exportMenu.classList.toggle('open');
        });

        document.addEventListener('click', function(e) {
            if (!exportBtn.contains(e.target) && !exportMenu.contains(e.target)) {
                exportMenu.classList.remove('open');
            }
        });
    }
}

// 导出明细数据
async function exportDetailData(format) {
    if (detailState.data.length === 0) {
        showStatus('没有数据可导出', 'error');
        return;
    }

    try {
        const response = await fetch('/jingqing_anjian/api/jqajcfcxytj/export', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                leixing: detailState.params.leixing,
                kssj: detailState.params.kssj,
                jssj: detailState.params.jssj,
                format: format,
                data_type: 'detail',
                click_field: detailState.params.click_field,
                region: detailState.params.region
            })
        });

        if (response.ok) {
            const blob = await response.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `警情案件处罚统计明细_${new Date().getTime()}.${format === 'excel' ? 'xlsx' : 'csv'}`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            showStatus('导出成功', 'success');
            setTimeout(() => hideStatus(), 3000);
        } else {
            showStatus('导出失败', 'error');
        }
    } catch (error) {
        console.error('导出失败:', error);
        showStatus('导出失败: ' + error.message, 'error');
    }
}

// 显示状态消息
function showStatus(message, type) {
    const statusEl = document.getElementById('detailStatus');
    statusEl.textContent = message;
    statusEl.className = `status-message ${type}`;
    statusEl.style.display = 'block';
}

// 隐藏状态消息
function hideStatus() {
    const statusEl = document.getElementById('detailStatus');
    statusEl.style.display = 'none';
}
