// 主JavaScript文件 - 兼配Chrome 88

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    console.log('市局基础管控中心系统已加载');
    
    // 初始化页面功能
    initPage();
    
    // 模拟数据加载
    loadMockData();
    
    // 加载案件类型数据
    loadCaseTypes();
});

function initPage() {
    // 这里可以添加页面初始化逻辑
    console.log('页面初始化完成');
}

function loadMockData() {
    // 模拟统计数据
    const statsData = {
        totalUsers: 1248,
        activeDevices: 356,
        alerts: 23,
        completedTasks: 89
    };
    
    // 更新统计数据显示
    updateStatsDisplay(statsData);
}

function updateStatsDisplay(data) {
    const statsElements = {
        'totalUsers': document.querySelector('[data-stat="totalUsers"]'),
        'activeDevices': document.querySelector('[data-stat="activeDevices"]'),
        'alerts': document.querySelector('[data-stat="alerts"]'),
        'completedTasks': document.querySelector('[data-stat="completedTasks"]')
    };
    
    // 更新每个统计项
    for (const [key, element] of Object.entries(statsElements)) {
        if (element) {
            element.textContent = data[key];
        }
    }
}

// 基础工具函数
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function showAlert(message, type = 'info') {
    // 简单的提示功能
    alert(message);
}

// 添加基本的页面交互功能
function setupEventListeners() {
    // 这里可以添加事件监听器
    console.log('事件监听器设置完成');
}

// 警情案件查询功能 - 统计分析
function searchCases() {
    const caseType = document.getElementById('caseType').value;
    const startTime = document.getElementById('startTime').value;
    const endTime = document.getElementById('endTime').value;
    
    // 验证时间范围
    if (startTime && endTime && new Date(startTime) > new Date(endTime)) {
        alert('开始时间不能晚于结束时间');
        return;
    }
    
    // 显示加载中
    document.getElementById('loading').style.display = 'block';
    
    // 构建查询参数
    const params = new URLSearchParams();
    if (caseType) params.append('case_type', caseType);
    if (startTime) params.append('start_time', startTime);
    if (endTime) params.append('end_time', endTime);
    
    // 发起AJAX请求
    fetch(`/api/case_stats?${params.toString()}`)
        .then(response => response.json())
        .then(result => {
            // 隐藏加载中
            document.getElementById('loading').style.display = 'none';
            
            // 处理查询结果
            if (result.success) {
                displaySearchResults(result.data, caseType, startTime, endTime);
            } else {
                alert('查询失败，请稍后重试');
            }
        })
        .catch(error => {
            // 隐藏加载中
            document.getElementById('loading').style.display = 'none';
            console.error('查询出错:', error);
            alert('查询出错，请稍后重试');
        });
}

// 警情案件查询功能 - 案件详情
function searchCaseDetails() {
    const caseType = document.getElementById('caseTypeDetails').value;
    const startTime = document.getElementById('startTimeDetails').value;
    const endTime = document.getElementById('endTimeDetails').value;

    // 验证时间范围
    if (startTime && endTime && new Date(startTime) > new Date(endTime)) {
        alert('开始时间不能晚于结束时间');
        return;
    }

    // 显示加载中
    document.getElementById('loading').style.display = 'block';

    // 构建查询参数
    const params = new URLSearchParams();
    if (caseType) params.append('case_type', caseType);
    if (startTime) params.append('start_time', startTime);
    if (endTime) params.append('end_time', endTime);

    // 发起AJAX请求
    fetch(`/api/case_details?${params.toString()}`)
        .then(response => response.json())
        .then(result => {
            // 隐藏加载中
            document.getElementById('loading').style.display = 'none';

            // 处理查询结果
            if (result.success) {
                displayCaseDetails(result.field_config, result.data, caseType, startTime, endTime);
            } else {
                alert('查询失败，请稍后重试');
            }
        })
        .catch(error => {
            // 隐藏加载中
            document.getElementById('loading').style.display = 'none';
            console.error('查询出错:', error);
            alert('查询出错，请稍后重试');
        });
}

// 警情案件查询功能 - 人员详情
function searchCasePerson() {
    const caseType = document.getElementById('caseTypePerson').value;
    const startTime = document.getElementById('startTimePerson').value;
    const endTime = document.getElementById('endTimePerson').value;

    // 验证时间范围
    if (startTime && endTime && new Date(startTime) > new Date(endTime)) {
        alert('开始时间不能晚于结束时间');
        return;
    }

    // 显示加载中
    document.getElementById('loading').style.display = 'block';

    // 构建查询参数
    const params = new URLSearchParams();
    if (caseType) params.append('case_type', caseType);
    if (startTime) params.append('start_time', startTime);
    if (endTime) params.append('end_time', endTime);

    fetch(`/api/case_ry_details?${params.toString()}`)
        .then(response => response.json())
        .then(result => {
            document.getElementById('loading').style.display = 'none';
            if (result.success) {
                displayCasePerson(result.columns || [], result.data || [], caseType, startTime, endTime);
            } else {
                alert('查询失败，请稍后重试');
            }
        })
        .catch(error => {
            document.getElementById('loading').style.display = 'none';
            console.error('查询出错:', error);
            alert('查询出错，请稍后重试');
        });
}

function displaySearchResults(data, caseType, startTime, endTime) {
    const resultsContainer = document.getElementById('searchResults');
    
    // 构建查询条件显示
    let queryInfo = `查询条件：`;
    if (caseType) {
        const typeText = document.getElementById('caseType').options[document.getElementById('caseType').selectedIndex].text;
        queryInfo += ` 案件类型: ${typeText}`;
    }
    if (startTime) queryInfo += ` 开始时间: ${formatDateTime(startTime)}`;
    if (endTime) queryInfo += ` 结束时间: ${formatDateTime(endTime)}`;
    
    // 保存查询参数用于导出文件名
    window.currentQueryParams = {
        caseType: caseType,
        caseTypeText: caseType ? document.getElementById('caseType').options[document.getElementById('caseType').selectedIndex].text : '全部类型',
        startTime: startTime,
        endTime: endTime
    };
    
    // 如果有数据则显示表格，否则显示无数据提示
    if (data.length > 0) {
        // 定义显示字段顺序
        const displayFields = [
            "地区",
            "原始警情",
            "同比原始警情",
            "环比原始警情",
            "确认警情",
            "同比确认警情",
            "环比确认警情",
            "行政案件",
            "同比行政案件",
            "环比行政案件",
            "刑事案件",
            "同比刑事案件",
            "环比刑事案件",
            "拘留人员",
            "同比拘留人员",
            "环比拘留人员",
            "刑事拘留人员",
            "同比刑事拘留人员",
            "环比刑事拘留人员",
            "未成年人案件",
            "同比未成年案件",
            "环比未成年案件"
        ];
        
        // 构建表头
        let tableHtml = `
            <div style="background: #f0f8ff; padding: 15px; border-radius: 4px; margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>${queryInfo}</strong>
                </div>
                <div>
                    <button class="btn-export" onclick="exportToCSV()">导出CSV</button>
                </div>
            </div>
            <div class="results-table-container">
                <table class="results-table">
                    <thead>
                        <tr>
        `;
        
        // 添加表头（按配置顺序）
        displayFields.forEach(field => {
            tableHtml += `<th>${field}</th>`;
        });
        
        tableHtml += `
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        // 添加数据行
        data.forEach(item => {
            tableHtml += `<tr>`;
            // 按配置顺序添加数据列
            displayFields.forEach(field => {
                // 如果字段值为空或未定义，显示0
                const value = item[field];
                tableHtml += `<td>${(value === null || value === undefined || value === '') ? '0' : value}</td>`;
            });
            tableHtml += `</tr>`;
        });

        // 计算合计行
        const totals = calculateTotals(data, displayFields);

        // 添加合计行
        tableHtml += `<tr class="total-row">`;
        displayFields.forEach((field, index) => {
            if (index === 0) {
                // 第一列显示"合计"
                tableHtml += `<td>合计</td>`;
            } else {
                // 其他列显示合计值
                tableHtml += `<td>${formatNumber(totals[field] || 0)}</td>`;
            }
        });
        tableHtml += `</tr>`;
        
        tableHtml += `
                    </tbody>
                </table>
                <p style="text-align: center; color: #4caf50; padding: 10px;">
                    查询成功！找到 ${data.length} 条相关记录
                </p>
            </div>
        `;
        
        resultsContainer.innerHTML = tableHtml;
    } else {
        // 显示无数据提示
        resultsContainer.innerHTML = `
            <div style="background: #f0f8ff; padding: 15px; border-radius: 4px; margin-bottom: 15px;">
                <strong>${queryInfo}</strong>
            </div>
            <p style="text-align: center; color: #666; padding: 40px;">
                未找到相关记录
            </p>
        `;
    }
}

// 显示案件详情查询结果
function displayCaseDetails(fieldConfig, data, caseType, startTime, endTime) {
    const resultsContainer = document.getElementById('detailsResults');

    // 构建查询条件显示
    let queryInfo = `查询条件：`;
    if (caseType) {
        const typeText = document.getElementById('caseTypeDetails').options[document.getElementById('caseTypeDetails').selectedIndex].text;
        queryInfo += ` 案件类型: ${typeText}`;
    }
    if (startTime) queryInfo += ` 开始时间: ${formatDateTime(startTime)}`;
    if (endTime) queryInfo += ` 结束时间: ${formatDateTime(endTime)}`;

    // 保存查询参数用于导出文件名
    window.currentDetailsQueryParams = {
        caseType: caseType,
        caseTypeText: caseType ? document.getElementById('caseTypeDetails').options[document.getElementById('caseTypeDetails').selectedIndex].text : '全部类型',
        startTime: startTime,
        endTime: endTime,
        fieldConfig: fieldConfig
    };

    // 如果有数据则显示表格，否则显示无数据提示
    if (data.length > 0) {
        // 构建表头
        let tableHtml = `
            <div style="background: #f0f8ff; padding: 15px; border-radius: 4px; margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>${queryInfo}</strong>
                </div>
                <div>
                    <button class="btn-export" onclick="exportDetailsToCSV()">导出CSV</button>
                </div>
            </div>
            <div class="results-table-container">
                <table class="results-table">
                    <thead>
                        <tr>
        `;

        // 根据字段配置添加表头
        fieldConfig.forEach(field => {
            tableHtml += `<th>${field.display_name}</th>`;
        });

        tableHtml += `
                        </tr>
                    </thead>
                    <tbody>
        `;

        // 添加数据行
        data.forEach(item => {
            tableHtml += `<tr>`;
            // 按配置顺序添加数据列
            fieldConfig.forEach(field => {
                const value = item[field.field] || '';
                tableHtml += `<td title="${value}">${value}</td>`;
            });
            tableHtml += `</tr>`;
        });

        tableHtml += `
                    </tbody>
                </table>
                <p style="text-align: center; color: #4caf50; padding: 10px;">
                    查询成功！找到 ${data.length} 条相关记录
                </p>
            </div>
        `;

        resultsContainer.innerHTML = tableHtml;
    } else {
        // 显示无数据提示
        resultsContainer.innerHTML = `
            <div style="background: #f0f8ff; padding: 15px; border-radius: 4px; margin-bottom: 15px;">
                <strong>${queryInfo}</strong>
            </div>
            <p style="text-align: center; color: #666; padding: 40px;">
                未找到相关记录
            </p>
        `;
    }
}

// 显示人员详情查询结果（列名按数据库顺序）
function displayCasePerson(columns, data, caseType, startTime, endTime) {
    const resultsContainer = document.getElementById('personResults');

    // 构建查询条件显示
    let queryInfo = `查询条件：`;
    if (caseType) {
        const select = document.getElementById('caseTypePerson');
        const typeText = select.options[select.selectedIndex].text;
        queryInfo += ` 案件类型: ${typeText}`;
    }
    if (startTime) queryInfo += ` 开始时间: ${formatDateTime(startTime)}`;
    if (endTime) queryInfo += ` 结束时间: ${formatDateTime(endTime)}`;

    // 保存参数用于导出
    window.currentPersonQueryParams = {
        caseType: caseType,
        caseTypeText: caseType ? document.getElementById('caseTypePerson').options[document.getElementById('caseTypePerson').selectedIndex].text : '全部类型',
        startTime: startTime,
        endTime: endTime,
        columns: columns
    };

    if (data.length > 0 && columns.length > 0) {
        let tableHtml = `
            <div style="background: #f0f8ff; padding: 15px; border-radius: 4px; margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <strong>${queryInfo}</strong>
                </div>
                <div>
                    <button class=\"btn-export\" onclick=\"exportPersonToCSV()\">导出CSV</button>
                </div>
            </div>
            <div class=\"results-table-container\">
                <table class=\"results-table\">
                    <thead>
                        <tr>
        `;

        columns.forEach(col => { tableHtml += `<th>${col}</th>`; });

        tableHtml += `
                        </tr>
                    </thead>
                    <tbody>
        `;

        data.forEach(item => {
            tableHtml += `<tr>`;
            columns.forEach(col => {
                const value = item[col] || '';
                tableHtml += `<td title=\"${value}\">${value}</td>`;
            });
            tableHtml += `</tr>`;
        });

        tableHtml += `
                    </tbody>
                </table>
                <p style=\"text-align: center; color: #4caf50; padding: 10px;\">查询成功！找到 ${data.length} 条相关记录</p>
            </div>
        `;

        resultsContainer.innerHTML = tableHtml;
    } else {
        resultsContainer.innerHTML = `
            <div style=\"background: #f0f8ff; padding: 15px; border-radius: 4px; margin-bottom: 15px;\"> <strong>${queryInfo}</strong> </div>
            <p style=\"text-align: center; color: #666; padding: 40px;\">未找到相关记录</p>
        `;
    }
}

// 导出CSV功能 - 统计分析
function exportToCSV() {
    // 获取当前查询结果数据
    const resultsContainer = document.getElementById('searchResults');
    const table = resultsContainer.querySelector('.results-table');
    
    if (!table) {
        alert('没有可导出的数据');
        return;
    }
    
    // 定义显示字段顺序
    const displayFields = [
        "地区",
        "原始警情",
        "同比原始警情",
        "环比原始警情",
        "确认警情",
        "同比确认警情",
        "环比确认警情",
        "行政案件",
        "同比行政案件",
        "环比行政案件",
        "刑事案件",
        "同比刑事案件",
        "环比刑事案件",
        "拘留人员",
        "同比拘留人员",
        "环比拘留人员",
        "刑事拘留人员",
        "同比刑事拘留人员",
        "环比刑事拘留人员",
        "未成年人案件",
        "同比未成年案件",
        "环比未成年案件"
    ];
    
    // 构建CSV内容
    let csvContent = '';
    
    // 添加表头
    csvContent += displayFields.join(',') + '\n';
    
    // 添加数据行（不包括合计行）
    const rows = table.querySelectorAll('tbody tr');
    let dataRows = Array.from(rows);

    // 检查是否有合计行（最后一行且有total-row类）
    const hasTotalRow = rows.length > 0 && rows[rows.length - 1].classList.contains('total-row');
    if (hasTotalRow) {
        // 排除合计行，只处理数据行
        dataRows = dataRows.slice(0, -1);
    }

    dataRows.forEach(row => {
        const cols = row.querySelectorAll('td');
        const rowData = [];
        cols.forEach(col => {
            // 转义包含逗号或双引号的字段
            let cellData = col.textContent;
            if (cellData.includes(',') || cellData.includes('"')) {
                cellData = `"${cellData.replace(/"/g, '""')}"`;
            }
            rowData.push(cellData);
        });
        csvContent += rowData.join(',') + '\n';
    });

    // 如果有合计行，添加合计行到CSV
    if (hasTotalRow) {
        const totalRow = rows[rows.length - 1];
        const totalCols = totalRow.querySelectorAll('td');
        const totalData = [];
        totalCols.forEach(col => {
            // 转义包含逗号或双引号的字段
            let cellData = col.textContent;
            if (cellData.includes(',') || cellData.includes('"')) {
                cellData = `"${cellData.replace(/"/g, '""')}"`;
            }
            totalData.push(cellData);
        });
        csvContent += totalData.join(',') + '\n';
    }
    
    // 生成文件名
    const queryParams = window.currentQueryParams;
    let fileName = '统计分析查询结果';
    
    if (queryParams) {
        // 格式化时间范围
        let timeRange = '';
        if (queryParams.startTime && queryParams.endTime) {
            const start = formatDateTimeForFilename(queryParams.startTime);
            const end = formatDateTimeForFilename(queryParams.endTime);
            timeRange = `${start}_${end}`;
        } else if (queryParams.startTime) {
            timeRange = formatDateTimeForFilename(queryParams.startTime);
        } else if (queryParams.endTime) {
            timeRange = formatDateTimeForFilename(queryParams.endTime);
        }
        
        // 添加案件类型
        const caseType = queryParams.caseTypeText || '全部类型';
        
        fileName = `统计分析_${timeRange}_${caseType}`;
    }
    
    // 添加.csv扩展名
    fileName += '.csv';
    
    // 创建并下载文件
    const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', fileName);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// 导出CSV功能 - 案件详情
function exportDetailsToCSV() {
    // 获取当前查询结果数据
    const queryParams = window.currentDetailsQueryParams;

    if (!queryParams || !queryParams.fieldConfig) {
        alert('没有可导出的数据');
        return;
    }

    // 构建CSV内容
    let csvContent = '';

    // 根据字段配置添加表头
    const headers = queryParams.fieldConfig.map(field => field.display_name);
    csvContent += headers.join(',') + '\n';

    // 发起请求获取完整数据用于导出
    const params = new URLSearchParams();
    if (queryParams.caseType) params.append('case_type', queryParams.caseType);
    if (queryParams.startTime) params.append('start_time', queryParams.startTime);
    if (queryParams.endTime) params.append('end_time', queryParams.endTime);

    fetch(`/api/case_details?${params.toString()}`)
        .then(response => response.json())
        .then(result => {
            if (result.success && result.data.length > 0) {
                // 添加数据行
                result.data.forEach(item => {
                    const rowData = [];
                    queryParams.fieldConfig.forEach(field => {
                        let cellData = item[field.field] || '';
                        // 转换为字符串
                        cellData = String(cellData);
                        // 转义包含逗号或双引号的字段
                        if (cellData.includes(',') || cellData.includes('"')) {
                            cellData = `"${cellData.replace(/"/g, '""')}"`;
                        }
                        rowData.push(cellData);
                    });
                    csvContent += rowData.join(',') + '\n';
                });

                // 生成文件名
                let fileName = '案件详情查询结果';
                if (queryParams) {
                    // 格式化时间范围
                    let timeRange = '';
                    if (queryParams.startTime && queryParams.endTime) {
                        const start = formatDateTimeForFilename(queryParams.startTime);
                        const end = formatDateTimeForFilename(queryParams.endTime);
                        timeRange = `${start}_${end}`;
                    } else if (queryParams.startTime) {
                        timeRange = formatDateTimeForFilename(queryParams.startTime);
                    } else if (queryParams.endTime) {
                        timeRange = formatDateTimeForFilename(queryParams.endTime);
                    }

                    // 添加案件类型
                    const caseType = queryParams.caseTypeText || '全部类型';

                    fileName = `案件详情_${timeRange}_${caseType}`;
                }

                // 添加.csv扩展名
                fileName += '.csv';

                // 创建并下载文件
                const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
                const link = document.createElement('a');
                const url = URL.createObjectURL(blob);
                link.setAttribute('href', url);
                link.setAttribute('download', fileName);
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            } else {
                alert('没有可导出的数据');
            }
        })
        .catch(error => {
            console.error('导出出错:', error);
            alert('导出出错，请稍后重试');
        });
}

// 导出CSV功能 - 人员详情（列顺序跟随数据库）
function exportPersonToCSV() {
    const queryParams = window.currentPersonQueryParams;
    if (!queryParams || !queryParams.columns || queryParams.columns.length === 0) {
        alert('没有可导出的数据');
        return;
    }

    let csvContent = '';
    // 表头
    csvContent += queryParams.columns.join(',') + '\n';

    const params = new URLSearchParams();
    if (queryParams.caseType) params.append('case_type', queryParams.caseType);
    if (queryParams.startTime) params.append('start_time', queryParams.startTime);
    if (queryParams.endTime) params.append('end_time', queryParams.endTime);

    fetch(`/api/case_ry_details?${params.toString()}`)
        .then(response => response.json())
        .then(result => {
            if (result.success && result.data && result.data.length > 0) {
                result.data.forEach(item => {
                    const rowData = [];
                    queryParams.columns.forEach(col => {
                        let cellData = item[col] || '';
                        cellData = String(cellData);
                        if (cellData.includes(',') || cellData.includes('"')) {
                            cellData = `"${cellData.replace(/"/g, '""')}"`;
                        }
                        rowData.push(cellData);
                    });
                    csvContent += rowData.join(',') + '\n';
                });

                // 文件名
                let fileName = '人员详情';
                let timeRange = '';
                if (queryParams.startTime && queryParams.endTime) {
                    timeRange = `${formatDateTimeForFilename(queryParams.startTime)}_${formatDateTimeForFilename(queryParams.endTime)}`;
                } else if (queryParams.startTime) {
                    timeRange = formatDateTimeForFilename(queryParams.startTime);
                } else if (queryParams.endTime) {
                    timeRange = formatDateTimeForFilename(queryParams.endTime);
                }
                const caseTypeText = queryParams.caseTypeText || '全部类型';
                fileName = `人员详情_${timeRange}_${caseTypeText}.csv`;

                const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
                const link = document.createElement('a');
                const url = URL.createObjectURL(blob);
                link.setAttribute('href', url);
                link.setAttribute('download', fileName);
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            } else {
                alert('没有可导出的数据');
            }
        })
        .catch(err => {
            console.error('导出失败', err);
            alert('导出失败，请稍后重试');
        });
}

// 格式化日期时间用于文件名
function formatDateTimeForFilename(datetimeStr) {
    if (!datetimeStr) return '';
    const date = new Date(datetimeStr);
    return date.toISOString().slice(0, 19).replace(/:/g, '-').replace('T', '_');
}

function formatDateTime(datetimeStr) {
    const date = new Date(datetimeStr);
    return date.toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    }).replace(/\//g, '-');
}

// 计算合计行数据
function calculateTotals(data, displayFields) {
    const totals = {};

    // 初始化所有字段的合计值为0
    displayFields.forEach(field => {
        totals[field] = 0;
    });

    // 遍历所有数据行，累加数值字段
    data.forEach(item => {
        displayFields.forEach(field => {
            // 跳过地区字段（第一列），只计算数值字段
            if (field !== "地区") {
                const value = item[field];
                // 尝试将值转换为数字，如果不是数字则忽略
                const numValue = parseFloat(value);
                if (!isNaN(numValue)) {
                    totals[field] += numValue;
                }
            }
        });
    });

    return totals;
}

// 加载案件类型数据
function loadCaseTypes() {
    const caseTypeSelect = document.getElementById('caseType');
    const caseTypeDetailsSelect = document.getElementById('caseTypeDetails');
    const caseTypePersonSelect = document.getElementById('caseTypePerson');

    // 首页等不含案件查询模块时，直接跳过加载，避免空节点报错
    if (!caseTypeSelect && !caseTypeDetailsSelect && !caseTypePersonSelect) {
        return;
    }

    // 发起AJAX请求获取案件类型数据
    fetch('/api/case_types')
        .then(response => response.json())
        .then(result => {
            if (result.success) {
                // 清空现有选项（保留"全部类型"选项）
                if (caseTypeSelect) caseTypeSelect.innerHTML = '<option value="">全部类型</option>';
                if (caseTypeDetailsSelect) caseTypeDetailsSelect.innerHTML = '<option value="">全部类型</option>';
                if (caseTypePersonSelect) caseTypePersonSelect.innerHTML = '<option value="">全部类型</option>';

                // 添加从数据库获取的选项
                result.data.forEach(item => {
                    if (caseTypeSelect) {
                        const option1 = document.createElement('option');
                        option1.value = item.leixing;
                        option1.textContent = item.leixing;
                        caseTypeSelect.appendChild(option1);
                    }

                    if (caseTypeDetailsSelect) {
                        const option2 = document.createElement('option');
                        option2.value = item.leixing;
                        option2.textContent = item.leixing;
                        caseTypeDetailsSelect.appendChild(option2);
                    }

                    if (caseTypePersonSelect) {
                        const option3 = document.createElement('option');
                        option3.value = item.leixing;
                        option3.textContent = item.leixing;
                        caseTypePersonSelect.appendChild(option3);
                    }
                });
            } else {
                console.error('获取案件类型数据失败:', result.message);
            }
        })
        .catch(error => {
            console.error('获取案件类型数据出错:', error);
        });
}


