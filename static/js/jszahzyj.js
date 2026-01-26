/**
 * 精神障碍患者预警模块前端脚本
 */

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    // 初始化默认时间
    initDefaultTimes();

    // 初始化分局多选框
    initFenjuSelect();

    // 初始化导出按钮
    initExportButton();

    // 更新分局选择显示文本
    updateFenjuSelectedText();
});

/**
 * 初始化默认时间
 * 所有时间控件初始值为空
 */
function initDefaultTimes() {
    // 不设置默认值，保持输入框为空
    // 用户需要手动选择时间
}

/**
 * 初始化分局多选框
 */
function initFenjuSelect() {
    const selectBox = document.getElementById('fenjuSelectBox');
    const selectMenu = document.getElementById('fenjuSelectMenu');
    const checkboxes = selectMenu.querySelectorAll('input[type="checkbox"]');

    // 点击选择框显示/隐藏菜单
    selectBox.addEventListener('click', function(e) {
        e.stopPropagation();
        selectMenu.classList.toggle('show');
    });

    // 点击复选框时更新显示文本
    checkboxes.forEach(function(checkbox) {
        checkbox.addEventListener('change', function() {
            updateFenjuSelectedText();
        });
    });

    // 点击页面其他地方关闭菜单
    document.addEventListener('click', function(e) {
        if (!selectBox.contains(e.target) && !selectMenu.contains(e.target)) {
            selectMenu.classList.remove('show');
        }
    });
}

/**
 * 更新分局选择显示文本
 */
function updateFenjuSelectedText() {
    const selectMenu = document.getElementById('fenjuSelectMenu');
    const selectedText = document.getElementById('fenjuSelectedText');
    const checkboxes = selectMenu.querySelectorAll('input[type="checkbox"]:checked');

    if (checkboxes.length === 0) {
        selectedText.textContent = '请选择分局';
    } else {
        const selected = Array.from(checkboxes).map(cb => cb.value);
        selectedText.textContent = selected.join(', ');
    }
}

/**
 * 初始化导出按钮
 */
function initExportButton() {
    const exportBtn = document.getElementById('exportBtn');
    const exportMenu = document.getElementById('exportMenu');

    // 点击导出按钮显示/隐藏菜单
    exportBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        exportMenu.classList.toggle('show');
    });

    // 点击页面其他地方关闭菜单
    document.addEventListener('click', function(e) {
        if (!exportBtn.contains(e.target) && !exportMenu.contains(e.target)) {
            exportMenu.classList.remove('show');
        }
    });
}

/**
 * 导出数据
 * @param {string} format 导出格式 ('xlsx' 或 'csv')
 */
function exportData(format) {
    // 获取当前查询参数
    const liguanStart = document.getElementById('liguan_start').value;
    const liguanEnd = document.getElementById('liguan_end').value;
    const maodunStart = document.getElementById('maodun_start').value;
    const maodunEnd = document.getElementById('maodun_end').value;

    // 获取选中的分局
    const selectMenu = document.getElementById('fenjuSelectMenu');
    const checkboxes = selectMenu.querySelectorAll('input[type="checkbox"]:checked');
    const fenjuList = Array.from(checkboxes).map(cb => cb.value);

    // 验证必填参数
    if (!liguanStart || !liguanEnd || !maodunStart || !maodunEnd || fenjuList.length === 0) {
        alert('请先设置查询条件并执行查询');
        return;
    }

    // 构建导出URL
    const params = new URLSearchParams();
    params.append('format', format);
    params.append('liguan_start', liguanStart);
    params.append('liguan_end', liguanEnd);
    params.append('maodun_start', maodunStart);
    params.append('maodun_end', maodunEnd);
    fenjuList.forEach(fenju => params.append('fenju', fenju));

    // 生成导出URL并下载
    const exportUrl = `/jszahzyj/export?${params.toString()}`;
    window.location.href = exportUrl;

    // 关闭导出菜单
    document.getElementById('exportMenu').classList.remove('show');
}

/**
 * 格式化日期时间为显示格式
 * @param {string} dateTimeStr
 * @returns {string}
 */
function formatDateTime(dateTimeStr) {
    if (!dateTimeStr) return '';
    const date = new Date(dateTimeStr);
    if (isNaN(date.getTime())) return dateTimeStr;

    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}
