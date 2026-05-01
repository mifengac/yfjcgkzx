(function () {
  'use strict';

  var endpoints = window.__TYGJ_BACKGROUND_ENDPOINTS__ || {};
  var uploadToken = '';
  var lastResult = null;

  function $(id) {
    return document.getElementById(id);
  }

  function esc(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function setStatus(message, isError) {
    var el = $('bgStatus');
    el.textContent = message || '';
    el.classList.toggle('error', Boolean(isError));
  }

  function setBusy(isBusy) {
    $('loadColumnsBtn').disabled = isBusy;
    $('checkBtn').disabled = isBusy || !canCheck();
    updateExportState(isBusy);
  }

  function canCheck() {
    return Boolean(
      uploadToken &&
      $('nameColumnSelect').value &&
      $('idColumnSelect').value &&
      $('nameColumnSelect').value !== $('idColumnSelect').value
    );
  }

  function hasExportableRows(data) {
    var details = (data && data.details) || {};
    return Boolean(
      (data && data.overview && data.overview.length) ||
      (details.prior_case && details.prior_case.length) ||
      (details.dispute && details.dispute.length) ||
      (details.mental_health && details.mental_health.length)
    );
  }

  function updateExportState(isBusy) {
    var btn = $('exportBtn');
    var hasRows = hasExportableRows(lastResult);
    btn.hidden = !lastResult;
    btn.disabled = Boolean(isBusy) || !hasRows;
  }

  function show(el, visible) {
    el.hidden = !visible;
  }

  function resetResults() {
    lastResult = null;
    updateExportState(false);
    ['statsCard', 'overviewCard', 'invalidCard', 'detailCard'].forEach(function (id) {
      show($(id), false);
    });
    ['statsGrid', 'overviewTable', 'invalidTable', 'priorCaseTable', 'disputeTable', 'mentalHealthTable'].forEach(function (id) {
      $(id).innerHTML = '';
    });
  }

  function renderTable(targetId, rows) {
    var target = $(targetId);
    if (!rows || !rows.length) {
      target.innerHTML = '<div class="tygj-empty">暂无数据</div>';
      return;
    }
    var cols = Object.keys(rows[0]);
    var html = '<div class="tygj-table-wrap"><table class="tygj-table"><thead><tr>' +
      cols.map(function (col) { return '<th>' + esc(col) + '</th>'; }).join('') +
      '</tr></thead><tbody>';
    rows.forEach(function (row) {
      html += '<tr>' + cols.map(function (col) {
        return '<td>' + esc(row[col]) + '</td>';
      }).join('') + '</tr>';
    });
    html += '</tbody></table></div>';
    target.innerHTML = html;
  }

  function renderStats(stats) {
    var keys = [
      '去重后人数',
      '命中人数',
      '前科命中人数',
      '矛盾纠纷命中人数',
      '精神障碍命中人数',
      '无效身份证行数'
    ];
    $('statsGrid').innerHTML = keys.map(function (key) {
      return '<div class="tygj-stat"><div class="tygj-stat-value">' + esc(stats[key] || 0) +
        '</div><div class="tygj-stat-label">' + esc(key) + '</div></div>';
    }).join('');
    $('statsNote').textContent = '有效身份证行数 ' + (stats['有效身份证行数'] || 0) +
      '，重复身份证行数 ' + (stats['重复身份证行数'] || 0);
    show($('statsCard'), true);
  }

  function columnOptions(columns) {
    return '<option value="">请选择列</option>' +
      (columns || []).map(function (col) {
        return '<option value="' + esc(col.index) + '">' + esc(col.display) + '</option>';
      }).join('');
  }

  function pickColumn(columns, patterns) {
    var matched = (columns || []).find(function (col) {
      var text = String((col.label || '') + ' ' + (col.display || ''));
      return patterns.some(function (pattern) { return pattern.test(text); });
    });
    return matched ? String(matched.index) : '';
  }

  function populateColumns(data) {
    var columns = data.columns || [];
    var nameSelect = $('nameColumnSelect');
    var idSelect = $('idColumnSelect');
    nameSelect.innerHTML = columnOptions(columns);
    idSelect.innerHTML = columnOptions(columns);
    nameSelect.disabled = false;
    idSelect.disabled = false;
    nameSelect.value = pickColumn(columns, [/姓名/, /名字/]);
    idSelect.value = pickColumn(columns, [/身份证/, /公民身份号码/, /证件号码/, /证号/]);
    uploadToken = data.token || '';
    $('checkBtn').disabled = !canCheck();
    setStatus('已加载：' + (data.filename || '') + ' / ' + (data.sheet_name || ''), false);
  }

  function loadColumns() {
    var fileInput = $('bgFile');
    var file = fileInput.files && fileInput.files[0];
    if (!file) {
      setStatus('请先选择 xlsx 文件', true);
      return;
    }
    resetResults();
    uploadToken = '';
    $('nameColumnSelect').disabled = true;
    $('nameColumnSelect').innerHTML = '<option value="">请先加载列</option>';
    $('idColumnSelect').disabled = true;
    $('idColumnSelect').innerHTML = '<option value="">请先加载列</option>';
    setBusy(true);
    setStatus('正在读取表头...', false);

    var form = new FormData();
    form.append('file', file);
    fetch(endpoints.upload, { method: 'POST', body: form })
      .then(function (res) { return res.json().then(function (payload) { return { ok: res.ok, payload: payload }; }); })
      .then(function (result) {
        if (!result.ok || !result.payload.success) {
          throw new Error(result.payload.message || '加载列失败');
        }
        populateColumns(result.payload.data || {});
      })
      .catch(function (err) {
        setStatus(err.message || '加载列失败', true);
      })
      .finally(function () {
        setBusy(false);
      });
  }

  function runCheck() {
    if (!uploadToken || !$('nameColumnSelect').value || !$('idColumnSelect').value) {
      setStatus('请选择姓名列和身份证列', true);
      return;
    }
    if ($('nameColumnSelect').value === $('idColumnSelect').value) {
      setStatus('姓名列和身份证列不能相同', true);
      return;
    }
    resetResults();
    setBusy(true);
    setStatus('正在审查...', false);

    fetch(endpoints.check, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        token: uploadToken,
        name_column_index: $('nameColumnSelect').value,
        id_column_index: $('idColumnSelect').value
      })
    })
      .then(function (res) { return res.json().then(function (payload) { return { ok: res.ok, payload: payload }; }); })
      .then(function (result) {
        if (!result.ok || !result.payload.success) {
          throw new Error(result.payload.message || '审查失败');
        }
        renderResult(result.payload.data || {});
      })
      .catch(function (err) {
        setStatus(err.message || '审查失败', true);
      })
      .finally(function () {
        setBusy(false);
      });
  }

  function renderResult(data) {
    var details = data.details || {};
    lastResult = data;
    renderStats(data.stats || {});
    renderTable('overviewTable', data.overview || []);
    renderTable('invalidTable', data.invalid_samples || []);
    renderTable('priorCaseTable', details.prior_case || []);
    renderTable('disputeTable', details.dispute || []);
    renderTable('mentalHealthTable', details.mental_health || []);
    show($('overviewCard'), true);
    show($('invalidCard'), Boolean((data.invalid_samples || []).length));
    show($('detailCard'), true);
    updateExportState(false);
    setStatus((data.overview || []).length ? '审查完成，已隐藏未命中人员' : '审查完成，暂无命中人员', false);
  }

  function formatTimestamp(date) {
    function pad(value) {
      return String(value).padStart(2, '0');
    }
    return String(date.getFullYear()) +
      pad(date.getMonth() + 1) +
      pad(date.getDate()) +
      pad(date.getHours()) +
      pad(date.getMinutes()) +
      pad(date.getSeconds());
  }

  function exportTableHtml(title, rows) {
    if (!rows || !rows.length) {
      return '';
    }
    var cols = Object.keys(rows[0]);
    var html = '<h2>' + esc(title) + '</h2><table><thead><tr>' +
      cols.map(function (col) { return '<th>' + esc(col) + '</th>'; }).join('') +
      '</tr></thead><tbody>';
    rows.forEach(function (row) {
      html += '<tr>' + cols.map(function (col) {
        return '<td>' + esc(row[col]) + '</td>';
      }).join('') + '</tr>';
    });
    return html + '</tbody></table>';
  }

  function statsRows(stats) {
    return Object.keys(stats || {}).map(function (key) {
      return { 指标: key, 数值: stats[key] };
    });
  }

  function buildExportHtml(data) {
    var details = data.details || {};
    return '<!DOCTYPE html><html><head><meta charset="UTF-8" />' +
      '<style>body{font-family:Microsoft YaHei,Arial,sans-serif;}table{border-collapse:collapse;margin-bottom:18px;}' +
      'th,td{border:1px solid #999;padding:6px 8px;mso-number-format:"\\@";}th{background:#e9f3ef;font-weight:bold;}' +
      'h1{font-size:20px;}h2{font-size:16px;margin-top:18px;}</style></head><body>' +
      '<h1>审查结果</h1>' +
      exportTableHtml('审查概览', statsRows(data.stats || {})) +
      exportTableHtml('人员命中情况', data.overview || []) +
      exportTableHtml('前科', details.prior_case || []) +
      exportTableHtml('矛盾纠纷', details.dispute || []) +
      exportTableHtml('精神障碍', details.mental_health || []) +
      '</body></html>';
  }

  function exportResult() {
    if (!hasExportableRows(lastResult)) {
      setStatus('暂无命中结果可导出', true);
      return;
    }
    var blob = new Blob(['\ufeff', buildExportHtml(lastResult)], {
      type: 'application/vnd.ms-excel;charset=utf-8'
    });
    var link = document.createElement('a');
    var url = URL.createObjectURL(blob);
    link.href = url;
    link.download = '审查结果' + formatTimestamp(new Date()) + '.xls';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    setStatus('导出完成', false);
  }

  function initTabs() {
    document.querySelectorAll('.tygj-tab').forEach(function (tab) {
      tab.addEventListener('click', function () {
        document.querySelectorAll('.tygj-tab').forEach(function (item) { item.classList.remove('active'); });
        document.querySelectorAll('.tygj-tab-panel').forEach(function (panel) { panel.classList.remove('active'); });
        tab.classList.add('active');
        $('tab-' + tab.dataset.tab).classList.add('active');
      });
    });
  }

  function init() {
    initTabs();
    $('loadColumnsBtn').addEventListener('click', loadColumns);
    $('checkBtn').addEventListener('click', runCheck);
    $('exportBtn').addEventListener('click', exportResult);
    $('nameColumnSelect').addEventListener('change', function () {
      $('checkBtn').disabled = !canCheck();
    });
    $('idColumnSelect').addEventListener('change', function () {
      $('checkBtn').disabled = !canCheck();
    });
    $('bgFile').addEventListener('change', function () {
      uploadToken = '';
      $('checkBtn').disabled = true;
      $('nameColumnSelect').disabled = true;
      $('nameColumnSelect').innerHTML = '<option value="">请先加载列</option>';
      $('idColumnSelect').disabled = true;
      $('idColumnSelect').innerHTML = '<option value="">请先加载列</option>';
      resetResults();
      setStatus('', false);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
