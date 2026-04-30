(function () {
  'use strict';

  var endpoints = window.__TYGJ_BACKGROUND_ENDPOINTS__ || {};
  var uploadToken = '';

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
    $('checkBtn').disabled = isBusy || !uploadToken || !$('idColumnSelect').value;
  }

  function show(el, visible) {
    el.hidden = !visible;
  }

  function resetResults() {
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

  function populateColumns(data) {
    var select = $('idColumnSelect');
    select.innerHTML = '<option value="">请选择身份证列</option>' +
      (data.columns || []).map(function (col) {
        return '<option value="' + esc(col.index) + '">' + esc(col.display) + '</option>';
      }).join('');
    select.disabled = false;
    uploadToken = data.token || '';
    $('checkBtn').disabled = !uploadToken;
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
    if (!uploadToken || !$('idColumnSelect').value) {
      setStatus('请选择身份证列', true);
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
    renderStats(data.stats || {});
    renderTable('overviewTable', data.overview || []);
    renderTable('invalidTable', data.invalid_samples || []);
    renderTable('priorCaseTable', details.prior_case || []);
    renderTable('disputeTable', details.dispute || []);
    renderTable('mentalHealthTable', details.mental_health || []);
    show($('overviewCard'), true);
    show($('invalidCard'), Boolean((data.invalid_samples || []).length));
    show($('detailCard'), true);
    setStatus('审查完成', false);
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
    $('idColumnSelect').addEventListener('change', function () {
      $('checkBtn').disabled = !uploadToken || !$('idColumnSelect').value;
    });
    $('bgFile').addEventListener('change', function () {
      uploadToken = '';
      $('checkBtn').disabled = true;
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
