(function () {
  'use strict';

  var endpoints = window.__MDJFJQFX_ENDPOINTS__ || {};
  var hiddenCols = { group_code: true, ori_code: true, confirm_code: true };
  var detailState = { params: null, page: 1, pageSize: 20, total: 0 };
  var finePager = { rows: [], page: 1, pageSize: 10 };
  var detailColWidths = [150, 165, 135, 95, 135, 135, 260, 380, 380, 140, 140, 90, 180];

  function pad(value) { return String(value).padStart(2, '0'); }
  function fmtDate(date) {
    return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate()) +
      ' ' + pad(date.getHours()) + ':' + pad(date.getMinutes()) + ':' + pad(date.getSeconds());
  }
  function esc(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  function setStatus(message, isError) {
    var el = document.getElementById('mdjfjqfxStatus');
    el.textContent = message || '';
    el.classList.toggle('error', Boolean(isError));
  }
  function buildQs(params) {
    var search = new URLSearchParams();
    Object.keys(params).forEach(function (key) {
      var value = params[key];
      if (Array.isArray(value)) {
        value.forEach(function (item) { if (item !== '') search.append(key, item); });
      } else if (value !== null && value !== undefined && value !== '') {
        search.append(key, value);
      }
    });
    return search.toString();
  }
  function selectedBranches() {
    return Array.prototype.map.call(
      document.querySelectorAll('.mdjfjqfx-branch-cb:checked'),
      function (item) { return item.value; }
    );
  }
  function currentParams() {
    return {
      start_time: document.getElementById('mdjfjqfxStart').value.trim(),
      end_time: document.getElementById('mdjfjqfxEnd').value.trim(),
      group_by: document.getElementById('mdjfjqfxGroupBy').value,
      repeat_min: document.getElementById('mdjfjqfxRepeatMin').value,
      ssfjdm: selectedBranches()
    };
  }
  function postHeight() {
    window.requestAnimationFrame(function () {
      if (window.parent && window.parent !== window) {
        window.parent.postMessage({ type: 'mdj-mdjfjqfx-height', height: document.body.scrollHeight || 760 }, '*');
      }
    });
  }

  function initTimes() {
    var end = new Date();
    end = new Date(end.getFullYear(), end.getMonth(), end.getDate(), 0, 0, 0);
    var start = new Date(end.getTime() - 8 * 24 * 60 * 60 * 1000);
    document.getElementById('mdjfjqfxStart').value = fmtDate(start);
    document.getElementById('mdjfjqfxEnd').value = fmtDate(end);
    if (typeof window.flatpickr === 'function') {
      var locale = (window.flatpickr.l10ns && (window.flatpickr.l10ns.zh || window.flatpickr.l10ns.zh_cn)) || 'zh';
      var cfg = { enableTime: true, enableSeconds: true, time_24hr: true, allowInput: true, dateFormat: 'Y-m-d H:i:S', locale: locale };
      window.flatpickr(document.getElementById('mdjfjqfxStart'), cfg);
      window.flatpickr(document.getElementById('mdjfjqfxEnd'), cfg);
    }
  }

  function updateBranchText() {
    var checked = Array.prototype.map.call(document.querySelectorAll('.mdjfjqfx-branch-cb:checked'), function (item) {
      return item.dataset.label || item.value;
    });
    document.getElementById('mdjfjqfxBranchText').textContent =
      checked.length ? (checked.length <= 2 ? checked.join('、') : checked.slice(0, 2).join('、') + ' 等' + checked.length + '项') : '全部分局';
  }
  function loadOptions() {
    fetch(endpoints.options)
      .then(function (res) { return res.json(); })
      .then(function (payload) {
        if (!payload.success) throw new Error(payload.message || '选项加载失败');
        var menu = document.getElementById('mdjfjqfxBranchMenu');
        var branches = (payload.data && payload.data.branches) || [];
        menu.innerHTML = branches.map(function (item) {
          return '<label class="mdjfjqfx-ms-option"><input type="checkbox" class="mdjfjqfx-branch-cb" value="' +
            esc(item.value) + '" data-label="' + esc(item.label) + '" /> ' + esc(item.label) + '</label>';
        }).join('');
        menu.querySelectorAll('input').forEach(function (input) { input.addEventListener('change', updateBranchText); });
        updateBranchText();
      })
      .catch(function (err) { setStatus(err.message, true); });
  }

  function metricMap(tableName) {
    if (tableName === 'overall') {
      return {
        '原始警情数': 'original_total',
        '转案数': 'converted',
        '原始确认均纠纷性质': 'both_mdj',
        '重复警情数': 'repeat',
        '重复警情转案数': 'repeat_converted'
      };
    }
    if (tableName === 'fine') {
      return { '警情数': 'fine', '转案数': 'fine_converted' };
    }
    return { '重复警情数': 'repeat', '重复转案数': 'repeat_converted' };
  }
  function renderTable(targetId, rows, tableName) {
    var target = document.getElementById(targetId);
    if (!rows || !rows.length) {
      target.innerHTML = '<div class="mdjfjqfx-empty">暂无数据</div>';
      return;
    }
    var cols = Object.keys(rows[0]).filter(function (key) { return !hiddenCols[key]; });
    var links = metricMap(tableName);
    var html = '<div class="mdjfjqfx-table-wrap"><table class="mdjfjqfx-table"><thead><tr>' +
      cols.map(function (col) { return '<th>' + esc(col) + '</th>'; }).join('') +
      '</tr></thead><tbody>';
    rows.forEach(function (row, rowIndex) {
      html += '<tr>';
      cols.forEach(function (col) {
        var dimension = links[col];
        var value = row[col];
        var clickable = dimension && Number(value || 0) > 0;
        html += clickable
          ? '<td class="mdjfjqfx-link" data-table="' + tableName + '" data-row="' + rowIndex + '" data-dimension="' + dimension + '">' + esc(value) + '</td>'
          : '<td>' + esc(value) + '</td>';
      });
      html += '</tr>';
    });
    html += '</tbody></table></div>';
    target.innerHTML = html;
    target.querySelectorAll('.mdjfjqfx-link').forEach(function (cell) {
      cell.addEventListener('click', function () {
        openDetail(rows[Number(cell.dataset.row)], cell.dataset.dimension, tableName);
      });
    });
  }

  function readFinePageSize() {
    var selector = document.getElementById('mdjfjqfxFinePageSize');
    var parsed = selector ? Number(selector.value) : finePager.pageSize;
    return Number.isFinite(parsed) ? parsed : 10;
  }

  function renderFineTable() {
    finePager.pageSize = readFinePageSize();
    var total = finePager.rows.length;
    var totalPages = finePager.pageSize <= 0 ? 1 : Math.max(1, Math.ceil(total / finePager.pageSize));
    finePager.page = Math.max(1, Math.min(finePager.page, totalPages));

    var pageRows = finePager.rows;
    if (finePager.pageSize > 0) {
      var start = (finePager.page - 1) * finePager.pageSize;
      pageRows = finePager.rows.slice(start, start + finePager.pageSize);
    }
    renderTable('mdjfjqfxFine', pageRows, 'fine');
    updateFinePager(total, totalPages);
  }

  function updateFinePager(total, totalPages) {
    var prev = document.getElementById('mdjfjqfxFinePrev');
    var next = document.getElementById('mdjfjqfxFineNext');
    var info = document.getElementById('mdjfjqfxFinePageInfo');
    if (!prev || !next || !info) return;

    var allMode = finePager.pageSize <= 0;
    prev.disabled = allMode || finePager.page <= 1 || total === 0;
    next.disabled = allMode || finePager.page >= totalPages || total === 0;
    info.textContent = total === 0
      ? '第 0 / 0 页，共 0 组'
      : '第 ' + finePager.page + ' / ' + totalPages + ' 页，共 ' + total + ' 组';
  }

  function loadSummary() {
    setStatus('正在查询，请稍候...', false);
    document.getElementById('mdjfjqfxQuery').disabled = true;
    fetch(endpoints.summary + '?' + buildQs(currentParams()))
      .then(function (res) { return res.json(); })
      .then(function (payload) {
        document.getElementById('mdjfjqfxQuery').disabled = false;
        if (!payload.success) throw new Error(payload.message || '查询失败');
        var data = payload.data || {};
        renderTable('mdjfjqfxOverall', data.overall || [], 'overall');
        finePager.rows = data.fine || [];
        finePager.page = 1;
        renderFineTable();
        renderTable('mdjfjqfxRepeat', data.repeat || [], 'repeat');
        setStatus('统计时段：' + (data.start_time || '') + ' 至 ' + (data.end_time || ''), false);
        postHeight();
      })
      .catch(function (err) {
        document.getElementById('mdjfjqfxQuery').disabled = false;
        setStatus(err.message || '查询失败', true);
        postHeight();
      });
  }

  function openDetail(row, dimension, tableName) {
    detailState.params = Object.assign({}, currentParams(), {
      dimension: dimension,
      group_code: row.group_code || '__TOTAL__',
      ori_code: row.ori_code || '',
      confirm_code: row.confirm_code || ''
    });
    detailState.page = 1;
    document.getElementById('mdjfjqfxDrawerTitle').textContent =
      (row['分组'] || '总计') + ' - ' + dimension + ' 明细';
    document.getElementById('mdjfjqfxDrawer').classList.add('open');
    document.body.classList.add('mdjfjqfx-drawer-lock');
    loadDetailPage();
  }
  function loadDetailPage() {
    var params = Object.assign({}, detailState.params, { page: detailState.page, page_size: detailState.pageSize });
    document.getElementById('mdjfjqfxDetailBody').innerHTML = '<div class="mdjfjqfx-empty">正在加载详情...</div>';
    fetch(endpoints.detail + '?' + buildQs(params))
      .then(function (res) { return res.json(); })
      .then(function (payload) {
        if (!payload.success) throw new Error(payload.message || '详情查询失败');
        var data = payload.data || {};
        detailState.total = Number(data.total || 0);
        renderDetailRows(data.rows || []);
        var totalPages = detailState.pageSize <= 0 ? 1 : Math.max(1, Math.ceil(detailState.total / detailState.pageSize));
        document.getElementById('mdjfjqfxDrawerMeta').textContent = '共 ' + detailState.total + ' 条';
        document.getElementById('mdjfjqfxPageInfo').textContent = '第 ' + detailState.page + ' / ' + totalPages + ' 页';
        document.getElementById('mdjfjqfxPrev').disabled = detailState.page <= 1;
        document.getElementById('mdjfjqfxNext').disabled = detailState.page >= totalPages;
        postHeight();
      })
      .catch(function (err) {
        document.getElementById('mdjfjqfxDetailBody').innerHTML = '<div class="mdjfjqfx-empty">' + esc(err.message) + '</div>';
      });
  }
  function renderDetailRows(rows) {
    var body = document.getElementById('mdjfjqfxDetailBody');
    if (!rows.length) {
      body.innerHTML = '<div class="mdjfjqfx-empty">暂无详情数据</div>';
      return;
    }
    var cols = Object.keys(rows[0]);
    var colgroup = '<colgroup>' + cols.map(function (_col, index) {
      return '<col style="width:' + (detailColWidths[index] || 160) + 'px" />';
    }).join('') + '</colgroup>';
    body.innerHTML = '<div class="mdjfjqfx-table-wrap mdjfjqfx-detail-table-wrap"><table class="mdjfjqfx-table mdjfjqfx-detail-table">' + colgroup + '<thead><tr>' +
      cols.map(function (col) { return '<th>' + esc(col) + '</th>'; }).join('') +
      '</tr></thead><tbody>' + rows.map(function (row) {
        return '<tr>' + cols.map(function (col) {
          var value = esc(row[col]);
          return '<td><div class="mdjfjqfx-detail-cell" title="' + value + '">' + value + '</div></td>';
        }).join('') + '</tr>';
      }).join('') + '</tbody></table></div>';
  }
  function closeDrawer() {
    document.getElementById('mdjfjqfxDrawer').classList.remove('open');
    document.body.classList.remove('mdjfjqfx-drawer-lock');
  }
  function exportUrl(base, params) {
    window.open(base + '?' + buildQs(params), '_blank');
  }

  function initEvents() {
    var branchMs = document.getElementById('mdjfjqfxBranchMs');
    document.getElementById('mdjfjqfxBranchText').addEventListener('click', function (event) {
      event.stopPropagation();
      document.getElementById('mdjfjqfxBranchMenu').classList.toggle('open');
    });
    document.addEventListener('click', function (event) {
      if (!branchMs.contains(event.target)) document.getElementById('mdjfjqfxBranchMenu').classList.remove('open');
    });
    document.getElementById('mdjfjqfxQuery').addEventListener('click', loadSummary);
    document.getElementById('mdjfjqfxFinePageSize').addEventListener('change', function () {
      finePager.page = 1;
      renderFineTable();
      postHeight();
    });
    document.getElementById('mdjfjqfxFinePrev').addEventListener('click', function () {
      if (finePager.page > 1) {
        finePager.page -= 1;
        renderFineTable();
        postHeight();
      }
    });
    document.getElementById('mdjfjqfxFineNext').addEventListener('click', function () {
      var totalPages = finePager.pageSize <= 0 ? 1 : Math.max(1, Math.ceil(finePager.rows.length / finePager.pageSize));
      if (finePager.page < totalPages) {
        finePager.page += 1;
        renderFineTable();
        postHeight();
      }
    });
    document.getElementById('mdjfjqfxExportSummary').addEventListener('click', function () {
      exportUrl(endpoints.exportSummary, currentParams());
    });
    document.getElementById('mdjfjqfxExportDetails').addEventListener('click', function () {
      exportUrl(endpoints.exportDetails, currentParams());
    });
    document.getElementById('mdjfjqfxExportDetail').addEventListener('click', function () {
      if (detailState.params) exportUrl(endpoints.exportDetail, detailState.params);
    });
    document.getElementById('mdjfjqfxCloseDrawer').addEventListener('click', closeDrawer);
    document.getElementById('mdjfjqfxDrawerShade').addEventListener('click', closeDrawer);
    document.getElementById('mdjfjqfxPrev').addEventListener('click', function () {
      if (detailState.page > 1) { detailState.page -= 1; loadDetailPage(); }
    });
    document.getElementById('mdjfjqfxNext').addEventListener('click', function () {
      var totalPages = Math.max(1, Math.ceil(detailState.total / detailState.pageSize));
      if (detailState.page < totalPages) { detailState.page += 1; loadDetailPage(); }
    });
  }

  function init() {
    initTimes();
    initEvents();
    loadOptions();
    loadSummary();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
