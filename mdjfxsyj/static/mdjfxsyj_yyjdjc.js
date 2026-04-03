(function () {
  'use strict';

  var endpoints = window.__MDJ_YYJDJC_ENDPOINTS__ || {};
  var SOURCE_KEYS = ['police', 'workorder', 'dispute'];
  var PAGE_SIZE_OPTIONS = [10, 20, 30, 50, 100, 0];

  var state = {
    loading: false,
    lastPostedHeight: 0,
    heightTimer: 0,
    sources: {
      police: createSourceState(),
      workorder: createSourceState(),
      dispute: createSourceState()
    }
  };

  function createSourceState() {
    return {
      columns: [],
      rows: [],
      count: 0,
      error: '',
      page: 1,
      pageSize: 10,
      collapsed: false
    };
  }

  function pad(value) {
    return String(value).padStart(2, '0');
  }

  function formatDateTime(date) {
    return [
      date.getFullYear(),
      '-',
      pad(date.getMonth() + 1),
      '-',
      pad(date.getDate()),
      ' ',
      pad(date.getHours()),
      ':',
      pad(date.getMinutes()),
      ':',
      pad(date.getSeconds())
    ].join('');
  }

  function buildDefaultRange() {
    var now = new Date();
    var end = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
    var start = new Date(end.getTime() - 8 * 24 * 60 * 60 * 1000);
    return {
      start: formatDateTime(start),
      end: formatDateTime(end)
    };
  }

  function buildQueryString(params) {
    var search = new URLSearchParams();
    Object.keys(params).forEach(function (key) {
      var value = params[key];
      if (value !== null && value !== undefined && value !== '') {
        search.append(key, value);
      }
    });
    return search.toString();
  }

  function getQueryParams() {
    return {
      start_time: document.getElementById('yyjdjcStartTime').value.trim(),
      end_time: document.getElementById('yyjdjcEndTime').value.trim()
    };
  }

  function escapeHtml(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function setStatus(message, isError) {
    var statusEl = document.getElementById('yyjdjcStatus');
    statusEl.textContent = message || '';
    statusEl.classList.toggle('mdj-yyjdjc-status-error', Boolean(isError));
  }

  function toggleBusy(loading) {
    state.loading = loading;
    document.getElementById('yyjdjcQueryBtn').disabled = loading;
    document.getElementById('yyjdjcExportAllBtn').disabled = loading;
    document.querySelectorAll('[data-source-export]').forEach(function (button) {
      button.disabled = loading;
    });
  }

  function getSourceState(key) {
    return state.sources[key];
  }

  function getSourceElements(key) {
    return {
      section: document.querySelector('section[data-source="' + key + '"]'),
      body: document.getElementById('yyjdjc-' + key + '-body'),
      metaEl: document.getElementById('yyjdjc-' + key + '-meta'),
      errorEl: document.getElementById('yyjdjc-' + key + '-error'),
      emptyEl: document.getElementById('yyjdjc-' + key + '-empty'),
      wrapEl: document.getElementById('yyjdjc-' + key + '-wrap'),
      tableEl: document.getElementById('yyjdjc-' + key + '-table'),
      paginationEl: document.getElementById('yyjdjc-' + key + '-pagination'),
      collapseButton: document.querySelector('[data-collapse-toggle="' + key + '"]')
    };
  }

  function normalizePageSize(value) {
    var parsed = Number(value);
    if (parsed === 0) {
      return 0;
    }
    return PAGE_SIZE_OPTIONS.indexOf(parsed) >= 0 ? parsed : 10;
  }

  function getColumns(source) {
    if (Array.isArray(source.columns) && source.columns.length) {
      return source.columns.slice();
    }
    if (Array.isArray(source.rows) && source.rows.length && source.rows[0] && typeof source.rows[0] === 'object') {
      return Object.keys(source.rows[0]);
    }
    return [];
  }

  function getRows(source) {
    return Array.isArray(source.rows) ? source.rows : [];
  }

  function getTotalRows(source) {
    return getRows(source).length;
  }

  function getTotalPages(source) {
    var totalRows = getTotalRows(source);
    if (!totalRows) {
      return 0;
    }
    if (source.pageSize === 0) {
      return 1;
    }
    return Math.max(1, Math.ceil(totalRows / source.pageSize));
  }

  function clampPage(source, totalPages) {
    if (totalPages <= 0) {
      return 1;
    }
    var page = Number(source.page || 1);
    if (!Number.isFinite(page) || page < 1) {
      page = 1;
    }
    if (page > totalPages) {
      page = totalPages;
    }
    return page;
  }

  function getVisibleRows(source, totalPages) {
    var rows = getRows(source);
    if (!rows.length) {
      return [];
    }
    if (source.pageSize === 0) {
      return rows;
    }
    var page = clampPage(source, totalPages);
    var startIndex = (page - 1) * source.pageSize;
    return rows.slice(startIndex, startIndex + source.pageSize);
  }

  function syncCollapseState(key, elements) {
    var source = getSourceState(key);
    var collapsed = Boolean(source.collapsed);
    var button = elements && elements.collapseButton ? elements.collapseButton : document.querySelector('[data-collapse-toggle="' + key + '"]');
    var body = elements && elements.body ? elements.body : document.getElementById('yyjdjc-' + key + '-body');
    var section = elements && elements.section ? elements.section : document.querySelector('section[data-source="' + key + '"]');

    if (section) {
      section.classList.toggle('is-collapsed', collapsed);
    }
    if (body) {
      body.hidden = collapsed;
    }
    if (button) {
      button.classList.toggle('is-collapsed', collapsed);
      button.setAttribute('aria-expanded', String(!collapsed));
      button.setAttribute('aria-label', collapsed ? '展开模块' : '收起模块');
      button.title = collapsed ? '展开模块' : '收起模块';
    }
  }

  function renderTable(tableEl, columns, rows) {
    var thead = tableEl.querySelector('thead');
    var tbody = tableEl.querySelector('tbody');

    thead.innerHTML =
      '<tr>' +
      columns
        .map(function (column) {
          return '<th>' + escapeHtml(column) + '</th>';
        })
        .join('') +
      '</tr>';

    tbody.innerHTML = rows
      .map(function (row) {
        return (
          '<tr>' +
          columns
            .map(function (column) {
              var cellValue = row && Object.prototype.hasOwnProperty.call(row, column) ? row[column] : '';
              var text = cellValue === null || cellValue === undefined ? '' : String(cellValue);
              return '<td title="' + escapeHtml(text) + '">' + escapeHtml(text) + '</td>';
            })
            .join('') +
          '</tr>'
        );
      })
      .join('');
  }

  function renderPagination(key, source, totalPages) {
    var paginationEl = document.getElementById('yyjdjc-' + key + '-pagination');
    if (!paginationEl) {
      return;
    }

    var rows = getRows(source);
    if (!rows.length) {
      paginationEl.innerHTML = '';
      return;
    }

    var pageSize = source.pageSize;
    var isAllMode = pageSize === 0;
    var currentPage = clampPage(source, totalPages);
    source.page = currentPage;

    var pageSizeOptionsHtml = PAGE_SIZE_OPTIONS.map(function (option) {
      var label = option === 0 ? '全部' : String(option);
      return '<option value="' + option + '"' + (option === pageSize ? ' selected' : '') + '>' + label + '</option>';
    }).join('');

    var infoText = isAllMode
      ? '共 ' + rows.length + ' 条，全部显示'
      : '第 ' + currentPage + ' / ' + totalPages + ' 页，共 ' + rows.length + ' 条';

    var controlsHtml =
      '<label class="mdj-yyjdjc-page-size-label">每页 ' +
      '<select class="mdj-yyjdjc-page-size-select" data-role="page-size" data-source="' +
      key +
      '">' +
      pageSizeOptionsHtml +
      '</select> 条</label>';

    if (!isAllMode) {
      controlsHtml +=
        '<button type="button" data-role="page-action" data-action="prev" data-source="' +
        key +
        '"' +
        (currentPage <= 1 ? ' disabled' : '') +
        '>上一页</button>' +
        '<button type="button" data-role="page-action" data-action="next" data-source="' +
        key +
        '"' +
        (currentPage >= totalPages ? ' disabled' : '') +
        '>下一页</button>';
    }

    paginationEl.innerHTML =
      '<div class="mdj-yyjdjc-pagination-info">' + escapeHtml(infoText) + '</div>' +
      '<div class="mdj-yyjdjc-pagination-controls">' +
      controlsHtml +
      '</div>';
  }

  function renderSource(key) {
    var source = getSourceState(key);
    var elements = getSourceElements(key);
    var rows = getRows(source);
    var columns = getColumns(source);
    var totalRows = rows.length;
    var hasError = Boolean(source.error);

    syncCollapseState(key, elements);

    if (elements.metaEl) {
      elements.metaEl.textContent = '共 ' + totalRows + ' 条';
    }

    if (hasError) {
      if (elements.errorEl) {
        elements.errorEl.hidden = false;
        elements.errorEl.textContent = source.error;
      }
      if (elements.emptyEl) {
        elements.emptyEl.hidden = true;
      }
      if (elements.wrapEl) {
        elements.wrapEl.hidden = true;
      }
      if (elements.tableEl) {
        elements.tableEl.querySelector('thead').innerHTML = '';
        elements.tableEl.querySelector('tbody').innerHTML = '';
      }
      if (elements.paginationEl) {
        elements.paginationEl.innerHTML = '';
      }
      return;
    }

    if (elements.errorEl) {
      elements.errorEl.hidden = true;
      elements.errorEl.textContent = '';
    }

    if (!totalRows) {
      if (elements.emptyEl) {
        elements.emptyEl.hidden = false;
      }
      if (elements.wrapEl) {
        elements.wrapEl.hidden = true;
      }
      if (elements.tableEl) {
        elements.tableEl.querySelector('thead').innerHTML = '';
        elements.tableEl.querySelector('tbody').innerHTML = '';
      }
      if (elements.paginationEl) {
        elements.paginationEl.innerHTML = '';
      }
      return;
    }

    if (elements.emptyEl) {
      elements.emptyEl.hidden = true;
    }
    if (elements.wrapEl) {
      elements.wrapEl.hidden = false;
    }

    var totalPages = getTotalPages(source);
    var visibleRows = getVisibleRows(source, totalPages);

    if (elements.tableEl) {
      renderTable(elements.tableEl, columns, visibleRows);
    }
    renderPagination(key, source, totalPages);
  }

  function renderAllSources() {
    SOURCE_KEYS.forEach(function (key) {
      renderSource(key);
    });
  }

  function postHeight() {
    var height = Math.max(document.documentElement.scrollHeight || 0, document.body.scrollHeight || 0);
    if (!height || Math.abs(height - state.lastPostedHeight) <= 1) {
      return;
    }
    state.lastPostedHeight = height;
    if (window.parent && window.parent !== window) {
      window.parent.postMessage({ type: 'mdj-yyjdjc-height', height: height }, '*');
    }
  }

  function schedulePostHeight() {
    if (state.heightTimer) {
      window.cancelAnimationFrame(state.heightTimer);
    }
    state.heightTimer = window.requestAnimationFrame(function () {
      state.heightTimer = window.requestAnimationFrame(postHeight);
    });
  }

  function applySourceData(key, sourceData) {
    var source = getSourceState(key);
    source.columns = Array.isArray(sourceData.columns) ? sourceData.columns.slice() : [];
    source.rows = Array.isArray(sourceData.rows) ? sourceData.rows.slice() : [];
    source.count = source.rows.length;
    source.error = sourceData.error || '';
    source.page = 1;
  }

  async function loadData() {
    var params = getQueryParams();
    toggleBusy(true);
    setStatus('正在查询，请稍候...', false);

    try {
      var response = await fetch(endpoints.apiData + '?' + buildQueryString(params));
      var payload;
      try {
        payload = await response.json();
      } catch (parseError) {
        throw new Error('查询失败，服务端返回格式异常');
      }

      if (!response.ok || !payload.success) {
        throw new Error(payload.message || '查询失败');
      }

      var data = payload.data || {};
      var sources = data.sources || {};

      SOURCE_KEYS.forEach(function (key) {
        applySourceData(key, sources[key] || {});
      });

      renderAllSources();

      var totalCount = SOURCE_KEYS.reduce(function (sum, key) {
        return sum + getTotalRows(getSourceState(key));
      }, 0);

      setStatus(
        '查询完成，时间范围：' +
          (data.start_time || '') +
          ' 至 ' +
          (data.end_time || '') +
          '，共 ' +
          totalCount +
          ' 条。',
        false
      );
      schedulePostHeight();
    } catch (error) {
      setStatus(error.message || '查询失败', true);
      schedulePostHeight();
    } finally {
      toggleBusy(false);
    }
  }

  function triggerAllExport() {
    var params = getQueryParams();
    window.open(endpoints.exportAll + '?' + buildQueryString(params), '_blank');
  }

  function triggerSourceExport(source, format) {
    var params = getQueryParams();
    params.source = source;
    params.format = format;
    window.open(endpoints.exportSource + '?' + buildQueryString(params), '_blank');
  }

  function updatePageSize(key, value) {
    var source = getSourceState(key);
    source.pageSize = normalizePageSize(value);
    source.page = 1;
    renderSource(key);
    schedulePostHeight();
  }

  function handlePageAction(key, action) {
    var source = getSourceState(key);
    var totalPages = getTotalPages(source);
    if (!totalPages) {
      return;
    }

    if (action === 'prev' && source.page > 1) {
      source.page -= 1;
    } else if (action === 'next' && source.page < totalPages) {
      source.page += 1;
    } else {
      return;
    }

    renderSource(key);
    schedulePostHeight();
  }

  function toggleSourceCollapse(key) {
    var source = getSourceState(key);
    source.collapsed = !source.collapsed;
    renderSource(key);
    schedulePostHeight();
  }

  function initFlatpickr() {
    var defaults = buildDefaultRange();
    var startInput = document.getElementById('yyjdjcStartTime');
    var endInput = document.getElementById('yyjdjcEndTime');

    if (typeof window.flatpickr !== 'function') {
      startInput.value = defaults.start;
      endInput.value = defaults.end;
      return;
    }

    var locale = (window.flatpickr.l10ns && (window.flatpickr.l10ns.zh || window.flatpickr.l10ns.zh_cn)) || 'zh';
    var commonConfig = {
      enableTime: true,
      enableSeconds: true,
      time_24hr: true,
      allowInput: true,
      dateFormat: 'Y-m-d H:i:S',
      locale: locale
    };

    var startPicker = window.flatpickr(startInput, commonConfig);
    var endPicker = window.flatpickr(endInput, commonConfig);
    startPicker.setDate(defaults.start, true, 'Y-m-d H:i:S');
    endPicker.setDate(defaults.end, true, 'Y-m-d H:i:S');
  }

  function initEvents() {
    document.getElementById('yyjdjcQueryBtn').addEventListener('click', loadData);
    document.getElementById('yyjdjcExportAllBtn').addEventListener('click', triggerAllExport);

    document.querySelectorAll('[data-collapse-toggle]').forEach(function (button) {
      button.addEventListener('click', function () {
        toggleSourceCollapse(button.dataset.collapseToggle);
      });
    });

    document.addEventListener('click', function (event) {
      var pageActionButton = event.target.closest('[data-role="page-action"]');
      if (pageActionButton) {
        handlePageAction(pageActionButton.dataset.source, pageActionButton.dataset.action);
        return;
      }

      var exportButton = event.target.closest('[data-source-export]');
      if (exportButton) {
        var details = exportButton.closest('details');
        if (details) {
          details.open = false;
        }
        triggerSourceExport(exportButton.dataset.sourceExport, exportButton.dataset.format);
      }
    });

    document.addEventListener('change', function (event) {
      var target = event.target;
      if (target && target.matches('[data-role="page-size"]')) {
        updatePageSize(target.dataset.source, target.value);
      }
    });

    document.querySelectorAll('details.mdj-yyjdjc-export').forEach(function (details) {
      details.addEventListener('toggle', schedulePostHeight);
    });

    SOURCE_KEYS.forEach(function (key) {
      syncCollapseState(key);
    });
  }

  function init() {
    initFlatpickr();
    initEvents();
    schedulePostHeight();
    loadData();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
