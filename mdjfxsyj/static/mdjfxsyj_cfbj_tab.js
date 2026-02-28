/**
 * mdjfxsyj_cfbj_tab.js
 * 矛盾纠纷重复报警统计 Tab 前端逻辑
 *
 * 依赖全局变量：window.__MDJ_CFBJ_ENDPOINTS__（在主模板中注入）
 *   apiSummary  — 统计 API
 *   apiDetail   — 明细 API
 *   exportData  — 导出接口
 *   modalDetail — 弹出框 iframe 页面
 */
(function () {
  'use strict';

  /* -----------------------------------------------------------------------
   * 明细列固定顺序（与 SQL 查询顺序一致）
   * --------------------------------------------------------------------- */
  var DETAIL_COLS = [
    '警情编号', '警情性质', '原始警情性质', '报警时间', '报警人联系电话',
    '报警电话次数', '报警地址', '报警内容', '处警情况',
    '所属市局', '所属分局', '所属派出所'
  ];

  /* -----------------------------------------------------------------------
   * 分页状态
   * --------------------------------------------------------------------- */
  var _detailAllRows  = [];   // 全量明细数据
  var _detailPage     = 1;    // 当前页（1-based）
  var _detailPageSize = 20;   // 每页条数，0=全部

  /* -----------------------------------------------------------------------
   * 工具
   * --------------------------------------------------------------------- */

  /** 格式化日期为 'YYYY-MM-DD HH:MM:SS' */
  function fmtDate(d) {
    var pad = function (n) { return String(n).padStart(2, '0'); };
    return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate())
      + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  }

  /** 构建 query string（支持数组参数） */
  function buildQs(params) {
    var parts = [];
    Object.keys(params).forEach(function (k) {
      var v = params[k];
      if (Array.isArray(v)) {
        v.forEach(function (item) {
          if (item !== null && item !== undefined && item !== '') {
            parts.push(encodeURIComponent(k) + '=' + encodeURIComponent(item));
          }
        });
      } else if (v !== null && v !== undefined && v !== '') {
        parts.push(encodeURIComponent(k) + '=' + encodeURIComponent(v));
      }
    });
    return parts.join('&');
  }

  /** 获取当前查询参数对象 */
  function getQueryParams() {
    return {
      start_time: document.getElementById('cfbjStart').value.trim(),
      end_time:   document.getElementById('cfbjEnd').value.trim(),
      fenju:      getSelectedFenju(),
      min_cs:     document.getElementById('cfbjMinCs').value.trim(),
    };
  }

  /* -----------------------------------------------------------------------
   * 默认时间初始化
   * --------------------------------------------------------------------- */

  function initDefaultTimes() {
    var now   = new Date();
    var today = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
    var start = new Date(today.getTime() - 7 * 24 * 3600 * 1000);
    document.getElementById('cfbjStart').value = fmtDate(start);
    document.getElementById('cfbjEnd').value   = fmtDate(today);
  }

  /* -----------------------------------------------------------------------
   * 分局多选下拉
   * --------------------------------------------------------------------- */

  function getSelectedFenju() {
    var cbs = document.querySelectorAll('.cfbj-fenju-cb:checked');
    return Array.prototype.map.call(cbs, function (cb) { return cb.value; });
  }

  function updateFenjuDisplay() {
    var sel = getSelectedFenju();
    var display = document.getElementById('cfbjFenjuDisplay');
    display.textContent = sel.length ? sel.join('、') : '全部分局';
  }

  function initFenjuDropdown() {
    var ms      = document.getElementById('cfbjFenjuMs');
    var display = document.getElementById('cfbjFenjuDisplay');
    var dropdown = document.getElementById('cfbjFenjuDropdown');

    display.addEventListener('click', function (e) {
      e.stopPropagation();
      dropdown.classList.toggle('open');
    });

    document.querySelectorAll('.cfbj-fenju-cb').forEach(function (cb) {
      cb.addEventListener('change', updateFenjuDisplay);
    });

    document.addEventListener('click', function (e) {
      if (!ms.contains(e.target)) dropdown.classList.remove('open');
    });
  }

  /* -----------------------------------------------------------------------
   * 报警次数校验
   * --------------------------------------------------------------------- */

  function validateMinCs() {
    var val = document.getElementById('cfbjMinCs').value.trim();
    if (!val) return true;  // 空表示不过滤
    var n = parseInt(val, 10);
    if (isNaN(n) || n < 1 || n > 100 || String(n) !== val) {
      document.getElementById('cfbjErr').textContent = '报警次数只允许填写 1-100 之间的整数';
      return false;
    }
    return true;
  }

  /* -----------------------------------------------------------------------
   * 显示/隐藏 loading 状态
   * --------------------------------------------------------------------- */

  function setStatus(msg) {
    document.getElementById('cfbjStatus').textContent = msg || '';
  }
  function setErr(msg) {
    document.getElementById('cfbjErr').textContent = msg || '';
  }

  /* -----------------------------------------------------------------------
   * 表格渲染
   * --------------------------------------------------------------------- */

  var SUMMARY_COLS = ['所属分局', '总数', '重复数', '发生率'];

  /**
   * 渲染统计表（含可点击行 + 总计行加粗）
   */
  function renderSummaryTable(rows) {
    var thead  = document.getElementById('cfbjThead');
    var tbody  = document.getElementById('cfbjTbody');
    var noData = document.getElementById('cfbjNoData');

    if (!rows || rows.length === 0) {
      thead.innerHTML = '';
      tbody.innerHTML = '';
      noData.style.display = '';
      return;
    }
    noData.style.display = 'none';

    thead.innerHTML = '<tr>' + SUMMARY_COLS.map(function (c) {
      return '<th>' + c + '</th>';
    }).join('') + '</tr>';

    var html = rows.map(function (row) {
      var isTotalRow = row['所属分局'] === '总计';
      var rowStyle   = isTotalRow ? ' style="font-weight:800; background:#f0f4ff;"' : '';

      var zdCell = isTotalRow
        ? '<td>' + escHtml(row['总数'] != null ? String(row['总数']) : '') + '</td>'
        : '<td class="cfbj-link" data-fenju="' + escHtml(row['所属分局'] || '') + '" data-type="总数">'
            + escHtml(row['总数'] != null ? String(row['总数']) : '') + '</td>';
      var cfCell = isTotalRow
        ? '<td>' + escHtml(row['重复数'] != null ? String(row['重复数']) : '') + '</td>'
        : '<td class="cfbj-link" data-fenju="' + escHtml(row['所属分局'] || '') + '" data-type="重复数">'
            + escHtml(row['重复数'] != null ? String(row['重复数']) : '') + '</td>';
      var fslVal = row['发生率'];
      var fslStr = (fslVal != null && fslVal !== '') ? (parseFloat(fslVal) * 100).toFixed(2) + '%' : '';

      return '<tr' + rowStyle + '>'
        + '<td class="cfbj-left">' + escHtml(row['所属分局'] || '') + '</td>'
        + zdCell + cfCell
        + '<td>' + escHtml(fslStr) + '</td>'
        + '</tr>';
    }).join('');
    tbody.innerHTML = html;

    tbody.querySelectorAll('td.cfbj-link').forEach(function (td) {
      td.addEventListener('click', function () {
        openModal(td.dataset.fenju, td.dataset.type);
      });
    });
  }

  /** 渲染明细表（固定列序 + 分页）*/
  function renderDetailPage() {
    var thead  = document.getElementById('cfbjThead');
    var tbody  = document.getElementById('cfbjTbody');
    var noData = document.getElementById('cfbjNoData');
    var total  = _detailAllRows.length;

    if (total === 0) {
      thead.innerHTML = '';
      tbody.innerHTML = '';
      noData.style.display = '';
      updatePaginationUI(0, 1, 1);
      return;
    }
    noData.style.display = 'none';

    var pageSize = _detailPageSize;
    var totalPages, start, end;
    if (pageSize === 0) {
      start = 0; end = total; totalPages = 1; _detailPage = 1;
    } else {
      totalPages  = Math.ceil(total / pageSize);
      _detailPage = Math.max(1, Math.min(_detailPage, totalPages));
      start       = (_detailPage - 1) * pageSize;
      end         = Math.min(start + pageSize, total);
    }
    var pageRows = _detailAllRows.slice(start, end);

    // 列：优先按固定顺序，再补充剩余列
    var firstRow = pageRows.length > 0 ? pageRows[0] : {};
    var availCols = DETAIL_COLS.filter(function (c) {
      return Object.prototype.hasOwnProperty.call(firstRow, c);
    });
    Object.keys(firstRow).forEach(function (k) {
      if (availCols.indexOf(k) === -1) availCols.push(k);
    });

    thead.innerHTML = '<tr>' + availCols.map(function (c) {
      return '<th>' + escHtml(c) + '</th>';
    }).join('') + '</tr>';

    var html = pageRows.map(function (row) {
      return '<tr>' + availCols.map(function (c) {
        var v = row[c];
        return '<td>' + escHtml(v != null ? String(v) : '') + '</td>';
      }).join('') + '</tr>';
    }).join('');
    tbody.innerHTML = html;

    updatePaginationUI(total, _detailPage, totalPages);
  }

  function updatePaginationUI(total, page, totalPages) {
    var allMode = _detailPageSize === 0;
    document.getElementById('cfbjTotalInfo').textContent = '共 ' + total + ' 条';
    document.getElementById('cfbjPageInfo').textContent  =
      '第 ' + (allMode ? 1 : page) + ' / ' + (allMode ? 1 : totalPages) + ' 页';
    document.getElementById('cfbjPrevBtn').disabled = allMode || page <= 1;
    document.getElementById('cfbjNextBtn').disabled = allMode || page >= totalPages;
  }

  function escHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  /* -----------------------------------------------------------------------
   * 分页控件
   * --------------------------------------------------------------------- */

  function showPagination(visible) {
    document.getElementById('cfbjPagination').style.display = visible ? '' : 'none';
  }

  function initPagination() {
    document.getElementById('cfbjPageSize').addEventListener('change', function () {
      _detailPageSize = parseInt(this.value, 10);
      _detailPage = 1;
      renderDetailPage();
    });
    document.getElementById('cfbjPrevBtn').addEventListener('click', function () {
      if (_detailPage > 1) { _detailPage--; renderDetailPage(); }
    });
    document.getElementById('cfbjNextBtn').addEventListener('click', function () {
      var tp = _detailPageSize === 0 ? 1 : Math.ceil(_detailAllRows.length / _detailPageSize);
      if (_detailPage < tp) { _detailPage++; renderDetailPage(); }
    });
  }

  /* -----------------------------------------------------------------------
   * 数据查询
   * --------------------------------------------------------------------- */

  var _currentParams = null;  // 缓存最近一次查询参数（导出用）

  function doQuery() {
    setErr('');
    if (!validateMinCs()) return;

    var params = getQueryParams();
    _currentParams = params;

    var isDetail = document.getElementById('cfbjShowDetail').checked;
    var endpoint = isDetail
      ? window.__MDJ_CFBJ_ENDPOINTS__.apiDetail
      : window.__MDJ_CFBJ_ENDPOINTS__.apiSummary;

    setStatus('查询中，请稍候…');
    document.getElementById('cfbjQueryBtn').disabled = true;

    fetch(endpoint + '?' + buildQs(params))
      .then(function (r) { return r.json(); })
      .then(function (data) {
        setStatus('');
        document.getElementById('cfbjQueryBtn').disabled = false;
        if (!data.success) {
          setErr(data.message || '查询失败');
          return;
        }
        if (isDetail) {
          _detailAllRows  = data.data || [];
          _detailPage     = 1;
          _detailPageSize = parseInt(document.getElementById('cfbjPageSize').value, 10);
          renderDetailPage();
          showPagination(true);
          setStatus('');
        } else {
          showPagination(false);
          renderSummaryTable(data.data);
          setStatus('统计时段：' + (data.start_time || '') + ' 至 ' + (data.end_time || ''));
        }
      })
      .catch(function (err) {
        setStatus('');
        document.getElementById('cfbjQueryBtn').disabled = false;
        setErr('网络错误：' + err.message);
      });
  }

  /* -----------------------------------------------------------------------
   * 显示详情开关
   * --------------------------------------------------------------------- */

  function initDetailSwitch() {
    document.getElementById('cfbjShowDetail').addEventListener('change', function () {
      if (!_currentParams) return;
      _detailPage = 1;
      doQuery();
      showPagination(this.checked);
    });
  }

  /* -----------------------------------------------------------------------
   * 导出
   * --------------------------------------------------------------------- */

  function initExportDropdown() {
    var dd = document.getElementById('cfbjExportDd');

    // 点击外部关闭
    document.addEventListener('click', function (e) {
      if (!dd.contains(e.target)) dd.classList.remove('open');
    });

    dd.querySelectorAll('[data-cfbj-fmt]').forEach(function (a) {
      a.addEventListener('click', function (e) {
        e.preventDefault();
        dd.classList.remove('open');

        var fmt = a.dataset.cfbjFmt;
        var isDetail = document.getElementById('cfbjShowDetail').checked;
        var params = Object.assign({}, _currentParams || getQueryParams(), {
          fmt:       fmt,
          data_type: isDetail ? 'detail' : 'summary',
        });

        window.open(window.__MDJ_CFBJ_ENDPOINTS__.exportData + '?' + buildQs(params), '_blank');
      });
    });
  }

  /* -----------------------------------------------------------------------
   * 弹出框（Modal）
   * --------------------------------------------------------------------- */

  function openModal(fenjuExact, detailType) {
    var params = Object.assign({}, _currentParams || getQueryParams(), {
      fenju_exact: fenjuExact,
      detail_type: detailType,
    });
    // 去掉 fenju 数组（弹出框按 fenju_exact 精确过滤）
    delete params.fenju;

    var title = (fenjuExact || '全部') + ' — ' + detailType + '明细';
    document.getElementById('cfbjModalTitle').textContent = title;
    document.getElementById('cfbjModalFrame').src =
      window.__MDJ_CFBJ_ENDPOINTS__.modalDetail + '?' + buildQs(params);
    document.getElementById('cfbjModal').classList.add('open');
  }

  window.cfbjCloseModal = function (e) {
    if (!e || e.target === document.getElementById('cfbjModal')) {
      document.getElementById('cfbjModal').classList.remove('open');
      document.getElementById('cfbjModalFrame').src = 'about:blank';
    }
  };

  /* -----------------------------------------------------------------------
   * 初始化入口
   * --------------------------------------------------------------------- */

  function init() {
    initDefaultTimes();
    initFenjuDropdown();
    initDetailSwitch();
    initExportDropdown();
    initPagination();

    document.getElementById('cfbjQueryBtn').addEventListener('click', doQuery);

    document.getElementById('cfbjMinCs').addEventListener('input', function () {
      setErr('');
    });

    // 初始化时自动查询（统计视图）
    doQuery();
  }

  // 等 DOM 就绪后初始化
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
