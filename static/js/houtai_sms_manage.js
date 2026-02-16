(function () {
  const tabButtons = Array.from(document.querySelectorAll('.tab-btn'));
  const tabPanels = Array.from(document.querySelectorAll('.tab-panel'));

  function activateTab(tabId) {
    tabButtons.forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.tab === tabId);
    });
    tabPanels.forEach((panel) => {
      panel.classList.toggle('active', panel.id === tabId);
    });
    if (tabId === 'tab-sms-manage') {
      loadSmsList();
    }
  }

  tabButtons.forEach((btn) => {
    btn.addEventListener('click', () => activateTab(btn.dataset.tab));
  });

  const state = {
    page: 1,
    pageSize: 20,
    total: 0,
    keyword: '',
    rows: [],
  };

  const els = {
    keyword: document.getElementById('sms-keyword'),
    tbody: document.querySelector('#sms-table tbody'),
    pageText: document.getElementById('sms-page-text'),
    prevBtn: document.getElementById('sms-prev-btn'),
    nextBtn: document.getElementById('sms-next-btn'),
    checkAll: document.getElementById('sms-check-all'),
    searchBtn: document.getElementById('sms-search-btn'),
    resetBtn: document.getElementById('sms-reset-btn'),
    addBtn: document.getElementById('sms-add-btn'),
    batchDeleteBtn: document.getElementById('sms-batch-delete-btn'),
    xh: document.getElementById('sms-xh'),
    xq: document.getElementById('sms-xq'),
    xqdm: document.getElementById('sms-xqdm'),
    sspcs: document.getElementById('sms-sspcs'),
    sspcsdm: document.getElementById('sms-sspcsdm'),
    xm: document.getElementById('sms-xm'),
    zw: document.getElementById('sms-zw'),
    lxdh: document.getElementById('sms-lxdh'),
    bz: document.getElementById('sms-bz'),
  };

  if (!els.tbody) {
    return;
  }

  function esc(text) {
    return String(text == null ? '' : text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  async function api(url, options) {
    const resp = await fetch(url, options || {});
    const data = await resp.json();
    if (!resp.ok || !data.success) {
      throw new Error(data.message || `请求失败(${resp.status})`);
    }
    return data;
  }

  function renderRows() {
    els.tbody.innerHTML = state.rows
      .map((row, idx) => {
        const key = `${row.sspcsdm || ''}__${row.xm || ''}`;
        return `
          <tr>
            <td><input type="checkbox" class="sms-row-check" data-key="${esc(key)}"></td>
            <td>${esc(row.xh)}</td>
            <td>${esc(row.xq)}</td>
            <td>${esc(row.xqdm)}</td>
            <td>${esc(row.sspcs)}</td>
            <td>${esc(row.sspcsdm)}</td>
            <td>${esc(row.xm)}</td>
            <td>${esc(row.zw)}</td>
            <td>${esc(row.lxdh)}</td>
            <td>${esc(row.bz)}</td>
            <td>${esc(row.lrsj)}</td>
            <td>
              <button type="button" class="btn sms-edit-btn" data-index="${idx}">编辑</button>
              <button type="button" class="btn danger sms-del-btn" data-key="${esc(key)}">删除</button>
            </td>
          </tr>
        `;
      })
      .join('');

    const totalPages = Math.max(1, Math.ceil(state.total / state.pageSize));
    els.pageText.textContent = `第 ${state.page} / ${totalPages} 页，共 ${state.total} 条`;
    els.prevBtn.disabled = state.page <= 1;
    els.nextBtn.disabled = state.page >= totalPages;

    Array.from(document.querySelectorAll('.sms-edit-btn')).forEach((btn) => {
      btn.addEventListener('click', () => {
        const row = state.rows[Number(btn.dataset.index)];
        fillForm(row);
      });
    });

    Array.from(document.querySelectorAll('.sms-del-btn')).forEach((btn) => {
      btn.addEventListener('click', async () => {
        const key = btn.dataset.key || '';
        if (!confirm('确认删除该记录吗？')) return;
        const [sspcsdm, xm] = key.split('__');
        await deleteRows([{ sspcsdm, xm }]);
      });
    });
  }

  function fillForm(row) {
    els.xh.value = row.xh == null ? '' : row.xh;
    els.xq.value = row.xq || '';
    els.xqdm.value = row.xqdm || '';
    els.sspcs.value = row.sspcs || '';
    els.sspcsdm.value = row.sspcsdm || '';
    els.xm.value = row.xm || '';
    els.zw.value = row.zw || '';
    els.lxdh.value = row.lxdh || '';
    els.bz.value = row.bz || '';
  }

  function currentFormData() {
    return {
      xh: els.xh.value ? Number(els.xh.value) : null,
      xq: els.xq.value,
      xqdm: els.xqdm.value,
      sspcs: els.sspcs.value,
      sspcsdm: els.sspcsdm.value,
      xm: els.xm.value,
      zw: els.zw.value,
      lxdh: els.lxdh.value,
      bz: els.bz.value,
    };
  }

  async function loadSmsList() {
    state.keyword = (els.keyword.value || '').trim();
    const q = new URLSearchParams({
      page: String(state.page),
      page_size: String(state.pageSize),
      keyword: state.keyword,
    });
    try {
      const data = await api(`/houtai/api/sms/list?${q.toString()}`);
      state.rows = data.data || [];
      state.total = Number(data.total || 0);
      renderRows();
    } catch (err) {
      alert(err.message || String(err));
    }
  }

  async function saveOne() {
    const payload = currentFormData();
    if (!payload.sspcsdm || !payload.xm) {
      alert('sspcsdm 和 xm 不能为空');
      return;
    }
    try {
      const data = await api('/houtai/api/sms/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      alert(data.message || '保存成功');
      state.page = 1;
      await loadSmsList();
    } catch (err) {
      alert(err.message || String(err));
    }
  }

  async function deleteRows(rows) {
    if (!rows.length) {
      alert('请选择要删除的记录');
      return;
    }
    try {
      const data = await api('/houtai/api/sms/batch_delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rows }),
      });
      alert(data.message || '删除成功');
      await loadSmsList();
    } catch (err) {
      alert(err.message || String(err));
    }
  }

  els.searchBtn.addEventListener('click', () => {
    state.page = 1;
    loadSmsList();
  });

  els.resetBtn.addEventListener('click', () => {
    els.keyword.value = '';
    state.page = 1;
    loadSmsList();
  });

  els.addBtn.addEventListener('click', saveOne);

  els.prevBtn.addEventListener('click', () => {
    if (state.page <= 1) return;
    state.page -= 1;
    loadSmsList();
  });

  els.nextBtn.addEventListener('click', () => {
    const totalPages = Math.max(1, Math.ceil(state.total / state.pageSize));
    if (state.page >= totalPages) return;
    state.page += 1;
    loadSmsList();
  });

  els.checkAll.addEventListener('change', () => {
    const checked = !!els.checkAll.checked;
    Array.from(document.querySelectorAll('.sms-row-check')).forEach((x) => {
      x.checked = checked;
    });
  });

  els.batchDeleteBtn.addEventListener('click', async () => {
    const checks = Array.from(document.querySelectorAll('.sms-row-check'))
      .filter((x) => x.checked)
      .map((x) => x.dataset.key || '');
    if (!checks.length) {
      alert('请先勾选记录');
      return;
    }
    if (!confirm(`确认删除选中的 ${checks.length} 条记录吗？`)) {
      return;
    }
    const rows = checks.map((k) => {
      const [sspcsdm, xm] = k.split('__');
      return { sspcsdm, xm };
    });
    await deleteRows(rows);
  });
})();
