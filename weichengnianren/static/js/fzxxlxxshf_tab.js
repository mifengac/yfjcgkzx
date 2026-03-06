(function () {
  const endpoints = window.__WCNR_FZXXLXXSHF_ENDPOINTS__ || {};
  const state = {
    initialized: false,
    branchOptions: [],
    selectedBranches: [],
    page: 1,
    pageSize: 20,
    totalPages: 1,
    total: 0,
    lastFilters: null,
  };

  function $(id) {
    return document.getElementById(id);
  }

  function normalizeDateTime(value) {
    const raw = (value || "").trim();
    if (!raw) return "";
    const text = raw.replace("T", " ");
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(text)) {
      return `${text}:00`;
    }
    return text;
  }

  function toInputDateTime(value) {
    const text = (value || "").trim();
    if (!text) return "";
    return text.replace(" ", "T");
  }

  function setError(message) {
    const el = $("fzxxlxxshfErr");
    if (!el) return;
    if (!message) {
      el.classList.add("js-hidden");
      el.textContent = "";
      return;
    }
    el.classList.remove("js-hidden");
    el.textContent = message;
  }

  function setStatus(message) {
    const el = $("fzxxlxxshfStatus");
    if (el) {
      el.textContent = message || "";
    }
  }

  function selectedBranchesFromUI() {
    const dropdown = $("fzxxlxxshfBranchDropdown");
    if (!dropdown) return [];
    return Array.from(dropdown.querySelectorAll('input[type="checkbox"]:checked'))
      .map((item) => item.value)
      .filter((value) => value && value !== "_all");
  }

  function updateBranchDisplay() {
    const display = $("fzxxlxxshfBranchDisplay");
    if (!display) return;
    const selected = state.selectedBranches || [];
    const total = state.branchOptions.length;
    if (selected.length === 0 || selected.length === total) {
      display.textContent = "全部";
    } else {
      display.textContent = `已选 ${selected.length} 项`;
    }
  }

  function renderBranchOptions(options) {
    const dropdown = $("fzxxlxxshfBranchDropdown");
    if (!dropdown) return;

    const selectedSet = new Set(state.selectedBranches || []);
    state.branchOptions = (options || [])
      .map((item) => ({
        value: (item && item.value) || "",
        label: (item && item.label) || ((item && item.value) || ""),
      }))
      .filter((item) => item.value);

    let html = '<label><input type="checkbox" value="_all"><span> 全选</span></label>';
    state.branchOptions.forEach((item) => {
      html += `<label><input type="checkbox" value="${item.value}"><span> ${item.label}</span></label>`;
    });
    dropdown.innerHTML = html;

    dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach((cb) => {
      cb.checked = selectedSet.has(cb.value);
    });

    const allBox = dropdown.querySelector('input[value="_all"]');
    const itemBoxes = Array.from(dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
    if (allBox) {
      allBox.checked = itemBoxes.length > 0 && itemBoxes.every((item) => item.checked);
    }

    state.selectedBranches = selectedBranchesFromUI();
    updateBranchDisplay();
  }

  function renderTable(records) {
    const tbl = $("fzxxlxxshfTbl");
    if (!tbl) return;
    tbl.innerHTML = "";

    if (!records || records.length === 0) {
      tbl.innerHTML = "<tbody><tr><td class='no-data'>暂无符合条件的数据</td></tr></tbody>";
      return;
    }

    const headers = Object.keys(records[0]);
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    headers.forEach((header) => {
      const th = document.createElement("th");
      th.textContent = header;
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    tbl.appendChild(thead);

    const tbody = document.createElement("tbody");
    records.forEach((row) => {
      const tr = document.createElement("tr");
      headers.forEach((header) => {
        const td = document.createElement("td");
        td.textContent = row[header] == null ? "" : String(row[header]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    tbl.appendChild(tbody);
  }

  function updatePagination(payload) {
    state.page = payload.page || 1;
    state.pageSize = payload.page_size || 20;
    state.totalPages = payload.total_pages || 1;
    state.total = payload.total || 0;

    const pageInfo = $("fzxxlxxshfPageInfo");
    const currentPage = $("fzxxlxxshfCurrentPage");
    const prevBtn = $("fzxxlxxshfPrevBtn");
    const nextBtn = $("fzxxlxxshfNextBtn");

    if (pageInfo) {
      pageInfo.textContent = `第 ${state.page}/${state.totalPages} 页，共 ${state.total} 条`;
    }
    if (currentPage) {
      currentPage.textContent = String(state.page);
    }
    if (prevBtn) {
      prevBtn.disabled = state.page <= 1;
    }
    if (nextBtn) {
      nextBtn.disabled = state.page >= state.totalPages;
    }
  }

  function buildFilters(page) {
    return {
      start_time: normalizeDateTime(($("fzxxlxxshfStartTime") || {}).value || ""),
      end_time: normalizeDateTime(($("fzxxlxxshfEndTime") || {}).value || ""),
      branches: state.selectedBranches || [],
      page: page || state.page || 1,
      page_size: Number(($("fzxxlxxshfPageSize") || {}).value || state.pageSize || 20),
    };
  }

  async function loadDefaults() {
    const resp = await fetch(endpoints.defaults);
    const data = await resp.json();
    if (!resp.ok || !data.success) {
      throw new Error((data && data.message) || "加载默认值失败");
    }

    const startEl = $("fzxxlxxshfStartTime");
    const endEl = $("fzxxlxxshfEndTime");
    const pageSizeEl = $("fzxxlxxshfPageSize");
    if (startEl) startEl.value = toInputDateTime(data.start_time || "");
    if (endEl) endEl.value = toInputDateTime(data.end_time || "");
    if (pageSizeEl) pageSizeEl.value = String(data.page_size || 20);

    state.selectedBranches = [];
    renderBranchOptions(data.branch_options || []);
    state.page = data.page || 1;
    state.pageSize = data.page_size || 20;
  }

  async function queryData(page) {
    const filters = buildFilters(page);
    setError("");
    setStatus("加载中...");

    try {
      const resp = await fetch(endpoints.query, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(filters),
      });
      const data = await resp.json();
      if (!resp.ok || !data.success) {
        throw new Error((data && data.message) || "查询失败");
      }

      const options = data.branch_options || [];
      const selectedSet = new Set(state.selectedBranches || []);
      state.selectedBranches = options
        .map((item) => item && item.value)
        .filter((value) => value && selectedSet.has(value));
      renderBranchOptions(options);
      renderTable(data.records || []);
      updatePagination(data);
      state.lastFilters = data.filters || filters;
      setStatus(`当前页 ${data.count || 0} 条，共 ${data.total || 0} 条`);
    } catch (error) {
      setError(error.message || String(error));
      setStatus("");
      renderTable([]);
      updatePagination({ page: 1, page_size: 20, total_pages: 1, total: 0 });
    }
  }

  function exportData(fmt) {
    const filters = state.lastFilters || buildFilters(state.page || 1);
    const params = new URLSearchParams({
      format: fmt || "xlsx",
      start_time: filters.start_time || "",
      end_time: filters.end_time || "",
      branches: (filters.branches || []).join(","),
    });
    window.location.href = `${endpoints.download}?${params.toString()}`;
  }

  function bindBranchSelect() {
    const display = $("fzxxlxxshfBranchDisplay");
    const dropdown = $("fzxxlxxshfBranchDropdown");
    if (!display || !dropdown) return;

    display.addEventListener("click", function (event) {
      event.stopPropagation();
      dropdown.classList.toggle("show");
    });

    dropdown.addEventListener("change", function (event) {
      const target = event.target;
      if (!target || target.tagName !== "INPUT") return;

      if (target.value === "_all") {
        const checked = target.checked;
        dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach((cb) => {
          cb.checked = checked;
        });
      } else {
        const allBox = dropdown.querySelector('input[value="_all"]');
        const itemBoxes = Array.from(dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
        if (allBox) {
          allBox.checked = itemBoxes.length > 0 && itemBoxes.every((item) => item.checked);
        }
      }

      state.selectedBranches = selectedBranchesFromUI();
      updateBranchDisplay();
    });

    document.addEventListener("click", function (event) {
      if (!dropdown.contains(event.target) && !display.contains(event.target)) {
        dropdown.classList.remove("show");
      }
    });
  }

  function bindExportDropdown() {
    const wrap = $("fzxxlxxshfExportWrap");
    const button = $("fzxxlxxshfExportBtn");
    if (!wrap || !button) return;

    button.addEventListener("click", function () {
      wrap.classList.toggle("open");
    });

    wrap.querySelectorAll("button[data-fmt]").forEach((item) => {
      item.addEventListener("click", function () {
        wrap.classList.remove("open");
        exportData(item.getAttribute("data-fmt") || "xlsx");
      });
    });

    document.addEventListener("click", function (event) {
      if (!wrap.contains(event.target)) {
        wrap.classList.remove("open");
      }
    });
  }

  function bindQueryActions() {
    const queryBtn = $("fzxxlxxshfQueryBtn");
    const pageSizeEl = $("fzxxlxxshfPageSize");
    const prevBtn = $("fzxxlxxshfPrevBtn");
    const nextBtn = $("fzxxlxxshfNextBtn");

    if (queryBtn) {
      queryBtn.addEventListener("click", function () {
        queryData(1);
      });
    }

    if (pageSizeEl) {
      pageSizeEl.addEventListener("change", function () {
        queryData(1);
      });
    }

    if (prevBtn) {
      prevBtn.addEventListener("click", function () {
        if (state.page > 1) {
          queryData(state.page - 1);
        }
      });
    }

    if (nextBtn) {
      nextBtn.addEventListener("click", function () {
        if (state.page < state.totalPages) {
          queryData(state.page + 1);
        }
      });
    }
  }

  async function initializeTab() {
    if (state.initialized) return;
    if (!$("tab-fzxxlxxshf")) return;

    state.initialized = true;
    bindBranchSelect();
    bindExportDropdown();
    bindQueryActions();

    try {
      await loadDefaults();
      await queryData(1);
    } catch (error) {
      setError(error.message || String(error));
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    const app = document.getElementById("weichengnianrenApp");
    if (app && app.getAttribute("data-active-tab") === "fzxxlxxshf") {
      initializeTab();
    }
  });

  document.addEventListener("wcnr:tabchange", function (event) {
    if (event.detail && event.detail.tab === "fzxxlxxshf") {
      initializeTab();
    }
  });
})();
