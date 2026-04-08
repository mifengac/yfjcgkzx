(function () {
  const state = {
    branchOptions: [],
    selectedBranches: [],
    lastFilters: null,
  };

  function $(id) {
    return document.getElementById(id);
  }

  function formatDateTime(dt) {
    const yyyy = dt.getFullYear();
    const mm = String(dt.getMonth() + 1).padStart(2, "0");
    const dd = String(dt.getDate()).padStart(2, "0");
    const hh = String(dt.getHours()).padStart(2, "0");
    const mi = String(dt.getMinutes()).padStart(2, "0");
    const ss = String(dt.getSeconds()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
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
    if (text.includes("T")) return text;
    return text.replace(" ", "T");
  }

  function setErr(msg) {
    const el = $("jsbrjqajtjErr");
    if (!el) return;
    if (!msg) {
      el.classList.add("jsbrjqajtj-hidden");
      el.textContent = "";
      return;
    }
    el.classList.remove("jsbrjqajtj-hidden");
    el.textContent = msg;
  }

  function setStatus(msg) {
    const el = $("jsbrjqajtjStatus");
    if (el) el.textContent = msg || "";
  }

  function selectedBranchesFromUI() {
    const dropdown = $("jsbrjqajtjBranchDropdown");
    if (!dropdown) return [];
    return Array.from(dropdown.querySelectorAll('input[type="checkbox"]:checked'))
      .map((x) => x.value)
      .filter((v) => v && v !== "_all");
  }

  function updateBranchDisplay() {
    const display = $("jsbrjqajtjBranchDisplay");
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
    const dropdown = $("jsbrjqajtjBranchDropdown");
    if (!dropdown) return;

    const selectedSet = new Set(state.selectedBranches || []);
    state.branchOptions = (options || [])
      .map((item) => ({
        value: (item && item.value) || "",
        label: (item && item.label) || ((item && item.value) || ""),
      }))
      .filter((item) => item.value);

    let html =
      '<label><input type="checkbox" value="_all"><span> 全选</span></label>';
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
      allBox.checked = itemBoxes.length > 0 && itemBoxes.every((x) => x.checked);
    }

    state.selectedBranches = selectedBranchesFromUI();
    updateBranchDisplay();
  }

  function renderTable(records) {
    const tbl = $("jsbrjqajtjTbl");
    if (!tbl) return;
    tbl.innerHTML = "";
    if (!records || records.length === 0) {
      tbl.innerHTML = "<tr><td class='no-data'>无符合条件数据</td></tr>";
      return;
    }

    const keys = Object.keys(records[0]);
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    keys.forEach((k) => {
      const th = document.createElement("th");
      th.textContent = k;
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    tbl.appendChild(thead);

    const tbody = document.createElement("tbody");
    records.forEach((row) => {
      const tr = document.createElement("tr");
      keys.forEach((k) => {
        const td = document.createElement("td");
        td.textContent = row[k] == null ? "" : String(row[k]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    tbl.appendChild(tbody);
  }

  function buildFilters() {
    return {
      start_time: normalizeDateTime(($("jsbrjqajtjStartTime") || {}).value || ""),
      end_time: normalizeDateTime(($("jsbrjqajtjEndTime") || {}).value || ""),
      branches: state.selectedBranches || [],
    };
  }

  async function loadDefaults() {
    const resp = await fetch("/jszahzyj/api/jsbrjqajtj/defaults");
    const js = await resp.json();
    if (!resp.ok || !js.success) {
      throw new Error((js && js.message) || "加载默认值失败");
    }
    const startEl = $("jsbrjqajtjStartTime");
    const endEl = $("jsbrjqajtjEndTime");
    if (startEl) startEl.value = toInputDateTime(js.start_time || formatDateTime(new Date()));
    if (endEl) endEl.value = toInputDateTime(js.end_time || formatDateTime(new Date()));
    renderBranchOptions(js.branch_options || []);
    state.selectedBranches = [];
    updateBranchDisplay();
  }

  async function queryData() {
    const filters = buildFilters();
    setErr("");
    setStatus("加载中...");
    try {
      const resp = await fetch("/jszahzyj/api/jsbrjqajtj/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(filters),
      });
      const js = await resp.json();
      if (!resp.ok || !js.success) {
        throw new Error((js && js.message) || "查询失败");
      }
      const options = js.branch_options || [];
      const selectedSet = new Set(state.selectedBranches || []);
      state.selectedBranches = options
        .map((item) => item && item.value)
        .filter((value) => value && selectedSet.has(value));
      renderBranchOptions(options);
      renderTable(js.records || []);
      state.lastFilters = js.filters || filters;
      setStatus(`记录数：${js.count || 0}`);
    } catch (err) {
      setErr(String(err));
      setStatus("");
      renderTable([]);
    }
  }

  function exportData(fmt) {
    const filters = state.lastFilters || buildFilters();
    const qs = new URLSearchParams({
      format: fmt || "xlsx",
      start_time: filters.start_time || "",
      end_time: filters.end_time || "",
      branches: (filters.branches || []).join(","),
    });
    window.location.href = "/jszahzyj/download/jsbrjqajtj?" + qs.toString();
  }

  function bindBranchSelect() {
    const display = $("jsbrjqajtjBranchDisplay");
    const dropdown = $("jsbrjqajtjBranchDropdown");
    if (!display || !dropdown) return;
    display.addEventListener("click", function (ev) {
      ev.stopPropagation();
      dropdown.classList.toggle("show");
    });
    dropdown.addEventListener("change", function (ev) {
      const t = ev.target;
      if (!t || t.tagName !== "INPUT") return;
      if (t.value === "_all") {
        const on = t.checked;
        dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach((cb) => {
          cb.checked = on;
        });
      } else {
        const allBox = dropdown.querySelector('input[value="_all"]');
        const itemBoxes = Array.from(dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
        if (allBox) allBox.checked = itemBoxes.length > 0 && itemBoxes.every((x) => x.checked);
      }
      state.selectedBranches = selectedBranchesFromUI();
      updateBranchDisplay();
    });
    document.addEventListener("click", function (ev) {
      if (!dropdown.contains(ev.target) && !display.contains(ev.target)) {
        dropdown.classList.remove("show");
      }
    });
  }

  function bindExportDropdown() {
    const wrap = $("jsbrjqajtjDd");
    const btn = $("jsbrjqajtjExportBtn");
    if (!wrap || !btn) return;
    btn.addEventListener("click", function () {
      wrap.classList.toggle("open");
    });
    wrap.querySelectorAll("button[data-fmt]").forEach((el) => {
      el.addEventListener("click", function () {
        wrap.classList.remove("open");
        exportData(el.getAttribute("data-fmt") || "xlsx");
      });
    });
    document.addEventListener("click", function (ev) {
      if (!wrap.contains(ev.target)) wrap.classList.remove("open");
    });
  }

  function initTabs() {
    const tabs = $("jszahzyjTabs");
    if (!tabs) return;
    tabs.querySelectorAll(".jszahzyj-tab-btn").forEach((btn) => {
      btn.addEventListener("click", function () {
        tabs.querySelectorAll(".jszahzyj-tab-btn").forEach((x) => x.classList.remove("active"));
        btn.classList.add("active");
        const tab = btn.getAttribute("data-tab");
        document.querySelectorAll(".jszahzyj-tab-panel").forEach((panel) => {
          panel.classList.toggle("active", panel.id === "tab-" + tab);
        });
      });
    });
  }

  function init() {
    if (!$("tab-jsbrjqajtj")) return;
    initTabs();
    bindBranchSelect();
    bindExportDropdown();
    const queryBtn = $("jsbrjqajtjQueryBtn");
    if (queryBtn) queryBtn.addEventListener("click", queryData);
    loadDefaults().catch((err) => setErr(String(err)));
  }

  document.addEventListener("DOMContentLoaded", init);
})();

