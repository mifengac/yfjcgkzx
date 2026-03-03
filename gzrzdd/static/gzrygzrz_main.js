// 关注人员工作日志 - 前端脚本

const gzrygzrzState = {
  branchOptions: [],
  selectedBranches: [],
  lastFilters: null,
};

const GZRYGZRZ_FIXED_BRANCH_OPTIONS = [
  { value: "云城分局", label: "云城分局" },
  { value: "云安分局", label: "云安分局" },
  { value: "罗定市公安局", label: "罗定市公安局" },
  { value: "新兴县公安局", label: "新兴县公安局" },
  { value: "郁南县公安局", label: "郁南县公安局" },
];

function gzrygzrz$(id) {
  return document.getElementById(id);
}

function gzrygzrzSetErr(msg) {
  const el = gzrygzrz$("gzrygzrzErr");
  if (el) el.textContent = msg || "";
}

function gzrygzrzSetStatus(msg) {
  const el = gzrygzrz$("gzrygzrzStatus");
  if (el) el.textContent = msg || "";
}

function gzrygzrzFormatDateTime(dt) {
  const yyyy = dt.getFullYear();
  const mm = String(dt.getMonth() + 1).padStart(2, "0");
  const dd = String(dt.getDate()).padStart(2, "0");
  const hh = String(dt.getHours()).padStart(2, "0");
  const mi = String(dt.getMinutes()).padStart(2, "0");
  const ss = String(dt.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function gzrygzrzFormatDateTimeForInput(dt) {
  return gzrygzrzFormatDateTime(dt).replace(" ", "T");
}

function gzrygzrzNormalizeDateTimeValue(value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  const text = raw.replace("T", " ");
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(text)) {
    return `${text}:00`;
  }
  return text;
}

function gzrygzrzInitDefaultTimes() {
  const startEl = gzrygzrz$("gzrygzrzStartTime");
  const endEl = gzrygzrz$("gzrygzrzEndTime");
  if (!startEl || !endEl) return;

  const now = new Date();
  const todayZero = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
  const start = new Date(todayZero.getTime() - 7 * 24 * 3600 * 1000);
  startEl.value = gzrygzrzFormatDateTimeForInput(start);
  endEl.value = gzrygzrzFormatDateTimeForInput(todayZero);
}

function gzrygzrzSelectedBranchesFromUI() {
  const dd = gzrygzrz$("gzrygzrzBranchDropdown");
  if (!dd) return [];
  return Array.from(dd.querySelectorAll('input[type="checkbox"]:checked'))
    .map((x) => x.value)
    .filter((v) => v && v !== "_all");
}

function gzrygzrzUpdateBranchDisplay() {
  const display = gzrygzrz$("gzrygzrzBranchDisplay");
  if (!display) return;
  const selected = gzrygzrzState.selectedBranches || [];
  const total = gzrygzrzState.branchOptions.length;
  if (selected.length === 0 || selected.length === total) {
    display.textContent = "全部";
  } else {
    display.textContent = `已选 ${selected.length} 项`;
  }
}

function gzrygzrzRenderBranchOptions(options) {
  const dd = gzrygzrz$("gzrygzrzBranchDropdown");
  if (!dd) return;

  const selectedSet = new Set(gzrygzrzState.selectedBranches || []);
  gzrygzrzState.branchOptions = (options || [])
    .map((item) => {
      if (typeof item === "string") {
        return { value: item, label: item };
      }
      return {
        value: (item && item.value) || "",
        label: (item && item.label) || ((item && item.value) || ""),
      };
    })
    .filter((item) => item.value);

  let html =
    '<label class="multi-select-option">' +
    '<input type="checkbox" value="_all">' +
    "<span>全选</span>" +
    "</label>";
  gzrygzrzState.branchOptions.forEach((b) => {
    html +=
      '<label class="multi-select-option">' +
      `<input type="checkbox" value="${b.value}">` +
      `<span>${b.label}</span>` +
      "</label>";
  });
  dd.innerHTML = html;

  dd.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach((cb) => {
    cb.checked = selectedSet.has(cb.value);
  });
  const allBox = dd.querySelector('input[value="_all"]');
  const itemBoxes = Array.from(dd.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
  if (allBox) {
    allBox.checked = itemBoxes.length > 0 && itemBoxes.every((x) => x.checked);
  }
  gzrygzrzState.selectedBranches = gzrygzrzSelectedBranchesFromUI();
  gzrygzrzUpdateBranchDisplay();
}

function gzrygzrzRenderTable(records) {
  const tbl = gzrygzrz$("gzrygzrzTbl");
  if (!tbl) return;
  tbl.classList.add("gzrygzrz-table");
  tbl.innerHTML = "";

  if (!records || records.length === 0) {
    tbl.innerHTML = "<tr><td class='muted'>无符合条件数据</td></tr>";
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
  records.forEach((r) => {
    const tr = document.createElement("tr");
    keys.forEach((k) => {
      const td = document.createElement("td");
      td.textContent = r[k] == null ? "" : String(r[k]);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  tbl.appendChild(tbody);
}

function gzrygzrzBuildFilters() {
  const start_time = gzrygzrzNormalizeDateTimeValue((gzrygzrz$("gzrygzrzStartTime") || {}).value || "");
  const end_time = gzrygzrzNormalizeDateTimeValue((gzrygzrz$("gzrygzrzEndTime") || {}).value || "");
  const sfczjjzx = (gzrygzrz$("gzrygzrzSfczjjzx") || {}).value || "";
  const branches = gzrygzrzState.selectedBranches || [];
  return { start_time, end_time, sfczjjzx, branches };
}

async function gzrygzrzQuery() {
  const filters = gzrygzrzBuildFilters();
  gzrygzrzSetErr("");
  gzrygzrzSetStatus("加载中...");

  try {
    const resp = await fetch("/gzrzdd/api/gzrygzrz/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(filters),
    });
    const js = await resp.json();
    if (!resp.ok || !js.success) {
      throw new Error((js && js.message) || "查询失败");
    }

    const serverOptions = js.branch_options || GZRYGZRZ_FIXED_BRANCH_OPTIONS;
    const selectedSet = new Set(gzrygzrzState.selectedBranches || []);
    gzrygzrzState.selectedBranches = serverOptions
      .map((item) => (typeof item === "string" ? item : item && item.value))
      .filter((value) => value && selectedSet.has(value));
    gzrygzrzRenderBranchOptions(serverOptions);
    gzrygzrzRenderTable(js.records || []);
    gzrygzrzState.lastFilters = js.filters || filters;
    gzrygzrzSetStatus(`记录数：${js.count || 0}`);
  } catch (e) {
    gzrygzrzSetErr(String(e));
    gzrygzrzSetStatus("");
    gzrygzrzRenderTable([]);
  }
}

function gzrygzrzExport(fmt) {
  const filters = gzrygzrzState.lastFilters || gzrygzrzBuildFilters();
  const qs = new URLSearchParams({
    format: fmt || "xlsx",
    start_time: filters.start_time || "",
    end_time: filters.end_time || "",
    sfczjjzx: filters.sfczjjzx || "",
    branches: (filters.branches || []).join(","),
  });
  window.location.href = "/gzrzdd/download/gzrygzrz?" + qs.toString();
}

function gzrygzrzBindBranchMultiSelect() {
  const wrap = gzrygzrz$("gzrygzrzBranchMs");
  const display = gzrygzrz$("gzrygzrzBranchDisplay");
  const dd = gzrygzrz$("gzrygzrzBranchDropdown");
  if (!wrap || !display || !dd) return;

  display.addEventListener("click", function () {
    dd.classList.toggle("open");
  });

  dd.addEventListener("change", function (ev) {
    const t = ev.target;
    if (!t || t.tagName !== "INPUT") return;
    if (t.value === "_all") {
      const on = t.checked;
      dd.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach((cb) => {
        cb.checked = on;
      });
    } else {
      const allBox = dd.querySelector('input[value="_all"]');
      const itemBoxes = Array.from(dd.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
      if (allBox) allBox.checked = itemBoxes.length > 0 && itemBoxes.every((x) => x.checked);
    }
    gzrygzrzState.selectedBranches = gzrygzrzSelectedBranchesFromUI();
    gzrygzrzUpdateBranchDisplay();
  });

  document.addEventListener("click", function (ev) {
    if (!wrap.contains(ev.target)) dd.classList.remove("open");
  });
}

function gzrygzrzBindExportDropdown() {
  const dd = gzrygzrz$("gzrygzrzDd");
  const btn = gzrygzrz$("gzrygzrzExportBtn");
  if (!dd || !btn) return;
  btn.addEventListener("click", function () {
    dd.classList.toggle("open");
  });
  dd.querySelectorAll("a[data-fmt]").forEach((a) => {
    a.addEventListener("click", function (ev) {
      ev.preventDefault();
      dd.classList.remove("open");
      gzrygzrzExport(a.getAttribute("data-fmt") || "xlsx");
    });
  });
  document.addEventListener("click", function (ev) {
    if (!dd.contains(ev.target)) dd.classList.remove("open");
  });
}

function gzrygzrzInit() {
  if (!gzrygzrz$("tab-gzrygzrz")) return;
  gzrygzrzInitDefaultTimes();
  gzrygzrzRenderBranchOptions(GZRYGZRZ_FIXED_BRANCH_OPTIONS);
  gzrygzrzBindBranchMultiSelect();
  gzrygzrzBindExportDropdown();
  const queryBtn = gzrygzrz$("gzrygzrzQueryBtn");
  if (queryBtn) queryBtn.addEventListener("click", gzrygzrzQuery);
}

document.addEventListener("DOMContentLoaded", gzrygzrzInit);
