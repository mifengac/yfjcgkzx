let _jiemiansanleiState = {
  page: 1,
  pageSize: 20,
  total: 0,
  filterKey: "",
};

let _jiemiansanleiInited = false;

function jiemiansanleiInit() {
  if (_jiemiansanleiInited) return;
  _jiemiansanleiInited = true;

  jiemiansanleiSetDefaultTimeRange();
  jiemiansanleiSetDefaultHbTimeRange();
  initMultiSelect("jiemiansanleiSourcesMs", [
    { value: "原始", label: "原始", checked: true },
    { value: "确认", label: "确认", checked: false },
  ]);
  initMultiSelect("jiemiansanleiCaseTypesMs", []);
  jiemiansanleiLoadCaseTypes();

  document.addEventListener("click", function (e) {
    const menu = document.getElementById("jiemiansanleiExportMenu");
    const btn = e.target && e.target.closest ? e.target.closest("button") : null;
    if (menu) {
      if (btn && btn.innerText === "导出") {
        return;
      }
      if (!menu.contains(e.target)) {
        menu.style.display = "none";
      }
    }

    document.querySelectorAll(".ms-panel").forEach((panel) => {
      const root = panel.closest(".ms");
      if (root && root.contains(e.target)) return;
      panel.style.display = "none";
    });
  });
}

function _tryInitJiemiansanlei() {
  if (document.getElementById("jiemiansanlei-tab")) {
    jiemiansanleiInit();
  }
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", _tryInitJiemiansanlei);
} else {
  _tryInitJiemiansanlei();
}

function jiemiansanleiSetDefaultTimeRange() {
  const startEl = document.getElementById("jiemiansanleiStartTime");
  const endEl = document.getElementById("jiemiansanleiEndTime");
  if (!startEl || !endEl) return;

  const now = new Date();
  const end = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
  const start = new Date(end);
  start.setDate(start.getDate() - 7);

  startEl.value = jiemiansanleiToDatetimeLocal(start);
  endEl.value = jiemiansanleiToDatetimeLocal(end);
}

function jiemiansanleiSetDefaultHbTimeRange() {
  const hbStartEl = document.getElementById("jiemiansanleiHbStartTime");
  const hbEndEl = document.getElementById("jiemiansanleiHbEndTime");
  const endEl = document.getElementById("jiemiansanleiEndTime");
  if (!hbStartEl || !hbEndEl) return;

  let baseEnd = new Date();
  if (endEl && endEl.value) {
    baseEnd = new Date(endEl.value);
  }
  baseEnd.setHours(0, 0, 0, 0);

  const hbEnd = new Date(baseEnd);
  hbEnd.setDate(hbEnd.getDate() - 7);
  const hbStart = new Date(baseEnd);
  hbStart.setDate(hbStart.getDate() - 14);

  hbStartEl.value = jiemiansanleiToDatetimeLocal(hbStart);
  hbEndEl.value = jiemiansanleiToDatetimeLocal(hbEnd);
}

function jiemiansanleiToDatetimeLocal(d) {
  const pad = (n) => String(n).padStart(2, "0");
  return (
    d.getFullYear() +
    "-" +
    pad(d.getMonth() + 1) +
    "-" +
    pad(d.getDate()) +
    "T" +
    pad(d.getHours()) +
    ":" +
    pad(d.getMinutes()) +
    ":" +
    pad(d.getSeconds())
  );
}

function jiemiansanleiFormatDateTime(dateTimeStr) {
  if (!dateTimeStr) return "";
  const date = new Date(dateTimeStr);
  const pad = (n) => String(n).padStart(2, "0");
  return (
    date.getFullYear() +
    "-" +
    pad(date.getMonth() + 1) +
    "-" +
    pad(date.getDate()) +
    " " +
    pad(date.getHours()) +
    ":" +
    pad(date.getMinutes()) +
    ":" +
    pad(date.getSeconds())
  );
}

function initMultiSelect(containerId, options) {
  const root = document.getElementById(containerId);
  if (!root) return;

  root.innerHTML = "";
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "ms-btn";

  const text = document.createElement("span");
  text.id = containerId + "-text";
  text.textContent = "请选择";
  btn.appendChild(text);

  const panel = document.createElement("div");
  panel.className = "ms-panel";
  panel.id = containerId + "-panel";

  btn.addEventListener("click", function () {
    document.querySelectorAll(".ms-panel").forEach((other) => {
      if (other.id !== panel.id) other.style.display = "none";
    });
    panel.style.display = panel.style.display === "block" ? "none" : "block";
  });

  root.appendChild(btn);
  root.appendChild(panel);

  setMultiSelectOptions(containerId, options || []);
}

function setMultiSelectOptions(containerId, options) {
  const panel = document.getElementById(containerId + "-panel");
  if (!panel) return;

  panel.innerHTML = "";
  (options || []).forEach((opt) => {
    const row = document.createElement("label");
    row.className = "ms-option";

    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = String(opt.value);
    cb.checked = !!opt.checked;
    cb.addEventListener("change", function () {
      updateMultiSelectText(containerId);
    });

    const label = document.createElement("span");
    label.textContent = String(opt.label);

    row.appendChild(cb);
    row.appendChild(label);
    panel.appendChild(row);
  });

  updateMultiSelectText(containerId);
}

function getMultiSelectValues(containerId) {
  const panel = document.getElementById(containerId + "-panel");
  if (!panel) return [];
  return Array.from(panel.querySelectorAll("input[type='checkbox']:checked")).map((item) => item.value);
}

function updateMultiSelectText(containerId) {
  const textEl = document.getElementById(containerId + "-text");
  if (!textEl) return;
  const values = getMultiSelectValues(containerId);
  textEl.textContent = values.length ? values.join("、") : "请选择";
}

function showJiemiansanleiMessage(message, type) {
  const el = document.getElementById("jiemiansanleiStatusMessage");
  if (!el) return;
  el.innerText = message;
  el.className = "status-message " + (type === "error" ? "error" : "success");
  el.style.display = "block";
}

function jiemiansanleiLoadCaseTypes() {
  fetch("/xunfang/jiemiansanlei/case_types")
    .then((response) => response.json())
    .then((data) => {
      if (!data || !data.success) {
        showJiemiansanleiMessage((data && data.message) || "加载警情性质失败", "error");
        return;
      }
      setMultiSelectOptions(
        "jiemiansanleiCaseTypesMs",
        (data.data || []).map((item) => ({ value: item, label: item, checked: false }))
      );
    })
    .catch((error) => showJiemiansanleiMessage("加载警情性质失败: " + error, "error"));
}

function toggleJiemiansanleiExportMenu() {
  const menu = document.getElementById("jiemiansanleiExportMenu");
  if (!menu) return;
  menu.style.display = menu.style.display === "none" ? "block" : "none";
}

function _getJiemiansanleiPageSizeRaw() {
  const el = document.getElementById("jiemiansanleiPageSize");
  return (el && el.value) || "20";
}

function _getJiemiansanleiFilters() {
  const streetOnly = !!(document.getElementById("jiemiansanleiStreetOnly") || {}).checked;
  const streetFilterMode = streetOnly ? "recommended" : "none";
  return {
    startTime: document.getElementById("jiemiansanleiStartTime").value,
    endTime: document.getElementById("jiemiansanleiEndTime").value,
    hbStartTime: document.getElementById("jiemiansanleiHbStartTime").value,
    hbEndTime: document.getElementById("jiemiansanleiHbEndTime").value,
    leixingList: getMultiSelectValues("jiemiansanleiCaseTypesMs"),
    yuanshiquerenList: getMultiSelectValues("jiemiansanleiSourcesMs"),
    streetFilterMode: streetFilterMode,
    streetOnly: streetOnly,
    minorOnly: !!(document.getElementById("jiemiansanleiMinorOnly") || {}).checked,
    pageSizeRaw: _getJiemiansanleiPageSizeRaw(),
  };
}

function _buildJiemiansanleiFilterKey(filters) {
  return JSON.stringify({
    startTime: filters.startTime,
    endTime: filters.endTime,
    leixingList: filters.leixingList.slice().sort(),
    yuanshiquerenList: filters.yuanshiquerenList.slice().sort(),
    streetFilterMode: filters.streetFilterMode,
    minorOnly: filters.minorOnly,
    pageSizeRaw: filters.pageSizeRaw,
  });
}

function _validateJiemiansanleiQueryFilters(filters) {
  if (!filters.startTime || !filters.endTime) {
    showJiemiansanleiMessage("请填写完整的时间范围", "error");
    return false;
  }
  if (new Date(filters.startTime) > new Date(filters.endTime)) {
    showJiemiansanleiMessage("开始时间不能晚于结束时间", "error");
    return false;
  }
  if (!filters.leixingList.length) {
    showJiemiansanleiMessage("请至少选择一个警情性质", "error");
    return false;
  }
  if (!filters.yuanshiquerenList.length) {
    showJiemiansanleiMessage("请至少选择一个警情性质口径", "error");
    return false;
  }
  return true;
}

function jiemiansanleiQuery(page) {
  const filters = _getJiemiansanleiFilters();
  if (!_validateJiemiansanleiQueryFilters(filters)) {
    return;
  }

  const pageSize = filters.pageSizeRaw === "all" ? "all" : parseInt(filters.pageSizeRaw, 10);
  const filterKey = _buildJiemiansanleiFilterKey(filters);

  if (typeof page === "number") {
    _jiemiansanleiState.page = Math.max(1, parseInt(page, 10));
  } else if (_jiemiansanleiState.filterKey !== filterKey) {
    _jiemiansanleiState.page = 1;
  }

  _jiemiansanleiState.filterKey = filterKey;
  _jiemiansanleiState.pageSize = pageSize;

  document.getElementById("jiemiansanleiProgressContainer").style.display = "block";
  document.getElementById("jiemiansanleiResultContainer").style.display = "none";
  showJiemiansanleiMessage("正在查询...", "success");

  fetch("/xunfang/jiemiansanlei/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      startTime: jiemiansanleiFormatDateTime(filters.startTime),
      endTime: jiemiansanleiFormatDateTime(filters.endTime),
      leixingList: filters.leixingList,
      yuanshiquerenList: filters.yuanshiquerenList,
      page: _jiemiansanleiState.page,
      pageSize: pageSize,
      streetFilterMode: filters.streetFilterMode,
      streetOnly: filters.streetOnly,
      minorOnly: filters.minorOnly,
    }),
  })
    .then((response) => response.json())
    .then((resp) => {
      document.getElementById("jiemiansanleiProgressContainer").style.display = "none";
      if (!resp || !resp.success) {
        showJiemiansanleiMessage((resp && resp.message) || "查询失败", "error");
        return;
      }

      const data = resp.data || {};
      _jiemiansanleiState.total = data.total || 0;
      _jiemiansanleiState.page = data.page || 1;
      renderJiemiansanleiTable(data.rows || [], data.street_filter || null);
      updateJiemiansanleiPager();
      showJiemiansanleiMessage("查询完成，共 " + _jiemiansanleiState.total + " 条", "success");
    })
    .catch((error) => {
      document.getElementById("jiemiansanleiProgressContainer").style.display = "none";
      showJiemiansanleiMessage("查询失败: " + error, "error");
    });
}

function renderJiemiansanleiTable(rows, streetFilterInfo) {
  const container = document.getElementById("jiemiansanleiResultTable");
  const resultContainer = document.getElementById("jiemiansanleiResultContainer");
  if (!container || !resultContainer) return;

  renderJiemiansanleiStreetFilterNote(streetFilterInfo);

  if (!rows.length) {
    container.innerHTML = "<p>暂无数据</p>";
    resultContainer.style.display = "block";
    return;
  }

  const headers = Object.keys(rows[0]);
  let html = "<table><thead><tr>";
  headers.forEach((header) => {
    html += "<th>" + escapeHtml(header) + "</th>";
  });
  html += "</tr></thead><tbody>";
  rows.forEach((row) => {
    html += "<tr>";
    headers.forEach((header) => {
      const value = row[header] == null ? "" : String(row[header]);
      html += "<td>" + escapeHtml(value) + "</td>";
    });
    html += "</tr>";
  });
  html += "</tbody></table>";
  container.innerHTML = html;
  resultContainer.style.display = "block";
}

function renderJiemiansanleiStreetFilterNote(info) {
  const note = document.getElementById("jiemiansanleiStreetFilterNote");
  if (!note) return;
  if (!info || !info.description) {
    note.style.display = "none";
    note.innerHTML = "";
    return;
  }
  note.innerHTML = escapeHtml(String(info.description));
  note.style.display = "block";
}

function escapeHtml(unsafe) {
  return unsafe
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function updateJiemiansanleiPager() {
  const pageInfo = document.getElementById("jiemiansanleiPageInfo");
  const pager = document.getElementById("jiemiansanleiPager");
  if (!pageInfo || !pager) return;

  if (_jiemiansanleiState.pageSize === "all") {
    pager.style.display = "none";
    pageInfo.innerText = "全部";
    return;
  }

  pager.style.display = "flex";
  const size = _jiemiansanleiState.pageSize || 20;
  const totalPages = Math.max(1, Math.ceil((_jiemiansanleiState.total || 0) / size));
  pageInfo.innerText = "第 " + _jiemiansanleiState.page + " / " + totalPages + " 页";
}

function jiemiansanleiPrevPage() {
  if (_jiemiansanleiState.pageSize === "all") return;
  const page = Math.max(1, (_jiemiansanleiState.page || 1) - 1);
  jiemiansanleiQuery(page);
}

function jiemiansanleiNextPage() {
  if (_jiemiansanleiState.pageSize === "all") return;
  const size = _jiemiansanleiState.pageSize || 20;
  const totalPages = Math.max(1, Math.ceil((_jiemiansanleiState.total || 0) / size));
  const page = Math.min(totalPages, (_jiemiansanleiState.page || 1) + 1);
  jiemiansanleiQuery(page);
}

function jiemiansanleiExport(fmt) {
  const filters = _getJiemiansanleiFilters();
  if (!_validateJiemiansanleiQueryFilters(filters)) {
    return;
  }

  document.getElementById("jiemiansanleiExportMenu").style.display = "none";
  showJiemiansanleiMessage("正在导出...", "success");

  fetch("/xunfang/jiemiansanlei/export?format=" + encodeURIComponent(fmt), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      startTime: jiemiansanleiFormatDateTime(filters.startTime),
      endTime: jiemiansanleiFormatDateTime(filters.endTime),
      leixingList: filters.leixingList,
      yuanshiquerenList: filters.yuanshiquerenList,
      format: fmt,
      streetFilterMode: filters.streetFilterMode,
      streetOnly: filters.streetOnly,
      minorOnly: filters.minorOnly,
    }),
  })
    .then((resp) => {
      if (!resp.ok) {
        return resp.json().then((payload) => {
          throw new Error((payload && payload.message) || "导出失败");
        });
      }
      const contentDisposition = resp.headers.get("content-disposition") || "";
      return resp.blob().then((blob) => ({ blob, contentDisposition }));
    })
    .then(({ blob, contentDisposition }) => {
      const link = document.createElement("a");
      const url = window.URL.createObjectURL(blob);
      link.href = url;
      link.download =
        parseFilenameFromContentDisposition(contentDisposition) || ("街面三类警情地址分类." + fmt);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      showJiemiansanleiMessage("导出完成", "success");
    })
    .catch((error) => showJiemiansanleiMessage("导出失败: " + error.message, "error"));
}

function parseFilenameFromContentDisposition(contentDisposition) {
  if (!contentDisposition) return "";
  const matched = contentDisposition.match(/filename\\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i);
  const filename = (matched && (matched[1] || matched[2])) || "";
  try {
    return decodeURIComponent(filename);
  } catch (error) {
    return filename;
  }
}

function jiemiansanleiExportReport() {
  const filters = _getJiemiansanleiFilters();
  if (!filters.startTime || !filters.endTime) {
    showJiemiansanleiMessage("请填写完整的时间范围", "error");
    return;
  }
  if (!filters.hbStartTime || !filters.hbEndTime) {
    showJiemiansanleiMessage("请填写完整的环比时间范围", "error");
    return;
  }
  if (new Date(filters.startTime) > new Date(filters.endTime)) {
    showJiemiansanleiMessage("开始时间不能晚于结束时间", "error");
    return;
  }
  if (new Date(filters.hbStartTime) > new Date(filters.hbEndTime)) {
    showJiemiansanleiMessage("环比开始不能晚于环比结束", "error");
    return;
  }

  showJiemiansanleiMessage("正在导出报表...", "success");

  fetch("/xunfang/jiemiansanlei/export_report", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      startTime: jiemiansanleiFormatDateTime(filters.startTime),
      endTime: jiemiansanleiFormatDateTime(filters.endTime),
      hbStartTime: jiemiansanleiFormatDateTime(filters.hbStartTime),
      hbEndTime: jiemiansanleiFormatDateTime(filters.hbEndTime),
      streetFilterMode: filters.streetFilterMode,
      streetOnly: filters.streetOnly,
    }),
  })
    .then((resp) => {
      if (!resp.ok) {
        return resp.json().then((payload) => {
          throw new Error((payload && payload.message) || "导出报表失败");
        });
      }
      const contentDisposition = resp.headers.get("content-disposition") || "";
      return resp.blob().then((blob) => ({ blob, contentDisposition }));
    })
    .then(({ blob, contentDisposition }) => {
      const link = document.createElement("a");
      const url = window.URL.createObjectURL(blob);
      link.href = url;
      const fallbackName = (
        jiemiansanleiFormatDateTime(filters.startTime) +
        "-" +
        jiemiansanleiFormatDateTime(filters.endTime) +
        "_街面三类警情统计表.xlsx"
      )
        .replace(/[:\\/]/g, "-")
        .replace(/\s+/g, "_");
      link.download = parseFilenameFromContentDisposition(contentDisposition) || fallbackName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      showJiemiansanleiMessage("导出报表完成", "success");
    })
    .catch((error) => showJiemiansanleiMessage("导出报表失败: " + error.message, "error"));
}
