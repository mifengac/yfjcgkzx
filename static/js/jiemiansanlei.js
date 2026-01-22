// 街面三类警情：地址分类（前端逻辑）

let _jiemiansanleiState = {
  page: 1,
  pageSize: 20,
  total: 0,
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
    if (!menu) return;
    if (btn && btn.innerText === "导出") return;
    if (menu.contains(e.target)) return;
    menu.style.display = "none";

    // 点击空白处关闭下拉多选
    document.querySelectorAll(".ms-panel").forEach((p) => {
      const root = p.closest(".ms");
      if (root && root.contains(e.target)) return;
      p.style.display = "none";
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

  let baseEnd;
  if (endEl && endEl.value) {
    baseEnd = new Date(endEl.value);
  } else {
    baseEnd = new Date();
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
    document.querySelectorAll(".ms-panel").forEach((p) => {
      if (p.id !== panel.id) p.style.display = "none";
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
  return Array.from(panel.querySelectorAll("input[type='checkbox']:checked")).map((x) => x.value);
}

function updateMultiSelectText(containerId) {
  const textEl = document.getElementById(containerId + "-text");
  if (!textEl) return;
  const values = getMultiSelectValues(containerId);
  textEl.textContent = values.length ? values.join("，") : "请选择";
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
    .then((r) => r.json())
    .then((data) => {
      if (!data || !data.success) {
        showJiemiansanleiMessage((data && data.message) || "加载警情性质失败", "error");
        return;
      }
      setMultiSelectOptions(
        "jiemiansanleiCaseTypesMs",
        (data.data || []).map((t) => ({ value: t, label: t, checked: false }))
      );
    })
    .catch((e) => showJiemiansanleiMessage("加载警情性质失败: " + e, "error"));
}

function toggleJiemiansanleiExportMenu() {
  const menu = document.getElementById("jiemiansanleiExportMenu");
  if (!menu) return;
  menu.style.display = menu.style.display === "none" ? "block" : "none";
}

function jiemiansanleiQuery(page) {
  const startTime = document.getElementById("jiemiansanleiStartTime").value;
  const endTime = document.getElementById("jiemiansanleiEndTime").value;
  const leixingList = getMultiSelectValues("jiemiansanleiCaseTypesMs");
  const yuanshiquerenList = getMultiSelectValues("jiemiansanleiSourcesMs");
  const pageSizeRaw = document.getElementById("jiemiansanleiPageSize").value || "20";

  if (!startTime || !endTime) {
    showJiemiansanleiMessage("请填写完整的时间范围", "error");
    return;
  }
  if (new Date(startTime) > new Date(endTime)) {
    showJiemiansanleiMessage("开始时间不能晚于结束时间", "error");
    return;
  }
  if (!leixingList.length) {
    showJiemiansanleiMessage("请至少选择一个警情性质", "error");
    return;
  }
  if (!yuanshiquerenList.length) {
    showJiemiansanleiMessage("请至少选择一个 yuanshiqueren（原始/确认）", "error");
    return;
  }

  const pageSize = pageSizeRaw === "all" ? "all" : parseInt(pageSizeRaw, 10);
  _jiemiansanleiState.page = page ? parseInt(page, 10) : 1;
  _jiemiansanleiState.pageSize = pageSize;

  document.getElementById("jiemiansanleiProgressContainer").style.display = "block";
  document.getElementById("jiemiansanleiResultContainer").style.display = "none";
  showJiemiansanleiMessage("正在查询...", "success");

  fetch("/xunfang/jiemiansanlei/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      startTime: jiemiansanleiFormatDateTime(startTime),
      endTime: jiemiansanleiFormatDateTime(endTime),
      leixingList: leixingList,
      yuanshiquerenList: yuanshiquerenList,
      page: _jiemiansanleiState.page,
      pageSize: pageSize,
    }),
  })
    .then((r) => r.json())
    .then((resp) => {
      document.getElementById("jiemiansanleiProgressContainer").style.display = "none";
      if (!resp || !resp.success) {
        showJiemiansanleiMessage((resp && resp.message) || "查询失败", "error");
        return;
      }
      const data = resp.data || {};
      _jiemiansanleiState.total = data.total || 0;
      _jiemiansanleiState.page = data.page || 1;
      renderJiemiansanleiTable(data.rows || []);
      updateJiemiansanleiPager();
      showJiemiansanleiMessage("查询完成，共 " + _jiemiansanleiState.total + " 条", "success");
    })
    .catch((e) => {
      document.getElementById("jiemiansanleiProgressContainer").style.display = "none";
      showJiemiansanleiMessage("查询失败: " + e, "error");
    });
}

function renderJiemiansanleiTable(rows) {
  const container = document.getElementById("jiemiansanleiResultTable");
  if (!container) return;

  if (!rows.length) {
    container.innerHTML = "<p>暂无数据</p>";
    document.getElementById("jiemiansanleiResultContainer").style.display = "block";
    return;
  }

  const headers = Object.keys(rows[0]);
  let html = '<table><thead><tr>';
  headers.forEach((h) => (html += "<th>" + h + "</th>"));
  html += "</tr></thead><tbody>";
  rows.forEach((r) => {
    html += "<tr>";
    headers.forEach((h) => {
      const v = r[h] == null ? "" : String(r[h]);
      html += "<td>" + escapeHtml(v) + "</td>";
    });
    html += "</tr>";
  });
  html += "</tbody></table>";
  container.innerHTML = html;
  document.getElementById("jiemiansanleiResultContainer").style.display = "block";
}

function escapeHtml(unsafe) {
  return unsafe
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
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
  const p = Math.max(1, (_jiemiansanleiState.page || 1) - 1);
  jiemiansanleiQuery(p);
}

function jiemiansanleiNextPage() {
  if (_jiemiansanleiState.pageSize === "all") return;
  const size = _jiemiansanleiState.pageSize || 20;
  const totalPages = Math.max(1, Math.ceil((_jiemiansanleiState.total || 0) / size));
  const p = Math.min(totalPages, (_jiemiansanleiState.page || 1) + 1);
  jiemiansanleiQuery(p);
}

function jiemiansanleiExport(fmt) {
  const startTime = document.getElementById("jiemiansanleiStartTime").value;
  const endTime = document.getElementById("jiemiansanleiEndTime").value;
  const leixingList = getMultiSelectValues("jiemiansanleiCaseTypesMs");
  const yuanshiquerenList = getMultiSelectValues("jiemiansanleiSourcesMs");

  if (!startTime || !endTime) {
    showJiemiansanleiMessage("请填写完整的时间范围", "error");
    return;
  }
  if (!leixingList.length || !yuanshiquerenList.length) {
    showJiemiansanleiMessage("请先选择警情性质与 yuanshiqueren", "error");
    return;
  }

  document.getElementById("jiemiansanleiExportMenu").style.display = "none";
  showJiemiansanleiMessage("正在导出...", "success");

  fetch("/xunfang/jiemiansanlei/export?format=" + encodeURIComponent(fmt), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      startTime: jiemiansanleiFormatDateTime(startTime),
      endTime: jiemiansanleiFormatDateTime(endTime),
      leixingList: leixingList,
      yuanshiquerenList: yuanshiquerenList,
      format: fmt,
    }),
  })
    .then((resp) => {
      if (!resp.ok) {
        return resp.json().then((j) => {
          throw new Error((j && j.message) || "导出失败");
        });
      }
      const cd = resp.headers.get("content-disposition") || "";
      return resp.blob().then((blob) => ({ blob, cd }));
    })
    .then(({ blob, cd }) => {
      const a = document.createElement("a");
      const url = window.URL.createObjectURL(blob);
      a.href = url;
      a.download = parseFilenameFromContentDisposition(cd) || ("街面三类警情地址分类." + fmt);
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
      showJiemiansanleiMessage("导出完成", "success");
    })
    .catch((e) => showJiemiansanleiMessage("导出失败: " + e.message, "error"));
}

function parseFilenameFromContentDisposition(cd) {
  if (!cd) return "";
  const m = cd.match(/filename\\*=UTF-8''([^;]+)|filename=\"?([^\";]+)\"?/i);
  const name = (m && (m[1] || m[2])) || "";
  try {
    return decodeURIComponent(name);
  } catch (e) {
    return name;
  }
}

function jiemiansanleiExportReport() {
  const startTime = document.getElementById("jiemiansanleiStartTime").value;
  const endTime = document.getElementById("jiemiansanleiEndTime").value;
  const hbStartTime = document.getElementById("jiemiansanleiHbStartTime").value;
  const hbEndTime = document.getElementById("jiemiansanleiHbEndTime").value;

  if (!startTime || !endTime) {
    showJiemiansanleiMessage("请填写完整的时间范围", "error");
    return;
  }
  if (!hbStartTime || !hbEndTime) {
    showJiemiansanleiMessage("请填写完整的环比时间范围", "error");
    return;
  }
  if (new Date(startTime) > new Date(endTime)) {
    showJiemiansanleiMessage("开始时间不能晚于结束时间", "error");
    return;
  }
  if (new Date(hbStartTime) > new Date(hbEndTime)) {
    showJiemiansanleiMessage("环比开始不能晚于环比结束", "error");
    return;
  }

  showJiemiansanleiMessage("正在导出报表...", "success");

  fetch("/xunfang/jiemiansanlei/export_report", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      startTime: jiemiansanleiFormatDateTime(startTime),
      endTime: jiemiansanleiFormatDateTime(endTime),
      hbStartTime: jiemiansanleiFormatDateTime(hbStartTime),
      hbEndTime: jiemiansanleiFormatDateTime(hbEndTime),
    }),
  })
    .then((resp) => {
      if (!resp.ok) {
        return resp.json().then((j) => {
          throw new Error((j && j.message) || "导出报表失败");
        });
      }
      const cd = resp.headers.get("content-disposition") || "";
      return resp.blob().then((blob) => ({ blob, cd }));
    })
    .then(({ blob, cd }) => {
      const a = document.createElement("a");
      const url = window.URL.createObjectURL(blob);
      a.href = url;
      const fallbackName = (
        jiemiansanleiFormatDateTime(startTime) +
        "-" +
        jiemiansanleiFormatDateTime(endTime) +
        "_街面三类警情统计表.xlsx"
      )
        .replace(/[:\\/]/g, "-")
        .replace(/\s+/g, "_");
      a.download =
        parseFilenameFromContentDisposition(cd) || fallbackName;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
      showJiemiansanleiMessage("导出报表完成", "success");
    })
    .catch((e) => showJiemiansanleiMessage("导出报表失败: " + e.message, "error"));
}
