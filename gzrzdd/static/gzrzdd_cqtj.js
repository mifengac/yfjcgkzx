// 工作日志超期统计 - 前端脚本（同页 tabs）

const cqtjState = {
  riskTypes: [],
  branches: [],
};

function cqtj$(id) {
  return document.getElementById(id);
}

function cqtjSetErr(t) {
  const el = cqtj$("cqtjErr");
  if (el) el.textContent = t || "";
}

function cqtjSetStatus(t) {
  const el = cqtj$("cqtjStatus");
  if (el) el.textContent = t || "";
}

function cqtjGetMode() {
  const el = document.querySelector('input[name="cqtj_mode"]:checked');
  return el ? el.value : "detail";
}

function cqtjGetRiskTypes() {
  const dd = cqtj$("cqtjRiskTypesDropdown");
  if (!dd) return [];
  const checked = Array.from(dd.querySelectorAll('input[type="checkbox"]:checked'))
    .map((x) => x.value)
    .filter((v) => v && v !== "_all");
  return checked;
}

function cqtjGetBranches() {
  const dd = cqtj$("cqtjBranchesDropdown");
  if (!dd) return [];
  const checked = Array.from(dd.querySelectorAll('input[type="checkbox"]:checked'))
    .map((x) => x.value)
    .filter((v) => v && v !== "_all");
  return checked;
}

function cqtjNormalizeDateTimeValue(value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  const text = raw.replace("T", " ");
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(text)) {
    return `${text}:00`;
  }
  return text;
}

function cqtjGetTimeRange() {
  return {
    startTime: cqtjNormalizeDateTimeValue((cqtj$("cqtjStartTime") || {}).value || ""),
    endTime: cqtjNormalizeDateTimeValue((cqtj$("cqtjEndTime") || {}).value || ""),
  };
}

function cqtjValidateTimeRange(startTime, endTime) {
  if (!startTime || !endTime) return;
  const startMs = Date.parse(startTime.replace(" ", "T"));
  const endMs = Date.parse(endTime.replace(" ", "T"));
  if (!Number.isNaN(startMs) && !Number.isNaN(endMs) && startMs > endMs) {
    throw new Error("工作日志开始时间不能晚于结束时间");
  }
}

function cqtjRender(records, mode) {
  const tbl = cqtj$("cqtjTbl");
  if (!tbl) return;
  tbl.classList.add("cqtj-table");
  tbl.innerHTML = "";

  if (!records || records.length === 0) {
    tbl.innerHTML = "<tr><td class='muted'>无数据</td></tr>";
    return;
  }

  const keys = Object.keys(records[0]).filter((k) => k !== "__row_color");

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
    const color = r.__row_color || "normal";
    if (mode === "detail") {
      if (color === "red") tr.classList.add("cqtj-red");
      if (color === "yellow") tr.classList.add("cqtj-yellow");
    }
    keys.forEach((k) => {
      const td = document.createElement("td");
      td.textContent = r[k] == null ? "" : String(r[k]);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  tbl.appendChild(tbody);
}

async function cqtjQuery(levelOverride) {
  const mode = cqtjGetMode();
  const level = levelOverride || (cqtj$("cqtjWarnBtn") && cqtj$("cqtjWarnBtn").dataset.level) || "remind";
  const riskTypes = cqtjGetRiskTypes();
  const branches = cqtjGetBranches();
  const timeRange = cqtjGetTimeRange();

  cqtjSetErr("");
  cqtjSetStatus("加载中...");
  try {
    cqtjValidateTimeRange(timeRange.startTime, timeRange.endTime);
    const resp = await fetch("/gzrzdd/api/cqtj/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        mode: mode,
        level: level,
        risk_types: riskTypes,
        branches: branches,
        start_time: timeRange.startTime,
        end_time: timeRange.endTime,
      }),
    });
    const js = await resp.json();
    if (!resp.ok || !js.success) throw new Error((js && js.message) || "查询失败");

    cqtjRender(js.records || [], mode);
    const timeText =
      timeRange.startTime || timeRange.endTime
        ? `，工作日志时间=${timeRange.startTime || "不限"} 至 ${timeRange.endTime || "不限"}`
        : "";
    cqtjSetStatus(`记录数：${js.count || 0}（${js.now || ""}）${timeText}`);
    const warnBtn = cqtj$("cqtjWarnBtn");
    if (warnBtn) warnBtn.dataset.level = level;
  } catch (e) {
    cqtjSetErr(String(e));
    cqtjSetStatus("");
    cqtjRender([], mode);
  }
}

function cqtjExport(fmt) {
  try {
    cqtjSetErr("");
    const mode = cqtjGetMode();
    const warnBtn = cqtj$("cqtjWarnBtn");
    const level = (warnBtn && warnBtn.dataset.level) || "remind";
    const riskTypes = cqtjGetRiskTypes();
    const branches = cqtjGetBranches();
    const timeRange = cqtjGetTimeRange();
    cqtjValidateTimeRange(timeRange.startTime, timeRange.endTime);
    const qs = new URLSearchParams({
      format: fmt,
      mode: mode,
      level: level,
      risk_types: riskTypes.join(","),
      branches: branches.join(","),
      start_time: timeRange.startTime,
      end_time: timeRange.endTime,
    });
    window.location.href = "/gzrzdd/download/cqtj?" + qs.toString();
  } catch (e) {
    cqtjSetErr(String(e));
  }
}

function gzrzddInitTabs() {
  const tabs = document.getElementById("gzrzddTabs");
  if (!tabs) return;
  const repeatBtn = tabs.querySelector('.tab-btn[data-tab="repeat"]');
  const cqtjBtn = tabs.querySelector('.tab-btn[data-tab="cqtj"]');
  if (repeatBtn) repeatBtn.textContent = "矛盾纠纷风险人员工作日志重复度统计";
  if (cqtjBtn) cqtjBtn.textContent = "矛盾纠纷风险人员工作日志超期统计";
  let gzryBtn = tabs.querySelector('.tab-btn[data-tab="gzrygzrz"]');
  if (!gzryBtn) {
    gzryBtn = document.createElement("button");
    gzryBtn.className = "tab-btn";
    gzryBtn.setAttribute("data-tab", "gzrygzrz");
    gzryBtn.textContent = "关注人员工作日志";
    tabs.appendChild(gzryBtn);
  }

  tabs.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", function () {
      tabs.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      const t = btn.getAttribute("data-tab");
      document.getElementById("tab-repeat").classList.toggle("active", t === "repeat");
      document.getElementById("tab-cqtj").classList.toggle("active", t === "cqtj");
      const gzryPanel = document.getElementById("tab-gzrygzrz");
      if (gzryPanel) gzryPanel.classList.toggle("active", t === "gzrygzrz");
      if (t === "cqtj") {
        // 首次进入时自动加载默认数据
        if (!btn.dataset.loaded) {
          btn.dataset.loaded = "1";
          cqtjQuery("remind");
        }
      }
    });
  });
}

function cqtjInit() {
  gzrzddInitTabs();

  // 风险类型：下拉多选框
  const riskWrap = cqtj$("cqtjRiskTypes");
  const riskDisplay = cqtj$("cqtjRiskTypesDisplay");
  const riskDropdown = cqtj$("cqtjRiskTypesDropdown");
  function applyRiskStateToUI() {
    if (!riskDropdown) return;
    const wanted = new Set(cqtjState.riskTypes || []);
    riskDropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach((cb) => {
      cb.checked = wanted.has(cb.value);
    });
    const all = riskDropdown.querySelector('input[value="_all"]');
    const items = Array.from(riskDropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
    if (all) all.checked = items.length > 0 && items.every((cb) => cb.checked);
  }
  function updateRiskDisplay() {
    const vals = cqtjState.riskTypes || [];
    if (!riskDisplay) return;
    if (!vals || vals.length === 0) {
      riskDisplay.textContent = "全部";
    } else if (vals.length === 3) {
      riskDisplay.textContent = "全部";
    } else {
      riskDisplay.textContent = "已选 " + vals.length + " 项";
    }
  }
  if (riskWrap && riskDisplay && riskDropdown) {
    riskDisplay.addEventListener("click", function () {
      applyRiskStateToUI();
      riskDropdown.classList.toggle("open");
    });
    riskDropdown.addEventListener("change", function (ev) {
      const t = ev.target;
      if (!t || t.tagName !== "INPUT") return;
      if (t.value === "_all") {
        const on = t.checked;
        riskDropdown
          .querySelectorAll('input[type="checkbox"]:not([value="_all"])')
          .forEach((cb) => (cb.checked = on));
      } else {
        const all = riskDropdown.querySelector('input[value="_all"]');
        const items = Array.from(
          riskDropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])')
        );
        if (all) all.checked = items.length > 0 && items.every((cb) => cb.checked);
      }
      cqtjState.riskTypes = cqtjGetRiskTypes();
      updateRiskDisplay();
    });
    document.addEventListener("click", function (ev) {
      if (!riskWrap.contains(ev.target)) riskDropdown.classList.remove("open");
    });
    cqtjState.riskTypes = cqtjGetRiskTypes();
    updateRiskDisplay();
  }

  // 分局：下拉多选框
  const brWrap = cqtj$("cqtjBranches");
  const brDisplay = cqtj$("cqtjBranchesDisplay");
  const brDropdown = cqtj$("cqtjBranchesDropdown");
  function applyBranchStateToUI() {
    if (!brDropdown) return;
    const wanted = new Set(cqtjState.branches || []);
    brDropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach((cb) => {
      cb.checked = wanted.has(cb.value);
    });
    const all = brDropdown.querySelector('input[value="_all"]');
    const items = Array.from(brDropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
    if (all) all.checked = items.length > 0 && items.every((cb) => cb.checked);
  }
  function updateBranchDisplay() {
    const vals = cqtjState.branches || [];
    if (!brDisplay) return;
    if (!vals || vals.length === 0) brDisplay.textContent = "全部";
    else if (vals.length === 5) brDisplay.textContent = "全部";
    else brDisplay.textContent = "已选 " + vals.length + " 项";
  }
  if (brWrap && brDisplay && brDropdown) {
    brDisplay.addEventListener("click", function () {
      applyBranchStateToUI();
      brDropdown.classList.toggle("open");
    });
    brDropdown.addEventListener("change", function (ev) {
      const t = ev.target;
      if (!t || t.tagName !== "INPUT") return;
      if (t.value === "_all") {
        const on = t.checked;
        brDropdown
          .querySelectorAll('input[type="checkbox"]:not([value="_all"])')
          .forEach((cb) => (cb.checked = on));
      } else {
        const all = brDropdown.querySelector('input[value="_all"]');
        const items = Array.from(
          brDropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])')
        );
        if (all) all.checked = items.length > 0 && items.every((cb) => cb.checked);
      }
      cqtjState.branches = cqtjGetBranches();
      updateBranchDisplay();
    });
    document.addEventListener("click", function (ev) {
      if (!brWrap.contains(ev.target)) brDropdown.classList.remove("open");
    });
    cqtjState.branches = cqtjGetBranches();
    updateBranchDisplay();
  }

  const warnBtn = cqtj$("cqtjWarnBtn");
  if (warnBtn) {
    warnBtn.dataset.level = "remind";
    warnBtn.addEventListener("click", function () {
      const cur = warnBtn.dataset.level || "remind";
      const next = cur === "warn" ? "remind" : "warn";
      warnBtn.dataset.level = next;
      warnBtn.textContent = next === "warn" ? "警告(已启用)" : "警告";
      cqtjQuery(next);
    });
  }

  const queryBtn = cqtj$("cqtjQueryBtn");
  if (queryBtn) queryBtn.addEventListener("click", () => cqtjQuery());

  document.querySelectorAll('input[name="cqtj_mode"]').forEach((el) => {
    el.addEventListener("change", () => cqtjQuery());
  });

  const dd = cqtj$("cqtjDd");
  const exportBtn = cqtj$("cqtjExportBtn");
  if (dd && exportBtn) {
    exportBtn.addEventListener("click", function () {
      dd.classList.toggle("open");
    });
    dd.querySelectorAll("a[data-fmt]").forEach((a) => {
      a.addEventListener("click", function (ev) {
        ev.preventDefault();
        dd.classList.remove("open");
        const fmt = a.getAttribute("data-fmt") || "xlsx";
        cqtjExport(fmt);
      });
    });
    document.addEventListener("click", function (ev) {
      if (!dd.contains(ev.target)) dd.classList.remove("open");
    });
  }
}

document.addEventListener("DOMContentLoaded", cqtjInit);
