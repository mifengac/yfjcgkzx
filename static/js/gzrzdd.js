// 工作日志督导 - 主页面脚本

let lastResultId = "";
let lastCount = 5;
let lastStartTime = "";
let lastEndTime = "";

function $(id) {
  return document.getElementById(id);
}

function setErr(t) {
  $("err").textContent = t || "";
}

function setStatus(t) {
  $("status").textContent = t || "";
}

function normalizeDateTimeValue(value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  const text = raw.replace("T", " ");
  const m = text.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2})(?::(\d{2}))?$/);
  if (!m) {
    throw new Error("工作日志时间格式应为 YYYY-MM-DD HH:MM:SS");
  }
  return `${m[1]} ${m[2]}:${m[3] || "00"}`;
}

function validateTimeRange(startTime, endTime) {
  if (!startTime || !endTime) return;
  const startMs = Date.parse(startTime.replace(" ", "T"));
  const endMs = Date.parse(endTime.replace(" ", "T"));
  if (!Number.isNaN(startMs) && !Number.isNaN(endMs) && startMs > endMs) {
    throw new Error("工作日志开始时间不能晚于结束时间");
  }
}

function renderTable(payload) {
  const tbl = $("tbl");
  tbl.innerHTML = "";
  const rows = (payload && payload.rows) || [];
  const cols = (payload && payload.cols) || [];
  const data = (payload && payload.data) || [];

  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  const th0 = document.createElement("th");
  th0.textContent = "统计";
  trh.appendChild(th0);
  cols.forEach((c) => {
    const th = document.createElement("th");
    th.textContent = c;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  tbl.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((r, i) => {
    const tr = document.createElement("tr");
    const td0 = document.createElement("td");
    td0.className = "rowh";
    td0.textContent = r || "合计";
    tr.appendChild(td0);
    cols.forEach((c, j) => {
      const v = (data[i] || [])[j] || 0;
      const td = document.createElement("td");
      if (v > 0) {
        const a = document.createElement("a");
        a.textContent = v;
        a.href =
          "/gzrzdd/detail?result_id=" +
          encodeURIComponent(lastResultId) +
          "&branch=" +
          encodeURIComponent(c) +
          "&count=" +
          encodeURIComponent(lastCount);
        a.target = "_blank";
        td.appendChild(a);
      } else {
        td.textContent = "0";
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  tbl.appendChild(tbody);
}

async function run() {
  setErr("");
  $("btn").disabled = true;
  $("exportBtn").disabled = true;
  setStatus("统计中...");

  try {
    lastCount = parseInt($("count").value || "5", 10) || 5;
    lastStartTime = normalizeDateTimeValue(($("workStartTime") || {}).value || "");
    lastEndTime = normalizeDateTimeValue(($("workEndTime") || {}).value || "");
    validateTimeRange(lastStartTime, lastEndTime);

    const body = {
      count: lastCount,
      chongfudu: parseFloat($("chongfudu").value || "80"),
      start_time: lastStartTime,
      end_time: lastEndTime,
    };

    const resp = await fetch("/gzrzdd/api/stats", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const js = await resp.json();
    if (!resp.ok || !js.success) {
      throw new Error((js && js.message) || "统计失败");
    }
    lastResultId = js.result_id;
    renderTable(js.pivot || {});
    const timeText =
      lastStartTime || lastEndTime
        ? "，工作日志时间=" + (lastStartTime || "不限") + " 至 " + (lastEndTime || "不限")
        : "";
    setStatus(
      "完成：result_id=" +
        js.result_id +
        "，count=" +
        js.count +
        "，阈值=" +
        js.threshold_percent +
        "%" +
        timeText
    );
    $("exportBtn").disabled = false;
  } catch (e) {
    setErr(String(e));
    setStatus("");
  } finally {
    $("btn").disabled = false;
  }
}

document.addEventListener("DOMContentLoaded", function () {
  $("btn").addEventListener("click", run);

  const dd = $("dd");
  $("exportBtn").addEventListener("click", function () {
    if (dd.classList.contains("open")) dd.classList.remove("open");
    else dd.classList.add("open");
  });
  dd.querySelectorAll("a[data-fmt]").forEach((a) => {
    a.addEventListener("click", function (ev) {
      ev.preventDefault();
      dd.classList.remove("open");
      if (!lastResultId) return;
      const fmt = a.getAttribute("data-fmt");
      window.location.href =
        "/gzrzdd/download/summary?result_id=" +
        encodeURIComponent(lastResultId) +
        "&format=" +
        encodeURIComponent(fmt) +
        "&count=" +
        encodeURIComponent(lastCount);
    });
  });
  document.addEventListener("click", function (ev) {
    if (!dd.contains(ev.target)) dd.classList.remove("open");
  });
});

