// 工作日志督导 - 明细页脚本

function $(id) {
  return document.getElementById(id);
}

function setErr(t) {
  $("err").textContent = t || "";
}

function setStatus(t) {
  $("status").textContent = t || "";
}

function render(records) {
  const tbl = $("tbl");
  tbl.innerHTML = "";
  if (!records || records.length === 0) {
    tbl.innerHTML = "<tr><td class='muted'>无数据</td></tr>";
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

async function load() {
  const qs = new URLSearchParams(location.search);
  const resultId = qs.get("result_id") || "";
  const branch = qs.get("branch") || "";
  const station = qs.get("station") || "";
  const count = qs.get("count") || "5";

  $("subtitle").textContent = station + " / " + branch;

  setErr("");
  setStatus("加载中...");
  try {
    const url =
      "/gzrzdd/api/detail?result_id=" +
      encodeURIComponent(resultId) +
      "&branch=" +
      encodeURIComponent(branch) +
      "&station=" +
      encodeURIComponent(station);
    const resp = await fetch(url);
    const js = await resp.json();
    if (!resp.ok || !js.success) {
      throw new Error((js && js.message) || "加载失败");
    }
    render(js.records || []);
    setStatus("记录数：" + (js.count || 0));

    const dd = $("dd");
    $("exportBtn").addEventListener("click", function () {
      if (dd.classList.contains("open")) dd.classList.remove("open");
      else dd.classList.add("open");
    });
    dd.querySelectorAll("a[data-fmt]").forEach((a) => {
      a.addEventListener("click", function (ev) {
        ev.preventDefault();
        dd.classList.remove("open");
        const fmt = a.getAttribute("data-fmt");
        window.location.href =
          "/gzrzdd/download/detail?result_id=" +
          encodeURIComponent(resultId) +
          "&branch=" +
          encodeURIComponent(branch) +
          "&station=" +
          encodeURIComponent(station) +
          "&format=" +
          encodeURIComponent(fmt) +
          "&count=" +
          encodeURIComponent(count);
      });
    });
    document.addEventListener("click", function (ev) {
      if (!dd.contains(ev.target)) dd.classList.remove("open");
    });
  } catch (e) {
    setErr(String(e));
    setStatus("");
  }
}

document.addEventListener("DOMContentLoaded", load);

