let uploadId = "";
let rowCount = 0;

function $(id) {
  return document.getElementById(id);
}

function setErr(t) {
  $("err").textContent = t || "";
}

function setStatus(t) {
  $("status").textContent = t || "";
}

function setBusy(b) {
  $("uploadBtn").disabled = b;
  $("runBtn").disabled = b || !uploadId;
}

function parseFilenameFromDisposition(disposition) {
  if (!disposition) return "";
  const m1 = /filename\*=UTF-8''([^;]+)/i.exec(disposition);
  if (m1 && m1[1]) return decodeURIComponent(m1[1]);
  const m2 = /filename=\"?([^\";]+)\"?/i.exec(disposition);
  return (m2 && m2[1]) || "";
}

function renderColumns(columns) {
  const sel = $("sourceColumns");
  sel.innerHTML = "";
  (columns || []).forEach((c) => {
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
  });
}

function renderPreview(rows) {
  const tbl = $("previewTbl");
  tbl.innerHTML = "";
  if (!rows || rows.length === 0) return;
  const cols = Object.keys(rows[0] || {});
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  cols.forEach((c) => {
    const th = document.createElement("th");
    th.textContent = c;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  tbl.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    cols.forEach((c) => {
      const td = document.createElement("td");
      const v = r[c];
      td.textContent = v === null || v === undefined ? "" : String(v);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  tbl.appendChild(tbody);
}

function addTargetRow(name = "", desc = "") {
  const tbody = $("targetsTbl").querySelector("tbody");
  const tr = document.createElement("tr");

  const td1 = document.createElement("td");
  const nameInput = document.createElement("input");
  nameInput.type = "text";
  nameInput.placeholder = "如：姓名";
  nameInput.value = name;
  td1.appendChild(nameInput);

  const td2 = document.createElement("td");
  const descInput = document.createElement("input");
  descInput.type = "text";
  descInput.placeholder = "可选：提取规则/格式要求，如：身份证号18位，无法确定留空";
  descInput.value = desc;
  td2.appendChild(descInput);

  const td3 = document.createElement("td");
  const delBtn = document.createElement("button");
  delBtn.className = "btn btnDanger";
  delBtn.textContent = "删除";
  delBtn.onclick = () => tr.remove();
  td3.appendChild(delBtn);

  tr.appendChild(td1);
  tr.appendChild(td2);
  tr.appendChild(td3);
  tbody.appendChild(tr);
}

function getTargets() {
  const rows = $("targetsTbl").querySelectorAll("tbody tr");
  const out = [];
  rows.forEach((tr) => {
    const inputs = tr.querySelectorAll("input");
    const name = (inputs[0]?.value || "").trim();
    const desc = (inputs[1]?.value || "").trim();
    if (name) out.push({ name, desc });
  });
  return out;
}

function getSelectedColumns() {
  const sel = $("sourceColumns");
  return Array.from(sel.selectedOptions).map((o) => o.value);
}

function useDefaultPrompt() {
  $("prompt").value =
    "请根据 rows 中的输入字段，按 targets 提取信息。\n" +
    "要求：\n" +
    "1) 对身份证号、手机号、日期等尽量规范化；\n" +
    "2) 如果无法从输入中判断，返回空字符串；\n" +
    "3) 不要臆造不存在的信息。";
}

async function uploadAndParse() {
  setErr("");
  setStatus("");

  const f = $("file").files[0];
  if (!f) {
    setErr("请选择文件");
    return;
  }
  const ext = (f.name.split(".").pop() || "").toLowerCase();
  if (!["xlsx", "csv"].includes(ext)) {
    setErr("仅支持 xlsx/csv");
    return;
  }

  setBusy(true);
  setStatus("上传中...");
  try {
    const fd = new FormData();
    fd.append("file", f);
    const resp = await fetch("/shuju_tiqu/api/upload", { method: "POST", body: fd });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.message || "上传失败");

    uploadId = data.upload_id;
    rowCount = data.row_count || 0;
    $("fileInfo").style.display = "";
    $("fileInfo").textContent = `${data.filename}（${rowCount} 行）`;
    renderColumns(data.columns || []);
    renderPreview(data.preview_rows || []);
    $("runBtn").disabled = false;
    setStatus("解析完成，可配置字段并开始提取。");

    // 小文件默认逐行；大一点默认批量
    if (rowCount <= 200) $("batchSize").value = "1";
    else $("batchSize").value = "10";
  } catch (e) {
    uploadId = "";
    rowCount = 0;
    $("runBtn").disabled = true;
    setErr(String(e?.message || e));
  } finally {
    setBusy(false);
  }
}

async function runExtract() {
  setErr("");
  setStatus("");
  if (!uploadId) {
    setErr("请先上传文件");
    return;
  }
  const sourceColumns = getSelectedColumns();
  if (!sourceColumns.length) {
    setErr("请选择至少 1 个输入字段");
    return;
  }
  const targets = getTargets();
  if (!targets.length) {
    setErr("请至少新增 1 个输出字段");
    return;
  }

  const outFormat = $("outFormat").value || "xlsx";
  const batchSize = parseInt($("batchSize").value || "10", 10) || 10;
  const concurrency = parseInt($("concurrency").value || "2", 10) || 2;
  const prompt = $("prompt").value || "";

  setBusy(true);
  setStatus("提取中...（行数较多时可能需要较长时间）");
  try {
    const body = {
      upload_id: uploadId,
      source_columns: sourceColumns,
      targets,
      prompt,
      out_format: outFormat,
      batch_size: batchSize,
      concurrency,
    };

    const resp = await fetch("/shuju_tiqu/api/extract", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      const data = await resp.json().catch(() => null);
      throw new Error((data && data.message) || `提取失败（HTTP ${resp.status}）`);
    }

    const blob = await resp.blob();
    const dispo = resp.headers.get("Content-Disposition") || "";
    const filename = parseFilenameFromDisposition(dispo) || `result.${outFormat}`;

    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);
    setStatus("完成：已开始下载。");
  } catch (e) {
    setErr(String(e?.message || e));
  } finally {
    setBusy(false);
  }
}

window.addEventListener("DOMContentLoaded", () => {
  $("uploadBtn").onclick = uploadAndParse;
  $("runBtn").onclick = runExtract;
  $("addTargetBtn").onclick = () => addTargetRow("", "");
  $("clearTargetsBtn").onclick = () => {
    $("targetsTbl").querySelector("tbody").innerHTML = "";
  };
  $("useDefaultPromptBtn").onclick = useDefaultPrompt;

  // 默认提供两个空行，方便直接填
  addTargetRow("", "");
  addTargetRow("", "");
});

