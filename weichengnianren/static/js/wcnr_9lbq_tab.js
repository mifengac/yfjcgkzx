(function () {
  const endpoints = window.__WCNR_9LBQ_ENDPOINTS__ || {};

  function init() {
    const fileEl = document.getElementById("wcnr9lbqFile");
    const colEl = document.getElementById("wcnr9lbqColumn");
    const queryBtn = document.getElementById("wcnr9lbqQueryBtn");
    const statusEl = document.getElementById("wcnr9lbqStatus");
    const errEl = document.getElementById("wcnr9lbqErr");
    const tbl = document.getElementById("wcnr9lbqTbl");
    const pageSizeEl = document.getElementById("wcnr9lbqPageSize");
    const prevBtn = document.getElementById("wcnr9lbqPrevBtn");
    const nextBtn = document.getElementById("wcnr9lbqNextBtn");
    const pageInfoEl = document.getElementById("wcnr9lbqPageInfo");
    const tplBtn = document.getElementById("wcnr9lbqTplBtn");
    const exportWrap = document.getElementById("wcnr9lbqExportWrap");
    const exportBtn = document.getElementById("wcnr9lbqExportBtn");
    const exportMenu = document.getElementById("wcnr9lbqExportMenu");
    const exportXlsxBtn = document.getElementById("wcnr9lbqExportXlsxBtn");
    const exportCsvBtn = document.getElementById("wcnr9lbqExportCsvBtn");

    if (!fileEl || !colEl || !queryBtn || !tbl) return;

    let allRows = [];
    let currentPage = 1;

    function getPageSize() {
      const value = String(pageSizeEl.value || "20");
      if (value === "all") return null;
      const number = Number(value);
      return Number.isFinite(number) && number > 0 ? number : 20;
    }

    function parseDownloadName(contentDisposition, fallbackName) {
      if (!contentDisposition) return fallbackName;
      const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
      if (utf8Match && utf8Match[1]) {
        return decodeURIComponent(utf8Match[1]);
      }
      const normalMatch = contentDisposition.match(/filename="?([^";]+)"?/i);
      if (normalMatch && normalMatch[1]) {
        return normalMatch[1];
      }
      return fallbackName;
    }

    function buildDisplayHeaders(headers) {
      let ordered = headers.slice();
      if (ordered.includes("证件号码")) {
        ordered = ["证件号码"].concat(ordered.filter((item) => item !== "证件号码"));
      }

      const afterLogoutTimeCols = ["所属市局", "所属市局代码", "所属分局", "所属分局代码"];
      if (!ordered.includes("户籍注销时间")) {
        return ordered;
      }

      ordered = ordered.filter((item) => !afterLogoutTimeCols.includes(item));
      const logoutIndex = ordered.indexOf("户籍注销时间");
      const insertCols = afterLogoutTimeCols.filter((item) => headers.includes(item));
      return ordered
        .slice(0, logoutIndex + 1)
        .concat(insertCols)
        .concat(ordered.slice(logoutIndex + 1));
    }

    function renderTable() {
      const total = allRows.length;
      const pageSize = getPageSize();
      let totalPages = 1;
      let pageRows = allRows;

      if (pageSize != null) {
        totalPages = Math.max(1, Math.ceil(total / pageSize));
        currentPage = Math.min(Math.max(currentPage, 1), totalPages);
        const start = (currentPage - 1) * pageSize;
        pageRows = allRows.slice(start, start + pageSize);
      } else {
        currentPage = 1;
      }

      prevBtn.disabled = currentPage <= 1 || total === 0;
      nextBtn.disabled = currentPage >= totalPages || total === 0;
      pageInfoEl.textContent = `第 ${currentPage}/${totalPages} 页，共 ${total} 条`;

      if (!pageRows.length) {
        tbl.innerHTML = "<thead><tr><th>无数据</th></tr></thead><tbody><tr><td class='no-data'>无数据</td></tr></tbody>";
        return;
      }

      const headers = buildDisplayHeaders(Object.keys(pageRows[0]));
      const head = `<thead><tr>${headers.map((item) => `<th>${item}</th>`).join("")}</tr></thead>`;
      const body = pageRows
        .map((row) => {
          const tds = headers
            .map((header) => `<td>${row[header] == null ? "" : String(row[header])}</td>`)
            .join("");
          return `<tr>${tds}</tr>`;
        })
        .join("");
      tbl.innerHTML = `${head}<tbody>${body}</tbody>`;
    }

    async function exportResult(fmt) {
      errEl.textContent = "";

      const file = fileEl.files && fileEl.files[0];
      const columnName = String(colEl.value || "").trim();
      if (!file) {
        errEl.textContent = "请先上传文件";
        return;
      }
      if (!columnName) {
        errEl.textContent = "请输入身份证列名";
        return;
      }

      exportBtn.disabled = true;
      exportXlsxBtn.disabled = true;
      exportCsvBtn.disabled = true;
      statusEl.textContent = "导出中...";

      try {
        const formData = new FormData();
        formData.append("file", file);
        formData.append("column_name", columnName);
        formData.append("fmt", fmt);

        const resp = await fetch(endpoints.export, {
          method: "POST",
          body: formData,
        });

        if (!resp.ok) {
          let message = "导出失败";
          try {
            const errData = await resp.json();
            message = errData.message || message;
          } catch (_ignore) {}
          throw new Error(message);
        }

        const blob = await resp.blob();
        const fallback = `未成年人9类标签${Date.now()}.${fmt}`;
        const filename = parseDownloadName(resp.headers.get("Content-Disposition"), fallback);
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
        statusEl.textContent = `导出完成: ${filename}`;
      } catch (error) {
        errEl.textContent = error.message || String(error);
        statusEl.textContent = "";
      } finally {
        exportBtn.disabled = false;
        exportXlsxBtn.disabled = false;
        exportCsvBtn.disabled = false;
      }
    }

    async function query() {
      errEl.textContent = "";
      statusEl.textContent = "";

      const file = fileEl.files && fileEl.files[0];
      const columnName = String(colEl.value || "").trim();
      if (!file) {
        errEl.textContent = "请先上传文件";
        return;
      }
      if (!columnName) {
        errEl.textContent = "请输入身份证列名";
        return;
      }

      queryBtn.disabled = true;
      statusEl.textContent = "查询中...";

      try {
        const formData = new FormData();
        formData.append("file", file);
        formData.append("column_name", columnName);

        const resp = await fetch(endpoints.query, {
          method: "POST",
          body: formData,
        });
        const data = await resp.json();
        if (!resp.ok || !data.success) {
          throw new Error((data && data.message) || "查询失败");
        }

        allRows = data.rows || [];
        currentPage = 1;
        renderTable();

        const info = data.extract_info || {};
        statusEl.textContent =
          `提取: 源数据行数 ${info["源数据行数"] || 0}，非空身份证 ${info["非空身份证数"] || 0}，` +
          `有效18位 ${info["有效18位身份证数"] || 0}，无效 ${info["无效身份证数"] || 0}，` +
          `去重后 ${info["去重后身份证数"] || 0}；查询结果 ${data.total || 0} 条`;
      } catch (error) {
        errEl.textContent = error.message || String(error);
        allRows = [];
        currentPage = 1;
        renderTable();
        statusEl.textContent = "";
      } finally {
        queryBtn.disabled = false;
      }
    }

    function closeExportMenu() {
      exportMenu.classList.remove("show");
    }

    queryBtn.addEventListener("click", function (event) {
      event.preventDefault();
      query();
    });

    tplBtn.addEventListener("click", function (event) {
      event.preventDefault();
      window.location.href = endpoints.template;
    });

    exportBtn.addEventListener("click", function (event) {
      event.preventDefault();
      exportMenu.classList.toggle("show");
    });

    exportXlsxBtn.addEventListener("click", function (event) {
      event.preventDefault();
      closeExportMenu();
      exportResult("xlsx");
    });

    exportCsvBtn.addEventListener("click", function (event) {
      event.preventDefault();
      closeExportMenu();
      exportResult("csv");
    });

    document.addEventListener("click", function (event) {
      if (!exportWrap.contains(event.target)) {
        closeExportMenu();
      }
    });

    pageSizeEl.addEventListener("change", function () {
      currentPage = 1;
      renderTable();
    });

    prevBtn.addEventListener("click", function (event) {
      event.preventDefault();
      if (currentPage > 1) {
        currentPage -= 1;
      }
      renderTable();
    });

    nextBtn.addEventListener("click", function (event) {
      event.preventDefault();
      currentPage += 1;
      renderTable();
    });

    renderTable();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
