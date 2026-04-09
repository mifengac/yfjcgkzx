(function () {
  function $(id) {
    return document.getElementById(id);
  }

  const state = {
    branchOptions: [],
    typeOptions: [],
    riskOptions: [],
    selectedBranches: [],
    selectedTypes: [],
    selectedRisks: [],
    lastFilters: null,
    drawerContentWidth: null,
    drawerSlowTimer: null,
  };

  function toInputDateTime(value) {
    const text = String(value || "").trim();
    return text ? text.replace(" ", "T") : "";
  }

  function normalizeDateTime(value) {
    const raw = String(value || "").trim().replace("T", " ");
    if (!raw) return "";
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(raw)) return raw + ":00";
    return raw;
  }

  function escapeHtml(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function setError(message) {
    const el = $("jszahzTopicError");
    if (!el) return;
    if (!message) {
      el.classList.add("jszahz-topic-hidden");
      el.textContent = "";
      return;
    }
    el.classList.remove("jszahz-topic-hidden");
    el.textContent = message;
  }

  function setStatus(message) {
    const el = $("jszahzTopicStatus");
    if (el) el.textContent = message || "";
  }

  function setBatchInfo(batch) {
    const el = $("jszahzTopicBatchInfo");
    if (!el) return;
    if (!batch) {
      el.textContent = "当前无生效批次，请先上传 Excel。";
      return;
    }
    el.textContent =
      "当前生效批次：" +
      (batch.source_file_name || "未知文件") +
      " | 导入行数：" +
      (batch.imported_row_count || 0) +
      " | 生成标签数：" +
      (batch.generated_tag_count || 0) +
      " | 匹配主表人数：" +
      (batch.matched_person_count || 0);
  }

  function updateMultiDisplay(displayId, selected, total) {
    const display = $(displayId);
    if (!display) return;
    let text = "全部";
    if (selected.length > 0 && selected.length < total) {
      text = "已选 " + selected.length + " 项";
    }
    display.innerHTML = "<span>" + escapeHtml(text) + "</span>";
  }

  function renderOptions(menuId, options, selectedValues, displayId) {
    const menu = $(menuId);
    if (!menu) return;
    const selectedSet = new Set(selectedValues || []);
    let html = '<label><input type="checkbox" value="__all__"> 全部</label>';
    (options || []).forEach(function (item) {
      const value = String(item.value || "");
      const label = String(item.label || value);
      const checked = selectedSet.has(value) ? " checked" : "";
      html +=
        '<label><input type="checkbox" value="' +
        escapeHtml(value) +
        '"' +
        checked +
        "> " +
        escapeHtml(label) +
        "</label>";
    });
    menu.innerHTML = html;

    const allBox = menu.querySelector('input[value="__all__"]');
    const itemBoxes = Array.from(menu.querySelectorAll('input[type="checkbox"]:not([value="__all__"])'));
    if (allBox) {
      allBox.checked = itemBoxes.length > 0 && itemBoxes.every(function (item) { return item.checked; });
    }
    updateMultiDisplay(displayId, selectedValues || [], (options || []).length);
  }

  function bindMultiSelect(displayId, menuId, stateKey, optionsKey) {
    const display = $(displayId);
    const menu = $(menuId);
    if (!display || !menu) return;

    display.addEventListener("click", function (event) {
      event.stopPropagation();
      menu.classList.toggle("show");
    });

    menu.addEventListener("change", function (event) {
      const target = event.target;
      if (!target || target.tagName !== "INPUT") return;
      const items = Array.from(menu.querySelectorAll('input[type="checkbox"]:not([value="__all__"])'));
      if (target.value === "__all__") {
        items.forEach(function (item) {
          item.checked = target.checked;
        });
      } else {
        const allBox = menu.querySelector('input[value="__all__"]');
        if (allBox) {
          allBox.checked = items.length > 0 && items.every(function (item) { return item.checked; });
        }
      }
      state[stateKey] = items.filter(function (item) { return item.checked; }).map(function (item) { return item.value; });
      updateMultiDisplay(displayId, state[stateKey], state[optionsKey].length);
    });
  }

  function bindMenuClose() {
    document.addEventListener("click", function (event) {
      ["jszahzTopicBranchMenu", "jszahzTopicTypeMenu", "jszahzTopicRiskMenu"].forEach(function (menuId) {
        const menu = $(menuId);
        const display = $(menuId.replace("Menu", "Display"));
        if (!menu || !display) return;
        if (!menu.contains(event.target) && !display.contains(event.target)) {
          menu.classList.remove("show");
        }
      });
    });
  }

  function buildFilters() {
    return {
      start_time: normalizeDateTime(($("jszahzTopicStartTime") || {}).value || ""),
      end_time: normalizeDateTime(($("jszahzTopicEndTime") || {}).value || ""),
      branch_codes: state.selectedBranches.slice(),
      person_types: state.selectedTypes.slice(),
      risk_labels: state.selectedRisks.slice(),
    };
  }

  function renderSummary(records, count, message) {
    const tbody = $("jszahzTopicTable").querySelector("tbody");
    if (!tbody) return;
    if (!records || records.length === 0) {
      tbody.innerHTML = '<tr><td colspan="3" class="no-data">' + escapeHtml(message || "暂无数据") + "</td></tr>";
      $("jszahzTopicSummary").textContent = "当前去重患者数：0";
      return;
    }

    tbody.innerHTML = records
      .map(function (row) {
        const branchCode = String(row["分局代码"] || "");
        const branchName = String(row["分局名称"] || "");
        const countValue = Number(row["去重患者数"] || 0);
        return (
          "<tr>" +
          "<td>" + escapeHtml(branchCode) + "</td>" +
          "<td>" + escapeHtml(branchName) + "</td>" +
          '<td><button type="button" class="jszahz-topic-link" data-branch-code="' + escapeHtml(branchCode) + '" data-branch-name="' + escapeHtml(branchName) + '">' + countValue + "</button></td>" +
          "</tr>"
        );
      })
      .join("");
    $("jszahzTopicSummary").textContent = "当前去重患者数：" + count;

    tbody.querySelectorAll(".jszahz-topic-link").forEach(function (button) {
      button.addEventListener("click", function () {
        openDrawer(button.getAttribute("data-branch-code") || "__ALL__", button.getAttribute("data-branch-name") || "汇总");
      });
    });
  }

  function clearDrawerLoadingTimer() {
    if (state.drawerSlowTimer) {
      window.clearTimeout(state.drawerSlowTimer);
      state.drawerSlowTimer = null;
    }
  }

  function showDrawerLoading() {
    const loading = $("jszahzTopicDrawerLoading");
    const title = $("jszahzTopicDrawerLoadingTitle");
    const hint = $("jszahzTopicDrawerLoadingHint");
    if (!loading) return;
    clearDrawerLoadingTimer();
    if (title) {
      title.textContent = "正在查询详细数据及关联统计，请稍候...";
    }
    if (hint) {
      hint.textContent = "当前会按身份证号去重加载明细，并统计 6 类关联数据";
    }
    loading.classList.remove("jszahz-topic-hidden");
    state.drawerSlowTimer = window.setTimeout(function () {
      if (hint && !loading.classList.contains("jszahz-topic-hidden")) {
        hint.textContent = "查询时间较长，正在统计关联数据，请继续等待";
      }
    }, 3000);
  }

  function hideDrawerLoading() {
    const loading = $("jszahzTopicDrawerLoading");
    clearDrawerLoadingTimer();
    if (loading) {
      loading.classList.add("jszahz-topic-hidden");
    }
  }

  function openDrawer(branchCode, branchName) {
    const filters = state.lastFilters || buildFilters();
    const title = $("jszahzTopicDrawerTitle");
    const frame = $("jszahzTopicDrawerFrame");
    const drawer = $("jszahzTopicDrawer");
    if (!frame || !drawer) return;

    if (title) {
      title.textContent = (branchName || "汇总") + " - 详细数据";
    }

    state.drawerContentWidth = null;
    applyDrawerWidth();
    showDrawerLoading();
    drawer.classList.add("open");

    const params = new URLSearchParams({
      branch_code: branchCode || "__ALL__",
      branch_name: branchName || "汇总",
      start_time: filters.start_time || "",
      end_time: filters.end_time || "",
      person_types: (filters.person_types || []).join(","),
      risk_labels: (filters.risk_labels || []).join(","),
      _ts: String(Date.now()),
    });
    frame.src = "/jszahzyj/jszahzztk/detail_page?" + params.toString();
  }

  function closeDrawer() {
    const drawer = $("jszahzTopicDrawer");
    const frame = $("jszahzTopicDrawerFrame");
    state.drawerContentWidth = null;
    applyDrawerWidth();
    hideDrawerLoading();
    if (frame) frame.src = "about:blank";
    if (drawer) drawer.classList.remove("open");
  }

  function applyDrawerWidth() {
    const panel = $("jszahzTopicDrawerPanel");
    if (!panel) return;
    if (!state.drawerContentWidth) {
      panel.style.removeProperty("--jszahz-topic-drawer-width");
      return;
    }

    const viewportWidth =
      window.innerWidth <= 960
        ? Math.max(window.innerWidth - 12, 360)
        : Math.max(Math.floor(window.innerWidth * 0.75), 640);
    const preferredWidth = Math.min(
      Math.max(Number(state.drawerContentWidth) + 40, 640),
      viewportWidth
    );
    panel.style.setProperty("--jszahz-topic-drawer-width", preferredWidth + "px");
  }

  function handleDrawerMessage(event) {
    const frame = $("jszahzTopicDrawerFrame");
    if (!frame || event.source !== frame.contentWindow) return;
    const data = event.data || {};
    if (data.type === "jszahz-topic-detail-ready") {
      hideDrawerLoading();
      return;
    }
    if (data.type === "jszahz-topic-detail-width") {
      state.drawerContentWidth = Number(data.width) || null;
      applyDrawerWidth();
      hideDrawerLoading();
    }
  }

  function handleDrawerFrameLoad() {
    hideDrawerLoading();
  }

  async function loadDefaults() {
    const response = await fetch("/jszahzyj/api/jszahzztk/defaults");
    const payload = await response.json();
    if (!response.ok || !payload.success) {
      throw new Error((payload && payload.message) || "加载默认值失败");
    }

    $("jszahzTopicStartTime").value = toInputDateTime(payload.start_time || "");
    $("jszahzTopicEndTime").value = toInputDateTime(payload.end_time || "");
    state.branchOptions = payload.branch_options || [];
    state.typeOptions = payload.person_type_options || [];
    state.riskOptions = payload.risk_options || [];
    state.selectedBranches = [];
    state.selectedTypes = [];
    state.selectedRisks = [];
    renderOptions("jszahzTopicBranchMenu", state.branchOptions, state.selectedBranches, "jszahzTopicBranchDisplay");
    renderOptions("jszahzTopicTypeMenu", state.typeOptions, state.selectedTypes, "jszahzTopicTypeDisplay");
    renderOptions("jszahzTopicRiskMenu", state.riskOptions, state.selectedRisks, "jszahzTopicRiskDisplay");
    setBatchInfo(payload.active_batch || null);
  }

  async function uploadExcel() {
    const input = $("jszahzTopicFile");
    if (!input || !input.files || !input.files[0]) {
      setError("请先选择 Excel 文件。");
      return;
    }
    const formData = new FormData();
    formData.append("file", input.files[0]);
    setError("");
    setStatus("Excel 上传处理中，请稍候...");
    const controller = new AbortController();
    const timer = window.setTimeout(function () {
      controller.abort();
    }, 60000);

    try {
      const response = await fetch("/jszahzyj/api/jszahzztk/upload", {
        method: "POST",
        body: formData,
        signal: controller.signal,
      });
      const payload = await response.json();
      if (!response.ok || !payload.success) {
        throw new Error((payload && payload.message) || "上传失败");
      }
      input.value = "";
      setBatchInfo(payload.active_batch || null);
      setStatus("上传成功，已切换为最新生效批次。");
    } catch (error) {
      if (error && error.name === "AbortError") {
        throw new Error("上传超过 60 秒仍未完成，后端可能正在等待数据库锁或执行长查询。");
      }
      throw error;
    } finally {
      window.clearTimeout(timer);
    }
  }

  async function querySummary() {
    const filters = buildFilters();
    setError("");
    setStatus("查询中，请稍候...");
    const response = await fetch("/jszahzyj/api/jszahzztk/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(filters),
    });
    const payload = await response.json();
    if (!response.ok || !payload.success) {
      throw new Error((payload && payload.message) || "查询失败");
    }
    state.lastFilters = payload.filters || filters;
    setBatchInfo(payload.active_batch || null);
    renderSummary(payload.records || [], payload.count || 0, payload.message || "");
    setStatus(payload.message || "查询完成。");
  }

  function exportSummary() {
    const filters = state.lastFilters || buildFilters();
    const params = new URLSearchParams({
      start_time: filters.start_time || "",
      end_time: filters.end_time || "",
      branch_codes: (filters.branch_codes || []).join(","),
      person_types: (filters.person_types || []).join(","),
      risk_labels: (filters.risk_labels || []).join(","),
    });
    window.location.href = "/jszahzyj/download/jszahzztk?" + params.toString();
  }

  function initDrawerEvents() {
    const closeBtn = $("jszahzTopicDrawerCloseBtn");
    const mask = $("jszahzTopicDrawerMask");
    const frame = $("jszahzTopicDrawerFrame");
    if (closeBtn) closeBtn.addEventListener("click", closeDrawer);
    if (mask) mask.addEventListener("click", closeDrawer);
    if (frame) frame.addEventListener("load", handleDrawerFrameLoad);
    window.addEventListener("message", handleDrawerMessage);
    window.addEventListener("resize", applyDrawerWidth);
  }

  function init() {
    if (!$("jszahzTopicApp")) return;
    bindMultiSelect("jszahzTopicBranchDisplay", "jszahzTopicBranchMenu", "selectedBranches", "branchOptions");
    bindMultiSelect("jszahzTopicTypeDisplay", "jszahzTopicTypeMenu", "selectedTypes", "typeOptions");
    bindMultiSelect("jszahzTopicRiskDisplay", "jszahzTopicRiskMenu", "selectedRisks", "riskOptions");
    bindMenuClose();
    initDrawerEvents();

    $("jszahzTopicUploadBtn").addEventListener("click", function () {
      uploadExcel().catch(function (error) {
        setError(String(error));
        setStatus("");
      });
    });
    $("jszahzTopicQueryBtn").addEventListener("click", function () {
      querySummary().catch(function (error) {
        setError(String(error));
        setStatus("");
      });
    });
    $("jszahzTopicExportBtn").addEventListener("click", exportSummary);

    loadDefaults().catch(function (error) {
      setError(String(error));
      setStatus("");
    });
  }

  document.addEventListener("DOMContentLoaded", init);
})();
