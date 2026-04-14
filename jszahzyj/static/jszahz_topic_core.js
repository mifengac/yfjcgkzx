(function (root) {
  var ns = root.JSZahzTopicApp = root.JSZahzTopicApp || {};

  ns.state = ns.state || {
    branchOptions: [],
    typeOptions: [],
    riskOptions: [],
    selectedBranches: [],
    selectedTypes: [],
    selectedRisks: [],
    managedOnly: true,
    lastFilters: null,
    drawerContentWidth: null,
    drawerSlowTimer: null,
  };

  ns.$ = function (id) {
    return document.getElementById(id);
  };

  ns.toInputDateTime = function (value) {
    var text = String(value || "").trim();
    return text ? text.replace(" ", "T") : "";
  };

  ns.normalizeDateTime = function (value) {
    var raw = String(value || "").trim().replace("T", " ");
    if (!raw) return "";
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(raw)) return raw + ":00";
    return raw;
  };

  ns.escapeHtml = function (text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;");
  };

  ns.setError = function (message) {
    var el = ns.$("jszahzTopicError");
    if (!el) return;
    if (!message) {
      el.classList.add("jszahz-topic-hidden");
      el.textContent = "";
      return;
    }
    el.classList.remove("jszahz-topic-hidden");
    el.textContent = message;
  };

  ns.setStatus = function (message) {
    var el = ns.$("jszahzTopicStatus");
    if (el) el.textContent = message || "";
  };

  ns.setBatchInfo = function (batch) {
    var el = ns.$("jszahzTopicBatchInfo");
    var baseBatch;
    var tagBatch;
    var parts = [];
    if (!el) return;
    if (!batch || (!batch.base_batch && !batch.tag_batch)) {
      el.textContent = "当前未上传基础/标签 Excel，查询时仅使用列管数据源。";
      return;
    }
    baseBatch = batch.base_batch || null;
    tagBatch = batch.tag_batch || null;
    if (baseBatch) {
      parts.push(
        "基础数据：" +
        (baseBatch.source_file_name || "未知文件") +
        " | 原始行数：" +
        (baseBatch.imported_row_count || 0) +
        " | 去重人数：" +
        (baseBatch.matched_person_count || 0)
      );
    } else {
      parts.push("基础数据：未上传");
    }
    if (tagBatch) {
      parts.push(
        "标签数据：" +
        (tagBatch.source_file_name || "未知文件") +
        " | 原始行数：" +
        (tagBatch.imported_row_count || 0) +
        " | 标签条数：" +
        (tagBatch.generated_tag_count || 0) +
        " | 标记人数：" +
        (tagBatch.matched_person_count || 0)
      );
    } else {
      parts.push("标签数据：未上传");
    }
    el.textContent = parts.join(" ; ");
  };

  ns.updateMultiDisplay = function (displayId, selected, total) {
    var display = ns.$(displayId);
    var text = "全部";
    if (!display) return;
    if (selected.length > 0 && selected.length < total) {
      text = "已选 " + selected.length + " 项";
    }
    display.innerHTML = "<span>" + ns.escapeHtml(text) + "</span>";
  };

  ns.renderOptions = function (menuId, options, selectedValues, displayId) {
    var menu = ns.$(menuId);
    var selectedSet = new Set(selectedValues || []);
    var html = '<label><input type="checkbox" value="__all__"> 全部</label>';
    if (!menu) return;
    (options || []).forEach(function (item) {
      var value = String(item.value || "");
      var label = String(item.label || value);
      var checked = selectedSet.has(value) ? " checked" : "";
      html +=
        '<label><input type="checkbox" value="' +
        ns.escapeHtml(value) +
        '"' +
        checked +
        '> ' +
        ns.escapeHtml(label) +
        '</label>';
    });
    menu.innerHTML = html;

    var allBox = menu.querySelector('input[value="__all__"]');
    var itemBoxes = Array.from(menu.querySelectorAll('input[type="checkbox"]:not([value="__all__"])'));
    if (allBox) {
      allBox.checked = itemBoxes.length > 0 && itemBoxes.every(function (item) { return item.checked; });
    }
    ns.updateMultiDisplay(displayId, selectedValues || [], (options || []).length);
  };

  ns.bindMultiSelect = function (displayId, menuId, stateKey, optionsKey) {
    var display = ns.$(displayId);
    var menu = ns.$(menuId);
    if (!display || !menu) return;

    display.addEventListener("click", function (event) {
      event.stopPropagation();
      menu.classList.toggle("show");
    });

    menu.addEventListener("change", function (event) {
      var target = event.target;
      var items;
      var allBox;
      if (!target || target.tagName !== "INPUT") return;
      items = Array.from(menu.querySelectorAll('input[type="checkbox"]:not([value="__all__"])'));
      if (target.value === "__all__") {
        items.forEach(function (item) {
          item.checked = target.checked;
        });
      } else {
        allBox = menu.querySelector('input[value="__all__"]');
        if (allBox) {
          allBox.checked = items.length > 0 && items.every(function (item) { return item.checked; });
        }
      }
      ns.state[stateKey] = items.filter(function (item) { return item.checked; }).map(function (item) { return item.value; });
      ns.updateMultiDisplay(displayId, ns.state[stateKey], ns.state[optionsKey].length);
    });
  };

  ns.bindMenuClose = function () {
    document.addEventListener("click", function (event) {
      ["jszahzTopicBranchMenu", "jszahzTopicTypeMenu", "jszahzTopicRiskMenu"].forEach(function (menuId) {
        var menu = ns.$(menuId);
        var display = ns.$(menuId.replace("Menu", "Display"));
        if (!menu || !display) return;
        if (!menu.contains(event.target) && !display.contains(event.target)) {
          menu.classList.remove("show");
        }
      });
    });
  };

  ns.buildFilters = function () {
    var managedOnlyEl = ns.$("jszahzTopicManagedOnly");
    return {
      branch_codes: ns.state.selectedBranches.slice(),
      person_types: ns.state.selectedTypes.slice(),
      risk_labels: ns.state.selectedRisks.slice(),
      managed_only: managedOnlyEl ? !!managedOnlyEl.checked : true,
    };
  };

  ns.renderSummary = function (records, count, message) {
    var table = ns.$("jszahzTopicTable");
    var tbody = table ? table.querySelector("tbody") : null;
    var summary = ns.$("jszahzTopicSummary");
    if (!tbody || !summary) return;
    if (!records || records.length === 0) {
      tbody.innerHTML = '<tr><td colspan="3" class="no-data">' + ns.escapeHtml(message || "暂无数据") + "</td></tr>";
      summary.textContent = "当前去重患者数：0";
      return;
    }

    tbody.innerHTML = records
      .map(function (row) {
        var branchCode = String(row["分局代码"] || "");
        var branchName = String(row["分局名称"] || "");
        var countValue = Number(row["去重患者数"] || 0);
        return (
          "<tr>" +
          "<td>" + ns.escapeHtml(branchCode) + "</td>" +
          "<td>" + ns.escapeHtml(branchName) + "</td>" +
          '<td><button type="button" class="jszahz-topic-link" data-branch-code="' + ns.escapeHtml(branchCode) + '" data-branch-name="' + ns.escapeHtml(branchName) + '">' + countValue + "</button></td>" +
          "</tr>"
        );
      })
      .join("");
    summary.textContent = "当前去重患者数：" + count;

    tbody.querySelectorAll(".jszahz-topic-link").forEach(function (button) {
      button.addEventListener("click", function () {
        ns.openDrawer(button.getAttribute("data-branch-code") || "__ALL__", button.getAttribute("data-branch-name") || "汇总");
      });
    });
  };

  ns.loadDefaults = async function () {
    var response = await fetch("/jszahzyj/api/jszahzztk/defaults");
    var payload = await response.json();
    var managedOnlyEl;
    if (!response.ok || !payload.success) {
      throw new Error((payload && payload.message) || "加载默认值失败");
    }

    ns.state.branchOptions = payload.branch_options || [];
    ns.state.typeOptions = payload.person_type_options || [];
    ns.state.riskOptions = payload.risk_options || [];
    ns.state.managedOnly = payload.managed_only !== false;
    ns.state.selectedBranches = [];
    ns.state.selectedTypes = [];
    ns.state.selectedRisks = [];
    managedOnlyEl = ns.$("jszahzTopicManagedOnly");
    if (managedOnlyEl) {
      managedOnlyEl.checked = ns.state.managedOnly;
    }
    ns.renderOptions("jszahzTopicBranchMenu", ns.state.branchOptions, ns.state.selectedBranches, "jszahzTopicBranchDisplay");
    ns.renderOptions("jszahzTopicTypeMenu", ns.state.typeOptions, ns.state.selectedTypes, "jszahzTopicTypeDisplay");
    ns.renderOptions("jszahzTopicRiskMenu", ns.state.riskOptions, ns.state.selectedRisks, "jszahzTopicRiskDisplay");
    ns.setBatchInfo(payload.active_batches || null);
  };

  ns.querySummary = async function () {
    var filters = ns.buildFilters();
    var response;
    var payload;
    ns.setError("");
    ns.setStatus("查询中，请稍候...");
    response = await fetch("/jszahzyj/api/jszahzztk/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(filters),
    });
    payload = await response.json();
    if (!response.ok || !payload.success) {
      throw new Error((payload && payload.message) || "查询失败");
    }
    ns.state.lastFilters = payload.filters || filters;
    ns.setBatchInfo(payload.active_batches || null);
    ns.renderSummary(payload.records || [], payload.count || 0, payload.message || "");
    ns.setStatus(payload.message || "查询完成。");
  };

  ns.exportSummary = function () {
    var filters = ns.state.lastFilters || ns.buildFilters();
    var params = new URLSearchParams({
      branch_codes: (filters.branch_codes || []).join(","),
      person_types: (filters.person_types || []).join(","),
      risk_labels: (filters.risk_labels || []).join(","),
      managed_only: filters.managed_only ? "1" : "0",
    });
    root.location.href = "/jszahzyj/download/jszahzztk?" + params.toString();
  };
})(window);
