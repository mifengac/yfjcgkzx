(function (root) {
  var ns = root.JSBrjqajtjApp = root.JSBrjqajtjApp || {};

  ns.state = ns.state || {
    branchOptions: [],
    selectedBranches: [],
    lastFilters: null,
  };

  ns.$ = function (id) {
    return document.getElementById(id);
  };

  ns.formatDateTime = function (dt) {
    var yyyy = dt.getFullYear();
    var mm = String(dt.getMonth() + 1).padStart(2, "0");
    var dd = String(dt.getDate()).padStart(2, "0");
    var hh = String(dt.getHours()).padStart(2, "0");
    var mi = String(dt.getMinutes()).padStart(2, "0");
    var ss = String(dt.getSeconds()).padStart(2, "0");
    return yyyy + "-" + mm + "-" + dd + " " + hh + ":" + mi + ":" + ss;
  };

  ns.normalizeDateTime = function (value) {
    var raw = (value || "").trim();
    var text;
    if (!raw) return "";
    text = raw.replace("T", " ");
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(text)) {
      return text + ":00";
    }
    return text;
  };

  ns.toInputDateTime = function (value) {
    var text = (value || "").trim();
    if (!text) return "";
    if (text.includes("T")) return text;
    return text.replace(" ", "T");
  };

  ns.setErr = function (msg) {
    var el = ns.$("jsbrjqajtjErr");
    if (!el) return;
    if (!msg) {
      el.classList.add("jsbrjqajtj-hidden");
      el.textContent = "";
      return;
    }
    el.classList.remove("jsbrjqajtj-hidden");
    el.textContent = msg;
  };

  ns.setStatus = function (msg) {
    var el = ns.$("jsbrjqajtjStatus");
    if (el) el.textContent = msg || "";
  };

  ns.selectedBranchesFromUI = function () {
    var dropdown = ns.$("jsbrjqajtjBranchDropdown");
    if (!dropdown) return [];
    return Array.from(dropdown.querySelectorAll('input[type="checkbox"]:checked'))
      .map(function (item) { return item.value; })
      .filter(function (value) { return value && value !== "_all"; });
  };

  ns.updateBranchDisplay = function () {
    var display = ns.$("jsbrjqajtjBranchDisplay");
    var selected = ns.state.selectedBranches || [];
    var total = ns.state.branchOptions.length;
    if (!display) return;
    if (selected.length === 0 || selected.length === total) {
      display.textContent = "全部";
      return;
    }
    display.textContent = "已选 " + selected.length + " 项";
  };

  ns.renderBranchOptions = function (options) {
    var dropdown = ns.$("jsbrjqajtjBranchDropdown");
    var selectedSet = new Set(ns.state.selectedBranches || []);
    var html = '<label><input type="checkbox" value="_all"><span> 全选</span></label>';
    if (!dropdown) return;

    ns.state.branchOptions = (options || [])
      .map(function (item) {
        return {
          value: (item && item.value) || "",
          label: (item && item.label) || ((item && item.value) || ""),
        };
      })
      .filter(function (item) { return item.value; });

    ns.state.branchOptions.forEach(function (item) {
      html += '<label><input type="checkbox" value="' + item.value + '"><span> ' + item.label + '</span></label>';
    });
    dropdown.innerHTML = html;

    dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach(function (cb) {
      cb.checked = selectedSet.has(cb.value);
    });
    var allBox = dropdown.querySelector('input[value="_all"]');
    var itemBoxes = Array.from(dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
    if (allBox) {
      allBox.checked = itemBoxes.length > 0 && itemBoxes.every(function (item) { return item.checked; });
    }

    ns.state.selectedBranches = ns.selectedBranchesFromUI();
    ns.updateBranchDisplay();
  };

  ns.buildFilters = function () {
    return {
      start_time: ns.normalizeDateTime((ns.$("jsbrjqajtjStartTime") || {}).value || ""),
      end_time: ns.normalizeDateTime((ns.$("jsbrjqajtjEndTime") || {}).value || ""),
      branches: ns.state.selectedBranches || [],
    };
  };

  ns.loadDefaults = async function () {
    var resp = await fetch("/jszahzyj/api/jsbrjqajtj/defaults");
    var payload = await resp.json();
    var startEl = ns.$("jsbrjqajtjStartTime");
    var endEl = ns.$("jsbrjqajtjEndTime");
    if (!resp.ok || !payload.success) {
      throw new Error((payload && payload.message) || "加载默认值失败");
    }
    if (startEl) startEl.value = ns.toInputDateTime(payload.start_time || ns.formatDateTime(new Date()));
    if (endEl) endEl.value = ns.toInputDateTime(payload.end_time || ns.formatDateTime(new Date()));
    ns.renderBranchOptions(payload.branch_options || []);
    ns.state.selectedBranches = [];
    ns.updateBranchDisplay();
  };

  ns.initTabs = function () {
    var tabs = ns.$("jszahzyjTabs");
    if (!tabs) return;
    tabs.querySelectorAll(".jszahzyj-tab-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        tabs.querySelectorAll(".jszahzyj-tab-btn").forEach(function (item) {
          item.classList.remove("active");
        });
        btn.classList.add("active");
        var tab = btn.getAttribute("data-tab");
        document.querySelectorAll(".jszahzyj-tab-panel").forEach(function (panel) {
          panel.classList.toggle("active", panel.id === "tab-" + tab);
        });
      });
    });
  };
})(window);
