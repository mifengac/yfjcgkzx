(function (root) {
  var ns = root.JSBrjqajtjApp = root.JSBrjqajtjApp || {};

  ns.renderTable = function (records) {
    var tbl = ns.$("jsbrjqajtjTbl");
    if (!tbl) return;
    tbl.innerHTML = "";
    if (!records || records.length === 0) {
      tbl.innerHTML = "<tr><td class='no-data'>无符合条件数据</td></tr>";
      return;
    }

    var keys = Object.keys(records[0]);
    var thead = document.createElement("thead");
    var trh = document.createElement("tr");
    keys.forEach(function (key) {
      var th = document.createElement("th");
      th.textContent = key;
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    tbl.appendChild(thead);

    var tbody = document.createElement("tbody");
    records.forEach(function (row) {
      var tr = document.createElement("tr");
      keys.forEach(function (key) {
        var td = document.createElement("td");
        td.textContent = row[key] == null ? "" : String(row[key]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    tbl.appendChild(tbody);
  };

  ns.queryData = async function () {
    var filters = ns.buildFilters();
    var resp;
    var payload;
    var options;
    var selectedSet;
    ns.setErr("");
    ns.setStatus("加载中...");
    try {
      resp = await fetch("/jszahzyj/api/jsbrjqajtj/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(filters),
      });
      payload = await resp.json();
      if (!resp.ok || !payload.success) {
        throw new Error((payload && payload.message) || "查询失败");
      }
      options = payload.branch_options || [];
      selectedSet = new Set(ns.state.selectedBranches || []);
      ns.state.selectedBranches = options
        .map(function (item) { return item && item.value; })
        .filter(function (value) { return value && selectedSet.has(value); });
      ns.renderBranchOptions(options);
      ns.renderTable(payload.records || []);
      ns.state.lastFilters = payload.filters || filters;
      ns.setStatus("记录数：" + (payload.count || 0));
    } catch (error) {
      ns.setErr(String(error));
      ns.setStatus("");
      ns.renderTable([]);
    }
  };

  ns.exportData = function (fmt) {
    var filters = ns.state.lastFilters || ns.buildFilters();
    var qs = new URLSearchParams({
      format: fmt || "xlsx",
      start_time: filters.start_time || "",
      end_time: filters.end_time || "",
      branches: (filters.branches || []).join(","),
    });
    root.location.href = "/jszahzyj/download/jsbrjqajtj?" + qs.toString();
  };

  ns.bindBranchSelect = function () {
    var display = ns.$("jsbrjqajtjBranchDisplay");
    var dropdown = ns.$("jsbrjqajtjBranchDropdown");
    if (!display || !dropdown) return;
    display.addEventListener("click", function (event) {
      event.stopPropagation();
      dropdown.classList.toggle("show");
    });
    dropdown.addEventListener("change", function (event) {
      var target = event.target;
      if (!target || target.tagName !== "INPUT") return;
      if (target.value === "_all") {
        dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])').forEach(function (cb) {
          cb.checked = target.checked;
        });
      } else {
        var allBox = dropdown.querySelector('input[value="_all"]');
        var itemBoxes = Array.from(dropdown.querySelectorAll('input[type="checkbox"]:not([value="_all"])'));
        if (allBox) {
          allBox.checked = itemBoxes.length > 0 && itemBoxes.every(function (item) { return item.checked; });
        }
      }
      ns.state.selectedBranches = ns.selectedBranchesFromUI();
      ns.updateBranchDisplay();
    });
    document.addEventListener("click", function (event) {
      if (!dropdown.contains(event.target) && !display.contains(event.target)) {
        dropdown.classList.remove("show");
      }
    });
  };

  ns.bindExportDropdown = function () {
    var wrap = ns.$("jsbrjqajtjDd");
    var btn = ns.$("jsbrjqajtjExportBtn");
    if (!wrap || !btn) return;
    btn.addEventListener("click", function () {
      wrap.classList.toggle("open");
    });
    wrap.querySelectorAll("button[data-fmt]").forEach(function (button) {
      button.addEventListener("click", function () {
        wrap.classList.remove("open");
        ns.exportData(button.getAttribute("data-fmt") || "xlsx");
      });
    });
    document.addEventListener("click", function (event) {
      if (!wrap.contains(event.target)) wrap.classList.remove("open");
    });
  };
})(window);
