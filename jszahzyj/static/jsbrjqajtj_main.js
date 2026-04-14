(function (root) {
  var ns = root.JSBrjqajtjApp = root.JSBrjqajtjApp || {};

  function init() {
    if (!ns.$ || !ns.$("tab-jsbrjqajtj")) return;
    var queryBtn = ns.$("jsbrjqajtjQueryBtn");
    ns.initTabs();
    ns.bindBranchSelect();
    ns.bindExportDropdown();
    if (queryBtn) {
      queryBtn.addEventListener("click", ns.queryData);
    }
    ns.loadDefaults().catch(function (error) {
      ns.setErr(String(error));
    });
  }

  document.addEventListener("DOMContentLoaded", init);
})(window);
