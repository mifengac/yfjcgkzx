(function (root) {
  var ns = root.JSZahzTopicApp = root.JSZahzTopicApp || {};

  function init() {
    var queryBtn;
    var uploadBtn;
    var exportBtn;
    if (!ns.$ || !ns.$("jszahzTopicApp")) return;

    ns.bindMultiSelect("jszahzTopicBranchDisplay", "jszahzTopicBranchMenu", "selectedBranches", "branchOptions");
    ns.bindMultiSelect("jszahzTopicTypeDisplay", "jszahzTopicTypeMenu", "selectedTypes", "typeOptions");
    ns.bindMultiSelect("jszahzTopicRiskDisplay", "jszahzTopicRiskMenu", "selectedRisks", "riskOptions");
    ns.bindMenuClose();
    ns.initDrawerEvents();

    uploadBtn = ns.$("jszahzTopicUploadBtn");
    queryBtn = ns.$("jszahzTopicQueryBtn");
    exportBtn = ns.$("jszahzTopicExportBtn");

    if (uploadBtn) {
      uploadBtn.addEventListener("click", function () {
        ns.uploadExcel().catch(function (error) {
          ns.setError(String(error));
          ns.setStatus("");
        });
      });
    }
    if (queryBtn) {
      queryBtn.addEventListener("click", function () {
        ns.querySummary().catch(function (error) {
          ns.setError(String(error));
          ns.setStatus("");
        });
      });
    }
    if (exportBtn) {
      exportBtn.addEventListener("click", ns.exportSummary);
    }

    ns.loadDefaults().catch(function (error) {
      ns.setError(String(error));
      ns.setStatus("");
    });
  }

  document.addEventListener("DOMContentLoaded", init);
})(window);
