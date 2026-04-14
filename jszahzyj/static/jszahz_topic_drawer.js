(function (root) {
  var ns = root.JSZahzTopicApp = root.JSZahzTopicApp || {};

  ns.clearDrawerLoadingTimer = function () {
    if (ns.state.drawerSlowTimer) {
      root.clearTimeout(ns.state.drawerSlowTimer);
      ns.state.drawerSlowTimer = null;
    }
  };

  ns.showDrawerLoading = function () {
    var loading = ns.$("jszahzTopicDrawerLoading");
    var title = ns.$("jszahzTopicDrawerLoadingTitle");
    var hint = ns.$("jszahzTopicDrawerLoadingHint");
    if (!loading) return;
    ns.clearDrawerLoadingTimer();
    if (title) {
      title.textContent = "正在查询详细数据及关联统计，请稍候...";
    }
    if (hint) {
      hint.textContent = "当前会按身份证号去重加载明细，并统计 7 类关联数据";
    }
    loading.classList.remove("jszahz-topic-hidden");
    ns.state.drawerSlowTimer = root.setTimeout(function () {
      if (hint && !loading.classList.contains("jszahz-topic-hidden")) {
        hint.textContent = "查询时间较长，正在统计关联数据，请继续等待";
      }
    }, 3000);
  };

  ns.hideDrawerLoading = function () {
    var loading = ns.$("jszahzTopicDrawerLoading");
    ns.clearDrawerLoadingTimer();
    if (loading) {
      loading.classList.add("jszahz-topic-hidden");
    }
  };

  ns.applyDrawerWidth = function () {
    var panel = ns.$("jszahzTopicDrawerPanel");
    var viewportWidth;
    var preferredWidth;
    if (!panel) return;
    if (!ns.state.drawerContentWidth) {
      panel.style.removeProperty("--jszahz-topic-drawer-width");
      return;
    }

    viewportWidth =
      root.innerWidth <= 960
        ? Math.max(root.innerWidth - 12, 360)
        : Math.max(Math.floor(root.innerWidth * 0.75), 640);
    preferredWidth = Math.min(
      Math.max(Number(ns.state.drawerContentWidth) + 40, 640),
      viewportWidth
    );
    panel.style.setProperty("--jszahz-topic-drawer-width", preferredWidth + "px");
  };

  ns.openDrawer = function (branchCode, branchName) {
    var filters = ns.state.lastFilters || ns.buildFilters();
    var title = ns.$("jszahzTopicDrawerTitle");
    var frame = ns.$("jszahzTopicDrawerFrame");
    var drawer = ns.$("jszahzTopicDrawer");
    var params;
    if (!frame || !drawer) return;

    if (title) {
      title.textContent = (branchName || "汇总") + " - 详细数据";
    }

    ns.state.drawerContentWidth = null;
    ns.applyDrawerWidth();
    ns.showDrawerLoading();
    drawer.classList.add("open");

    params = new URLSearchParams({
      branch_code: branchCode || "__ALL__",
      branch_name: branchName || "汇总",
      person_types: (filters.person_types || []).join(","),
      risk_labels: (filters.risk_labels || []).join(","),
      managed_only: filters.managed_only ? "1" : "0",
      _ts: String(Date.now()),
    });
    frame.src = "/jszahzyj/jszahzztk/detail_page?" + params.toString();
  };

  ns.closeDrawer = function () {
    var drawer = ns.$("jszahzTopicDrawer");
    var frame = ns.$("jszahzTopicDrawerFrame");
    ns.state.drawerContentWidth = null;
    ns.applyDrawerWidth();
    ns.hideDrawerLoading();
    if (frame) frame.src = "about:blank";
    if (drawer) drawer.classList.remove("open");
  };

  ns.handleDrawerMessage = function (event) {
    var frame = ns.$("jszahzTopicDrawerFrame");
    var data;
    if (!frame || event.source !== frame.contentWindow) return;
    data = event.data || {};
    if (data.type === "jszahz-topic-detail-ready") {
      ns.hideDrawerLoading();
      return;
    }
    if (data.type === "jszahz-topic-detail-width") {
      ns.state.drawerContentWidth = Number(data.width) || null;
      ns.applyDrawerWidth();
      ns.hideDrawerLoading();
    }
  };

  ns.handleDrawerFrameLoad = function () {
    ns.hideDrawerLoading();
  };

  ns.initDrawerEvents = function () {
    var closeBtn = ns.$("jszahzTopicDrawerCloseBtn");
    var mask = ns.$("jszahzTopicDrawerMask");
    var frame = ns.$("jszahzTopicDrawerFrame");
    if (closeBtn) closeBtn.addEventListener("click", ns.closeDrawer);
    if (mask) mask.addEventListener("click", ns.closeDrawer);
    if (frame) frame.addEventListener("load", ns.handleDrawerFrameLoad);
    root.addEventListener("message", ns.handleDrawerMessage);
    root.addEventListener("resize", ns.applyDrawerWidth);
  };
})(window);
