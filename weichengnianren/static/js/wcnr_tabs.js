(function () {
  function activateTab(app, tab) {
    if (!app) return;
    app.setAttribute("data-active-tab", tab);

    const tabs = app.querySelectorAll(".wcnr-tab-btn");
    tabs.forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-tab") === tab);
    });

    const panels = app.querySelectorAll(".wcnr-tab-panel");
    panels.forEach((panel) => {
      panel.classList.toggle("active", panel.id === `tab-${tab}`);
    });

    const url = new URL(window.location.href);
    url.searchParams.set("tab", tab);
    window.history.replaceState({}, "", url.toString());
    document.dispatchEvent(new CustomEvent("wcnr:tabchange", { detail: { tab } }));
  }

  function initTabs() {
    const app = document.getElementById("weichengnianrenApp");
    const tabs = document.getElementById("wcnrTabs");
    if (!app || !tabs) return;

    const initialTab = app.getAttribute("data-active-tab") || "wcnr9lbq";
    tabs.querySelectorAll(".wcnr-tab-btn").forEach((btn) => {
      btn.addEventListener("click", function () {
        activateTab(app, btn.getAttribute("data-tab") || "wcnr9lbq");
      });
    });

    activateTab(app, initialTab);
  }

  document.addEventListener("DOMContentLoaded", initTabs);
})();
