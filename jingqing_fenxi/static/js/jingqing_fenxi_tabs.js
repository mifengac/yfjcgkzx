(function() {
    function initTabSwitching() {
        var buttons = document.querySelectorAll('[data-tab-target]');
        var panels = document.querySelectorAll('[data-tab-panel]');
        buttons.forEach(function(button) {
            button.addEventListener('click', function() {
                var target = button.getAttribute('data-tab-target');
                buttons.forEach(function(item) { item.classList.remove('active'); });
                panels.forEach(function(panel) {
                    panel.classList.toggle('active', panel.getAttribute('data-tab-panel') === target);
                });
                button.classList.add('active');
            });
        });
    }

    document.addEventListener('DOMContentLoaded', function() {
        initTabSwitching();
        if (window.AnalysisTabPage) window.AnalysisTabPage.init();
        if (window.FightTopicTabPage) window.FightTopicTabPage.init();
        if (window.CustomCaseMonitorTabPage) window.CustomCaseMonitorTabPage.init();
    });
})();
