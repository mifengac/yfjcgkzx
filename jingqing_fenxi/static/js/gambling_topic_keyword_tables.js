(function() {
    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function showEmpty(boxId, containerId) {
        var box = document.getElementById(boxId);
        var container = document.getElementById(containerId);
        if (!box || !container) return;
        box.classList.add("active");
        container.innerHTML = '<div class="monitor-empty-state">无符合条件数据</div>';
    }

    function renderGamblingWayTable(result) {
        var box = document.getElementById("gambling-box-way");
        var container = document.getElementById("gambling-table-way");
        if (!box || !container) return;
        var columns = (result && result.columns) || [];
        var rows = (result && result.rows) || [];
        box.classList.add("active");
        if (!rows.length) {
            container.innerHTML = '<div class="monitor-empty-state">无符合条件数据</div>';
            return;
        }
        var html = '<div class="results-table-container"><table class="special-case-table"><thead><tr><th>地区</th>';
        columns.forEach(function(column) { html += "<th>" + escapeHtml(column) + "</th>"; });
        html += "<th>合计</th></tr></thead><tbody>";
        rows.forEach(function(row) {
            var counts = row.counts || {};
            html += "<tr><td>" + escapeHtml(row.cmdName || "") + "</td>";
            columns.forEach(function(column) { html += "<td>" + escapeHtml(counts[column] || 0) + "</td>"; });
            html += "<td>" + escapeHtml(row.total || 0) + "</td></tr>";
        });
        html += "</tbody></table></div>";
        container.innerHTML = html;
    }

    function renderWildernessTable(result) {
        var box = document.getElementById("gambling-box-wilderness");
        var container = document.getElementById("gambling-table-wilderness");
        if (!box || !container) return;
        var rows = (result && result.rows) || [];
        box.classList.add("active");
        if (!rows.length) {
            container.innerHTML = '<div class="monitor-empty-state">无符合条件数据</div>';
            return;
        }
        var html = '<div class="results-table-container"><table class="special-case-table"><thead><tr><th>地区</th><th>数量</th></tr></thead><tbody>';
        rows.forEach(function(row) {
            html += "<tr><td>" + escapeHtml(row.cmdName || "") + "</td><td>" + escapeHtml(row.total || 0) + "</td></tr>";
        });
        html += "</tbody></table></div>";
        container.innerHTML = html;
    }

    function renderVenueTable(result) {
        var box = document.getElementById("gambling-box-venue");
        var container = document.getElementById("gambling-table-venue");
        if (!box || !container) return;
        var rows = (result && result.rows) || [];
        box.classList.add("active");
        if (!rows.length) {
            showEmpty("gambling-box-venue", "gambling-table-venue");
            return;
        }
        var html = '<div class="results-table-container"><table class="special-case-table"><thead><tr><th>地区编码</th><th>地区</th><th>数量</th></tr></thead><tbody>';
        rows.forEach(function(row) {
            html += "<tr><td>" + escapeHtml(row.cmdId || "") + "</td><td>" + escapeHtml(row.cmdName || "") + "</td><td>" + escapeHtml(row.total || 0) + "</td></tr>";
        });
        html += "</tbody></table></div>";
        container.innerHTML = html;
    }

    window.GamblingTopicKeywordTables = {
        renderGamblingWayTable: renderGamblingWayTable,
        renderWildernessTable: renderWildernessTable,
        renderVenueTable: renderVenueTable
    };
})();
