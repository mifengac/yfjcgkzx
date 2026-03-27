(function() {
    var state = {
        branches: [],
        pageNum: 1,
        pageSize: 15,
        total: 0,
        initialized: false
    };

    function formatDateTimeLocal(value) {
        if (!value) return '';
        return String(value).replace(' ', 'T');
    }

    function formatRequestTime(value) {
        return value ? String(value).replace('T', ' ') : '';
    }

    function getSelectedBranches() {
        var boxes = document.querySelectorAll('#wanderBranchDropdown input[type="checkbox"]');
        var values = [];
        for (var i = 0; i < boxes.length; i++) {
            if (boxes[i].checked) values.push(boxes[i].value);
        }
        return values;
    }

    function renderBranchLabel() {
        var labelNode = document.querySelector('#wanderBranchDisplay span');
        if (!labelNode) return;
        var values = getSelectedBranches();
        if (values.length === 0) labelNode.textContent = '全部';
        else if (values.length === state.branches.length) labelNode.textContent = '全部分局';
        else labelNode.textContent = values.join('、');
    }

    function renderBranchOptions(branches) {
        state.branches = branches || [];
        var dropdown = document.getElementById('wanderBranchDropdown');
        if (!dropdown) return;
        dropdown.innerHTML = '';
        state.branches.forEach(function(branch) {
            var label = document.createElement('label');
            var input = document.createElement('input');
            input.type = 'checkbox';
            input.value = branch.value;
            input.addEventListener('change', renderBranchLabel);
            label.appendChild(input);
            label.appendChild(document.createTextNode(branch.label));
            dropdown.appendChild(label);
        });
        renderBranchLabel();
    }

    function setError(message) {
        var box = document.getElementById('wanderErr');
        if (!box) return;
        if (message) {
            box.textContent = message;
            box.classList.remove('special-case-hidden');
        } else {
            box.textContent = '';
            box.classList.add('special-case-hidden');
        }
    }

    function renderTable(rows) {
        var table = document.getElementById('wanderTable');
        if (!table) return;
        var html = '<thead><tr>' +
            '<th>接警号</th><th>报警时间</th><th>分局编码</th><th>管辖单位</th><th>警情级别</th><th>涉案地址</th><th>报警人</th><th>报警人电话</th><th>简要案情</th><th>反馈内容</th>' +
            '</tr></thead><tbody>';
        if (!rows || rows.length === 0) {
            html += '<tr><td colspan="10" style="text-align:center;color:#64748b;">无符合条件数据</td></tr>';
        } else {
            rows.forEach(function(row) {
                html += '<tr>' +
                    '<td>' + (row.caseNo || '') + '</td>' +
                    '<td>' + (row.callTime || '') + '</td>' +
                    '<td>' + (row.cmdId || '') + '</td>' +
                    '<td>' + (row.dutyDeptName || '') + '</td>' +
                    '<td>' + (row.caseLevelName || '') + '</td>' +
                    '<td>' + (row.occurAddress || '') + '</td>' +
                    '<td>' + (row.callerName || '') + '</td>' +
                    '<td>' + (row.callerPhone || '') + '</td>' +
                    '<td>' + (row.caseContents || '') + '</td>' +
                    '<td>' + (row.replies || '') + '</td>' +
                    '</tr>';
            });
        }
        html += '</tbody>';
        table.innerHTML = html;
    }

    function renderPagination() {
        var container = document.getElementById('wanderPagination');
        if (!container) return;
        var totalPages = Math.max(1, Math.ceil(state.total / state.pageSize));
        container.innerHTML =
            '<div class="pagination-meta">共 ' + state.total + ' 条，第 ' + state.pageNum + '/' + totalPages + ' 页</div>' +
            '<div class="pagination-controls">' +
            '<button type="button" id="wanderPrevPage" ' + (state.pageNum <= 1 ? 'disabled' : '') + '>上一页</button>' +
            '<button type="button" id="wanderNextPage" ' + (state.pageNum >= totalPages ? 'disabled' : '') + '>下一页</button>' +
            '</div>';
        document.getElementById('wanderPrevPage').addEventListener('click', function() {
            if (state.pageNum > 1) query(state.pageNum - 1);
        });
        document.getElementById('wanderNextPage').addEventListener('click', function() {
            if (state.pageNum < totalPages) query(state.pageNum + 1);
        });
    }

    function renderStatus(result) {
        var node = document.getElementById('wanderStatus');
        if (!node) return;
        var debug = result.debug || {};
        var traceId = debug.trace_id ? ('，trace=' + debug.trace_id) : '';
        var stats = '';
        if (typeof debug.upstream_row_count !== 'undefined') {
            stats = '，上游返回 ' + debug.upstream_row_count + ' 条，关键词命中 ' + (debug.keyword_match_count || 0) + ' 条，分局过滤后 ' + (debug.branch_filtered_count || 0) + ' 条';
        }
        node.textContent = '时间范围: ' + result.start_time + ' 至 ' + result.end_time + '，命中 ' + result.total + ' 条' + stats + traceId;
    }

    function buildPayload(pageNum) {
        return {
            start_time: formatRequestTime(document.getElementById('wanderStartTime').value),
            end_time: formatRequestTime(document.getElementById('wanderEndTime').value),
            branches: getSelectedBranches(),
            page_num: pageNum || state.pageNum,
            page_size: state.pageSize
        };
    }

    function query(pageNum) {
        var payload = buildPayload(pageNum);
        fetch('/jingqing_fenxi/api/wander-case/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
            .then(function(response) { return response.json(); })
            .then(function(result) {
                if (!result.success) throw new Error(result.message || '查询失败');
                state.pageNum = result.page_num;
                state.pageSize = result.page_size;
                state.total = result.total;
                setError('');
                renderStatus(result);
                renderTable(result.rows || []);
                renderPagination();
            })
            .catch(function(error) {
                setError(error.message || '查询失败');
                renderTable([]);
                state.total = 0;
                renderPagination();
            });
    }

    function exportData(format) {
        var payload = buildPayload(1);
        var params = new URLSearchParams({
            format: format,
            start_time: payload.start_time,
            end_time: payload.end_time,
            branches: payload.branches.join(',')
        });
        window.location.href = '/jingqing_fenxi/download/wander-case?' + params.toString();
    }

    function bindEvents() {
        var display = document.getElementById('wanderBranchDisplay');
        var dropdown = document.getElementById('wanderBranchDropdown');
        var menu = document.querySelector('#wanderDownloadMenu .download-menu-content');
        if (display) {
            display.addEventListener('click', function(event) {
                event.stopPropagation();
                dropdown.classList.toggle('show');
            });
        }
        document.addEventListener('click', function() {
            if (dropdown) dropdown.classList.remove('show');
            if (menu) menu.classList.remove('show');
        });
        if (dropdown) dropdown.addEventListener('click', function(event) { event.stopPropagation(); });
        document.querySelector('[data-special-case-type="wander"] [data-action="query"]').addEventListener('click', function() {
            query(1);
        });
        document.querySelector('#wanderDownloadMenu [data-action="toggle-export"]').addEventListener('click', function(event) {
            event.stopPropagation();
            menu.classList.toggle('show');
        });
        document.querySelectorAll('#wanderDownloadMenu [data-action="export"]').forEach(function(button) {
            button.addEventListener('click', function(event) {
                event.stopPropagation();
                menu.classList.remove('show');
                exportData(button.getAttribute('data-format'));
            });
        });
    }

    function initDefaults() {
        fetch('/jingqing_fenxi/api/wander-case/defaults')
            .then(function(response) { return response.json(); })
            .then(function(result) {
                if (!result.success) throw new Error(result.message || '初始化失败');
                document.getElementById('wanderStartTime').value = formatDateTimeLocal(result.start_time);
                document.getElementById('wanderEndTime').value = formatDateTimeLocal(result.end_time);
                renderBranchOptions(result.branches || []);
                query(1);
            })
            .catch(function(error) {
                setError(error.message || '初始化失败');
            });
    }

    function init() {
        if (state.initialized || !document.getElementById('wanderStartTime')) return;
        state.initialized = true;
        bindEvents();
        initDefaults();
    }

    window.WanderCaseTabPage = { init: init };
})();