(function(root, factory) {
    var api = factory();
    if (typeof module !== "undefined" && module.exports) {
        module.exports = api;
    } else {
        root.AnalysisCaseTypeSource = api;
    }
})(typeof globalThis !== "undefined" ? globalThis : this, function() {
    var SOURCES = {
        nature: {
            label: "警情性质",
            endpoint: "/jingqing_fenxi/natureTreeData"
        },
        plan: {
            label: "查询预案",
            endpoint: "/jingqing_fenxi/planTreeData"
        }
    };

    function normalizeSource(source) {
        return SOURCES[source] ? source : "nature";
    }

    function getSourceLabel(source) {
        return SOURCES[normalizeSource(source)].label;
    }

    function getSourceEndpoint(source) {
        return SOURCES[normalizeSource(source)].endpoint;
    }

    function nodeId(node) {
        return String((node && node.id) || "").trim();
    }

    function parentId(node) {
        return String((node && node.pId) || "").trim();
    }

    function getVisibleNodes(treeData) {
        return (treeData || []).filter(function(item) {
            return nodeId(item) && !parentId(item);
        });
    }

    function pushUnique(list, seen, value) {
        var token = String(value || "").trim();
        if (!token || seen[token]) return;
        seen[token] = true;
        list.push(token);
    }

    function buildChildrenMap(treeData) {
        var childrenMap = {};
        (treeData || []).forEach(function(item) {
            var pid = parentId(item);
            if (!pid) return;
            if (!childrenMap[pid]) childrenMap[pid] = [];
            childrenMap[pid].push(item);
        });
        return childrenMap;
    }

    function collectNatureSelection(treeData, selectedParentIds) {
        var selected = selectedParentIds || [];
        var childrenMap = buildChildrenMap(treeData);
        var nodeById = {};
        (treeData || []).forEach(function(item) {
            var id = nodeId(item);
            if (id && !nodeById[id]) nodeById[id] = item;
        });

        var codes = [];
        var names = [];
        var codeSeen = {};
        var nameSeen = {};

        function visit(node) {
            if (!node) return;
            var id = nodeId(node);
            pushUnique(codes, codeSeen, id);
            pushUnique(names, nameSeen, node.name);
            (childrenMap[id] || []).forEach(visit);
        }

        selected.forEach(function(id) {
            visit(nodeById[String(id || "").trim()]);
        });

        return { codes: codes, names: names };
    }

    function collectPlanSelection(treeData, selectedParentIds) {
        var selected = {};
        (selectedParentIds || []).forEach(function(id) {
            var token = String(id || "").trim();
            if (token) selected[token] = true;
        });

        var codes = [];
        var names = [];
        var codeSeen = {};
        var nameSeen = {};
        (treeData || []).forEach(function(item) {
            if (!selected[parentId(item)]) return;
            pushUnique(codes, codeSeen, item.tag);
            pushUnique(names, nameSeen, item.name);
        });
        return { codes: codes, names: names };
    }

    function collectSelectionPayload(treeData, selectedParentIds, source) {
        if (normalizeSource(source) === "plan") {
            return collectPlanSelection(treeData, selectedParentIds);
        }
        return collectNatureSelection(treeData, selectedParentIds);
    }

    return {
        normalizeSource: normalizeSource,
        getSourceLabel: getSourceLabel,
        getSourceEndpoint: getSourceEndpoint,
        getVisibleNodes: getVisibleNodes,
        collectSelectionPayload: collectSelectionPayload
    };
});
