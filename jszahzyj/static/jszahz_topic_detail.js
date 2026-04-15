(function (root) {
  const config = root.JSZahzTopicDetailConfig || {};
  const selectedRelationTypes = config.selectedRelationTypes || [];

  function notifyParentReady() {
    if (root.parent && root.parent !== root) {
      root.parent.postMessage({ type: "jszahz-topic-detail-ready" }, "*");
    }
  }

  function reportPreferredWidth() {
    const tableWrap = document.querySelector(".table-wrap");
    const page = document.querySelector(".page");
    const contentWidth = Math.max(
      tableWrap ? tableWrap.scrollWidth : 0,
      page ? page.scrollWidth : 0,
      640
    );
    if (root.parent && root.parent !== root) {
      root.parent.postMessage({
        type: "jszahz-topic-detail-width",
        width: contentWidth,
      }, "*");
    }
  }

  function ensureRelationTypesChecked() {
    const form = document.getElementById("jszahzTopicDetailForm");
    const boxes = Array.from(form.querySelectorAll('input[name="relation_types"]'));
    if (boxes.some(function (item) { return item.checked; })) {
      return;
    }
    boxes.forEach(function (item) { item.checked = true; });
  }

  function resetPageToFirst() {
    const pageInput = document.getElementById("jszahzTopicDetailPageInput");
    if (pageInput) {
      pageInput.value = "1";
    }
  }

  function applyToolbarFilters() {
    const form = document.getElementById("jszahzTopicDetailForm");
    closeRelationPicker();
    ensureRelationTypesChecked();
    resetPageToFirst();
    form.submit();
  }

  function goToPage(pageNumber) {
    const form = document.getElementById("jszahzTopicDetailForm");
    const pageInput = document.getElementById("jszahzTopicDetailPageInput");
    closeRelationPicker();
    ensureRelationTypesChecked();
    if (pageInput) {
      pageInput.value = String(pageNumber || 1);
    }
    form.submit();
  }

  function toggleAllRelationTypes(checked) {
    const form = document.getElementById("jszahzTopicDetailForm");
    Array.from(form.querySelectorAll('input[name="relation_types"]')).forEach(function (item) {
      item.checked = !!checked;
    });
  }

  function getRelationPicker() {
    return document.getElementById("jszahzTopicRelationPicker");
  }

  function toggleRelationPicker() {
    const picker = getRelationPicker();
    if (!picker) return;
    picker.classList.toggle("open");
  }

  function closeRelationPicker() {
    const picker = getRelationPicker();
    if (!picker) return;
    picker.classList.remove("open");
  }

  function initRelationPicker() {
    const picker = getRelationPicker();
    const trigger = document.getElementById("jszahzTopicRelationPickerTrigger");
    const menu = document.getElementById("jszahzTopicRelationPickerMenu");
    if (!picker || !trigger || !menu) {
      return;
    }

    trigger.addEventListener("click", function (event) {
      event.preventDefault();
      event.stopPropagation();
      toggleRelationPicker();
    });

    menu.addEventListener("click", function (event) {
      event.stopPropagation();
    });

    document.addEventListener("click", function (event) {
      if (!picker.contains(event.target)) {
        closeRelationPicker();
      }
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape") {
        closeRelationPicker();
      }
    });
  }

  function showExportOverlay() {
    const overlay = document.getElementById("jszahzTopicExportOverlay");
    if (overlay) {
      overlay.classList.remove("export-overlay-hidden");
    }
  }

  function hideExportOverlay() {
    const overlay = document.getElementById("jszahzTopicExportOverlay");
    if (overlay) {
      overlay.classList.add("export-overlay-hidden");
    }
  }

  function buildExportParams() {
    const form = document.getElementById("jszahzTopicDetailForm");
    ensureRelationTypesChecked();
    const formData = new FormData(form);
    const params = new URLSearchParams();
    formData.forEach(function (value, key) {
      if (key === "page" || key === "page_size") {
        return;
      }
      params.append(key, value);
    });
    return params;
  }

  function downloadDetail() {
    closeRelationPicker();
    showExportOverlay();
    root.location.href = config.downloadDetailUrl + "?" + buildExportParams().toString();
  }

  function buildRelationHref(relationType, zjhm, xm) {
    const params = new URLSearchParams({
      relation_type: relationType || "",
      zjhm: zjhm || "",
      xm: xm || "",
    });
    return config.relationPageUrl + "?" + params.toString();
  }

  function renderRelationPlaceholder(el, count) {
    const relationType = el.getAttribute("data-relation-type") || "";
    const zjhm = el.getAttribute("data-zjhm") || "";
    const xm = el.getAttribute("data-xm") || "";
    const safeCount = Number(count) || 0;

    if (safeCount > 0) {
      const link = document.createElement("a");
      link.className = "relation-link relation-link-active";
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.href = buildRelationHref(relationType, zjhm, xm);
      link.textContent = "查看(" + safeCount + ")";
      el.replaceWith(link);
      return;
    }

    const span = document.createElement("span");
    span.className = "relation-link relation-link-disabled";
    span.setAttribute("aria-disabled", "true");
    span.textContent = "查看(0)";
    el.replaceWith(span);
  }

  async function loadRelationCounts() {
    const placeholders = Array.from(document.querySelectorAll("[data-relation-placeholder='1']"));
    if (!placeholders.length) {
      reportPreferredWidth();
      return;
    }

    const zjhms = Array.from(new Set(placeholders
      .map(function (el) { return (el.getAttribute("data-zjhm") || "").trim(); })
      .filter(Boolean)));

    if (!zjhms.length) {
      placeholders.forEach(function (el) { renderRelationPlaceholder(el, 0); });
      reportPreferredWidth();
      return;
    }

    try {
      const response = await fetch(config.detailRelationCountsUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          zjhms: zjhms,
          relation_types: selectedRelationTypes,
        }),
      });
      const payload = await response.json();
      if (!response.ok || !payload.success) {
        throw new Error((payload && payload.message) || "关联统计加载失败");
      }

      const counts = payload.counts || {};
      placeholders.forEach(function (el) {
        const relationType = el.getAttribute("data-relation-type") || "";
        const zjhm = (el.getAttribute("data-zjhm") || "").trim().toUpperCase();
        const count = Number((((counts[relationType] || {})[zjhm]) || 0));
        renderRelationPlaceholder(el, count);
      });
    } catch (_error) {
      placeholders.forEach(function (el) {
        const span = document.createElement("span");
        span.className = "relation-link relation-link-disabled";
        span.setAttribute("aria-disabled", "true");
        span.textContent = "统计失败";
        el.replaceWith(span);
      });
    } finally {
      reportPreferredWidth();
    }
  }

  root.applyToolbarFilters = applyToolbarFilters;
  root.goToPage = goToPage;
  root.toggleAllRelationTypes = toggleAllRelationTypes;
  root.downloadDetail = downloadDetail;
  root.hideExportOverlay = hideExportOverlay;

  root.addEventListener("DOMContentLoaded", function () {
    const pageSizeSelect = document.getElementById("jszahzTopicDetailPageSize");
    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", applyToolbarFilters);
    }
    initRelationPicker();
    notifyParentReady();
    reportPreferredWidth();
  });
  root.addEventListener("load", notifyParentReady);
  root.addEventListener("load", reportPreferredWidth);
  root.addEventListener("resize", reportPreferredWidth);
  root.addEventListener("load", loadRelationCounts);
})(window);
