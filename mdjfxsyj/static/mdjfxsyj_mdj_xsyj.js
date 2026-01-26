(() => {
  const form = document.getElementById("filterForm");

  // 分局多选下拉
  const fenjuBox = document.getElementById("fenjuSelectBox");
  const fenjuMenu = document.getElementById("fenjuSelectMenu");
  const fenjuSelectedText = document.getElementById("fenjuSelectedText");

  const updateFenjuText = () => {
    const checked = [...fenjuMenu.querySelectorAll('input[type="checkbox"][name="fenju"]:checked')].map(
      (x) => x.value
    );
    if (!checked.length) {
      fenjuSelectedText.textContent = "请选择分局";
      return;
    }
    if (checked.length <= 2) {
      fenjuSelectedText.textContent = checked.join("，");
      return;
    }
    fenjuSelectedText.textContent = `${checked.slice(0, 2).join("，")} 等${checked.length}项`;
  };

  fenjuBox.addEventListener("click", (e) => {
    e.stopPropagation();
    fenjuMenu.classList.toggle("show");
  });

  fenjuMenu.addEventListener("change", () => updateFenjuText());

  // 导出下拉
  const exportBtn = document.getElementById("exportBtn");
  const exportMenu = document.getElementById("exportMenu");

  exportBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    exportMenu.classList.toggle("show");
  });

  exportMenu.querySelectorAll("button[data-format]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const fmt = btn.dataset.format;
      const params = new URLSearchParams(new FormData(form));
      params.delete("page"); // 导出不需要页码
      window.location.href = `${window.__MDJXS_EXPORT_URL__}?format=${encodeURIComponent(fmt)}&${params.toString()}`;
    });
  });

  document.addEventListener("click", () => {
    fenjuMenu.classList.remove("show");
    exportMenu.classList.remove("show");
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      fenjuMenu.classList.remove("show");
      exportMenu.classList.remove("show");
    }
  });

  // 初始化
  updateFenjuText();

  // 点“查询”默认回到第一页
  form.addEventListener("submit", () => {
    const pageInput = form.querySelector('input[name="page"]');
    if (pageInput) pageInput.value = "1";
  });

  // 翻页时保持 page_size 变化后从第一页开始
  const pageSize = document.getElementById("page_size");
  pageSize.addEventListener("change", () => {
    const pageInput = form.querySelector('input[name="page"]');
    if (pageInput) pageInput.value = "1";
    form.submit();
  });
})();
