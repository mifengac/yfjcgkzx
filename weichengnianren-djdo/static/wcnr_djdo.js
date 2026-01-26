(() => {
  const baseUrl = "/weichengnianren-djdo";

  // 全局变量：存储原始责任人数据
  let originalResponsibleData = null;

  const parseServerDt = (s) => {
    const [d, t] = s.split(" ");
    const [Y, M, D] = d.split("-").map(Number);
    const [h, m, sec] = t.split(":").map(Number);
    return new Date(Y, M - 1, D, h, m, sec);
  };

  const toLocalInputValue = (dt) => {
    const pad = (n) => String(n).padStart(2, "0");
    return `${dt.getFullYear()}-${pad(dt.getMonth() + 1)}-${pad(dt.getDate())}T${pad(
      dt.getHours()
    )}:${pad(dt.getMinutes())}:${pad(dt.getSeconds())}`;
  };

  const toServerValue = (localVal) => {
    if (!localVal) return "";
    return localVal.replace("T", " ");
  };

  const defaultStart = parseServerDt(window.__WCN_DJDO_DEFAULT_START__);
  const defaultEnd = parseServerDt(window.__WCN_DJDO_DEFAULT_END__);

  const globalStart = document.getElementById("globalStart");
  const globalEnd = document.getElementById("globalEnd");
  globalStart.value = toLocalInputValue(defaultStart);
  globalEnd.value = toLocalInputValue(defaultEnd);

  const caseTypeSelect = document.getElementById("caseTypeSelect");
  const caseTypeDropdown = document.getElementById("caseTypeDropdown");
  const caseTypeTrigger = document.getElementById("caseTypeTrigger");
  const caseTypePanel = document.getElementById("caseTypePanel");

  const updateCaseTypeTriggerText = () => {
    const selected = Array.from(caseTypeSelect.selectedOptions).map((opt) => opt.value);
    if (!selected.length) {
      caseTypeTrigger.textContent = "请选择类型";
      return;
    }
    if (selected.length <= 2) {
      caseTypeTrigger.textContent = selected.join("，");
      return;
    }
    caseTypeTrigger.textContent = `${selected.slice(0, 2).join("，")} 等${selected.length}项`;
  };

  const rebuildCaseTypePanel = () => {
    caseTypePanel.innerHTML = "";
    Array.from(caseTypeSelect.options).forEach((opt) => {
      const label = document.createElement("label");
      label.className = "multi-select-option";

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = Boolean(opt.selected);
      checkbox.addEventListener("change", () => {
        opt.selected = checkbox.checked;
        updateCaseTypeTriggerText();
      });

      const text = document.createElement("span");
      text.textContent = opt.value;

      label.appendChild(checkbox);
      label.appendChild(text);
      caseTypePanel.appendChild(label);
    });

    updateCaseTypeTriggerText();
  };

  const closeCaseTypePanel = () => caseTypePanel.classList.remove("show");
  caseTypeTrigger.addEventListener("click", (e) => {
    e.stopPropagation();
    caseTypePanel.classList.toggle("show");
  });
  document.addEventListener("click", (e) => {
    if (!caseTypeDropdown.contains(e.target)) {
      closeCaseTypePanel();
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeCaseTypePanel();
  });

  const metricCards = [...document.querySelectorAll(".card[data-metric]")];
  const charts = new Map();

  const colors = ["#60a5fa", "#34d399", "#f59e0b"];
  const rateColor = "#ef4444";

  // 加载案件类型列表
  const loadCaseTypes = async () => {
    try {
      const res = await fetch(`${baseUrl}/api/case_types`, { credentials: "same-origin" });
      const data = await res.json();
      if (!data.success) throw new Error(data.message || "获取类型失败");

      caseTypeSelect.innerHTML = ""; // 清空现有选项

      data.types.forEach(type => {
        const option = document.createElement("option");
        option.value = type;
        option.textContent = type;
        // 默认选中"打架斗殴"
        if (type === "打架斗殴") {
          option.selected = true;
        }
        caseTypeSelect.appendChild(option);
      });

      rebuildCaseTypePanel();
    } catch (e) {
      console.error("加载类型失败:", e);
      alert(e.message || String(e));
    }
  };

  // 获取选中的类型
  const getSelectedCaseTypes = () => {
    const selected = Array.from(caseTypeSelect.selectedOptions).map(opt => opt.value);
    return selected.length > 0 ? selected : null;
  };

  const buildChart = (canvas, labels, series, seriesData) => {
    const ctx = canvas.getContext("2d");
    return new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: series.map((name, idx) => ({
          label: name,
          data: seriesData[idx],
          backgroundColor: String(name || "").includes("率") ? rateColor : colors[idx % colors.length],
          borderRadius: 6,
        })),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", labels: { color: "#e6edf3" } },
          tooltip: {
            enabled: true,
            callbacks: {
              label: (ctx) => {
                const name = String(ctx.dataset.label || "");
                const v = Number(ctx.parsed && ctx.parsed.y ? ctx.parsed.y : 0);
                const isRate = name.includes("率");
                if (isRate) return `${name}: ${Number.isFinite(v) ? v.toFixed(2) : "0.00"}%`;
                if (Number.isFinite(v) && Number.isInteger(v)) return `${name}: ${v}`;
                return `${name}: ${Number.isFinite(v) ? v : 0}`;
              },
            },
          },
        },
        scales: {
          x: { ticks: { color: "#cbd5e1" }, grid: { color: "rgba(255,255,255,0.06)" } },
          y: { ticks: { color: "#cbd5e1" }, grid: { color: "rgba(255,255,255,0.06)" } },
        },
      },
    });
  };

  const renderMetric = async (card) => {
    const metric = card.dataset.metric;
    const startEl = card.querySelector("input.start");
    const endEl = card.querySelector("input.end");
    const sortByEl = card.querySelector("select.sortBy");
    const sortDirEl = card.querySelector("select.sortDir");
    const statEl = card.querySelector(".stat");

    const start = toServerValue(startEl.value);
    const end = toServerValue(endEl.value);
    const caseTypes = getSelectedCaseTypes();

    let url = `${baseUrl}/api/metric/${encodeURIComponent(metric)}?start_time=${encodeURIComponent(
      start
    )}&end_time=${encodeURIComponent(end)}`;

    if (caseTypes && caseTypes.length > 0) {
      url += `&case_types=${encodeURIComponent(caseTypes.join(","))}`;
    }

    const res = await fetch(url, { credentials: "same-origin" });
    const data = await res.json();
    if (!data.success) throw new Error(data.message || "请求失败");

    let rows = data.chart_rows || [];
    const sortIdx = Number(sortByEl.value || 0);
    const sortDir = sortDirEl.value || "desc";
    const series = data.series || [];

    const sortKey = series[sortIdx] || series[0];
    rows = [...rows].sort((a, b) => {
      const av = Number(a[sortKey] || 0);
      const bv = Number(b[sortKey] || 0);
      return sortDir === "asc" ? av - bv : bv - av;
    });

    const labels = rows.map((r) => r["地区"]);
    const seriesData = series.map((name) => rows.map((r) => Number(r[name] || 0)));

    const canvas = card.querySelector("canvas.chart");
    canvas.parentElement.style.height = "210px";

    if (charts.has(card)) {
      charts.get(card).destroy();
      charts.delete(card);
    }
    charts.set(card, buildChart(canvas, labels, series, seriesData));

    statEl.textContent = `明细：${data.count || 0} 条`;
  };

  const syncCardRangeFromGlobal = (card) => {
    card.querySelector("input.start").value = globalStart.value;
    card.querySelector("input.end").value = globalEnd.value;
  };

  metricCards.forEach((card) => {
    const metric = card.dataset.metric;
    syncCardRangeFromGlobal(card);
    const onChange = () => renderMetric(card).catch((e) => alert(e.message || String(e)));
    card.querySelector("input.start").addEventListener("change", onChange);
    card.querySelector("input.end").addEventListener("change", onChange);
    card.querySelector("select.sortBy").addEventListener("change", onChange);
    card.querySelector("select.sortDir").addEventListener("change", onChange);
    card.querySelector("button.download").addEventListener("click", () => {
      const start = toServerValue(card.querySelector("input.start").value);
      const end = toServerValue(card.querySelector("input.end").value);
      const caseTypes = getSelectedCaseTypes();

      let url = `${baseUrl}/api/export/xlsx?metric=${encodeURIComponent(metric)}&start_time=${encodeURIComponent(
        start
      )}&end_time=${encodeURIComponent(end)}`;

      if (caseTypes && caseTypes.length > 0) {
        url += `&case_types=${encodeURIComponent(caseTypes.join(","))}`;
      }

      window.open(url, "_blank");
    });
  });

  // 监听类型选择变化，自动刷新
  document.getElementById("caseTypeSelect").addEventListener("change", () => {
    Promise.all(metricCards.map((c) => renderMetric(c))).catch((e) => alert(e.message || String(e)));
  });

  document.getElementById("globalRefresh").addEventListener("click", () => {
    metricCards.forEach((card) => syncCardRangeFromGlobal(card));
    Promise.all(metricCards.map((c) => renderMetric(c))).catch((e) => alert(e.message || String(e)));
  });

  document.getElementById("btnExportXlsx").addEventListener("click", () => {
    const start = toServerValue(globalStart.value);
    const end = toServerValue(globalEnd.value);
    const caseTypes = getSelectedCaseTypes();

    let url = `${baseUrl}/api/export/details?start_time=${encodeURIComponent(start)}&end_time=${encodeURIComponent(end)}`;

    if (caseTypes && caseTypes.length > 0) {
      url += `&case_types=${encodeURIComponent(caseTypes.join(","))}`;
    }

    window.open(url, "_blank");
  });

  document.getElementById("btnExportPdf").addEventListener("click", () => {
    const start = toServerValue(globalStart.value);
    const end = toServerValue(globalEnd.value);
    const caseTypes = getSelectedCaseTypes();

    let url = `${baseUrl}/api/export/overview_pdf?start_time=${encodeURIComponent(start)}&end_time=${encodeURIComponent(end)}`;

    if (caseTypes && caseTypes.length > 0) {
      url += `&case_types=${encodeURIComponent(caseTypes.join(","))}`;
    }

    window.open(url, "_blank");
  });

  document.getElementById("btnSendSms").addEventListener("click", async () => {
    const dropdown = document.getElementById("smsDropdownMenu");
    dropdown.classList.toggle("show");
  });

  // 点击页面其他地方关闭下拉菜单
  document.addEventListener("click", (e) => {
    const dropdown = document.getElementById("smsDropdownMenu");
    const btn = document.getElementById("btnSendSms");
    if (!btn.contains(e.target) && !dropdown.contains(e.target)) {
      dropdown.classList.remove("show");
    }
  });

  // 短信功能相关变量
  let currentSmsType = null;
  let currentPassword = null;

  // 密码弹窗元素
  const passwordModal = document.getElementById("passwordModal");
  const passwordInput = document.getElementById("passwordInput");
  const passwordConfirm = document.getElementById("passwordConfirm");
  const passwordCancel = document.getElementById("passwordCancel");

  // 领导弹窗元素
  const leaderModal = document.getElementById("leaderModal");
  const leaderMobiles = document.getElementById("leaderMobiles");
  const leaderContent = document.getElementById("leaderContent");
  const leaderSend = document.getElementById("leaderSend");
  const leaderCancel = document.getElementById("leaderCancel");

  // 责任人弹窗元素
  const responsibleModal = document.getElementById("responsibleModal");
  const responsibleModules = document.getElementById("responsibleModules");
  const responsibleSend = document.getElementById("responsibleSend");
  const responsibleCancel = document.getElementById("responsibleCancel");

  // 显示密码弹窗
  const showPasswordModal = (type) => {
    currentSmsType = type;
    currentPassword = null;
    passwordInput.value = "";
    passwordModal.classList.add("show");
    passwordInput.focus();
  };

  // 隐藏密码弹窗
  const hidePasswordModal = () => {
    passwordModal.classList.remove("show");
  };

  const cancelPasswordModal = () => {
    currentPassword = null;
    currentSmsType = null;
    passwordModal.classList.remove("show");
  };

  // 验证密码并获取预览数据
  const verifyPassword = async (password) => {
    const start = toServerValue(globalStart.value);
    const end = toServerValue(globalEnd.value);
    const caseTypes = getSelectedCaseTypes();

    // 先通过一个简单的API调用验证密码
    // 我们使用 /api/sms/config 接口，但需要添加密码验证
    // 或者我们可以直接调用发送接口进行验证
    try {
      // 使用一个临时请求验证密码
      const testRes = await fetch(`${baseUrl}/api/sms/send`, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          password: password,
          type: "leader",
          start_time: start,
          end_time: end,
          mobiles: [],  // 空数组，不会真正发送
          content: "test",  // 测试内容
        }),
      });

      // 如果返回403，说明密码错误
      if (testRes.status === 403) {
        throw new Error("密码错误");
      }
    } catch (e) {
      if (e.message === "密码错误") {
        throw e;
      }
      // 其他错误忽略，继续执行
    }

    // 密码验证通过，保存密码供后续发送使用
    currentPassword = password;

    if (currentSmsType === "leader") {
      // 获取领导短信预览（不需要密码）
      let url = `${baseUrl}/api/sms/preview/leader?start_time=${encodeURIComponent(start)}&end_time=${encodeURIComponent(end)}`;

      if (caseTypes && caseTypes.length > 0) {
        url += `&case_types=${encodeURIComponent(caseTypes.join(","))}`;
      }

      const res = await fetch(url, { credentials: "same-origin" });
      const data = await res.json();
      if (!data.success) throw new Error(data.message || "获取预览失败");

      hidePasswordModal();
      showLeaderModal(data.mobiles, data.content);
    } else if (currentSmsType === "responsible") {
      // 获取责任人短信数据（不需要密码）
      let url = `${baseUrl}/api/sms/preview/responsible?start_time=${encodeURIComponent(start)}&end_time=${encodeURIComponent(
        end
      )}`;

      if (caseTypes && caseTypes.length > 0) {
        url += `&case_types=${encodeURIComponent(caseTypes.join(","))}`;
      }

      const res = await fetch(url, { credentials: "same-origin" });
      const data = await res.json();
      if (!data.success) throw new Error(data.message || "获取数据失败");

      // 保存原始数据用于一键清除
      originalResponsibleData = data.modules;
      hidePasswordModal();
      showResponsibleModal(data.modules);
    }
  };

  // 显示领导弹窗
  const showLeaderModal = (mobiles, content) => {
    leaderMobiles.value = (mobiles || []).join("\n");
    leaderContent.value = content || "";
    leaderModal.classList.add("show");
  };

  // 隐藏领导弹窗
  const hideLeaderModal = () => {
    leaderModal.classList.remove("show");
  };

  // 发送给领导
  const sendToLeader = async () => {
    const mobilesText = leaderMobiles.value.trim();
    const content = leaderContent.value.trim();

    if (!mobilesText) {
      alert("请输入手机号码");
      return;
    }
    if (!content) {
      alert("请输入短信内容");
      return;
    }

    const mobiles = mobilesText.split("\n").map((m) => m.trim()).filter((m) => m);
    const start = toServerValue(globalStart.value);
    const end = toServerValue(globalEnd.value);
    const caseTypes = getSelectedCaseTypes();

    try {
      const res = await fetch(`${baseUrl}/api/sms/send`, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          password: currentPassword,
          type: "leader",
          start_time: start,
          end_time: end,
          mobiles: mobiles,
          content: content,
          case_types: caseTypes,
        }),
      });
      const data = await res.json();
      if (!data.success) throw new Error(data.message || "发送失败");

      hideLeaderModal();
      alert(`发送成功：${data.inserted || 0} 条短信已发送`);
    } catch (e) {
      alert(e.message || String(e));
    }
  };

  // 显示责任人弹窗
  const showResponsibleModal = (modules) => {
    responsibleModules.innerHTML = "";

    if (!modules || Object.keys(modules).length === 0) {
      responsibleModules.innerHTML = '<p class="no-data">暂无待处理数据</p>';
      responsibleModal.classList.add("show");
      return;
    }

    Object.entries(modules).forEach(([key, module]) => {
      const moduleDiv = document.createElement("div");
      moduleDiv.className = "module-section";
      moduleDiv.dataset.moduleKey = key;

      const title = document.createElement("h4");
      title.textContent = module.title;
      moduleDiv.appendChild(title);

      if (!module.items || module.items.length === 0) {
        const noData = document.createElement("p");
        noData.className = "no-data";
        noData.textContent = "暂无待处理数据";
        moduleDiv.appendChild(noData);
      } else {
        module.items.forEach((item, idx) => {
          const itemDiv = document.createElement("div");
          itemDiv.className = "sms-item";
          itemDiv.dataset.itemIndex = idx;

          const info = document.createElement("div");
          info.className = "item-info";
          info.innerHTML = `
            <div><strong>案件：</strong>${item.案件名称_脱敏 || item.案件名称}</div>
            <div><strong>姓名：</strong>${item.姓名}</div>
            <div><strong>号码：</strong>${(item.联系电话 || []).join(", ")}</div>
          `;
          itemDiv.appendChild(info);

          const templateDiv = document.createElement("div");
          templateDiv.className = "item-template";
          const templateLabel = document.createElement("label");
          templateLabel.textContent = "短信模板：";
          const templateTextarea = document.createElement("textarea");
          templateTextarea.rows = 3;
          templateTextarea.value = item.短信模板 || "";
          templateDiv.appendChild(templateLabel);
          templateDiv.appendChild(templateTextarea);
          itemDiv.appendChild(templateDiv);

          const mobilesDiv = document.createElement("div");
          mobilesDiv.className = "item-mobiles";
          const mobilesLabel = document.createElement("label");
          mobilesLabel.textContent = "发送号码：";
          const mobilesTextarea = document.createElement("textarea");
          mobilesTextarea.rows = 2;
          mobilesTextarea.value = (item.联系电话 || []).join("\n");
          mobilesDiv.appendChild(mobilesLabel);
          mobilesDiv.appendChild(mobilesTextarea);
          itemDiv.appendChild(mobilesDiv);

          moduleDiv.appendChild(itemDiv);
        });
      }

      responsibleModules.appendChild(moduleDiv);
    });

    responsibleModal.classList.add("show");
  };

  // 隐藏责任人弹窗
  const hideResponsibleModal = () => {
    responsibleModal.classList.remove("show");
  };

  // 发送给责任人
  const sendToResponsible = async () => {
    const items = [];

    const moduleSections = responsibleModules.querySelectorAll(".module-section");
    moduleSections.forEach((section) => {
      const smsItems = section.querySelectorAll(".sms-item");
      smsItems.forEach((itemDiv) => {
        const templateTextarea = itemDiv.querySelector(".item-template textarea");
        const mobilesTextarea = itemDiv.querySelector(".item-mobiles textarea");

        const content = templateTextarea.value.trim();
        const mobilesText = mobilesTextarea.value.trim();

        if (!content || !mobilesText) return;

        const mobiles = mobilesText.split("\n").map((m) => m.trim()).filter((m) => m);
        mobiles.forEach((mobile) => {
          items.push({ mobile, content });
        });
      });
    });

    if (items.length === 0) {
      alert("没有可发送的短信");
      return;
    }

    try {
      const res = await fetch(`${baseUrl}/api/sms/send`, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          password: currentPassword,
          type: "responsible",
          items: items,
        }),
      });
      const data = await res.json();
      if (!data.success) throw new Error(data.message || "发送失败");

      hideResponsibleModal();
      const failedCount = (data.failed || []).length;
      let msg = `发送成功：${data.inserted || 0} 条短信已发送`;
      if (failedCount > 0) {
        msg += `\n失败：${failedCount} 条`;
      }
      alert(msg);
    } catch (e) {
      alert(e.message || String(e));
    }
  };

  // 下拉菜单点击事件
  document.querySelectorAll("#smsDropdownMenu .dropdown-item").forEach((item) => {
    item.addEventListener("click", () => {
      const type = item.dataset.type;
      document.getElementById("smsDropdownMenu").classList.remove("show");
      showPasswordModal(type);
    });
  });

  // 密码弹窗事件
  passwordConfirm.addEventListener("click", async () => {
    const password = passwordInput.value.trim();
    if (!password) {
      alert("请输入密码");
      return;
    }
    try {
      await verifyPassword(password);
    } catch (e) {
      if (e.message && e.message.includes("密码")) {
        alert("密码输入错误");
      } else {
        alert(e.message || String(e));
      }
    }
  });

  passwordCancel.addEventListener("click", cancelPasswordModal);

  passwordInput.addEventListener("keypress", (e) => {
    if (e.key === "Enter") {
      passwordConfirm.click();
    }
  });

  // 领导弹窗事件
  leaderSend.addEventListener("click", sendToLeader);
  leaderCancel.addEventListener("click", hideLeaderModal);

  // 责任人弹窗事件
  responsibleSend.addEventListener("click", sendToResponsible);
  responsibleCancel.addEventListener("click", hideResponsibleModal);

  // 一键清除按钮事件
  const responsibleClear = document.getElementById("responsibleClear");
  responsibleClear.addEventListener("click", () => {
    if (!originalResponsibleData) {
      alert("没有可清除的数据");
      return;
    }

    // 重新加载原始数据
    showResponsibleModal(originalResponsibleData);
  });

  // 点击弹窗外部关闭
  [passwordModal, leaderModal, responsibleModal].forEach((modal) => {
    modal.addEventListener("click", (e) => {
      if (e.target !== modal) return;
      if (modal === passwordModal) {
        cancelPasswordModal();
        return;
      }
      modal.classList.remove("show");
    });
  });

  document.getElementById("fileImport").addEventListener("change", async (ev) => {
    const file = ev.target.files && ev.target.files[0];
    if (!file) return;
    if (!file.name.toLowerCase().endsWith(".xls")) {
      alert("仅支持 xls 格式文件");
      ev.target.value = "";
      return;
    }
    const form = new FormData();
    form.append("file", file);
    try {
      const res = await fetch(`${baseUrl}/api/import/sx_xls`, {
        method: "POST",
        body: form,
        credentials: "same-origin",
      });
      const data = await res.json();
      if (!data.success) throw new Error(data.message || "导入失败");
      alert(`导入成功：${JSON.stringify(data.stats || {}, null, 2)}`);
    } catch (e) {
      alert(e.message || String(e));
    } finally {
      ev.target.value = "";
    }
  });

  // 加载类型列表
  loadCaseTypes().then(() => {
    // 加载完类型后再渲染卡片
    Promise.all(metricCards.map((c) => renderMetric(c))).catch((e) => alert(e.message || String(e)));
  });
})();
