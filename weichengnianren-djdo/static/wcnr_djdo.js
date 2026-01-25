(() => {
  const baseUrl = "/weichengnianren-djdo";

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

  const metricCards = [...document.querySelectorAll(".card[data-metric]")];
  const charts = new Map();

  const colors = ["#60a5fa", "#34d399", "#f59e0b"];
  const rateColor = "#ef4444";

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

    const url = `${baseUrl}/api/metric/${encodeURIComponent(metric)}?start_time=${encodeURIComponent(
      start
    )}&end_time=${encodeURIComponent(end)}`;
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
      const url = `${baseUrl}/api/export/xlsx?metric=${encodeURIComponent(metric)}&start_time=${encodeURIComponent(
        start
      )}&end_time=${encodeURIComponent(end)}`;
      window.open(url, "_blank");
    });
  });

  document.getElementById("globalRefresh").addEventListener("click", () => {
    metricCards.forEach((card) => syncCardRangeFromGlobal(card));
    Promise.all(metricCards.map((c) => renderMetric(c))).catch((e) => alert(e.message || String(e)));
  });

  document.getElementById("btnExportXlsx").addEventListener("click", () => {
    const start = toServerValue(globalStart.value);
    const end = toServerValue(globalEnd.value);
    window.open(
      `${baseUrl}/api/export/details?start_time=${encodeURIComponent(start)}&end_time=${encodeURIComponent(end)}`,
      "_blank"
    );
  });

  document.getElementById("btnExportPdf").addEventListener("click", () => {
    const start = toServerValue(globalStart.value);
    const end = toServerValue(globalEnd.value);
    window.open(
      `${baseUrl}/api/export/overview_pdf?start_time=${encodeURIComponent(start)}&end_time=${encodeURIComponent(end)}`,
      "_blank"
    );
  });

  document.getElementById("btnSendSms").addEventListener("click", async () => {
    const pwd = prompt("请输入密码");
    if (pwd === null) return;
    const start = toServerValue(globalStart.value);
    const end = toServerValue(globalEnd.value);
    try {
      const res = await fetch(`${baseUrl}/api/sms/send`, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password: pwd, start_time: start, end_time: end }),
      });
      const data = await res.json();
      if (!data.success) throw new Error(data.message || "发送失败");
      alert(`发送成功：${data.inserted || 0} 条\n${data.preview ? "内容预览：" + data.preview : ""}`);
    } catch (e) {
      alert(e.message || String(e));
    }
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

  Promise.all(metricCards.map((c) => renderMetric(c))).catch((e) => alert(e.message || String(e)));
})();
