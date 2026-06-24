const state = {
  payload: null,
  universe: "",
  dataType: "",
  startDate: "",
  endDate: "",
  status: "",
  search: "",
};

const statusText = {
  ok: "全覆盖",
  partial: "部分缺失",
  stale: "尾部缺失",
  missing: "完全缺失",
};

const els = {
  refreshBtn: document.querySelector("#refreshBtn"),
  exportLink: document.querySelector("#exportLink"),
  statusLine: document.querySelector("#statusLine"),
  startDate: document.querySelector("#startDate"),
  endDate: document.querySelector("#endDate"),
  universeSelect: document.querySelector("#universeSelect"),
  dataTypeSelect: document.querySelector("#dataTypeSelect"),
  statusSelect: document.querySelector("#statusSelect"),
  searchInput: document.querySelector("#searchInput"),
  metricCoverage: document.querySelector("#metricCoverage"),
  metricTotal: document.querySelector("#metricTotal"),
  metricDates: document.querySelector("#metricDates"),
  metricProblem: document.querySelector("#metricProblem"),
  chartSubtitle: document.querySelector("#chartSubtitle"),
  lineChart: document.querySelector("#lineChart"),
  worstDates: document.querySelector("#worstDates"),
  heatmap: document.querySelector("#heatmap"),
  dateRange: document.querySelector("#dateRange"),
  rowsBody: document.querySelector("#rowsBody"),
  rowCount: document.querySelector("#rowCount"),
  detailPanel: document.querySelector("#detailPanel"),
  detailContent: document.querySelector("#detailContent"),
  closeDetail: document.querySelector("#closeDetail"),
};

function pct(value) {
  return `${Math.round((Number(value) || 0) * 1000) / 10}%`;
}

function formatDate(value) {
  if (!value || value.length !== 8) return value || "--";
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function toInputDate(value) {
  return formatDate(value) === "--" ? "" : formatDate(value);
}

function compactDate(value) {
  return value ? value.replaceAll("-", "") : "";
}

function dayClass(coverage) {
  if (coverage >= 1) return "good";
  if (coverage >= 0.95) return "ok";
  if (coverage >= 0.75) return "warn";
  return "bad";
}

function setDefaultDates() {
  const end = new Date();
  const start = new Date();
  start.setDate(end.getDate() - 90);
  els.endDate.value = end.toISOString().slice(0, 10);
  els.startDate.value = start.toISOString().slice(0, 10);
  state.endDate = compactDate(els.endDate.value);
  state.startDate = compactDate(els.startDate.value);
}

function setLoading(isLoading) {
  els.refreshBtn.disabled = isLoading;
  els.refreshBtn.textContent = isLoading ? "扫描中" : "刷新扫描";
}

function queryParams(refresh = false) {
  const params = new URLSearchParams();
  if (state.universe) params.set("universe", state.universe);
  if (state.dataType) params.set("data_type", state.dataType);
  if (state.startDate) params.set("start_date", state.startDate);
  if (state.endDate) params.set("end_date", state.endDate);
  if (refresh) params.set("refresh", "1");
  return params;
}

async function loadData(refresh = false) {
  setLoading(true);
  try {
    const res = await fetch(`/api/scan?${queryParams(refresh).toString()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.payload = await res.json();
    syncControlsFromPayload();
    render();
  } catch (err) {
    els.statusLine.innerHTML = `<span class="warning">扫描失败：${err.message}</span>`;
  } finally {
    setLoading(false);
  }
}

function syncControlsFromPayload() {
  const payload = state.payload;
  if (!payload) return;
  const config = payload.config || {};
  renderOptions(els.universeSelect, config.universes || [], payload.query?.universe);
  renderOptions(els.dataTypeSelect, config.data_types || [], payload.query?.data_type);
  state.universe = payload.query?.universe || els.universeSelect.value;
  state.dataType = payload.query?.data_type || els.dataTypeSelect.value;
  state.startDate = payload.query?.start_date || state.startDate;
  state.endDate = payload.query?.end_date || state.endDate;
  els.startDate.value = toInputDate(state.startDate);
  els.endDate.value = toInputDate(state.endDate);
}

function renderOptions(select, items, value) {
  const current = value || select.value;
  select.innerHTML = "";
  for (const item of items) {
    const option = document.createElement("option");
    option.value = item.key;
    option.textContent = item.name;
    select.appendChild(option);
  }
  select.value = current || items[0]?.key || "";
}

function render() {
  const payload = state.payload;
  if (!payload) return;
  renderStatusLine(payload);
  renderMetrics(payload.overview || {});
  renderLineChart(payload.date_summary || []);
  renderWorstDates(payload.worst_dates || []);
  renderHeatmap(payload.date_summary || []);
  renderRows();
  updateExportLink();
}

function renderStatusLine(payload) {
  const source = payload.source === "xtquant" ? "真实 xtquant 数据" : "Demo 数据";
  const query = payload.query || {};
  const warnings = payload.warnings || [];
  const warningHtml = warnings.length ? `<span class="warning">${warnings.slice(0, 2).join("；")}</span>` : "";
  els.statusLine.innerHTML = `
    <span>来源：<strong>${source}</strong></span>
    <span>基准：<strong>${query.universe_name || "--"}</strong></span>
    <span>数据：<strong>${query.data_type_name || "--"}</strong></span>
    <span>扫描时间：<strong>${payload.scanned_at}</strong></span>
    ${warningHtml}
  `;
}

function renderMetrics(overview) {
  els.metricCoverage.textContent = pct(overview.avg_coverage);
  els.metricTotal.textContent = overview.total ?? "--";
  els.metricDates.textContent = overview.expected_count ?? "--";
  els.metricProblem.textContent = overview.problem ?? "--";
}

function renderLineChart(days) {
  if (!days.length) {
    els.lineChart.innerHTML = `<div class="empty-state">所选区间没有可检查日期。</div>`;
    els.chartSubtitle.textContent = "--";
    return;
  }
  const width = 900;
  const height = 260;
  const pad = 28;
  const xStep = days.length > 1 ? (width - pad * 2) / (days.length - 1) : 0;
  const points = days.map((day, index) => {
    const x = pad + index * xStep;
    const y = pad + (1 - day.coverage) * (height - pad * 2);
    return { ...day, x, y };
  });
  const line = points.map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const area = `${pad},${height - pad} ${line} ${width - pad},${height - pad}`;
  const markers = points
    .filter((point) => point.coverage < 1)
    .map(
      (point) =>
        `<circle cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="4" class="chart-dot ${dayClass(point.coverage)}">
          <title>${formatDate(point.date)} 覆盖率 ${pct(point.coverage)}，缺失 ${point.missing}/${point.total}</title>
        </circle>`,
    )
    .join("");

  els.lineChart.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="每日数据覆盖率折线图">
      <line x1="${pad}" y1="${pad}" x2="${width - pad}" y2="${pad}" class="grid-line"></line>
      <line x1="${pad}" y1="${height / 2}" x2="${width - pad}" y2="${height / 2}" class="grid-line"></line>
      <line x1="${pad}" y1="${height - pad}" x2="${width - pad}" y2="${height - pad}" class="grid-line"></line>
      <text x="4" y="${pad + 4}" class="axis-label">100%</text>
      <text x="10" y="${height / 2 + 4}" class="axis-label">50%</text>
      <text x="16" y="${height - pad + 4}" class="axis-label">0%</text>
      <polygon points="${area}" class="chart-area"></polygon>
      <polyline points="${line}" class="chart-line"></polyline>
      ${markers}
    </svg>
  `;
  els.chartSubtitle.textContent = `${formatDate(days[0].date)} 至 ${formatDate(days[days.length - 1].date)}，按日期统计覆盖率`;
}

function renderWorstDates(days) {
  els.worstDates.innerHTML = "";
  if (!days.length) {
    els.worstDates.innerHTML = `<p class="muted">暂无数据。</p>`;
    return;
  }
  for (const day of days.slice(0, 8)) {
    const item = document.createElement("div");
    item.className = "worst-item";
    item.innerHTML = `
      <strong>${formatDate(day.date)}</strong>
      <span>${pct(day.coverage)}</span>
      <div class="bar"><i class="${dayClass(day.coverage)}" style="width:${pct(day.coverage)}"></i></div>
      <small>覆盖 ${day.covered}/${day.total}，缺失 ${day.missing}</small>
    `;
    els.worstDates.appendChild(item);
  }
}

function renderHeatmap(days) {
  els.heatmap.innerHTML = "";
  if (!days.length) {
    els.dateRange.textContent = "--";
    return;
  }
  els.dateRange.textContent = `${formatDate(days[0].date)} 至 ${formatDate(days[days.length - 1].date)}`;
  for (const day of days) {
    const item = document.createElement("div");
    item.className = `day ${dayClass(day.coverage)}`;
    item.title = `${formatDate(day.date)} 覆盖率 ${pct(day.coverage)}，覆盖 ${day.covered}/${day.total}`;
    item.textContent = Math.round(day.coverage * 100);
    els.heatmap.appendChild(item);
  }
}

function filteredRows() {
  const rows = state.payload?.rows || [];
  const query = state.search.trim().toUpperCase();
  return rows.filter((row) => {
    if (state.status && row.status !== state.status) return false;
    if (query && !String(row.code || "").toUpperCase().includes(query)) return false;
    return true;
  });
}

function renderRows() {
  const rows = filteredRows();
  els.rowsBody.innerHTML = "";
  els.rowCount.textContent = `${rows.length} 条`;
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><strong>${row.code || "--"}</strong></td>
      <td><span class="badge ${row.status}">${statusText[row.status] || row.status}</span></td>
      <td>${pct(row.coverage)}</td>
      <td>${formatDate(row.first_date)}</td>
      <td>${formatDate(row.latest_date)}</td>
      <td>${row.missing_count}</td>
      <td>${row.note}</td>
    `;
    tr.addEventListener("click", () => openDetail(row));
    els.rowsBody.appendChild(tr);
  }
}

function openDetail(row) {
  const chips = row.missing_dates.length
    ? row.missing_dates
        .slice(0, 160)
        .map((date) => `<span class="date-chip">${formatDate(date)}</span>`)
        .join("")
    : `<p class="muted">所选区间全覆盖。</p>`;
  els.detailContent.innerHTML = `
    <h3>${row.code}</h3>
    <span class="badge ${row.status}">${statusText[row.status] || row.status}</span>
    <div class="detail-meta">
      <div class="meta-box"><span>基准池</span><strong>${row.universe_name}</strong></div>
      <div class="meta-box"><span>数据类型</span><strong>${row.data_type_name}</strong></div>
      <div class="meta-box"><span>覆盖率</span><strong>${pct(row.coverage)}</strong></div>
      <div class="meta-box"><span>缺失数</span><strong>${row.missing_count}</strong></div>
      <div class="meta-box"><span>首个日期</span><strong>${formatDate(row.first_date)}</strong></div>
      <div class="meta-box"><span>最新日期</span><strong>${formatDate(row.latest_date)}</strong></div>
    </div>
    <p class="muted">${row.note}</p>
    <h2>缺失日期</h2>
    <div class="missing-list">${chips}</div>
  `;
  els.detailPanel.classList.add("open");
}

function updateExportLink() {
  els.exportLink.href = `/api/export.csv?${queryParams(false).toString()}`;
}

function applyControlsAndLoad(refresh = false) {
  state.startDate = compactDate(els.startDate.value);
  state.endDate = compactDate(els.endDate.value);
  state.universe = els.universeSelect.value;
  state.dataType = els.dataTypeSelect.value;
  loadData(refresh);
}

els.refreshBtn.addEventListener("click", () => applyControlsAndLoad(true));
els.startDate.addEventListener("change", () => applyControlsAndLoad(false));
els.endDate.addEventListener("change", () => applyControlsAndLoad(false));
els.universeSelect.addEventListener("change", () => applyControlsAndLoad(false));
els.dataTypeSelect.addEventListener("change", () => applyControlsAndLoad(false));
els.statusSelect.addEventListener("change", (event) => {
  state.status = event.target.value;
  renderRows();
});
els.searchInput.addEventListener("input", (event) => {
  state.search = event.target.value;
  renderRows();
});
els.closeDetail.addEventListener("click", () => els.detailPanel.classList.remove("open"));

setDefaultDates();
loadData(false);
