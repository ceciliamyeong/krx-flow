let dashboardData = null;
let currentMarket = "KOSPI";
let currentRange = "1y";

// GitHub Pages 하위경로 대응용 base path
const BASE = location.pathname.split('/')[1]
  ? `/${location.pathname.split('/')[1]}`
  : '';

function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem("theme", theme);
  const label = document.getElementById("theme-label");
  if (label) label.textContent = theme === "dark" ? "Light" : "Dark";
}

function initTheme() {
  const saved = localStorage.getItem("theme");
  const theme = saved || (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  applyTheme(theme);
  const btn = document.getElementById("theme-toggle");
  if (btn) {
    btn.addEventListener("click", () => {
      const now = document.documentElement.getAttribute("data-theme") || "light";
      applyTheme(now === "dark" ? "light" : "dark");
    });
  }
}

function safeParseJSON(text) {
  // Python json.dumps(allow_nan=True)가 NaN을 그대로 쓰는 경우가 있어 JS JSON.parse가 터짐.
  // 파일에서 NaN이 나오면 null로 치환해서 파싱.
  return JSON.parse(text.replace(/\bNaN\b/g, "null"));
}

function fetchDashboard() {
  fetch(`${BASE}/data/derived/dashboard/latest.json`)
    .then(res => {
      if (!res.ok) throw new Error("JSON load failed: " + res.status);
      return res.text();
    })
    .then(text => safeParseJSON(text))
    .then(data => {
      dashboardData = data;
      const dateEl = document.getElementById("date");
      if (dateEl) dateEl.innerText = `${data.date} 장마감`;
      render();
    })
    .catch(err => {
      console.error(err);
      const dateEl = document.getElementById("date");
      if (dateEl) dateEl.innerText = "데이터 로딩 실패";
    });
}

function setMarket(market) {
  currentMarket = market;
  document.getElementById("btn-kospi")?.classList.toggle("active", market === "KOSPI");
  document.getElementById("btn-kosdaq")?.classList.toggle("active", market === "KOSDAQ");
  render();
}
window.setMarket = setMarket;

function setRange(r) {
  currentRange = r;
  document.getElementById("btn-range-1y")?.classList.toggle("active", r === "1y");
  document.getElementById("btn-range-all")?.classList.toggle("active", r === "all");
  renderCharts();
}
window.setRange = setRange;

function formatKRW(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "-";
  const abs = Math.abs(x);
  if (abs >= 1e12) return (x / 1e12).toFixed(2) + "조";
  if (abs >= 1e8) return (x / 1e8).toFixed(0) + "억";
  return String(Math.round(x));
}

function pctClass(p) {
  if (p === null || p === undefined || Number.isNaN(p)) return "muted";
  if (p > 0) return "pos";
  if (p < 0) return "neg";
  return "muted";
}

function renderFlows(m) {
  const flows = document.getElementById("flows");
  const signals = document.getElementById("signals");
  if (!flows || !signals) return;

  flows.innerHTML = "";
  signals.innerHTML = "";

  const f = m?.investor_net_krw || {};
  const sig = m?.flow_signal || {};

  const items = [
    { key: "foreign", label: "외국인" },
    { key: "institution", label: "기관" },
    { key: "individual", label: "개인" },
  ];

  for (const it of items) {
    const val = f[it.key];
    const chip = document.createElement("div");
    chip.className = "chip mono " + (val > 0 ? "pos" : val < 0 ? "neg" : "");
    chip.textContent = `${it.label} ${formatKRW(val)}`;
    flows.appendChild(chip);

    const s = sig[it.key];
    const badge = document.createElement("div");
    badge.className = `badge sig ${s || "WEAK"}`;
    badge.textContent = `${it.label}: ${s || "-"}`;
    signals.appendChild(badge);
  }
}

function renderUpjong() {
  const topEl = document.getElementById("upjong-top");
  const botEl = document.getElementById("upjong-bottom");
  if (!topEl || !botEl) return;

  topEl.innerHTML = "";
  botEl.innerHTML = "";

  const up = dashboardData?.extras?.upjong || { top: [], bottom: [] };

  const renderList = (root, arr) => {
    if (!arr || arr.length === 0) {
      const empty = document.createElement("div");
      empty.className = "muted";
      empty.style.fontWeight = "800";
      empty.textContent = "데이터 없음";
      root.appendChild(empty);
      return;
    }
    for (const r of arr) {
      const item = document.createElement("div");
      item.className = "item";
      const left = document.createElement("div");
      left.innerHTML = `<div class="name">${r.name ?? "-"}</div>`;
      const right = document.createElement("div");
      right.className = "right mono";
      const pct = Number(r.return_pct);
      right.innerHTML = `<span class="badge ${pctClass(pct)}">${(pct>=0?"+":"")}${pct.toFixed(2)}%</span>`;
      item.appendChild(left);
      item.appendChild(right);
      root.appendChild(item);
    }
  };

  renderList(topEl, up.top);
  renderList(botEl, up.bottom);
}

function renderTop10List() {
  const listEl = document.getElementById("top10-list");
  if (!listEl) return;
  listEl.innerHTML = "";

  const arr = dashboardData?.extras?.top10_treemap?.[currentMarket] || [];
  if (!arr || arr.length === 0) {
    const empty = document.createElement("div");
    empty.className = "muted";
    empty.style.fontWeight = "800";
    empty.textContent = "데이터 없음";
    listEl.appendChild(empty);
    return;
  }

  for (const r of arr) {
    const item = document.createElement("div");
    item.className = "item";

    const left = document.createElement("div");
    left.innerHTML = `
      <div class="name">${r.name ?? "-"}</div>
      <div class="sub mono">시총 ${formatKRW(r.mcap)} · 종가 ${formatKRW(r.close)}</div>
    `;

    const right = document.createElement("div");
    right.className = "right mono";
    const pct = Number(r.return_1d);
    const pctTxt = (pct >= 0 ? "+" : "") + (Number.isFinite(pct) ? pct.toFixed(2) : "-") + "%";
    right.innerHTML = `<span class="badge ${pctClass(pct)}">${pctTxt}</span>`;

    item.appendChild(left);
    item.appendChild(right);
    listEl.appendChild(item);
  }
}

function renderCharts() {
  const chart1 = document.getElementById("chart-close-turnover");
  const chart2 = document.getElementById("chart-investor-ratio");
  if (!chart1 || !chart2) return;

  const mk = currentMarket.toLowerCase(); // kospi/kosdaq
  const suffix = currentRange === "all" ? "all" : "1y";

  chart1.src = `${BASE}/data/derived/charts/${mk}_close_vs_turnover_${suffix}.png`;
  chart2.src = `${BASE}/data/derived/charts/${mk}_investor_net_ratio_${suffix}.png`;
}

function renderTreemap() {
  const img = document.getElementById("treemap");
  const title = document.getElementById("treemap-title");
  if (!img || !title) return;
  title.textContent = `${currentMarket} TOP10 (Treemap)`;
  img.src = `${BASE}/data/derived/charts/treemap_${currentMarket.toLowerCase()}_top10_latest.png`;
}

function render() {
  if (!dashboardData) return;

  const m = dashboardData.markets?.[currentMarket];
  if (!m) return;

  const closeEl = document.getElementById("close");
  const turnEl = document.getElementById("turnover");
  if (closeEl) closeEl.innerText = (m.close ?? "-");
  if (turnEl) turnEl.innerText = (m.turnover_readable ? `거래대금: ${m.turnover_readable}` : `거래대금: ${formatKRW(m.turnover_krw)}`);

  renderFlows(m);
  renderUpjong();
  renderTreemap();
  renderTop10List();
  renderCharts();
}

initTheme();
fetchDashboard();
