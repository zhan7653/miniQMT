"use strict";

/* ------------------------------------------------------------------ utils */

const $ = (selector) => document.querySelector(selector);

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    let detail = `${response.status}`;
    try {
      const body = await response.json();
      if (body && body.detail) detail = body.detail;
    } catch (_) { /* keep status */ }
    throw new Error(detail);
  }
  return response.json();
}

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === "class") node.className = value;
    else if (key === "text") node.textContent = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  for (const child of [].concat(children)) {
    if (child == null) continue;
    node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
  }
  return node;
}

function clickable(node, handler) {
  node.classList.add("clickable");
  node.setAttribute("tabindex", "0");
  node.setAttribute("role", "button");
  node.addEventListener("click", handler);
  node.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      handler(event);
    }
  });
  return node;
}

function cssVar(name, fallback) {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

/* ------------------------------------------------------------- formatting */

function money(value) {
  if (value == null || value === "") return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return number.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function qty(value) {
  if (value == null || value === "") return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return number.toLocaleString("zh-CN", { maximumFractionDigits: 0 });
}

function price(value) {
  if (value == null || value === "") return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return number.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 4 });
}

function percent(value) {
  if (value == null) return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return (number * 100).toFixed(2) + "%";
}

// 带涨跌语义的数字:红涨绿跌(A 股习惯),正数加 +,约等于零保持中性。
function signed(value, { asPercent = false } = {}) {
  if (value == null || value === "") return el("span", { text: "—" });
  const number = Number(value);
  if (!Number.isFinite(number)) return el("span", { text: String(value) });
  // 按展示精度归一化:落在 (-0.005, 0) 的尾差既不该带负号也不该着色。
  const rendered = Number(asPercent ? (number * 100).toFixed(2) : number.toFixed(2));
  const cls = rendered > 0 ? "up" : rendered < 0 ? "down" : "";
  const shown = rendered === 0 ? 0 : number;
  const text = asPercent ? (shown * 100).toFixed(2) + "%" : money(shown);
  return el("span", { class: cls, text: (rendered > 0 ? "+" : "") + text });
}

// "2026-07-26T22:54:35.175728" → "2026-07-26 22:54:35";非 ISO 串原样返回。
function fmtDateTime(value) {
  if (!value) return "—";
  const match = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)/.exec(String(value));
  return match ? `${match[1]} ${match[2]}` : String(value);
}

function shortId(id) {
  if (!id) return "—";
  const text = String(id);
  return text.length > 14 ? text.slice(0, 12) + "…" : text;
}

/* -------------------------------------------------------------- 中文映射 */

const STATUS_LABELS = {
  ok: ["ok", "正常"], up_to_date: ["ok", "已最新"], complete: ["ok", "完成"],
  blocked: ["bad", "阻断"], failed: ["bad", "失败"], skipped: ["muted", "跳过"],
  running: ["warn", "运行中"], active: ["ok", "活跃"],
  pending: ["warn", "待执行"], partially_filled: ["warn", "部分成交"],
  partial: ["warn", "部分成交"], filled: ["ok", "已成交"], rejected: ["bad", "已拒绝"],
  expired: ["muted", "已过期"], cancelled: ["muted", "已撤销"],
};

function statusLabel(status) {
  const entry = STATUS_LABELS[status];
  return entry ? entry[1] : (status || "—");
}

function statusBadge(status) {
  const [kind, label] = STATUS_LABELS[status] || ["muted", status || "—"];
  return el("span", { class: `badge ${kind}`, text: label });
}

const EVENT_LABELS = {
  portfolio_intent_received: "收到组合意图",
  risk_assessed: "风控评估",
  intent_not_scheduled: "意图未排单",
  order_not_created: "未生成订单",
  order_created: "创建订单",
  order_rejected: "订单拒绝",
  order_expired: "订单过期",
  fill_created: "成交",
  cash_dividend_paid: "现金分红入账",
  corporate_action_ex_date: "除权除息",
  corporate_action_entitlement: "权益登记",
  rights_issue_declined: "放弃配股",
  share_distribution_listed: "送转股上市",
  split_applied: "拆并股执行",
  share_cost_basis_adjusted: "持仓成本调整",
  portfolio_valued: "组合估值",
  simulation_marked_incomplete: "模拟标记不完整",
};

const STAGE_LABELS = {
  resolve: "解析目标日", universe: "官方标的池", bars: "行情双源采集", no_trade: "停牌共识",
  research: "研究快照", status: "状态采集", evidence: "行动/因子证据", candidate: "候选合成",
  validate: "增量验证", extend: "原子发布", accounts: "账户推进", data: "数据阶段", calendar: "交易日历",
};

const SIDE_LABELS = { buy: "买入", sell: "卖出" };

const REASON_LABELS = {
  static_allocation: "静态权重配置",
  no_next_trading_session: "无下一交易日",
  day_order_unfilled: "当日未成交",
  day_order_partial_remainder: "部分成交后过期",
  clock_advanced_past_expiry: "超过有效期",
  insufficient_cash: "现金不足",
  invalid_order_quantity: "数量无效",
  missing_raw_close: "缺少收盘价",
  missing_raw_bar: "缺少行情",
  missing_raw_ohlc: "缺少 OHLC",
  suspended: "停牌",
  zero_volume: "零成交量",
  zero_liquidity: "无流动性",
  limit_up_locked: "涨停无法成交",
  limit_down_locked: "跌停无法成交",
  partial_fills_disabled: "不允许部分成交",
  liquidity_capacity: "流动性上限",
  t_plus_sellable_capacity: "T+1 可卖上限",
  cash_capacity: "现金上限",
};

const ACTION_TYPE_LABELS = {
  cash_dividend: "现金分红",
  stock_dividend: "送转股",
  split: "拆并股",
  rights_issue: "配股",
};

// 明细字段的展示顺序、中文名与格式化方式
const PAYLOAD_FIELDS = [
  ["action_type", "类型", (v) => ACTION_TYPE_LABELS[v] || v],
  ["side", "方向", (v) => SIDE_LABELS[v] || v],
  ["quantity", "数量", qty],
  ["requested_quantity", "委托量", qty],
  ["remaining_quantity", "剩余", qty],
  ["entitled_quantity", "登记数量", qty],
  ["new_quantity", "新增数量", qty],
  ["price", "价格", price],
  ["reference_price", "参考价", price],
  ["amount", "金额", money],
  ["gross_cash", "税前金额", money],
  ["fee_total", "费用", money],
  ["fees", "费用", money],
  ["realized_pnl", "已实现盈亏", money],
  ["total_equity", "总权益", money],
  ["market_value", "市值", money],
  ["cash", "现金", money],
  ["nav", "净值", (v) => Number(v).toFixed(4)],
  ["status", "状态", statusLabel],
  ["reason", "原因", (v) => REASON_LABELS[v] || v],
  ["accepted", "风控", (v) => (v ? "通过" : "未通过")],
  // codes 是风控拒绝/降权的业务原因(如 position_reduced:510300.SH),必须可见。
  ["codes", "风控代码", (v) => (Array.isArray(v) ? (v.length ? v.join(", ") : null) : String(v))],
  ["quality", "质量", (v) => (v === "complete" ? "完整" : v === "incomplete" ? "不完整" : v)],
];

// 溯源指纹类字段只对调试有用,不进明细列。
const NOISY_PAYLOAD_KEYS = /^(source_|.*_hash$|.*_id$|snapshot_id|stale_instruments$|metadata$)/;

function humanPayload(payload) {
  if (!payload) return "—";
  const parts = [];
  for (const [key, label, format] of PAYLOAD_FIELDS) {
    if (payload[key] == null) continue;
    const formatted = format(payload[key]);
    if (formatted != null) parts.push(`${label} ${formatted}`);
  }
  let text = parts.join(" · ");
  if (!text) {
    const rest = Object.fromEntries(Object.entries(payload)
      .filter(([key]) => !NOISY_PAYLOAD_KEYS.test(key)));
    text = Object.keys(rest).length ? JSON.stringify(rest) : JSON.stringify(payload);
  }
  return text.length > 140 ? text.slice(0, 138) + "…" : text;
}

/* ------------------------------------------------------------------ table */

function table(headers, rows, opts = {}) {
  if (!rows.length) {
    return el("div", { class: "table-wrap" },
      el("div", { class: "empty table-empty", text: opts.empty || "暂无数据" }));
  }
  const head = el("thead", {}, el("tr", {}, headers.map((h) =>
    el("th", { class: h.num ? "num" : "", scope: "col", text: h.label }))));
  const body = el("tbody", {}, rows.map((cells, index) => {
    const tr = el("tr", {}, cells.map((cell, i) => {
      const td = el("td", { class: headers[i] && headers[i].num ? "num" : "" });
      if (cell instanceof Node) td.appendChild(cell);
      else td.textContent = cell == null ? "—" : String(cell);
      return td;
    }));
    if (opts.onRowClick) clickable(tr, () => opts.onRowClick(index));
    return tr;
  }));
  return el("div", { class: "table-wrap" }, el("table", {}, [head, body]));
}

/* ------------------------------------------------------------------- tabs */

for (const tab of document.querySelectorAll(".tab")) {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((item) => {
      item.classList.remove("active");
      // aria-current 对任意元素合法;aria-selected 只在 role="tab" 上有效。
      item.removeAttribute("aria-current");
    });
    document.querySelectorAll(".panel").forEach((item) => item.classList.remove("active"));
    tab.classList.add("active");
    tab.setAttribute("aria-current", "true");
    $(`#panel-${tab.dataset.tab}`).classList.add("active");
    refreshTab(tab.dataset.tab);
  });
}

function activeTab() {
  return document.querySelector(".tab.active").dataset.tab;
}

/* --------------------------------------------------------------- overview */

let sessionCutoff = null;

async function loadOverview() {
  const data = await api("/api/overview");
  const market = data.market || {};
  const schedule = data.schedule || {};
  sessionCutoff = data.config ? data.config.session_cutoff_local : null;

  const run = data.run || {};
  let runValue;
  let runSub = "";
  if (run.running) {
    runValue = statusBadge("running");
  } else if (run.last) {
    const ok = run.last.exit_code === 0;
    runValue = el("span", { class: `badge ${ok ? "ok" : "bad"}`, text: ok ? "成功" : "失败" });
    runSub = `退出码 ${run.last.exit_code} · ${fmtDateTime(run.last.finished_at)}`;
  } else {
    runValue = "—";
  }

  $("#overview-cards").replaceChildren(
    card("数据快照", market.published_end || market.error || "—",
      market.snapshot_id ? `${market.instruments} 个标的 · ${market.snapshot_id}` : ""),
    card("计划任务", schedule.exists
      ? `${schedule.time || "?"}（${schedule.enabled ? "已启用" : "已停用"}）`
      : (schedule.error ? "查询失败" : "未设置"),
      schedule.next_run_time ? `下次运行 ${fmtDateTime(schedule.next_run_time)}` : (schedule.error || "")),
    card("手动运行", runValue, runSub),
  );

  const accounts = $("#overview-accounts");
  const accountCards = (data.accounts || []).map((item) => {
    let value = item.exists ? money(item.equity || item.initial_cash) : "未创建";
    if (item.exists && item.nav != null) {
      const nav = Number(item.nav);
      if (Number.isFinite(nav)) {
        value = el("span", {}, [money(item.equity || item.initial_cash), " ",
          signed(nav - 1, { asPercent: true })]);
      }
    }
    const node = card(
      `${item.name}（${item.strategy === "static" ? "静态权重" : "Agent 决策"}）`,
      value,
      item.exists
        ? `头寸日 ${item.head_date || "—"} · 持仓 ${item.positions} · 挂单 ${item.pending_orders}`
        : `初始资金 ${money(item.initial_cash)}，首次运行时自动创建`,
    );
    clickable(node, () => {
      // The accounts dropdown is only populated when that tab loads, so stash
      // the choice and apply it after loadAccountList has filled the options.
      pendingAccountId = item.account_id;
      document.querySelector('[data-tab="accounts"]').click();
    });
    return node;
  });
  if (accountCards.length) accounts.replaceChildren(...accountCards);
  else accounts.replaceChildren(el("div", { class: "empty", text: "config/fundlab.yaml 中尚未配置账户" }));

  const last = $("#overview-last-run");
  if (data.last_report) {
    last.replaceChildren(renderReportSummary(data.last_report));
  } else {
    last.replaceChildren(el("div", { class: "empty", text: "还没有运行记录" }));
  }

  const now = new Date();
  const clock = now.toTimeString().slice(0, 8);
  $("#topbar-status").textContent = market.snapshot_id
    ? `快照 ${market.snapshot_id.slice(0, 18)}… · 数据至 ${market.published_end} · 更新于 ${clock}`
    : `快照信息不可用 · 更新于 ${clock}`;
}

function card(title, value, sub) {
  const valueNode = el("div", { class: "card-value" });
  if (value instanceof Node) valueNode.appendChild(value);
  else valueNode.textContent = value == null ? "—" : String(value);
  let subNode = null;
  if (sub instanceof Node) {
    subNode = el("div", { class: "card-sub" });
    subNode.appendChild(sub);
  } else if (sub) {
    subNode = el("div", { class: "card-sub", text: sub });
  }
  return el("div", { class: "card" }, [
    el("div", { class: "card-title", text: title }),
    valueNode,
    subNode,
  ]);
}

function renderReportSummary(report) {
  const stages = (report.stage_names || []).map((item) => {
    const [name, status] = String(item).split(":");
    const label = STAGE_LABELS[name] || name;
    return status ? `${label}·${statusLabel(status)}` : label;
  }).join("  →  ");
  return el("div", {}, [
    el("div", { class: "row" }, [
      statusBadge(report.status),
      el("strong", { text: `目标日 ${report.target_date || "—"}` }),
      el("span", { class: "hint", text: fmtDateTime(report.generated_at) }),
      report.blocked_stage
        ? el("span", { class: "badge bad", text: `阻断于 ${STAGE_LABELS[report.blocked_stage] || report.blocked_stage}` })
        : null,
    ]),
    el("div", { class: "hint", text: stages }),
  ]);
}

/* --------------------------------------------------------------- accounts */

let equityChart = null;
let chartAccountId = null;
let pendingAccountId = null;

async function loadAccountList() {
  const accounts = await api("/api/accounts");
  const select = $("#account-select");
  const wanted = pendingAccountId || select.value;
  pendingAccountId = null;
  select.replaceChildren(...accounts.map((item) =>
    el("option", { value: item.account_id, text: `${item.name}（${item.account_id}）` })));
  if (wanted && accounts.some((item) => item.account_id === wanted)) select.value = wanted;
  return accounts;
}

async function loadAccountDetail() {
  const accountId = $("#account-select").value;
  if (!accountId) {
    $("#account-detail").classList.add("hidden");
    $("#account-empty").classList.remove("hidden");
    return;
  }
  $("#account-empty").classList.add("hidden");
  $("#account-detail").classList.remove("hidden");
  const data = await api(`/api/accounts/${encodeURIComponent(accountId)}`);

  if (data.exists === false) {
    $("#account-metrics").replaceChildren(card(
      "账户尚未创建",
      "等待首次运行",
      `初始资金 ${money(data.cash)} · 首次每日运行时自动创建`,
    ));
    renderEquityChart([], accountId);
    const notCreated = () => el("div", { class: "empty", text: "账户尚未创建" });
    $("#positions-table").replaceChildren(notCreated());
    $("#orders-table").replaceChildren(notCreated());
    $("#events-table").replaceChildren(notCreated());
    return;
  }

  const fb = data.feedback || {};
  $("#account-metrics").replaceChildren(
    card("总权益", money(fb.final_equity || data.cash), `现金 ${money(data.cash)}`),
    card("累计收益", signed(fb.overall_return, { asPercent: true }),
      el("span", {}, ["本段收益 ", signed(fb.run_return, { asPercent: true })])),
    card("最大回撤", percent(fb.max_drawdown),
      `质量 ${fb.quality === "complete" ? "完整" : fb.quality === "incomplete" ? "不完整" : (fb.quality || "—")}`),
    card("已实现盈亏", signed(data.realized_pnl), `分红 ${money(data.dividend_income)}`),
    card("累计费用", money(data.fees_paid), `成交 ${fb.fills ?? "—"} / 订单 ${fb.orders ?? "—"}`),
  );

  renderEquityChart(data.equity_curve || [], accountId);

  $("#positions-table").replaceChildren(table(
    [
      { label: "标的" }, { label: "数量", num: true }, { label: "可卖", num: true },
      { label: "成本均价", num: true }, { label: "最新价", num: true },
      { label: "市值", num: true }, { label: "浮动盈亏", num: true },
    ],
    (data.positions || []).map((item) => [
      el("span", { class: "mono", text: item.instrument_id }),
      qty(item.quantity), qty(item.sellable_quantity),
      price(item.average_cost), price(item.last_price),
      money(item.market_value), signed(item.unrealized_pnl),
    ]),
    { empty: "当前无持仓" },
  ));

  $("#orders-table").replaceChildren(table(
    [
      { label: "标的" }, { label: "方向" }, { label: "执行日" },
      { label: "数量", num: true }, { label: "剩余", num: true }, { label: "状态" },
    ],
    (data.pending_orders || []).map((item) => [
      el("span", { class: "mono", text: item.instrument_id }),
      el("span", { class: item.side === "buy" ? "up" : "down", text: SIDE_LABELS[item.side] || item.side }),
      item.execution_date,
      qty(item.requested_quantity), qty(item.remaining_quantity),
      statusBadge(item.status),
    ]),
    { empty: "当前无挂单" },
  ));

  $("#events-table").replaceChildren(table(
    [{ label: "日期" }, { label: "事件" }, { label: "对象" }, { label: "明细" }],
    (data.recent_events || []).slice(0, 80).map((item) => {
      const payload = item.payload || {};
      const subject = payload.instrument_id
        ? el("span", { class: "mono", text: payload.instrument_id })
        : el("span", { class: "mono", title: item.entity_id || "", text: shortId(item.entity_id) });
      return [
        item.session_date,
        EVENT_LABELS[item.event_type] || item.event_type,
        subject,
        humanPayload(payload),
      ];
    }),
    { empty: "暂无事件" },
  ));
}

function renderEquityChart(curve, accountId) {
  const container = $("#equity-chart");
  if (!equityChart) equityChart = echarts.init(container);

  if (!curve.length) {
    chartAccountId = accountId;
    equityChart.setOption({
      graphic: [{
        type: "text", left: "center", top: "middle",
        style: { text: "暂无净值数据", fill: cssVar("--muted", "#64748b"), fontSize: 14, fontFamily: cssVar("--sans", "sans-serif") },
      }],
    }, true);
    return;
  }

  // 同一账户刷新时保留用户拖出来的缩放区间;切换账户则重置。
  let zoom = null;
  if (chartAccountId === accountId) {
    const prev = equityChart.getOption();
    if (prev && prev.dataZoom && prev.dataZoom.length) {
      zoom = { start: prev.dataZoom[0].start, end: prev.dataZoom[0].end };
    }
  }
  chartAccountId = accountId;

  const accent = cssVar("--accent", "#4f46e5");
  const warnColor = cssVar("--warn", "#d97706");
  const border = cssVar("--border", "#e2e8f0");
  const muted = cssVar("--muted", "#64748b");
  const sans = cssVar("--sans", "sans-serif");
  const showSlider = curve.length > 60;

  const dates = curve.map((item) => item.session_date);
  const equity = curve.map((item) => Number(item.total_equity));
  const nav = curve.map((item) => Number(item.nav));

  equityChart.setOption({
    color: [accent, warnColor],
    textStyle: { fontFamily: sans },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross", label: { backgroundColor: cssVar("--gray-800", "#1e293b") } },
    },
    legend: {
      top: 6,
      icon: "roundRect",
      itemWidth: 14,
      itemHeight: 3,
      textStyle: { color: cssVar("--gray-700", "#334155") },
      data: ["总权益", "净值"],
    },
    grid: { left: 16, right: 16, top: 44, bottom: showSlider ? 64 : 24, containLabel: true },
    xAxis: {
      type: "category",
      data: dates,
      boundaryGap: false,
      axisLabel: { color: muted },
      axisLine: { lineStyle: { color: border } },
      axisTick: { show: false },
    },
    yAxis: [
      {
        type: "value", scale: true,
        axisLabel: { color: muted, formatter: (v) => v.toLocaleString() },
        splitLine: { lineStyle: { color: border } },
      },
      {
        type: "value", scale: true, alignTicks: true,
        axisLabel: { color: muted, formatter: (v) => Number(v).toFixed(3) },
        splitLine: { show: false },
      },
    ],
    dataZoom: showSlider
      ? [
        { type: "inside", ...(zoom || {}) },
        {
          type: "slider", bottom: 8, height: 20, ...(zoom || {}),
          borderColor: border, fillerColor: "rgba(79, 70, 229, .12)",
          handleStyle: { color: accent },
        },
      ]
      : [{ type: "inside", ...(zoom || {}) }],
    series: [
      {
        name: "总权益", type: "line", data: equity, showSymbol: false,
        lineStyle: { width: 2 }, areaStyle: { opacity: 0.06 },
        tooltip: { valueFormatter: (v) => money(v) },
      },
      {
        name: "净值", type: "line", yAxisIndex: 1, data: nav, showSymbol: false,
        lineStyle: { width: 1.5 },
        tooltip: { valueFormatter: (v) => Number(v).toFixed(4) },
      },
    ],
  }, true);
  equityChart.resize();
}

window.addEventListener("resize", () => equityChart && equityChart.resize());
// 网格折行、面板切换等只改容器不改窗口的情况,window resize 不会触发。
new ResizeObserver(() => equityChart && equityChart.resize()).observe($("#equity-chart"));
$("#account-select").addEventListener("change", () => loadAccountDetail().then(clearError).catch(showError));

/* ------------------------------------------------------------------- runs */

async function loadRuns({ auto = false } = {}) {
  const runs = await api("/api/runs");
  // 自动触发的刷新(窗口重新聚焦、后台运行结束)不折叠用户展开的详情。
  if (!auto) $("#run-detail").classList.add("hidden");
  $("#runs-table").replaceChildren(table(
    [
      { label: "生成时间" }, { label: "目标日" }, { label: "状态" },
      { label: "阻断阶段" }, { label: "账户" },
    ],
    runs.map((item) => [
      fmtDateTime(item.generated_at), item.target_date, statusBadge(item.status),
      item.blocked_stage ? (STAGE_LABELS[item.blocked_stage] || item.blocked_stage) : "—",
      el("span", {}, (item.accounts || []).map((account) =>
        el("span", { class: "run-account" }, [
          el("span", { class: "mono", text: account.account_id }),
          statusBadge(account.status),
        ]))),
    ]),
    {
      empty: "暂无运行记录",
      onRowClick: (index) => showRunDetail(runs[index].file),
    },
  ));
}

async function showRunDetail(file) {
  const report = await api(`/api/runs/${encodeURIComponent(file)}`);
  const detail = $("#run-detail");
  detail.classList.remove("hidden");
  const stages = el("ul", { class: "stage-list" }, (report.stages || []).map((stage) =>
    el("li", {}, [
      statusBadge(stage.status),
      " ",
      el("strong", { text: STAGE_LABELS[stage.name] || stage.name }),
      el("div", { class: "stage-detail", text: JSON.stringify(stage.detail || {}) }),
    ])));
  detail.replaceChildren(
    el("h2", { text: `运行详情 · ${report.target_date || file}` }),
    renderReportSummary({
      status: report.status,
      target_date: report.target_date,
      generated_at: report.generated_at,
      stage_names: (report.stages || []).map((item) => `${item.name}:${item.status}`),
      blocked_stage: ((report.stages || []).find((item) => item.status === "blocked") || {}).name,
    }),
    stages,
    // replaceChildren 会把 null 强转成字面文本 "null",空列表必须整个省略。
    ...((report.accounts || []).length
      ? [table(
        [{ label: "账户" }, { label: "状态" }, { label: "推进", num: true }, { label: "权益", num: true }, { label: "错误" }],
        report.accounts.map((item) => [
          el("span", { class: "mono", text: item.account_id }), statusBadge(item.status),
          item.sessions_advanced, money(item.equity), item.error || "—",
        ]))]
      : []),
  );
  detail.scrollIntoView({ behavior: "smooth" });
}

/* --------------------------------------------------------------- schedule */

function renderCutoffHint() {
  $("#cutoff-hint").textContent = sessionCutoff
    ? `收盘数据在 ${sessionCutoff} 后才视为当日完整。`
    : "";
}

async function loadSchedule() {
  renderCutoffHint();
  // 首屏总览还没返回(或失败)时 sessionCutoff 为空,单独补拉一次。
  if (sessionCutoff == null) {
    api("/api/overview").then((data) => {
      sessionCutoff = data.config ? data.config.session_cutoff_local : null;
      renderCutoffHint();
    }).catch(() => {});
  }
  let state;
  try {
    state = await api("/api/schedule");
  } catch (error) {
    $("#schedule-cards").replaceChildren(card("计划任务", "查询失败", String(error.message)));
    return;
  }
  renderSchedule(state);
}

function renderSchedule(state) {
  $("#schedule-cards").replaceChildren(
    card("任务状态", state.exists ? (state.enabled ? "已启用" : "已停用") : "未设置",
      state.exists ? `${(state.days || []).length} 天/周 · ${state.time || "?"}` : "保存后自动创建"),
    card("下次运行", fmtDateTime(state.next_run_time), ""),
    card("上次运行", fmtDateTime(state.last_run_time),
      state.last_result != null ? `退出码 ${state.last_result}` : ""),
  );
  if (state.exists && state.time) {
    const input = $("#schedule-time");
    if (input.value === state.time) {
      input.dataset.loaded = state.time;
    } else {
      // 只有输入框仍是上次服务端值(用户没改)且没有焦点时才回填,
      // 否则自动刷新会吹掉用户正在编辑、尚未保存的时间。
      const pristine = input.dataset.loaded == null
        ? input.value === input.defaultValue
        : input.value === input.dataset.loaded;
      if (pristine && document.activeElement !== input) {
        input.value = state.time;
        input.dataset.loaded = state.time;
      }
    }
  }
  const toggle = $("#schedule-toggle");
  toggle.textContent = state.enabled ? "停用" : "启用";
  toggle.disabled = !state.exists;
  $("#schedule-delete").disabled = !state.exists;
  toggle.dataset.enabled = state.enabled ? "1" : "0";
}

function setMessage(node, kind, text, { autoclear = false } = {}) {
  node.className = kind ? `message ${kind}` : "message";
  node.textContent = text;
  if (autoclear) {
    setTimeout(() => {
      if (node.textContent === text) {
        node.textContent = "";
        node.className = "message";
      }
    }, 5000);
  }
}

$("#schedule-save").addEventListener("click", async () => {
  await scheduleAction(() => api("/api/schedule", {
    method: "PUT",
    body: JSON.stringify({ time: $("#schedule-time").value }),
  }), "计划任务已保存");
});

$("#schedule-toggle").addEventListener("click", async () => {
  const enable = $("#schedule-toggle").dataset.enabled !== "1";
  await scheduleAction(() => api("/api/schedule", {
    method: "PUT",
    body: JSON.stringify({ enabled: enable }),
  }), enable ? "已启用" : "已停用");
});

$("#schedule-delete").addEventListener("click", async () => {
  if (!window.confirm("确定删除 FundLab Daily 计划任务？")) return;
  await scheduleAction(async () => {
    await api("/api/schedule", { method: "DELETE" });
    return api("/api/schedule");
  }, "已删除");
});

async function scheduleAction(action, okMessage) {
  const message = $("#schedule-message");
  setMessage(message, "", "执行中…");
  try {
    const state = await action();
    renderSchedule(state);
    setMessage(message, "ok", okMessage, { autoclear: true });
  } catch (error) {
    setMessage(message, "bad", `失败：${error.message}`);
  }
}

/* ------------------------------------------------------------- manual run */

let runPolling = null;
let runWasRunning = false;

$("#run-now").addEventListener("click", async () => {
  const status = $("#run-status");
  setMessage(status, "", "启动中…");
  try {
    const result = await api("/api/daily/run", {
      method: "POST",
      body: JSON.stringify({
        skip_data: $("#run-skip-data").checked,
        skip_accounts: $("#run-skip-accounts").checked,
      }),
    });
    if (!result.started && result.reason === "already_running") {
      setMessage(status, "", "已有运行在进行中");
    }
    pollRunStatus();
  } catch (error) {
    setMessage(status, "bad", `启动失败：${error.message}`);
  }
});

async function pollRunStatus() {
  clearTimeout(runPolling);
  let state;
  try {
    state = await api("/api/daily/run/status");
  } catch (_) {
    runPolling = setTimeout(pollRunStatus, 4000);
    return;
  }
  const status = $("#run-status");
  const log = $("#run-log");
  if (state.log_tail && state.log_tail.length) {
    log.classList.remove("hidden");
    log.textContent = state.log_tail.join("\n");
    log.scrollTop = log.scrollHeight;
  }
  if (state.running) {
    runWasRunning = true;
    setMessage(status, "", `运行中…（自 ${fmtDateTime(state.started_at)}）`);
    $("#run-now").disabled = true;
    runPolling = setTimeout(pollRunStatus, 2000);
  } else {
    $("#run-now").disabled = false;
    if (state.last) {
      const ok = state.last.exit_code === 0;
      setMessage(status, ok ? "ok" : "bad", ok
        ? `完成（${fmtDateTime(state.last.finished_at)}）`
        : `结束，退出码 ${state.last.exit_code}（详见运行记录页）`);
    } else {
      setMessage(status, "", "");
    }
    // 刚结束的运行改变了计划任务卡片与运行记录,顺手刷新当前页。
    if (runWasRunning) {
      runWasRunning = false;
      if (activeTab() === "schedule") loadSchedule().then(clearError).catch(showError);
      else refreshTab(activeTab(), { auto: true });
    }
  }
}

/* ------------------------------------------------------------------ agent */

async function loadAgentTab() {
  const accounts = await api("/api/agent/accounts");
  const select = $("#agent-account-select");
  const current = select.value;
  select.replaceChildren(...accounts.map((item) =>
    el("option", { value: item.account_id, text: `${item.name}（${item.account_id}）` })));
  if (current && accounts.some((item) => item.account_id === current)) select.value = current;
  if (!accounts.length) {
    $("#decision-form").classList.add("hidden");
    $("#auto-decision").classList.add("hidden");
    $("#decisions-table").replaceChildren(
      el("div", { class: "empty", text: "config/fundlab.yaml 中没有 agent-file 策略的账户" }));
    return;
  }
  $("#decision-form").classList.remove("hidden");
  $("#auto-decision").classList.remove("hidden");
  const head = accounts.find((item) => item.account_id === select.value);
  const defaultDate = nextDay(head && head.head_date);
  if (!$("#decision-date").value) $("#decision-date").value = defaultDate;
  if (!$("#weight-rows").children.length) addWeightRow();
  await loadDecisions();
}

function nextDay(dateString) {
  // Pure UTC arithmetic: mixing local-time parsing with toISOString() would
  // return the SAME day for any timezone east of UTC (this UI runs in UTC+8).
  let base;
  if (dateString) {
    const [year, month, day] = dateString.split("-").map(Number);
    base = new Date(Date.UTC(year, month - 1, day));
  } else {
    const now = new Date();
    base = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate()));
  }
  base.setUTCDate(base.getUTCDate() + 1);
  return base.toISOString().slice(0, 10);
}

async function loadDecisions() {
  const accountId = $("#agent-account-select").value;
  if (!accountId) return;
  const decisions = await api(`/api/agent/decisions/${encodeURIComponent(accountId)}`);
  $("#decisions-table").replaceChildren(table(
    [{ label: "日期" }, { label: "有效" }, { label: "权重" }, { label: "理由" }],
    decisions.map((item) => [
      item.decision_date,
      item.valid ? el("span", { class: "badge ok", text: "有效" })
        : el("span", { class: "badge bad", text: "无效", title: item.error || "" }),
      item.target_weights
        ? Object.entries(item.target_weights)
          .map(([key, value]) => `${key} ${parseFloat((Number(value) * 100).toFixed(1))}%`)
          .join(" · ")
        : (item.error || "—"),
      item.reason || "—",
    ]),
    { empty: "还没有决策文件" },
  ));
}

function addWeightRow(symbol = "", weight = "") {
  const row = el("div", { class: "row weight-row" }, [
    el("input", { type: "text", placeholder: "如 510300.SH", "aria-label": "标的代码", value: symbol }),
    el("input", { type: "number", placeholder: "权重 0-1", "aria-label": "目标权重", step: "0.05", min: "0", max: "1", value: weight }),
    el("button", { class: "btn ghost small", text: "移除", onclick: (event) => {
      event.preventDefault();
      row.remove();
    } }),
  ]);
  $("#weight-rows").appendChild(row);
}

$("#add-weight").addEventListener("click", (event) => {
  event.preventDefault();
  addWeightRow();
});

$("#agent-account-select").addEventListener("change", () => loadDecisions().then(clearError).catch(showError));

async function runAgentDecide(dryRun) {
  const message = $("#agent-decide-message");
  const resultBox = $("#agent-decide-result");
  const accountId = $("#agent-account-select").value;
  if (!accountId) return;
  $("#agent-preview").disabled = true;
  $("#agent-decide").disabled = true;
  setMessage(message, "", dryRun ? "预演中…" : "决策中…");
  try {
    const result = await api(`/api/agent/decide/${encodeURIComponent(accountId)}`, {
      method: "POST",
      body: JSON.stringify({
        dry_run: dryRun,
        overwrite: $("#agent-overwrite").checked,
      }),
    });
    const weights = Object.entries(result.target_weights || {})
      .map(([k, v]) => `${k} ${parseFloat((Number(v) * 100).toFixed(1))}%`).join(" · ");
    const alreadyPresent = result.skipped === "already_present";
    resultBox.classList.remove("hidden");
    resultBox.replaceChildren(
      el("div", { class: "row" }, [
        el("strong", { text: `决策日 ${result.decision_date}` }),
        el("span", {
          class: "badge",
          text: result.existing_agent_id || (result.policy ? result.policy.agent_id : "—"),
        }),
        result.written
          ? el("span", { class: "badge ok", text: "已投递" })
          : alreadyPresent
            ? el("span", { class: "badge ok", text: "已存在" })
            : el("span", { class: "badge muted", text: "未落盘(预演)" }),
      ]),
      el("div", {
        text: alreadyPresent ? "已有有效决策，未改写文件。" : `目标权重:${weights}`,
      }),
      el("div", { class: "hint", text: result.reason || "" }),
    );
    const outcome = result.written
      ? `已投递:${result.decision_date}.json`
      : alreadyPresent ? "同日有效决策已存在，幂等跳过" : "预演完成,未写入文件";
    setMessage(message, "ok", outcome, { autoclear: result.written || alreadyPresent });
    if (result.written) await loadDecisions();
  } catch (error) {
    setMessage(message, "bad", `失败:${error.message}`);
  } finally {
    $("#agent-preview").disabled = false;
    $("#agent-decide").disabled = false;
  }
}

$("#agent-preview").addEventListener("click", () => runAgentDecide(true));
$("#agent-decide").addEventListener("click", () => runAgentDecide(false));

$("#decision-submit").addEventListener("click", async () => {
  const message = $("#decision-message");
  setMessage(message, "", "投递中…");
  const weights = {};
  for (const row of document.querySelectorAll(".weight-row")) {
    const [symbolInput, weightInput] = row.querySelectorAll("input");
    if (symbolInput.value.trim()) weights[symbolInput.value.trim()] = weightInput.value;
  }
  try {
    const result = await api(`/api/agent/decisions/${encodeURIComponent($("#agent-account-select").value)}`, {
      method: "POST",
      body: JSON.stringify({
        decision_date: $("#decision-date").value,
        target_weights: weights,
        reason: $("#decision-reason").value,
        overwrite: $("#decision-overwrite").checked,
      }),
    });
    setMessage(message, "ok", `已投递：${result.file}`, { autoclear: true });
    await loadDecisions();
  } catch (error) {
    setMessage(message, "bad", `失败：${error.message}`);
  }
});

/* ---------------------------------------------------------------- refresh */

function showError(error) {
  const banner = $("#global-error");
  banner.replaceChildren(
    el("span", { text: `加载失败：${error && error.message ? error.message : error}` }),
    el("button", { class: "btn small", text: "重试", onclick: () => refreshTab(activeTab()) }),
  );
  banner.classList.remove("hidden");
}

function clearError() {
  $("#global-error").classList.add("hidden");
}

async function refreshTab(name, { auto = false } = {}) {
  const panel = $(`#panel-${name}`);
  const refreshBtn = $("#refresh-btn");
  panel.classList.add("loading");
  refreshBtn.disabled = true;
  try {
    if (name === "overview") await loadOverview();
    else if (name === "accounts") { await loadAccountList(); await loadAccountDetail(); }
    else if (name === "runs") await loadRuns({ auto });
    else if (name === "schedule") { await loadSchedule(); await pollRunStatus(); }
    else if (name === "agent") await loadAgentTab();
    clearError();
  } catch (error) {
    showError(error);
  } finally {
    panel.classList.remove("loading");
    refreshBtn.disabled = false;
  }
}

$("#refresh-btn").addEventListener("click", () => refreshTab(activeTab()));

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refreshTab(activeTab(), { auto: true });
});

refreshTab("overview");
