"use strict";

/* ------------------------------------------------------------------ utils */

const $ = (selector) => document.querySelector(selector);
const UI_BUILD = "20260807-4";
const SCHEDULE_REFRESHING_MESSAGE = "Task scheduler state is refreshing";
let scheduleRetryTimer = null;

function scheduleIsRefreshing(value) {
  if (!value) return false;
  const message = typeof value === "string" ? value : (value.error || value.message);
  return message === SCHEDULE_REFRESHING_MESSAGE;
}

function accountIdFromLocation() {
  return new URLSearchParams(window.location.search).get("account");
}

function rememberAccountLocation(accountId) {
  const url = new URL(window.location.href);
  url.searchParams.set("ui", UI_BUILD);
  url.searchParams.set("account", accountId);
  url.hash = "accounts";
  window.history.replaceState(null, "", url);
}

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
  current: ["ok", "当前"], stale: ["warn", "已过期"], missing: ["muted", "无记录"],
  error: ["bad", "评估失败"], queued: ["warn", "待执行"], consumed: ["ok", "已进入账户"],
  hold: ["muted", "无需决策"], none: ["muted", "无决策"], invalid: ["bad", "决策损坏"],
  ready: ["ok", "有效"], idle: ["muted", "尚未成交"],
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
  zero_entitlements_pruned: "清理无效权益",
  portfolio_valued: "组合估值",
  simulation_marked_incomplete: "模拟标记不完整",
};

const STAGE_LABELS = {
  resolve: "解析目标日", universe: "官方标的池", bars: "行情双源采集", no_trade: "停牌共识",
  research: "研究快照", status: "状态采集", evidence: "行动/因子证据", candidate: "候选合成",
  validate: "增量验证", extend: "原子发布", accounts: "账户推进", data: "数据阶段", calendar: "交易日历",
};

const SIDE_LABELS = { buy: "买入", sell: "卖出" };

const CRISIS_ACTION_LABELS = {
  wait: "等待危机", hold: "保持不变", enter: "触发建仓", add_tranche: "分档加仓",
  exit: "触发退出", cooldown: "退出冷却", initialize_defensive: "建立防守仓",
  pending_orders: "等待挂单", risk_held: "持有风险仓", defensive: "防守等待",
};

const CRISIS_PARAMETER_LABELS = {
  risk_instruments: "风险 ETF 池", defensive_instrument: "防守 ETF", entry_mode: "入场模式",
  drawdown_days: "回撤窗口", event_lookback_days: "危机事件窗口", confirmation_days: "确认均线",
  volatility_days: "波动率窗口", minimum_drawdown: "最低回撤", rebound_threshold: "反弹确认",
  recovery_exit_gap: "接近前高退出", profit_take: "止盈", max_positions: "最多持仓数",
  entry_risk_weight: "初始风险仓", max_risk_weight: "最高风险仓", max_position_weight: "单标的上限",
  position_caps: "单独仓位上限", ladder_step: "加仓回撤间隔", tranche_weight: "每档仓位",
  target_volatility: "目标波动率", cooldown_days: "退出冷却天数", rebalance_threshold: "再平衡阈值",
};

const instrumentNames = {};

function rememberInstrumentName(instrumentId, instrumentName) {
  if (!instrumentId) return;
  if (instrumentName) instrumentNames[instrumentId] = instrumentName;
  else delete instrumentNames[instrumentId];
}

function rememberStrategyInstruments(accounts) {
  for (const account of Array.isArray(accounts) ? accounts : []) {
    for (const item of Array.isArray(account.strategy_instruments) ? account.strategy_instruments : []) {
      rememberInstrumentName(item.instrument_id, item.instrument_name);
    }
    for (const collection of [account.positions, account.pending_orders]) {
      for (const item of Array.isArray(collection) ? collection : []) {
        rememberInstrumentName(item.instrument_id, item.instrument_name);
      }
    }
    for (const item of Array.isArray(account.recent_events) ? account.recent_events : []) {
      rememberInstrumentName(item.payload && item.payload.instrument_id, item.instrument_name);
    }
    const nearest = account.nearest_signal;
    if (nearest) rememberInstrumentName(nearest.instrument_id, nearest.instrument_name);
  }
}

function instrumentLabel(instrumentId, instrumentName = null) {
  if (!instrumentId) return "—";
  const name = instrumentName || instrumentNames[instrumentId];
  return name ? `${name}（${instrumentId}）` : instrumentId;
}

function compactInstrumentList(items, limit = 4) {
  const visible = (items || []).slice(0, limit);
  const labels = visible.map((item) => instrumentLabel(item.instrument_id, item.instrument_name));
  if ((items || []).length > limit) labels.push(`另 ${items.length - limit} 个`);
  return labels.join("、");
}

function strategyUniverseSummary(item) {
  const universe = item.strategy_universe || "动态选股";
  const instruments = item.strategy_instruments || [];
  if (!instruments.length) return `${universe}：标的随规则每日筛选`;
  const separator = universe.includes("动态") ? "；固定参考标的：" : "：";
  return `${universe}${separator}${compactInstrumentList(instruments)}`;
}

const CRISIS_SHORT_NAMES = {
  "paper-crash-global": "全球宽池", "paper-crash-cn-small": "A股精简",
  "paper-crash-cn-wide": "A股宽池", "paper-crash-conservative": "保守反转",
  "paper-crash-aggressive": "激进阶梯", "paper-crash-vol-control": "波动控制",
  "paper-crash-fast-profit": "快速止盈", "paper-crash-semiconductor": "半导体危机",
};

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
  ["pruned_count", "清理记录", qty],
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
    refreshTab(tab.dataset.tab, { onlyIfNeeded: true });
  });
}

function activeTab() {
  return document.querySelector(".tab.active").dataset.tab;
}

/* --------------------------------------------------------------- overview */

let sessionCutoff = null;
let accountListCache = null;
const accountDetailCache = new Map();

async function loadOverview() {
  clearTimeout(scheduleRetryTimer);
  scheduleRetryTimer = null;
  const data = await api("/api/overview");
  const market = data.market || {};
  const schedule = data.schedule || {};
  sessionCutoff = data.config ? data.config.session_cutoff_local : null;
  accountListCache = data.accounts || [];
  accountDetailCache.clear();
  for (const tabName of ["accounts", "monitor", "agent"]) loadedTabs.delete(tabName);
  rememberStrategyInstruments(data.accounts);

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
    overviewScheduleCard(schedule),
    card("手动运行", runValue, runSub),
  );
  if (scheduleIsRefreshing(schedule)) {
    scheduleRetryTimer = setTimeout(() => refreshOverviewSchedule(0), 750);
  }

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
      `${item.name} · ${item.strategy_name || item.strategy_kind || "策略"}`,
      value,
      el("div", { class: "strategy-card-details" }, [
        el("div", { class: "strategy-idea", text: item.strategy_description || "—" }),
        el("div", {
          class: "strategy-instruments",
          text: strategyUniverseSummary(item),
        }),
        el("div", { class: "strategy-account-state", text: item.exists
          ? `头寸日 ${item.head_date || "—"} · 持仓 ${item.positions} · 挂单 ${item.pending_orders}`
          : `初始资金 ${money(item.initial_cash)}，首次运行时自动创建` }),
      ]),
    );
    clickable(node, () => {
      openAccountFromOverview(item.account_id).then(clearError).catch(showError);
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

  $("#topbar-status").textContent = market.snapshot_id
    ? `快照 ${market.snapshot_id.slice(0, 18)}… · canonical 数据至 ${market.published_end} · UI ${UI_BUILD}`
    : `快照信息不可用 · UI ${UI_BUILD}`;
}

function overviewScheduleCard(schedule) {
  let value;
  let sub = "";
  if (scheduleIsRefreshing(schedule)) {
    value = "正在读取";
    sub = "后台读取 Windows 计划任务状态";
  } else if (schedule.exists) {
    value = `${schedule.time || "?"}（${schedule.enabled ? "已启用" : "已停用"}）`;
    sub = schedule.next_run_time ? `下次运行 ${fmtDateTime(schedule.next_run_time)}` : "";
  } else {
    value = schedule.error ? "查询失败" : "未设置";
    sub = schedule.error || "";
  }
  const node = card("计划任务", value, sub);
  node.id = "overview-schedule-card";
  return node;
}

async function refreshOverviewSchedule(attempt) {
  if (activeTab() !== "overview") return;
  try {
    const state = await api("/api/schedule");
    $("#overview-schedule-card").replaceWith(overviewScheduleCard(state));
  } catch (error) {
    if (scheduleIsRefreshing(error)) {
      $("#overview-schedule-card").replaceWith(overviewScheduleCard({
        error: SCHEDULE_REFRESHING_MESSAGE,
      }));
      scheduleRetryTimer = setTimeout(
        () => refreshOverviewSchedule(attempt + 1),
        Math.min(750 * (attempt + 1), 5000),
      );
      return;
    }
    $("#overview-schedule-card").replaceWith(overviewScheduleCard({
      error: String(error.message || error),
    }));
  }
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

/* ------------------------------------------------------ strategy monitor */

let monitorPayload = null;
let monitorHistory = null;
let monitorPerformanceChart = null;
let monitorSignalChart = null;

function crisisAction(action) {
  return CRISIS_ACTION_LABELS[action] || action || "尚无评估";
}

function monitorAccount(accountId) {
  return (monitorPayload && monitorPayload.accounts || []).find((item) => item.account_id === accountId);
}

async function loadMonitor() {
  monitorPayload = await api("/api/agent/monitor");
  rememberStrategyInstruments(monitorPayload.accounts);
  const summary = monitorPayload.summary || {};
  const market = monitorPayload.market || {};
  $("#monitor-summary").replaceChildren(
    card("危机策略", `${summary.configured || 0} 个`, `数据截至 ${market.published_end || "—"}`),
    card("当前评估", `${summary.current || 0} / ${summary.configured || 0}`,
      summary.errors ? `${summary.errors} 个失败` : "全部按证据状态展示"),
    card("状态变化", `${summary.triggered || 0} 个`, "建仓、加仓或退出信号"),
    card("人工干预", `${summary.manual || 0} 个`, "人工介入后不再视为纯策略"),
  );

  const accounts = monitorPayload.accounts || [];
  const strategies = $("#monitor-strategies");
  if (!accounts.length) {
    strategies.replaceChildren(el("div", { class: "empty", text: "尚未配置危机策略账户" }));
    renderMonitorPerformance([]);
    return;
  }
  strategies.replaceChildren(...accounts.map((item) => monitorStrategyCard(item)));
  renderMonitorPerformance(accounts);

  const select = $("#monitor-account-select");
  const current = select.value;
  select.replaceChildren(...accounts.map((item) => el("option", {
    value: item.account_id,
    text: `${CRISIS_SHORT_NAMES[item.account_id] || item.name}（${item.account_id}）`,
  })));
  if (current && accounts.some((item) => item.account_id === current)) select.value = current;
  await loadMonitorDetail();
}

function monitorStrategyCard(item) {
  const evaluation = item.evaluation || {};
  const successful = evaluation.status === "ready" ? evaluation : (item.last_success || {});
  const nearest = item.nearest_signal;
  const performance = item.performance || {};
  const progress = nearest ? Math.max(0, Math.min(1, Number(nearest.trigger_progress))) : 0;
  const node = el("div", { class: "card monitor-strategy-card" }, [
    el("div", { class: "monitor-card-head" }, [
      el("strong", { text: CRISIS_SHORT_NAMES[item.account_id] || item.name }),
      statusBadge(item.evaluation_status),
    ]),
    el("div", { class: "monitor-action", text: crisisAction(successful.action) }),
    nearest ? el("div", { class: "monitor-progress-block" }, [
      el("div", { class: "hint", text: `${instrumentLabel(nearest.instrument_id, nearest.instrument_name)} · 触发进度 ${percent(progress)}` }),
      el("div", { class: "progress-track" }, el("span", {
        class: "progress-fill",
        style: `width:${(progress * 100).toFixed(1)}%`,
      })),
    ]) : el("div", { class: "hint", text: item.stale_reason || "暂无可用 ETF 信号" }),
    el("div", { class: "monitor-card-stats" }, [
      el("span", {}, ["累计 ", signed(performance.actual_return, { asPercent: true })]),
      el("span", {}, ["当前版本 ", signed(performance.current_config_return, { asPercent: true })]),
    ]),
    item.manual_intervention
      ? el("span", { class: "badge warn", text: "含人工干预" })
      : el("span", { class: "badge ok", text: "纯策略" }),
  ]);
  clickable(node, () => {
    $("#monitor-account-select").value = item.account_id;
    loadMonitorDetail().then(clearError).catch(showError);
  });
  return node;
}

function renderMonitorPerformance(accounts) {
  const container = $("#monitor-performance-chart");
  if (!monitorPerformanceChart) monitorPerformanceChart = echarts.init(container);
  const labels = accounts.map((item) => {
    const name = CRISIS_SHORT_NAMES[item.account_id] || item.name;
    return item.manual_intervention ? `${name}*` : name;
  });
  const actual = accounts.map((item) => {
    const value = Number((item.performance || {}).actual_return);
    return Number.isFinite(value) ? value * 100 : null;
  });
  const version = accounts.map((item) => {
    if (item.manual_intervention) return null;
    const value = Number((item.performance || {}).current_config_return);
    return Number.isFinite(value) ? value * 100 : null;
  });
  monitorPerformanceChart.setOption({
    color: [cssVar("--accent", "#4f46e5"), cssVar("--warn", "#d97706")],
    textStyle: { fontFamily: cssVar("--sans", "sans-serif") },
    tooltip: { trigger: "axis", valueFormatter: (value) => value == null ? "—" : `${Number(value).toFixed(2)}%` },
    legend: { top: 8, data: ["账户真实累计", "当前配置版本"] },
    grid: { left: 20, right: 20, top: 48, bottom: 20, containLabel: true },
    xAxis: { type: "category", data: labels, axisLabel: { interval: 0, rotate: labels.length > 5 ? 20 : 0 } },
    yAxis: { type: "value", axisLabel: { formatter: "{value}%" } },
    series: [
      { name: "账户真实累计", type: "bar", data: actual, barMaxWidth: 30 },
      { name: "当前配置版本", type: "bar", data: version, barMaxWidth: 30 },
    ],
  }, true);
  monitorPerformanceChart.resize();
}

async function loadMonitorDetail() {
  const accountId = $("#monitor-account-select").value;
  const account = monitorAccount(accountId);
  if (!account) return;
  renderMonitorDetailLoading(account);
  const history = await api(`/api/agent/evaluations/${encodeURIComponent(accountId)}?limit=90`);
  if ($("#monitor-account-select").value !== accountId) return;
  monitorHistory = history;
  renderMonitorDetail(account, history);
}

function renderMonitorDetailLoading(account) {
  $("#monitor-detail-freshness").replaceChildren(
    statusBadge("running"),
    el("span", { class: "hint", text: `正在读取 ${CRISIS_SHORT_NAMES[account.account_id] || account.name} 的正式评估证据` }),
  );
  $("#monitor-detail-metrics").replaceChildren(card("策略详情", "加载中…", "读取最近 90 个交易日"));
  $("#monitor-state-flow").replaceChildren();
  $("#monitor-signals-table").replaceChildren();
  $("#monitor-parameters-table").replaceChildren();
  $("#monitor-instrument-select").replaceChildren();
  $("#monitor-timeline-table").replaceChildren();
  if (monitorSignalChart) monitorSignalChart.clear();
}

function renderMonitorDetail(account, history) {
  const evaluation = account.evaluation || {};
  const successful = evaluation.status === "ready" ? evaluation : (account.last_success || {});
  const performance = account.performance || {};
  const decision = account.decision || { status: "none" };
  const execution = account.execution || {};
  const positions = execution.positions || [];
  const pendingOrders = execution.pending_orders || [];
  $("#monitor-detail-freshness").replaceChildren(...[
    statusBadge(account.evaluation_status),
    account.stale_reason ? el("span", { class: "hint", text: account.stale_reason }) : null,
  ].filter(Boolean));
  $("#monitor-detail-metrics").replaceChildren(
    card("策略信号", crisisAction(successful.action), successful.as_of ? `数据截至 ${successful.as_of}` : "无成功评估"),
    card("目标决策", statusLabel(decision.status), decision.decision_date ? `目标日 ${decision.decision_date}` : "未生成交易目标"),
    card("实际账户", positions.length ? `${positions.length} 个持仓` : "暂无持仓",
      `头寸日 ${execution.head_date || "—"} · 挂单 ${pendingOrders.length}`),
    card("累计收益", signed(performance.actual_return, { asPercent: true }),
      `当前配置 ${percent(performance.current_config_return)} · 自 ${performance.current_config_start || "—"}`),
  );
  renderMonitorFlow(account, successful);
  renderMonitorSignals(successful);
  renderMonitorParameters(account.parameters || {});
  renderMonitorHistory(history);
}

function renderMonitorFlow(account, successful) {
  const decision = account.decision || { status: "none" };
  const execution = account.execution || {};
  let executionStatus = "idle";
  let executionText = "尚未进入持仓";
  if ((execution.pending_orders || []).length) {
    executionStatus = "pending";
    executionText = `${execution.pending_orders.length} 个挂单`;
  } else if ((execution.positions || []).length) {
    executionStatus = "active";
    executionText = `${execution.positions.length} 个实际持仓`;
  }
  $("#monitor-state-flow").replaceChildren(
    monitorFlowStep("1. 策略信号", account.evaluation_status,
      crisisAction(successful.action), successful.reason),
    el("span", { class: "state-arrow", text: "→" }),
    monitorFlowStep("2. 目标决策", decision.status,
      statusLabel(decision.status), decision.decision_date ? `目标日 ${decision.decision_date}` : "无可执行文件"),
    el("span", { class: "state-arrow", text: "→" }),
    monitorFlowStep("3. 模拟执行", executionStatus,
      statusLabel(executionStatus), executionText),
  );
}

function monitorFlowStep(title, status, value, detail) {
  return el("div", { class: "state-step" }, [
    el("div", { class: "hint", text: title }),
    el("div", { class: "row compact-row" }, [statusBadge(status), el("strong", { text: value })]),
    el("div", { class: "hint", text: detail || "—" }),
  ]);
}

function renderMonitorSignals(evaluation) {
  const signals = evaluation.audit && evaluation.audit.signals || {};
  $("#monitor-signals-table").replaceChildren(table(
    [
      { label: "ETF" }, { label: "当前回撤", num: true }, { label: "事件回撤", num: true },
      { label: "低点反弹", num: true }, { label: "恢复比例", num: true },
      { label: "均线确认" }, { label: "年化波动", num: true },
    ],
    Object.entries(signals).map(([instrumentId, item]) => [
      el("span", { class: "mono", title: instrumentNames[instrumentId] || "", text: instrumentLabel(instrumentId) }),
      percent(item.current_drawdown), percent(item.event_drawdown), percent(item.rebound_from_low),
      percent(item.recovery_ratio), item.above_confirmation_average ? "是" : "否",
      percent(item.annualized_volatility),
    ]),
    { empty: "最近成功评估没有 ETF 信号" },
  ));
}

function renderMonitorParameters(parameters) {
  const percentKeys = new Set([
    "minimum_drawdown", "rebound_threshold", "recovery_exit_gap", "profit_take",
    "entry_risk_weight", "max_risk_weight", "max_position_weight", "ladder_step",
    "tranche_weight", "target_volatility", "rebalance_threshold",
  ]);
  const rows = Object.entries(parameters).map(([key, value]) => {
    let shown;
    if (key === "risk_instruments") shown = (value || []).map(instrumentLabel).join("、");
    else if (key === "defensive_instrument") shown = instrumentLabel(value);
    else if (key === "position_caps") shown = Object.entries(value || {})
      .map(([instrumentId, cap]) => `${instrumentLabel(instrumentId)} ${percent(cap)}`).join("；");
    else if (percentKeys.has(key)) shown = percent(value);
    else if (key.endsWith("_days")) shown = `${value} 个交易日`;
    else if (key === "entry_mode") shown = value === "ladder" ? "回撤阶梯" : "反转确认";
    else shown = String(value);
    return [CRISIS_PARAMETER_LABELS[key] || key, shown];
  });
  $("#monitor-parameters-table").replaceChildren(table(
    [{ label: "参数" }, { label: "当前配置" }], rows,
    { empty: "暂无策略参数" },
  ));
}

function renderMonitorHistory(history) {
  const latestReady = (history.records || []).filter((item) => item.status === "ready" && item.is_latest_for_date);
  const instruments = new Set();
  for (const record of latestReady) {
    for (const instrumentId of Object.keys(record.audit && record.audit.signals || {})) instruments.add(instrumentId);
  }
  const select = $("#monitor-instrument-select");
  const current = select.value;
  select.replaceChildren(...[...instruments].sort().map((instrumentId) => el("option", {
    value: instrumentId, text: instrumentLabel(instrumentId),
  })));
  if (current && instruments.has(current)) select.value = current;
  renderMonitorSignalChart(history, select.value);

  $("#monitor-timeline-table").replaceChildren(table(
    [
      { label: "数据日" }, { label: "状态" }, { label: "动作" },
      { label: "目标日" }, { label: "配置" }, { label: "说明" },
    ],
    (history.records || []).map((item) => [
      item.evidence_date,
      item.is_latest_for_date
        ? statusBadge(item.status === "ready" ? (item.is_current ? "current" : "ready") : "error")
        : el("span", { class: "badge muted", text: "旧修订" }),
      item.status === "ready" ? crisisAction(item.action) : item.error_type,
      item.decision_date || "—",
      el("span", { class: "mono", text: shortId(item.config_hash) }),
      item.status === "ready" ? item.reason : item.error,
    ]),
    { empty: "尚无正式评估历史" },
  ));
}

function renderMonitorSignalChart(history, instrumentId) {
  const container = $("#monitor-signal-chart");
  if (!monitorSignalChart) monitorSignalChart = echarts.init(container);
  const records = (history.records || [])
    .filter((item) => item.status === "ready" && item.is_latest_for_date)
    .slice().reverse();
  const points = records.map((item) => {
    const signal = item.audit && item.audit.signals && item.audit.signals[instrumentId];
    return { date: item.as_of, signal };
  }).filter((item) => item.signal);
  const dates = points.map((item) => item.date);
  const seriesData = (key) => points.map((item) => {
    const value = Number(item.signal[key]);
    return Number.isFinite(value) ? value * 100 : null;
  });
  const configMarks = (history.configuration_changes || []).filter((item) => !item.initial).map((item) => ({
    xAxis: item.as_of,
  }));
  monitorSignalChart.setOption({
    color: [cssVar("--accent", "#4f46e5"), cssVar("--bad", "#dc2626"),
      cssVar("--ok", "#16a34a"), cssVar("--warn", "#d97706")],
    textStyle: { fontFamily: cssVar("--sans", "sans-serif") },
    tooltip: { trigger: "axis", valueFormatter: (value) => value == null ? "—" : `${Number(value).toFixed(2)}%` },
    legend: { top: 8, data: ["当前回撤", "事件回撤", "低点反弹", "年化波动"] },
    grid: { left: 18, right: 18, top: 48, bottom: 30, containLabel: true },
    xAxis: { type: "category", data: dates, boundaryGap: false },
    yAxis: { type: "value", axisLabel: { formatter: "{value}%" } },
    series: [
      {
        name: "当前回撤", type: "line", data: seriesData("current_drawdown"), showSymbol: false,
        markLine: { silent: true, symbol: "none", label: { formatter: "配置切换" }, data: configMarks },
      },
      { name: "事件回撤", type: "line", data: seriesData("event_drawdown"), showSymbol: false },
      { name: "低点反弹", type: "line", data: seriesData("rebound_from_low"), showSymbol: false },
      { name: "年化波动", type: "line", data: seriesData("annualized_volatility"), showSymbol: false },
    ],
    graphic: points.length ? [] : [{
      type: "text", left: "center", top: "middle",
      style: { text: instrumentId ? "所选 ETF 暂无历史信号" : "暂无 ETF 信号", fill: cssVar("--muted", "#64748b") },
    }],
  }, true);
  monitorSignalChart.resize();
}

$("#monitor-account-select").addEventListener("change", () => loadMonitorDetail().then(clearError).catch(showError));
$("#monitor-instrument-select").addEventListener("change", () => {
  if (monitorHistory) renderMonitorSignalChart(monitorHistory, $("#monitor-instrument-select").value);
});

/* --------------------------------------------------------------- accounts */

let equityChart = null;
let chartAccountId = null;
let pendingAccountId = null;
let accountNameRetryTimer = null;

async function openAccountFromOverview(accountId) {
  const accountsAlreadyLoaded = loadedTabs.has("accounts");
  rememberAccountLocation(accountId);
  pendingAccountId = accountId;
  document.querySelector('[data-tab="accounts"]').click();
  if (!accountsAlreadyLoaded) return;

  const select = $("#account-select");
  const optionExists = [...select.options].some((option) => option.value === accountId);
  if (!optionExists) return;
  pendingAccountId = null;
  select.value = accountId;
  await loadAccountDetail();
}

async function loadAccountList({ force = false } = {}) {
  const accounts = !force && accountListCache
    ? accountListCache
    : await api("/api/accounts");
  accountListCache = accounts;
  rememberStrategyInstruments(accounts);
  const select = $("#account-select");
  const wanted = pendingAccountId || accountIdFromLocation() || select.value;
  pendingAccountId = null;
  select.replaceChildren(...accounts.map((item) =>
    el("option", {
      value: item.account_id,
      text: `${item.strategy_name || item.name}（${item.account_id}）`,
    })));
  if (wanted && accounts.some((item) => item.account_id === wanted)) select.value = wanted;
  return accounts;
}

async function loadAccountDetail({ force = false, nameRetryAttempt = 0 } = {}) {
  clearTimeout(accountNameRetryTimer);
  accountNameRetryTimer = null;
  const accountId = $("#account-select").value;
  if (!accountId) {
    $("#account-detail").classList.add("hidden");
    $("#account-empty").classList.remove("hidden");
    return;
  }
  $("#account-empty").classList.add("hidden");
  $("#account-detail").classList.remove("hidden");
  let data = !force ? accountDetailCache.get(accountId) : null;
  if (!data) {
    data = await api(`/api/accounts/${encodeURIComponent(accountId)}`);
    if (data.instrument_names_pending) accountDetailCache.delete(accountId);
    else accountDetailCache.set(accountId, data);
  }
  if ($("#account-select").value !== accountId) return;
  rememberStrategyInstruments([data]);
  renderAccountStrategy(data);
  if (data.instrument_names_pending) {
    accountNameRetryTimer = setTimeout(() => {
      if (activeTab() !== "accounts" || $("#account-select").value !== accountId) return;
      loadAccountDetail({
        force: true,
        nameRetryAttempt: nameRetryAttempt + 1,
      }).then(clearError).catch(showError);
    }, Math.min(750 * (nameRetryAttempt + 1), 5000));
  }

  if (data.exists === false) {
    $("#account-metrics").replaceChildren(card(
      "账户尚未创建",
      "等待首次运行",
      `初始资金 ${money(data.cash)} · 首次每日运行时自动创建`,
    ));
    renderEquityChart([], accountId, [], []);
    const notCreated = () => el("div", { class: "empty", text: "账户尚未创建" });
    $("#positions-table").replaceChildren(notCreated());
    $("#orders-table").replaceChildren(notCreated());
    $("#events-table").replaceChildren(notCreated());
    return;
  }

  const fb = data.feedback || {};
  const metrics = [
    card("总权益", money(fb.final_equity || data.cash), `现金 ${money(data.cash)}`),
    card("累计收益", signed(fb.overall_return, { asPercent: true }),
      el("span", {}, ["本段收益 ", signed(fb.run_return, { asPercent: true })])),
    card("最大回撤", percent(fb.max_drawdown),
      `质量 ${fb.quality === "complete" ? "完整" : fb.quality === "incomplete" ? "不完整" : (fb.quality || "—")}`),
    card("已实现盈亏", signed(data.realized_pnl), `分红 ${money(data.dividend_income)}`),
    card("累计费用", money(data.fees_paid), `成交 ${fb.fills ?? "—"} / 订单 ${fb.orders ?? "—"}`),
  ];
  const benchmark = data.benchmark;
  if (benchmark) {
    if (benchmark.status === "ready") {
      const evidenceRole = benchmark.actionability === "supporting_evidence"
        ? "可作辅助证据" : "观察期（不可触发调仓）";
      metrics.push(card(
        `相对 ${benchmark.instrument_id || "基准"}`,
        signed(benchmark.excess_return, { asPercent: true }),
        `组合 ${percent(benchmark.portfolio_return)} · 基准 ${percent(benchmark.benchmark_total_return)} · ${benchmark.common_trading_sessions}日 · ${evidenceRole}`,
      ));
    } else {
      metrics.push(card(
        `基准 ${benchmark.instrument_id || "159207.SZ"}`,
        benchmark.status === "waiting_for_first_investment" ? "等待建仓" : "数据不可用",
        benchmark.reason || "暂无可比数据",
      ));
    }
  }
  $("#account-metrics").replaceChildren(...metrics);

  renderEquityChart(
    data.equity_curve || [], accountId, data.benchmark_history || [],
    data.strategy_config_changes || [],
  );

  $("#positions-table").replaceChildren(table(
    [
      { label: "标的" }, { label: "数量", num: true }, { label: "可卖", num: true },
      { label: "成本均价", num: true }, { label: "最新价", num: true },
      { label: "市值", num: true }, { label: "浮动盈亏", num: true },
    ],
    (data.positions || []).map((item) => [
      el("span", {
        class: "instrument-cell",
        text: instrumentLabel(item.instrument_id, item.instrument_name),
      }),
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
      el("span", {
        class: "instrument-cell",
        text: instrumentLabel(item.instrument_id, item.instrument_name),
      }),
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
        ? el("span", {
          class: "instrument-cell",
          text: instrumentLabel(payload.instrument_id, item.instrument_name),
        })
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

function renderAccountStrategy(data) {
  const instruments = data.strategy_instruments || [];
  const instrumentRows = instruments.map((item) => [
    item.role || "固定标的",
    instrumentLabel(item.instrument_id, item.instrument_name),
  ]);
  $("#account-strategy-profile").replaceChildren(
    el("div", { class: "strategy-profile-head" }, [
      el("div", {}, [
        el("div", { class: "hint", text: "策略思想" }),
        el("strong", { class: "strategy-profile-name", text: data.strategy_name || data.strategy_kind || "—" }),
      ]),
      el("span", {
        class: `badge ${data.strategy_uses_llm ? "warn" : "ok"}`,
        text: data.strategy_uses_llm ? "含 LLM 复核" : "纯规则策略",
      }),
    ]),
    el("p", { class: "strategy-profile-description", text: data.strategy_description || "—" }),
    el("div", { class: "hint", text: data.strategy_universe || "—" }),
    instrumentRows.length
      ? table([{ label: "角色" }, { label: "标的中文名与代码" }], instrumentRows)
      : el("div", { class: "empty compact-empty", text: "标的由策略规则动态筛选，固定池中没有可逐项列出的股票。" }),
  );
}

function renderEquityChart(curve, accountId, benchmarkHistory = [], configChanges = []) {
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
  const benchmarkColor = cssVar("--good", "#0f766e");
  const border = cssVar("--border", "#e2e8f0");
  const muted = cssVar("--muted", "#64748b");
  const sans = cssVar("--sans", "sans-serif");
  const showSlider = curve.length > 60;

  const dates = curve.map((item) => item.session_date);
  const equity = curve.map((item) => Number(item.total_equity));
  const nav = curve.map((item) => Number(item.nav));
  const benchmarkByDate = new Map(benchmarkHistory.map((item) => [
    item.comparison_end || item.as_of, Number(item.benchmark_nav),
  ]));
  const benchmarkNav = dates.map((day) => (
    benchmarkByDate.has(day) ? benchmarkByDate.get(day) : null
  ));
  const hasBenchmark = benchmarkNav.some((value) => Number.isFinite(value));
  const legend = ["总权益", "净值", ...(hasBenchmark ? ["159207 含分红基准"] : [])];

  equityChart.setOption({
    color: [accent, warnColor, benchmarkColor],
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
      data: legend,
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
        markLine: {
          silent: true,
          symbol: "none",
          lineStyle: { color: warnColor, type: "dashed", width: 1 },
          label: { color: warnColor, formatter: "配置切换" },
          data: (configChanges || []).filter((item) => !item.initial).map((item) => ({
            xAxis: item.as_of,
          })),
        },
      },
      ...(hasBenchmark ? [{
        name: "159207 含分红基准", type: "line", yAxisIndex: 1,
        data: benchmarkNav, connectNulls: true, showSymbol: true, symbolSize: 6,
        lineStyle: { width: 1.8, type: "dashed" },
        tooltip: { valueFormatter: (v) => Number(v).toFixed(4) },
      }] : []),
    ],
  }, true);
  equityChart.resize();
}

window.addEventListener("resize", () => {
  if (equityChart) equityChart.resize();
  if (monitorPerformanceChart) monitorPerformanceChart.resize();
  if (monitorSignalChart) monitorSignalChart.resize();
});
// 网格折行、面板切换等只改容器不改窗口的情况,window resize 不会触发。
new ResizeObserver(() => equityChart && equityChart.resize()).observe($("#equity-chart"));
new ResizeObserver(() => monitorPerformanceChart && monitorPerformanceChart.resize())
  .observe($("#monitor-performance-chart"));
new ResizeObserver(() => monitorSignalChart && monitorSignalChart.resize())
  .observe($("#monitor-signal-chart"));
$("#account-select").addEventListener("change", () => {
  rememberAccountLocation($("#account-select").value);
  loadAccountDetail().then(clearError).catch(showError);
});

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

async function loadSchedule(retryAttempt = 0) {
  clearTimeout(scheduleRetryTimer);
  scheduleRetryTimer = null;
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
    if (scheduleIsRefreshing(error)) {
      $("#schedule-cards").replaceChildren(card(
        "计划任务", "正在读取", "后台读取 Windows 计划任务状态",
      ));
      scheduleRetryTimer = setTimeout(() => {
        if (activeTab() === "schedule") loadSchedule(retryAttempt + 1);
      }, Math.min(750 * (retryAttempt + 1), 5000));
      return;
    }
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
  await scheduleAction(() => api("/api/schedule", {
    method: "DELETE",
  }), "已删除");
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
      else refreshTab(activeTab(), { auto: true, force: true });
    }
  }
}

/* ------------------------------------------------------------------ agent */

async function loadAgentTab() {
  const accounts = await api("/api/agent/accounts");
  rememberStrategyInstruments(accounts);
  const select = $("#agent-account-select");
  const current = select.value;
  select.replaceChildren(...accounts.map((item) =>
    el("option", {
      value: item.account_id,
      text: `${item.strategy_name || item.name}（${item.account_id}）`,
    })));
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
        force_review: $("#agent-force-review").checked,
      }),
    });
    const weights = Object.entries(result.target_weights || {})
      .map(([k, v]) => `${k} ${parseFloat((Number(v) * 100).toFixed(1))}%`).join(" · ");
    const alreadyPresent = result.skipped === "already_present";
    const reviewedHold = result.held && result.review && result.review.review_completed;
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
            : reviewedHold
              ? el("span", { class: "badge muted", text: "评估后持有" })
              : el("span", { class: "badge muted", text: "未落盘(预演)" }),
      ]),
      el("div", {
        text: alreadyPresent
          ? "已有有效决策，未改写文件。"
          : weights ? `目标权重:${weights}` : "本次没有组合变更。",
      }),
      el("div", { class: "hint", text: result.reason || "" }),
      el("div", {
        class: "hint",
        text: result.email
          ? `邮件:${result.email.sent ? "已发送" : result.email.detail}`
          : "",
      }),
    );
    const outcome = result.written
      ? `已投递:${result.decision_date}.json`
      : alreadyPresent
        ? "同日有效决策已存在，幂等跳过"
        : reviewedHold ? "周度评估完成，本次不调仓" : "预演完成,未写入文件";
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

const loadedTabs = new Set();
const loadingTabs = new Set();
const tabLoadedAt = new Map();
const tabRefreshes = new Map();

function refreshTab(name, { auto = false, force = false, onlyIfNeeded = false } = {}) {
  if (onlyIfNeeded && (loadedTabs.has(name) || loadingTabs.has(name))) {
    return tabRefreshes.get(name) || Promise.resolve();
  }
  const existing = tabRefreshes.get(name);
  if (existing) return existing;
  const panel = $(`#panel-${name}`);
  const refreshBtn = $("#refresh-btn");
  loadingTabs.add(name);
  panel.classList.add("loading");
  refreshBtn.disabled = true;
  const pending = (async () => {
    try {
      if (name === "overview") await loadOverview();
      else if (name === "monitor") await loadMonitor();
      else if (name === "accounts") {
        await loadAccountList({ force });
        await loadAccountDetail({ force });
      }
      else if (name === "runs") await loadRuns({ auto });
      else if (name === "schedule") { await loadSchedule(); await pollRunStatus(); }
      else if (name === "agent") await loadAgentTab();
      loadedTabs.add(name);
      tabLoadedAt.set(name, Date.now());
      clearError();
    } catch (error) {
      showError(error);
    } finally {
      loadingTabs.delete(name);
      panel.classList.remove("loading");
      refreshBtn.disabled = false;
      tabRefreshes.delete(name);
    }
  })();
  tabRefreshes.set(name, pending);
  return pending;
}

$("#refresh-btn").addEventListener("click", () => refreshTab(activeTab(), { force: true }));

document.addEventListener("visibilitychange", () => {
  const name = activeTab();
  const age = Date.now() - (tabLoadedAt.get(name) || 0);
  if (!document.hidden && age > 5 * 60 * 1000) {
    refreshTab(name, { auto: true, force: true });
  }
});

const initialAccountId = accountIdFromLocation();
refreshTab("overview").then(() => {
  if (initialAccountId) {
    return openAccountFromOverview(initialAccountId);
  }
  return null;
}).then(clearError).catch(showError);
