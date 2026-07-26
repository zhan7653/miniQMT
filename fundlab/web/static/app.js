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

function money(value) {
  if (value == null || value === "") return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return number.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function percent(value) {
  if (value == null) return "—";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return (number * 100).toFixed(2) + "%";
}

function statusBadge(status) {
  const map = {
    ok: ["ok", "正常"], up_to_date: ["ok", "已最新"], complete: ["ok", "完成"],
    blocked: ["bad", "阻断"], failed: ["bad", "失败"], skipped: ["muted", "跳过"],
    running: ["warn", "运行中"], active: ["ok", "活跃"],
  };
  const [kind, label] = map[status] || ["muted", status || "—"];
  return el("span", { class: `badge ${kind}`, text: label });
}

const EVENT_LABELS = {
  portfolio_intent_received: "收到组合意图",
  order_created: "创建订单",
  order_rejected: "订单拒绝",
  order_expired: "订单过期",
  fill_created: "成交",
  cash_dividend_paid: "现金分红入账",
  corporate_action_entitlement: "权益登记",
  intent_not_scheduled: "意图未排单",
};

const STAGE_LABELS = {
  resolve: "解析目标日", universe: "官方标的池", bars: "行情双源采集", no_trade: "停牌共识",
  research: "研究快照", status: "状态采集", evidence: "行动/因子证据", candidate: "候选合成",
  validate: "增量验证", extend: "原子发布", accounts: "账户推进", data: "数据阶段", calendar: "交易日历",
};

function table(headers, rows, opts = {}) {
  if (!rows.length) return el("div", { class: "empty", text: opts.empty || "暂无数据" });
  const head = el("tr", {}, headers.map((h) =>
    el("th", { class: h.num ? "num" : "", text: h.label })));
  const body = rows.map((cells, index) => {
    const tr = el("tr", opts.onRowClick ? {
      class: "clickable",
      onclick: () => opts.onRowClick(index),
    } : {}, cells.map((cell, i) => {
      const td = el("td", { class: headers[i] && headers[i].num ? "num" : "" });
      if (cell instanceof Node) td.appendChild(cell);
      else td.textContent = cell == null ? "—" : String(cell);
      return td;
    }));
    return tr;
  });
  return el("div", { class: "table-wrap" }, el("table", {}, [head, ...body]));
}

/* ------------------------------------------------------------------- tabs */

for (const tab of document.querySelectorAll(".tab")) {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((item) => item.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((item) => item.classList.remove("active"));
    tab.classList.add("active");
    $(`#panel-${tab.dataset.tab}`).classList.add("active");
    refreshTab(tab.dataset.tab);
  });
}

function activeTab() {
  return document.querySelector(".tab.active").dataset.tab;
}

/* --------------------------------------------------------------- overview */

async function loadOverview() {
  const data = await api("/api/overview");
  const market = data.market || {};
  const schedule = data.schedule || {};
  const cards = $("#overview-cards");
  cards.replaceChildren(
    card("数据快照", market.published_end || market.error || "—",
      market.snapshot_id ? `${market.instruments} 个标的 · ${market.snapshot_id}` : ""),
    card("计划任务", schedule.exists
      ? `${schedule.time || "?"}（${schedule.enabled ? "已启用" : "已停用"}）`
      : (schedule.error ? "查询失败" : "未设置"),
      schedule.next_run_time ? `下次运行 ${schedule.next_run_time}` : (schedule.error || "")),
    card("手动运行", data.run && data.run.running ? "运行中" :
      (data.run && data.run.last ? `上次退出码 ${data.run.last.exit_code}` : "—"),
      data.run && data.run.last ? data.run.last.finished_at || "" : ""),
    card("收盘截止", data.config ? data.config.session_cutoff_local : "—",
      "此时间后才视为当日数据完整"),
  );

  const accounts = $("#overview-accounts");
  accounts.replaceChildren(...(data.accounts || []).map((item) => {
    const node = card(
      `${item.name}（${item.strategy === "static" ? "静态权重" : "Agent 决策"}）`,
      item.exists ? money(item.equity || item.initial_cash) : "未创建",
      item.exists
        ? `头寸日 ${item.head_date || "—"} · 持仓 ${item.positions} · 挂单 ${item.pending_orders}`
        : `初始资金 ${money(item.initial_cash)}，首次运行时自动创建`,
    );
    node.classList.add("clickable");
    node.addEventListener("click", () => {
      // The accounts dropdown is only populated when that tab loads, so stash
      // the choice and apply it after loadAccountList has filled the options.
      pendingAccountId = item.account_id;
      document.querySelector('[data-tab="accounts"]').click();
    });
    return node;
  }));

  const last = $("#overview-last-run");
  if (data.last_report) {
    last.replaceChildren(renderReportSummary(data.last_report));
  } else {
    last.replaceChildren(el("div", { class: "empty", text: "还没有运行记录" }));
  }

  const s = data.market && data.market.snapshot_id
    ? `快照 ${market.snapshot_id.slice(0, 18)}… · 数据至 ${market.published_end}`
    : "快照信息不可用";
  $("#topbar-status").textContent = s;
}

function card(title, value, sub) {
  return el("div", { class: "card" }, [
    el("div", { class: "card-title", text: title }),
    el("div", { class: "card-value", text: value == null ? "—" : String(value) }),
    sub ? el("div", { class: "card-sub", text: sub }) : null,
  ]);
}

function renderReportSummary(report) {
  const wrap = el("div");
  const head = el("div", { class: "row" }, [
    statusBadge(report.status),
    el("strong", { text: `目标日 ${report.target_date || "—"}` }),
    el("span", { class: "hint", text: report.generated_at || "" }),
    report.blocked_stage
      ? el("span", { class: "badge bad", text: `阻断于 ${STAGE_LABELS[report.blocked_stage] || report.blocked_stage}` })
      : null,
  ]);
  wrap.appendChild(head);
  wrap.appendChild(el("div", { class: "hint", text: (report.stage_names || []).join("  →  ") }));
  return wrap;
}

/* --------------------------------------------------------------- accounts */

let equityChart = null;
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
    return;
  }
  $("#account-detail").classList.remove("hidden");
  const data = await api(`/api/accounts/${encodeURIComponent(accountId)}`);

  if (data.exists === false) {
    $("#account-metrics").replaceChildren(card(
      "账户尚未创建",
      "等待首次运行",
      `初始资金 ${money(data.cash)} · 首次每日运行时自动创建`,
    ));
    if (equityChart) equityChart.clear();
    $("#positions-table").replaceChildren(el("div", { class: "empty", text: "账户尚未创建" }));
    $("#orders-table").replaceChildren(el("div", { class: "empty", text: "账户尚未创建" }));
    $("#events-table").replaceChildren(el("div", { class: "empty", text: "账户尚未创建" }));
    return;
  }

  const fb = data.feedback || {};
  $("#account-metrics").replaceChildren(
    card("总权益", money(fb.final_equity || data.cash), `现金 ${money(data.cash)}`),
    card("累计收益", percent(fb.overall_return), `本段收益 ${percent(fb.run_return)}`),
    card("最大回撤", percent(fb.max_drawdown), `质量 ${fb.quality || "—"}`),
    card("已实现盈亏", money(data.realized_pnl), `分红 ${money(data.dividend_income)}`),
    card("累计费用", money(data.fees_paid), `成交 ${fb.fills ?? "—"} / 订单 ${fb.orders ?? "—"}`),
  );

  renderEquityChart(data.equity_curve || []);

  $("#positions-table").replaceChildren(table(
    [
      { label: "标的" }, { label: "数量", num: true }, { label: "可卖", num: true },
      { label: "成本均价", num: true }, { label: "最新价", num: true },
      { label: "市值", num: true }, { label: "浮动盈亏", num: true },
    ],
    (data.positions || []).map((item) => [
      item.instrument_id, item.quantity, item.sellable_quantity,
      item.average_cost, item.last_price, money(item.market_value), money(item.unrealized_pnl),
    ]),
    { empty: "当前无持仓" },
  ));

  $("#orders-table").replaceChildren(table(
    [
      { label: "标的" }, { label: "方向" }, { label: "执行日" },
      { label: "数量", num: true }, { label: "剩余", num: true }, { label: "状态" },
    ],
    (data.pending_orders || []).map((item) => [
      item.instrument_id, item.side === "buy" ? "买入" : "卖出", item.execution_date,
      item.requested_quantity, item.remaining_quantity, item.status,
    ]),
    { empty: "当前无挂单" },
  ));

  $("#events-table").replaceChildren(table(
    [{ label: "日期" }, { label: "事件" }, { label: "对象" }, { label: "明细" }],
    (data.recent_events || []).slice(0, 80).map((item) => [
      item.session_date,
      EVENT_LABELS[item.event_type] || item.event_type,
      item.entity_id,
      compactPayload(item.payload),
    ]),
    { empty: "暂无事件" },
  ));
}

function compactPayload(payload) {
  if (!payload) return "—";
  const parts = [];
  for (const key of ["instrument_id", "side", "quantity", "price", "amount", "fee_total", "reason", "status"]) {
    if (payload[key] != null) parts.push(`${key}=${payload[key]}`);
  }
  const text = parts.length ? parts.join(" ") : JSON.stringify(payload);
  return text.length > 120 ? text.slice(0, 118) + "…" : text;
}

function renderEquityChart(curve) {
  const container = $("#equity-chart");
  if (!equityChart) equityChart = echarts.init(container);
  const dates = curve.map((item) => item.session_date);
  const equity = curve.map((item) => Number(item.total_equity));
  const nav = curve.map((item) => Number(item.nav));
  equityChart.setOption({
    tooltip: { trigger: "axis" },
    legend: { data: ["总权益", "净值"], top: 8 },
    grid: { left: 80, right: 60, top: 40, bottom: 60 },
    xAxis: { type: "category", data: dates },
    yAxis: [
      { type: "value", name: "总权益", scale: true, axisLabel: { formatter: (v) => v.toLocaleString() } },
      { type: "value", name: "净值", scale: true },
    ],
    dataZoom: [{ type: "inside" }, { type: "slider", bottom: 14 }],
    series: [
      { name: "总权益", type: "line", data: equity, showSymbol: false, lineStyle: { width: 2 } },
      { name: "净值", type: "line", yAxisIndex: 1, data: nav, showSymbol: false, lineStyle: { width: 1 } },
    ],
  }, true);
  equityChart.resize();
}

window.addEventListener("resize", () => equityChart && equityChart.resize());
$("#account-select").addEventListener("change", () => loadAccountDetail().catch(showError));

/* ------------------------------------------------------------------- runs */

async function loadRuns() {
  const runs = await api("/api/runs");
  $("#run-detail").classList.add("hidden");
  $("#runs-table").replaceChildren(table(
    [
      { label: "生成时间" }, { label: "目标日" }, { label: "状态" },
      { label: "阻断阶段" }, { label: "账户" },
    ],
    runs.map((item) => [
      item.generated_at, item.target_date, statusBadge(item.status),
      item.blocked_stage ? (STAGE_LABELS[item.blocked_stage] || item.blocked_stage) : "—",
      (item.accounts || []).map((account) => `${account.account_id}:${account.status}`).join("  "),
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
    (report.accounts || []).length
      ? table(
        [{ label: "账户" }, { label: "状态" }, { label: "推进", num: true }, { label: "权益", num: true }, { label: "错误" }],
        report.accounts.map((item) => [
          item.account_id, statusBadge(item.status), item.sessions_advanced,
          money(item.equity), item.error || "—",
        ]))
      : null,
  );
  detail.scrollIntoView({ behavior: "smooth" });
}

/* --------------------------------------------------------------- schedule */

async function loadSchedule() {
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
    card("下次运行", state.next_run_time || "—", ""),
    card("上次运行", state.last_run_time || "—",
      state.last_result != null ? `退出码 ${state.last_result}` : ""),
  );
  if (state.exists && state.time) $("#schedule-time").value = state.time;
  const toggle = $("#schedule-toggle");
  toggle.textContent = state.enabled ? "停用" : "启用";
  toggle.disabled = !state.exists;
  $("#schedule-delete").disabled = !state.exists;
  toggle.dataset.enabled = state.enabled ? "1" : "0";
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
  message.className = "message";
  message.textContent = "执行中…";
  try {
    const state = await action();
    renderSchedule(state);
    message.className = "message ok";
    message.textContent = okMessage;
  } catch (error) {
    message.className = "message bad";
    message.textContent = `失败：${error.message}`;
  }
}

/* ------------------------------------------------------------- manual run */

let runPolling = null;

$("#run-now").addEventListener("click", async () => {
  const status = $("#run-status");
  status.className = "message";
  status.textContent = "启动中…";
  try {
    const result = await api("/api/daily/run", {
      method: "POST",
      body: JSON.stringify({
        skip_data: $("#run-skip-data").checked,
        skip_accounts: $("#run-skip-accounts").checked,
      }),
    });
    if (!result.started && result.reason === "already_running") {
      status.textContent = "已有运行在进行中";
    }
    pollRunStatus();
  } catch (error) {
    status.className = "message bad";
    status.textContent = `启动失败：${error.message}`;
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
    status.className = "message";
    status.textContent = `运行中…（自 ${state.started_at}）`;
    $("#run-now").disabled = true;
    runPolling = setTimeout(pollRunStatus, 2000);
  } else {
    $("#run-now").disabled = false;
    if (state.last) {
      const ok = state.last.exit_code === 0;
      status.className = `message ${ok ? "ok" : "bad"}`;
      status.textContent = ok
        ? `完成（${state.last.finished_at}）`
        : `结束，退出码 ${state.last.exit_code}（详见运行记录页）`;
    } else {
      status.textContent = "";
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
    $("#decisions-table").replaceChildren(
      el("div", { class: "empty", text: "config/fundlab.yaml 中没有 agent-file 策略的账户" }));
    return;
  }
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
        ? Object.entries(item.target_weights).map(([key, value]) => `${key}=${value}`).join(" ")
        : (item.error || "—"),
      item.reason || "—",
    ]),
    { empty: "还没有决策文件" },
  ));
}

function addWeightRow(symbol = "", weight = "") {
  const row = el("div", { class: "row weight-row" }, [
    el("input", { type: "text", placeholder: "如 510300.SH", value: symbol }),
    el("input", { type: "number", placeholder: "权重 0-1", step: "0.05", min: "0", max: "1", value: weight }),
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

$("#agent-account-select").addEventListener("change", () => loadDecisions().catch(showError));

$("#decision-submit").addEventListener("click", async () => {
  const message = $("#decision-message");
  message.className = "message";
  message.textContent = "投递中…";
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
    message.className = "message ok";
    message.textContent = `已投递：${result.file}`;
    await loadDecisions();
  } catch (error) {
    message.className = "message bad";
    message.textContent = `失败：${error.message}`;
  }
});

/* ---------------------------------------------------------------- refresh */

function showError(error) {
  $("#topbar-status").textContent = `加载失败：${error.message}`;
}

async function refreshTab(name) {
  try {
    if (name === "overview") await loadOverview();
    else if (name === "accounts") { await loadAccountList(); await loadAccountDetail(); }
    else if (name === "runs") await loadRuns();
    else if (name === "schedule") { await loadSchedule(); await pollRunStatus(); }
    else if (name === "agent") await loadAgentTab();
  } catch (error) {
    showError(error);
  }
}

$("#refresh-btn").addEventListener("click", () => refreshTab(activeTab()));

refreshTab("overview");
