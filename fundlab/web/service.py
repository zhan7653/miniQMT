"""Read-mostly data assembly for the dashboard.

Everything here reads the same durable stores the CLI writes — the trading
SQLite repository, the market-data warehouse pointer, daily ops reports, and
agent decision files. The only writes are agent decision submissions, which
go through the exact validation the account run will apply later.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from fundlab.agent.evaluations import (
    AgentEvaluation,
    AgentEvaluationError,
    evaluation_root,
    list_agent_evaluations,
)
from fundlab.agent.tools import AgentMemory, AgentMemoryError
from fundlab.common.canonical import to_primitive
from fundlab.common.dates import audit_now
from fundlab.marketdata import MarketDataWarehouse, MarketTable
from fundlab.settings import DailyAccountSettings, FoundationSettings
from fundlab.strategies import AgentDecisionError, load_agent_decision, write_agent_decision
from fundlab.trading import TradingRepository, build_simulation_feedback
from fundlab.trading.repository import RunRecord
from fundlab.web.snapshot_cache import InstrumentNameResolver

_DECISION_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}\.json$")
_REPORT_NAME = re.compile(r"^daily-[A-Za-z0-9-]+\.json$")
_CHAIN_LIMIT = 20000

_STRATEGY_PRESENTATION: dict[str, tuple[str, str, str, bool]] = {
    "static": (
        "静态股债配置",
        "按固定目标权重持有并再平衡，不根据行情主动择时。",
        "固定 ETF 配置",
        False,
    ),
    "momentum-rotation": (
        "动量轮动",
        "比较风险资产的中期动量，趋势为正时持有风险资产，否则切到防守资产。",
        "固定 ETF 轮动",
        False,
    ),
    "dividend-value": (
        "红利价值（LLM 复核）",
        "先用财务与流动性规则筛选高股息股票，再由 LLM 做有边界、可审计的周度复核。",
        "沪深 A 股动态选股",
        True,
    ),
    "dividend-rules": (
        "红利价值（纯规则）",
        "按股息率、连续分红和流动性筛选股票，以等权方式低频调仓。",
        "沪深 A 股动态选股",
        False,
    ),
    "dual-momentum": (
        "双动量轮动",
        "同时使用绝对动量和相对动量，月末选择趋势更强的 ETF，弱势时转入防守资产。",
        "固定 ETF 池",
        False,
    ),
    "sector-momentum": (
        "行业动量轮动",
        "在人工确认的行业 ETF 白名单中做多周期动量筛选，并以波动率和集中度上限控制仓位。",
        "人工确认的 A 股行业 ETF 池",
        False,
    ),
    "moving-average-grid": (
        "自适应均线网格",
        "围绕冻结的 60 日均线锚点分档交易，以波动率调整格距，并用长期趋势限制下跌中的新增仓位。",
        "单只 ETF＋现金",
        False,
    ),
    "inverse-volatility": (
        "逆波动率配置",
        "月末按历史波动率的倒数分配权重，让低波动 ETF 获得更高权重。",
        "固定 ETF 池",
        False,
    ),
    "correlation-risk-parity": (
        "相关性风险平价",
        "根据波动率和资产相关性分配风险预算，控制单一资产对组合风险的贡献。",
        "固定 ETF 池",
        False,
    ),
    "trend-volatility-target": (
        "趋势过滤＋波动率目标",
        "月末先做趋势过滤，再按目标波动率控制风险仓位，未通过趋势时持有防守资产。",
        "固定 ETF 池",
        False,
    ),
    "low-beta-volatility": (
        "低贝塔低波动",
        "从 A 股中筛选相对基准贝塔较低、波动较小且流动性合格的股票，月度调仓。",
        "沪深 A 股动态选股",
        False,
    ),
    "st-removal-momentum": (
        "摘帽动量",
        "在近期摘帽股票中筛选流动性和动量较强的标的，未满足条件时持有防守资产。",
        "近期摘帽股票动态选股",
        False,
    ),
    "st-active-momentum": (
        "ST 动量",
        "在当前 ST 股票中筛选流动性和动量较强的标的，并用仓位上限控制风险。",
        "当前 ST 股票动态选股",
        False,
    ),
    "crisis-drawdown": (
        "危机回撤策略",
        "平时持有防守资产，只有深度回撤满足反转或阶梯条件时才分批建仓，恢复或止盈后退出。",
        "固定 ETF 危机观察池",
        False,
    ),
}

_CRISIS_VARIANTS: dict[str, tuple[str, str]] = {
    "paper-crash-global": ("全球宽基危机回撤", "覆盖 A 股、美股、港股、日本和德国等市场，等待全球宽基深跌后的反转机会。"),
    "paper-crash-cn-small": ("A 股危机回撤（精简池）", "只观察少量 A 股宽基，减少信号分散并保持低频。"),
    "paper-crash-cn-wide": ("A 股危机回撤（宽池）", "覆盖更多 A 股宽基与成长指数，在市场深跌后择优建仓。"),
    "paper-crash-conservative": ("危机回撤（保守反转）", "要求更充分的回撤与反转确认，并使用更克制的风险仓位。"),
    "paper-crash-aggressive": ("危机回撤（激进阶梯）", "按回撤加深程度分档买入，提高危机期间的建仓速度与仓位。"),
    "paper-crash-vol-control": ("危机回撤（波动控制）", "触发危机建仓后再按目标波动率压缩或放大风险仓位。"),
    "paper-crash-fast-profit": ("危机回撤（快速止盈）", "沿用深跌建仓条件，但采用更快的盈利退出规则。"),
    "paper-crash-semiconductor": ("中韩半导体危机回撤", "专门等待中韩半导体 ETF 出现深度回撤后的低频反转机会。"),
}


class DashboardError(ValueError):
    """User-facing request problem (bad account, invalid decision, ...)."""


@dataclass(frozen=True)
class DashboardService:
    settings: FoundationSettings
    name_resolver: InstrumentNameResolver | None = None

    # ------------------------------------------------------------- helpers

    def _repository(self) -> TradingRepository:
        return TradingRepository(self.settings.paths.trading_database)

    def _account_config(self, account_id: str) -> DailyAccountSettings | None:
        for item in self.settings.daily.accounts:
            if item.account_id == account_id:
                return item
        return None

    def _instrument_names(self, instrument_ids: set[str] | tuple[str, ...] = ()) -> dict[str, str]:
        if self.name_resolver is not None:
            return self.name_resolver.lookup(instrument_ids)
        try:
            warehouse = MarketDataWarehouse(self.settings.paths.market_data)
            snapshot_id = warehouse.current_snapshot_id()
            return dict(_instrument_names_for_snapshot(
                str(Path(self.settings.paths.market_data).resolve()),
                snapshot_id,
            ))
        except Exception:
            # A missing/unreadable market snapshot must not make account state
            # disappear from the dashboard. The UI falls back to the code.
            return {}

    def _instrument_name_state(
        self, instrument_ids: set[str],
    ) -> tuple[dict[str, str], bool]:
        if self.name_resolver is not None:
            lookup_state = getattr(self.name_resolver, "lookup_state", None)
            if callable(lookup_state):
                names, pending = lookup_state(instrument_ids)
                return dict(names), bool(pending)
        return self._instrument_names(instrument_ids), False

    def _strategy_instrument_ids(self, config: DailyAccountSettings) -> tuple[str, ...]:
        policy = self.settings.agent.policies.get(config.account_id)
        params: Mapping[str, Any] = {} if policy is None else policy.params
        return tuple(
            item["instrument_id"]
            for item in _configured_instruments(config, params, {})
        )

    def _strategy_profile(
        self,
        config: DailyAccountSettings,
        instrument_names: Mapping[str, str],
    ) -> dict[str, Any]:
        policy = self.settings.agent.policies.get(config.account_id)
        kind = config.strategy if policy is None else policy.kind
        title, description, universe, uses_llm = _STRATEGY_PRESENTATION.get(
            kind,
            (kind, "按配置中的策略规则生成目标仓位。", "配置驱动", False),
        )
        if kind == "crisis-drawdown" and config.account_id in _CRISIS_VARIANTS:
            title, description = _CRISIS_VARIANTS[config.account_id]
        params: Mapping[str, Any] = {} if policy is None else policy.params
        return {
            "strategy_kind": kind,
            "strategy_name": title,
            "strategy_description": description,
            "strategy_universe": universe,
            "strategy_uses_llm": uses_llm,
            "strategy_instruments": _configured_instruments(
                config,
                params,
                instrument_names,
            ),
        }

    def _agent_benchmark_reviews(
        self, account_id: str,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        policy = self.settings.agent.policies.get(account_id)
        if policy is None or policy.kind != "dividend-value":
            return None, []
        try:
            entries = AgentMemory(
                self.settings.agent.memory.root, account_id,
            ).entries()
        except AgentMemoryError as exc:
            return ({
                "status": "unavailable",
                "reason": f"AgentMemoryError: {exc}",
            }, [])
        latest: dict[str, Any] | None = None
        history: list[dict[str, Any]] = []
        for entry in entries:
            raw = entry.get("benchmark")
            if entry.get("event") != "review" or not isinstance(raw, Mapping) or not raw:
                continue
            benchmark = dict(raw)
            latest = benchmark | {
                "review_as_of": entry.get("as_of"),
                "benchmark_assessment": entry.get("benchmark_assessment"),
            }
            if benchmark.get("status") != "ready":
                continue
            history.append({
                "as_of": entry.get("as_of"),
                "comparison_end": benchmark.get("comparison_end"),
                "portfolio_nav": benchmark.get("portfolio_nav"),
                "benchmark_nav": benchmark.get("benchmark_nav_on_portfolio_scale"),
                "portfolio_return": benchmark.get("portfolio_return"),
                "benchmark_total_return": benchmark.get("benchmark_total_return"),
                "excess_return": benchmark.get("excess_return"),
                "common_trading_sessions": benchmark.get("common_trading_sessions"),
                "actionability": benchmark.get("actionability"),
            })
        return latest, history

    def _run_chain(self, repository: TradingRepository, head_run_id: str) -> list[RunRecord]:
        """Promoted history, oldest first."""
        chain: list[RunRecord] = []
        cursor: str | None = head_run_id
        while cursor is not None and len(chain) < _CHAIN_LIMIT:
            record = repository.run(cursor)
            chain.append(record)
            cursor = record.binding.parent_run_id
        chain.reverse()
        return chain

    # ------------------------------------------------------------ snapshot

    def market_summary(self) -> dict[str, Any]:
        if self.name_resolver is not None:
            return self.name_resolver.market_summary()
        try:
            warehouse = MarketDataWarehouse(self.settings.paths.market_data)
            snapshot_id = warehouse.current_snapshot_id()
            return dict(_market_summary_for_snapshot(
                str(Path(self.settings.paths.market_data).resolve()),
                snapshot_id,
            ))
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    # ------------------------------------------------------------ accounts

    def accounts(self) -> list[dict[str, Any]]:
        repository = self._repository()
        instrument_ids = {
            instrument_id
            for config in self.settings.daily.accounts
            for instrument_id in self._strategy_instrument_ids(config)
        }
        instrument_names = self._instrument_names(instrument_ids)
        found = []
        for config in self.settings.daily.accounts:
            entry: dict[str, Any] = {
                "account_id": config.account_id,
                "name": config.name,
                "strategy": config.strategy,
                "initial_cash": str(config.initial_cash),
                "weights": {key: str(value) for key, value in config.weights.items()},
                "exists": False,
                **self._strategy_profile(config, instrument_names),
            }
            try:
                account = repository.account(config.account_id)
            except KeyError:
                found.append(entry)
                continue
            entry["exists"] = True
            entry["status"] = account.status.value
            state, head_run_id = repository.selected_state(config.account_id)
            entry["head_run_id"] = head_run_id
            if head_run_id is not None:
                record = repository.run(head_run_id)
                entry["head_date"] = record.binding.end_date.isoformat()
                checkpoints = repository.checkpoints(head_run_id)
                if checkpoints:
                    latest = checkpoints[-1]
                    entry["equity"] = latest["total_equity"]
                    entry["nav"] = latest["nav"]
            entry["cash"] = str(state.cash)
            entry["positions"] = len({lot.instrument_id for lot in state.lots})
            entry["pending_orders"] = len(state.pending_orders)
            found.append(entry)
        return found

    def account_detail(self, account_id: str, *, events_limit: int = 200) -> dict[str, Any]:
        repository = self._repository()
        config = self._account_config(account_id)
        try:
            account = repository.account(account_id)
        except KeyError as exc:
            if config is not None:
                # Configured but not yet created: first daily run creates it.
                instrument_names = self._instrument_names(
                    self._strategy_instrument_ids(config),
                )
                return {
                    "account_id": account_id,
                    "name": config.name,
                    "exists": False,
                    "strategy": config.strategy,
                    "weights": {key: str(value) for key, value in config.weights.items()},
                    **self._strategy_profile(config, instrument_names),
                    "cash": str(config.initial_cash),
                    "equity_curve": [],
                    "positions": [],
                    "pending_orders": [],
                    "recent_events": [],
                    "feedback": None,
                    "runs": [],
                    "benchmark": None,
                    "benchmark_history": [],
                    "strategy_config_changes": [],
                }
            raise DashboardError(f"账户不存在: {account_id}") from exc
        state, head_run_id = repository.selected_state(account_id)

        equity_curve: list[dict[str, Any]] = []
        recent_events: list[dict[str, Any]] = []
        feedback: dict[str, Any] | None = None
        runs_summary: list[dict[str, Any]] = []
        if head_run_id is not None:
            chain = self._run_chain(repository, head_run_id)
            initial = account.initial_state
            equity_curve.append({
                "session_date": chain[0].binding.start_date.isoformat(),
                "total_equity": str(initial.cash),
                "nav": "1",
                "cash": str(initial.cash),
                "market_value": "0",
                "kind": "initial",
            })
            for record in chain:
                for checkpoint in repository.checkpoints(record.run_id):
                    equity_curve.append({
                        "session_date": checkpoint["session_date"],
                        "total_equity": checkpoint["total_equity"],
                        "nav": checkpoint["nav"],
                        "cash": checkpoint["cash"],
                        "market_value": checkpoint["market_value"],
                        "run_id": record.run_id,
                    })
            for record in reversed(chain):
                if len(recent_events) >= events_limit:
                    break
                events = repository.events(record.run_id)
                for event in reversed(events):
                    payload = event["payload"]
                    if not _account_event_visible(event["event_type"], payload):
                        continue
                    instrument_id = (
                        payload.get("instrument_id")
                        if isinstance(payload, Mapping)
                        else None
                    )
                    recent_events.append({
                        "run_id": record.run_id,
                        "session_date": event["session_date"],
                        "event_type": event["event_type"],
                        "entity_type": event["entity_type"],
                        "entity_id": event["entity_id"],
                        "payload": payload,
                        "instrument_name": None,
                    })
                    if len(recent_events) >= events_limit:
                        break
            head = chain[-1]
            fb = build_simulation_feedback(repository, head.run_id)
            feedback = {
                "run_id": fb.run_id,
                "quality": fb.quality,
                "incomplete_reasons": list(fb.incomplete_reasons),
                "sessions": fb.sessions,
                "starting_equity": str(fb.starting_equity),
                "final_equity": str(fb.final_equity),
                "run_return": str(fb.run_return),
                "overall_return": str(fb.overall_return),
                "max_drawdown": str(fb.max_drawdown),
                "orders": fb.orders,
                "fills": fb.fills,
                "fill_ratio": str(fb.fill_ratio),
                "rejected_orders": fb.rejected_orders,
                "expired_orders": fb.expired_orders,
                "turnover_amount": str(fb.turnover_amount),
                "fees": str(fb.fees),
                "slippage_amount": str(fb.slippage_amount),
                "realized_pnl": str(fb.realized_pnl),
                "dividend_income": str(fb.dividend_income),
            }
            for record in chain[-30:]:
                runs_summary.append({
                    "run_id": record.run_id,
                    "mode": record.binding.mode.value,
                    "start_date": record.binding.start_date.isoformat(),
                    "end_date": record.binding.end_date.isoformat(),
                    "status": record.status.value,
                    "strategy_id": record.binding.strategy_id,
                    "snapshot_id": record.binding.snapshot_id,
                })

        head_date = None if head_run_id is None else repository.run(head_run_id).binding.end_date
        positions: dict[str, dict[str, Any]] = {}
        for lot in state.lots:
            item = positions.setdefault(lot.instrument_id, {
                "instrument_id": lot.instrument_id,
                "quantity": 0,
                "cost_amount": Decimal("0"),
                "sellable_quantity": 0,
            })
            item["quantity"] += lot.quantity
            item["cost_amount"] += lot.cost_amount
            if head_date is None or lot.sellable_on <= head_date:
                item["sellable_quantity"] += lot.quantity
        position_rows = []
        for item in sorted(positions.values(), key=lambda value: value["instrument_id"]):
            quantity = item["quantity"]
            last_price = state.last_prices.get(item["instrument_id"])
            market_value = None if last_price is None else last_price * quantity
            cost = item["cost_amount"]
            position_rows.append({
                "instrument_id": item["instrument_id"],
                "instrument_name": None,
                "quantity": quantity,
                "sellable_quantity": item["sellable_quantity"],
                "cost_amount": str(cost),
                "average_cost": str((cost / quantity).quantize(Decimal("0.0001"))) if quantity else None,
                "last_price": None if last_price is None else str(last_price),
                "market_value": None if market_value is None else str(market_value),
                "unrealized_pnl": None if market_value is None else str(market_value - cost),
            })

        pending_orders = [{
            "order_id": order.order_id,
            "instrument_id": order.instrument_id,
            "instrument_name": None,
            "side": order.side.value,
            "created_on": order.created_on.isoformat(),
            "execution_date": order.execution_date.isoformat(),
            "expiry_date": order.expiry_date.isoformat(),
            "requested_quantity": order.requested_quantity,
            "remaining_quantity": order.remaining_quantity,
            "status": order.status.value,
        } for order in state.pending_orders]

        instrument_ids = set() if config is None else set(
            self._strategy_instrument_ids(config)
        )
        instrument_ids.update(positions)
        instrument_ids.update(order.instrument_id for order in state.pending_orders)
        instrument_ids.update(
            str(item["payload"].get("instrument_id"))
            for item in recent_events
            if isinstance(item.get("payload"), Mapping)
            and item["payload"].get("instrument_id") is not None
        )
        instrument_names, instrument_names_pending = self._instrument_name_state(instrument_ids)
        for item in position_rows:
            item["instrument_name"] = instrument_names.get(item["instrument_id"])
        for item in pending_orders:
            item["instrument_name"] = instrument_names.get(item["instrument_id"])
        for item in recent_events:
            payload = item.get("payload")
            instrument_id = (
                payload.get("instrument_id") if isinstance(payload, Mapping) else None
            )
            item["instrument_name"] = (
                instrument_names.get(str(instrument_id))
                if instrument_id is not None else None
            )

        benchmark, benchmark_history = self._agent_benchmark_reviews(account_id)
        strategy_config_changes: list[dict[str, Any]] = []
        policy = self.settings.agent.policies.get(account_id)
        if policy is not None and policy.kind == "crisis-drawdown":
            try:
                strategy_config_changes = _configuration_changes(
                    list_agent_evaluations(
                        evaluation_root(self.settings.daily.agent_decision_root),
                        account_id,
                    )
                )
            except AgentEvaluationError:
                # The dedicated monitor reports the evidence-store error.  The
                # ordinary account page must still expose canonical holdings.
                strategy_config_changes = []
        return {
            "account_id": account_id,
            "name": account.name,
            "exists": True,
            "status": account.status.value,
            "strategy": None if config is None else config.strategy,
            **({} if config is None else self._strategy_profile(config, instrument_names)),
            "weights": {} if config is None else {
                key: str(value) for key, value in config.weights.items()
            },
            "head_run_id": head_run_id,
            "cash": str(state.cash),
            "realized_pnl": str(state.realized_pnl),
            "fees_paid": str(state.fees_paid),
            "dividend_income": str(state.dividend_income),
            "incomplete_reasons": list(state.incomplete_reasons),
            "equity_curve": equity_curve,
            "positions": position_rows,
            "pending_orders": pending_orders,
            "recent_events": recent_events,
            "feedback": feedback,
            "runs": runs_summary,
            "benchmark": benchmark,
            "benchmark_history": benchmark_history,
            "strategy_config_changes": strategy_config_changes,
            "instrument_names_pending": instrument_names_pending,
        }

    # -------------------------------------------------------- daily reports

    def daily_reports(self, *, limit: int = 60) -> list[dict[str, Any]]:
        root = Path(self.settings.daily.report_root)
        if not root.is_dir():
            return []
        found_files: list[tuple[str, int, int]] = []
        for path in root.iterdir():
            if not path.is_file() or not _REPORT_NAME.match(path.name):
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            found_files.append((path.name, stat.st_mtime_ns, stat.st_size))
        files = tuple(sorted(found_files, key=lambda item: item[0]))
        entries = _daily_report_summaries(str(root.resolve()), files)
        return [dict(item) for item in entries[:limit]]

    def daily_report(self, file_name: str) -> dict[str, Any]:
        if (
            not _REPORT_NAME.match(file_name)
            or "/" in file_name or "\\" in file_name or ".." in file_name
        ):
            raise DashboardError(f"非法报告文件名: {file_name}")
        path = Path(self.settings.daily.report_root) / file_name
        try:
            exists = path.is_file()
        except OSError as exc:
            raise DashboardError(f"报告无法读取: {file_name}") from exc
        if not exists:
            raise DashboardError(f"报告不存在: {file_name}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DashboardError(f"报告损坏或无法读取: {file_name}") from exc
        if not isinstance(payload, Mapping):
            raise DashboardError(f"报告内容不是对象: {file_name}")
        return dict(payload)

    # ------------------------------------------------------ agent decisions

    def agent_accounts(self) -> list[dict[str, Any]]:
        repository = self._repository()
        agent_configs = tuple(
            config for config in self.settings.daily.accounts
            if config.strategy == "agent-file"
        )
        instrument_names = self._instrument_names({
            instrument_id
            for config in agent_configs
            for instrument_id in self._strategy_instrument_ids(config)
        })
        found = []
        for config in agent_configs:
            entry: dict[str, Any] = {
                "account_id": config.account_id,
                "name": config.name,
                "head_date": None,
                **self._strategy_profile(config, instrument_names),
            }
            try:
                repository.account(config.account_id)
                _, head_run_id = repository.selected_state(config.account_id)
                if head_run_id is not None:
                    entry["head_date"] = repository.run(head_run_id).binding.end_date.isoformat()
            except KeyError:
                pass
            found.append(entry)
        return found

    def agent_decisions(self, account_id: str) -> list[dict[str, Any]]:
        config = self._account_config(account_id)
        if config is None or config.strategy != "agent-file":
            raise DashboardError(f"不是 agent-file 账户: {account_id}")
        root = Path(self.settings.daily.agent_decision_root) / account_id
        if not root.is_dir():
            return []
        entries = []
        for path in sorted(root.iterdir(), reverse=True):
            if not path.is_file() or not _DECISION_NAME.match(path.name):
                continue
            try:
                decision_date = date.fromisoformat(path.name[:-5])
            except ValueError:
                entries.append({
                    "decision_date": path.name[:-5],
                    "file": path.name,
                    "valid": False,
                    "error": "文件名不是有效日期",
                })
                continue
            entry: dict[str, Any] = {
                "decision_date": decision_date.isoformat(),
                "file": path.name,
            }
            try:
                decision = load_agent_decision(
                    self.settings.daily.agent_decision_root, account_id, decision_date,
                )
                assert decision is not None
                entry["valid"] = True
                entry["target_weights"] = {
                    key: str(value) for key, value in decision.target_weights.items()
                }
                entry["reason"] = decision.reason
                entry["agent_id"] = decision.agent_id
            except AgentDecisionError as exc:
                entry["valid"] = False
                entry["error"] = str(exc)
            entries.append(entry)
        return entries

    def research_reports(self, limit: int = 50) -> list[dict[str, Any]]:
        root = Path(self.settings.paths.report_root) / "agent-research"
        if not root.is_dir():
            return []
        found: list[dict[str, Any]] = []
        for path in root.glob("*/*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, Mapping):
                continue
            report = payload.get("research_report")
            if not isinstance(report, Mapping):
                try:
                    report = json.loads(payload.get('final_text', ''))
                except (TypeError, json.JSONDecodeError):
                    report = None
            thesis = report.get("thesis") if isinstance(report, Mapping) else payload.get("final_text", "")
            if not isinstance(thesis, str) or not thesis.strip():
                continue
            # Early autonomous smoke runs had no profile/question and were
            # diagnostic probes rather than user-facing research. Keep them
            # in their files for audit, but do not present them as live cards.
            legacy = not bool(payload.get('question') or payload.get('profile'))
            if legacy and (
                not isinstance(report, Mapping)
                or not (report.get('evidence') or report.get('counter_evidence'))
            ):
                continue
            found.append({
                "as_of": payload.get("as_of") if isinstance(payload, Mapping) else None,
                "snapshot_id": payload.get("snapshot_id") if isinstance(payload, Mapping) else None,
                "model": payload.get("model") if isinstance(payload, Mapping) else None,
                "profile": payload.get("profile") if isinstance(payload, Mapping) else None,
                "subject": _research_subject(payload),
                "created_at": payload.get('created_at') or path.stat().st_mtime,
                "legacy": False,
                "recommendation": report.get("recommendation") if isinstance(report, Mapping) else None,
                "thesis": thesis,
                "path": str(path),
                "file_name": path.name,
                "directory": path.parent.name,
            })
        # Daily insight is immutable and idempotent by (as_of, profile,
        # thesis). Keep one card even when older runs left duplicate files.
        unique: dict[tuple[str, str, str], dict[str, Any]] = {}
        for item in sorted(found, key=lambda value: (str(value['as_of']), str(value['created_at'])), reverse=True):
            key = (str(item.get("as_of")), str(item.get("profile") or "legacy"), str(item.get("subject") or item.get("thesis")))
            if key not in unique:
                unique[key] = {**item, 'versions': []}
            unique[key]['versions'].append({k: item[k] for k in ('directory', 'file_name', 'created_at')})
        return sorted(unique.values(), key=lambda item: (str(item.get("as_of")), str(item.get("created_at"))), reverse=True)[:max(1, min(limit, 500))]

    def research_report(self, as_of: str, file_name: str) -> dict[str, Any]:
        try:
            if date.fromisoformat(as_of).isoformat() != as_of:
                raise ValueError
        except ValueError as exc:
            raise DashboardError("非法研究报告日期") from exc
        if Path(file_name).name != file_name or not re.fullmatch(r"[A-Za-z0-9]+\.json", file_name):
            raise DashboardError("非法研究报告文件名")
        root = (Path(self.settings.paths.report_root) / "agent-research").resolve()
        path = root / as_of / file_name
        if not path.resolve().is_relative_to(root) or not path.is_file():
            raise DashboardError("研究报告不存在")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DashboardError("研究报告损坏或无法读取") from exc
        if not isinstance(payload, Mapping):
            raise DashboardError("研究报告内容不是对象")
        return dict(payload)

    def opinion_snapshots(self, limit: int = 50) -> list[dict[str, Any]]:
        from fundlab.agent.opinion import OpinionRepository
        return OpinionRepository(self.settings.agent.opinion.root).list_snapshots(
            limit=max(1, min(limit, 500)),
        )

    def opinion_snapshot(self, as_of: str, snapshot_id: str | None = None) -> dict[str, Any]:
        from fundlab.agent.opinion import OpinionRepository
        try:
            parsed = date.fromisoformat(as_of)
        except ValueError as exc:
            raise DashboardError("非法舆论快照日期") from exc
        if parsed.isoformat() != as_of:
            raise DashboardError("非法舆论快照日期")
        value = OpinionRepository(self.settings.agent.opinion.root).load(
            as_of=as_of, snapshot_id=snapshot_id,
        )
        if value is None:
            raise DashboardError("舆论快照不存在")
        return value

    def opinion_detail(self, as_of: str, detail_ref: str, snapshot_id: str | None = None) -> dict[str, Any]:
        from fundlab.agent.opinion import OpinionRepository
        if not re.fullmatch(r"[0-9a-f]{32}", detail_ref):
            raise DashboardError("非法舆论详情引用")
        value = OpinionRepository(self.settings.agent.opinion.root).detail(
            as_of=as_of, detail_ref=detail_ref, snapshot_id=snapshot_id,
        )
        if value is None:
            raise DashboardError("舆论详情不存在")
        return value

    # ---------------------------------------------------- crisis monitoring

    def crisis_monitor(self) -> dict[str, Any]:
        """Aggregate the eight homogeneous crisis policies without recomputing signals."""

        market = self.market_summary()
        found: list[dict[str, Any]] = []
        for config in self.settings.daily.accounts:
            policy = self.settings.agent.policies.get(config.account_id)
            if policy is None or policy.kind != "crisis-drawdown":
                continue
            found.append(self._crisis_monitor_account(config, policy, market))
        return {
            "generated_at": audit_now().isoformat(),
            "market": market,
            "accounts": found,
            "summary": {
                "configured": len(found),
                "current": sum(item["evaluation_status"] == "current" for item in found),
                "errors": sum(item["evaluation_status"] == "error" for item in found),
                "triggered": sum(
                    (item.get("evaluation") or {}).get("action")
                    in {"enter", "add_tranche", "exit"}
                    for item in found
                ),
                "manual": sum(bool(item.get("manual_intervention")) for item in found),
            },
        }

    def crisis_evaluations(
        self, account_id: str, *, limit: int = 90,
    ) -> dict[str, Any]:
        config = self._account_config(account_id)
        policy = self.settings.agent.policies.get(account_id)
        if config is None or policy is None or policy.kind != "crisis-drawdown":
            raise DashboardError(f"不是危机策略账户: {account_id}")
        market = self.market_summary()
        try:
            records = list_agent_evaluations(
                evaluation_root(self.settings.daily.agent_decision_root),
                account_id,
                limit_dates=max(1, min(limit, 2000)),
            )
        except AgentEvaluationError as exc:
            raise DashboardError(f"评估证据损坏: {exc}") from exc
        latest_by_date = _latest_by_evidence_date(records)
        latest_ids = {item.revision_id for item in latest_by_date}
        current_snapshot = market.get("snapshot_id")
        current_end = market.get("published_end")
        return {
            "account_id": account_id,
            "name": config.name,
            "market": market,
            "configuration_changes": _configuration_changes(records),
            "records": [
                _evaluation_view(
                    item,
                    is_latest_for_date=item.revision_id in latest_ids,
                    is_current=(
                        item.status == "ready"
                        and item.as_of is not None
                        and item.as_of.isoformat() == current_end
                        and item.snapshot_id == current_snapshot
                        and item.revision_id == (
                            latest_by_date[0].revision_id if latest_by_date else None
                        )
                    ),
                )
                for item in records
            ],
        }

    def _crisis_monitor_account(self, config, policy, market) -> dict[str, Any]:
        root = evaluation_root(self.settings.daily.agent_decision_root)
        evidence_error = None
        try:
            records = list_agent_evaluations(root, config.account_id)
        except AgentEvaluationError as exc:
            records = ()
            evidence_error = str(exc)
        latest = records[0] if records else None
        last_ready = next((item for item in records if item.status == "ready"), None)
        current_end = market.get("published_end")
        current_snapshot = market.get("snapshot_id")
        if evidence_error is not None:
            evaluation_status = "error"
            stale_reason = f"评估证据损坏: {evidence_error}"
        elif latest is None:
            evaluation_status = "missing"
            stale_reason = "尚无正式评估记录"
        elif latest.status == "error":
            evaluation_status = "error"
            stale_reason = latest.error or "最近一次正式评估失败"
        elif (
            latest.as_of is not None
            and latest.as_of.isoformat() == current_end
            and latest.snapshot_id == current_snapshot
        ):
            evaluation_status = "current"
            stale_reason = None
        else:
            evaluation_status = "stale"
            stale_reason = "最近成功评估未绑定当前 canonical 快照"

        detail = self.account_detail(config.account_id)
        head_date = None
        if detail.get("head_run_id"):
            runs = detail.get("runs") or []
            head_date = runs[-1]["end_date"] if runs else None
        decision = self._crisis_decision_status(
            config.account_id,
            last_ready,
            head_date=head_date,
        )
        manual = self._manual_intervention(config.account_id)
        performance = _crisis_performance(detail, records)
        positions = detail.get("positions") or []
        pending_orders = detail.get("pending_orders") or []
        total_equity = (detail.get("feedback") or {}).get("final_equity")
        signal_ids = set()
        if last_ready is not None:
            signals = last_ready.audit.get("signals")
            if isinstance(signals, Mapping):
                signal_ids.update(map(str, signals))
        signal_ids.update(self._strategy_instrument_ids(config))
        instrument_names = self._instrument_names(signal_ids)
        return {
            "account_id": config.account_id,
            "name": config.name,
            **self._strategy_profile(config, instrument_names),
            "parameters": to_primitive(policy.params),
            "evaluation_status": evaluation_status,
            "stale_reason": stale_reason,
            "evidence_error": evidence_error,
            "evaluation": None if latest is None else _evaluation_view(latest),
            "last_success": (
                None
                if last_ready is None or last_ready is latest
                else _evaluation_view(last_ready)
            ),
            "nearest_signal": (
                None
                if last_ready is None
                else _nearest_signal(last_ready, instrument_names)
            ),
            "decision": decision,
            "execution": {
                "head_date": head_date,
                "cash": detail.get("cash"),
                "total_equity": total_equity,
                "positions": positions,
                "pending_orders": pending_orders,
            },
            "performance": performance,
            "manual_intervention": manual,
        }

    def _crisis_decision_status(
        self,
        account_id: str,
        evaluation: AgentEvaluation | None,
        *,
        head_date: str | None,
    ) -> dict[str, Any]:
        if evaluation is None or evaluation.decision_date is None:
            return {"status": "none"}
        decision_date = evaluation.decision_date
        try:
            decision = load_agent_decision(
                self.settings.daily.agent_decision_root,
                account_id,
                decision_date,
            )
        except AgentDecisionError as exc:
            return {
                "status": "invalid",
                "decision_date": decision_date.isoformat(),
                "error": str(exc),
            }
        if decision is None:
            return {
                "status": "hold" if evaluation.hold else "missing",
                "decision_date": decision_date.isoformat(),
            }
        consumed = head_date is not None and decision_date <= date.fromisoformat(head_date)
        return {
            "status": "consumed" if consumed else "queued",
            "decision_date": decision_date.isoformat(),
            "agent_id": decision.agent_id,
            "target_weights": {
                key: str(value) for key, value in decision.target_weights.items()
            },
            "reason": decision.reason,
            "content_hash": decision.content_hash,
        }

    def _manual_intervention(self, account_id: str) -> dict[str, Any] | None:
        decisions = self.agent_decisions(account_id)
        manual = [
            item for item in decisions
            if item.get("valid")
            and not str(item.get("agent_id") or "").startswith("crisis-drawdown-v")
        ]
        if not manual:
            return None
        first = min(manual, key=lambda item: item["decision_date"])
        return {
            "since": first["decision_date"],
            "agent_id": first.get("agent_id"),
            "detail": "该账户收益路径包含人工或非危机策略决策",
        }

    def submit_agent_decision(
        self,
        account_id: str,
        *,
        decision_date: str,
        target_weights: Mapping[str, Any],
        reason: str,
        agent_id: str = "dashboard",
        overwrite: bool = False,
    ) -> dict[str, Any]:
        config = self._account_config(account_id)
        if config is None or config.strategy != "agent-file":
            raise DashboardError(f"不是 agent-file 账户: {account_id}")
        try:
            parsed_date = date.fromisoformat(str(decision_date))
        except ValueError as exc:
            raise DashboardError(f"决策日期格式错误: {decision_date}") from exc
        if not isinstance(target_weights, Mapping) or not target_weights:
            raise DashboardError("目标权重不能为空")
        weights: dict[str, str] = {}
        total = Decimal("0")
        for symbol, value in target_weights.items():
            symbol = str(symbol).strip()
            if not symbol:
                raise DashboardError("存在空的标的代码")
            try:
                weight = Decimal(str(value))
            except InvalidOperation as exc:
                raise DashboardError(f"权重不是数字: {symbol}={value}") from exc
            if not weight.is_finite():
                raise DashboardError(f"权重必须是有限数字: {symbol}={value}")
            if weight < 0:
                raise DashboardError(f"权重不能为负: {symbol}")
            total += weight
            weights[symbol] = str(weight)
        if total > Decimal("1"):
            raise DashboardError(f"权重合计超过 1: {total}")
        if not str(reason).strip():
            raise DashboardError("必须填写决策理由")

        repository = self._repository()
        try:
            repository.account(account_id)
            _, head_run_id = repository.selected_state(account_id)
            if head_run_id is not None:
                head_date = repository.run(head_run_id).binding.end_date
                if parsed_date <= head_date:
                    raise DashboardError(
                        f"账户已推进到 {head_date.isoformat()}，不能再为 {parsed_date.isoformat()} 投递决策"
                    )
        except KeyError:
            pass

        path = (
            Path(self.settings.daily.agent_decision_root)
            / account_id / f"{parsed_date.isoformat()}.json"
        )
        if path.exists():
            try:
                load_agent_decision(
                    self.settings.daily.agent_decision_root, account_id, parsed_date,
                )
            except AgentDecisionError as exc:
                raise DashboardError(f"已有决策文件损坏，请先检查处理: {exc}") from exc
            if not overwrite:
                raise DashboardError(f"该日期已有决策文件，勾选覆盖后重试: {path.name}")
        try:
            decision = write_agent_decision(
                self.settings.daily.agent_decision_root,
                account_id=account_id,
                decision_date=parsed_date,
                target_weights=weights,
                reason=reason,
                agent_id=str(agent_id).strip() or "dashboard",
                overwrite=overwrite,
            )
        except AgentDecisionError as exc:
            raise DashboardError(f"决策文件校验失败: {exc}") from exc
        return {
            "account_id": account_id,
            "decision_date": parsed_date.isoformat(),
            "file": path.name,
            "content_hash": decision.content_hash,
        }

    def run_agent_decision(
        self,
        account_id: str,
        *,
        overwrite: bool = False,
        dry_run: bool = False,
        force_review: bool = False,
    ) -> dict[str, Any]:
        """Run the configured deterministic policy for one agent-file account.

        Same mechanism the scheduled task uses (`fundlab agent decide`); the
        dashboard only offers a button for it.
        """
        from fundlab.agent import AgentDecisionService, AgentPolicyError, AgentServiceError

        try:
            return AgentDecisionService(self.settings).decide(
                account_id,
                overwrite=overwrite,
                dry_run=dry_run,
                force_review=force_review,
            )
        except (AgentServiceError, AgentPolicyError, AgentDecisionError) as exc:
            raise DashboardError(str(exc)) from exc


def _evaluation_view(
    evaluation: AgentEvaluation,
    *,
    is_latest_for_date: bool | None = None,
    is_current: bool | None = None,
) -> dict[str, Any]:
    payload = evaluation.to_dict()
    payload.pop("source_path", None)
    if is_latest_for_date is not None:
        payload["is_latest_for_date"] = is_latest_for_date
    if is_current is not None:
        payload["is_current"] = is_current
    return payload


def _latest_by_evidence_date(
    records: tuple[AgentEvaluation, ...],
) -> tuple[AgentEvaluation, ...]:
    found: dict[date, AgentEvaluation] = {}
    for item in records:
        found.setdefault(item.evidence_date, item)
    return tuple(found[key] for key in sorted(found, reverse=True))


def _configuration_changes(
    records: tuple[AgentEvaluation, ...],
) -> list[dict[str, Any]]:
    ready = [
        item for item in reversed(_latest_by_evidence_date(records))
        if item.status == "ready" and item.as_of is not None
    ]
    changes: list[dict[str, Any]] = []
    previous = None
    for item in ready:
        if item.config_hash == previous:
            continue
        changes.append({
            "as_of": item.as_of.isoformat(),
            "decision_date": (
                None if item.decision_date is None else item.decision_date.isoformat()
            ),
            "config_hash": item.config_hash,
            "revision_id": item.revision_id,
            "initial": previous is None,
        })
        previous = item.config_hash
    return changes


def _crisis_performance(
    detail: Mapping[str, Any],
    records: tuple[AgentEvaluation, ...],
) -> dict[str, Any]:
    feedback = detail.get("feedback") or {}
    curve = detail.get("equity_curve") or []
    actual_return = feedback.get("overall_return")
    if actual_return is None and curve:
        actual_return = str(Decimal(str(curve[-1]["nav"])) - Decimal("1"))

    ready = [
        item for item in reversed(_latest_by_evidence_date(records))
        if item.status == "ready" and item.as_of is not None
    ]
    version_start = None
    version_return = None
    config_hash = None
    if ready:
        config_hash = ready[-1].config_hash
        segment = [ready[-1]]
        for item in reversed(ready[:-1]):
            if item.config_hash != config_hash:
                break
            segment.append(item)
        version_start = min(item.as_of for item in segment if item.as_of is not None)
        if curve:
            base = [
                item for item in curve
                if str(item.get("session_date") or "") <= version_start.isoformat()
            ]
            if base:
                base_nav = Decimal(str(base[-1]["nav"]))
                current_nav = Decimal(str(curve[-1]["nav"]))
                if base_nav > 0:
                    version_return = str(current_nav / base_nav - Decimal("1"))
    return {
        "actual_return": actual_return,
        "max_drawdown": feedback.get("max_drawdown"),
        "current_config_hash": config_hash,
        "current_config_start": None if version_start is None else version_start.isoformat(),
        "current_config_return": version_return,
    }


def _nearest_signal(
    evaluation: AgentEvaluation,
    instrument_names: Mapping[str, str] | None = None,
) -> dict[str, Any] | None:
    signals = evaluation.audit.get("signals")
    if not isinstance(signals, Mapping) or not signals:
        return None
    params = evaluation.parameters
    mode = str(params.get("entry_mode") or "reversal")
    minimum = _decimal_or_none(params.get("minimum_drawdown"))
    rebound_needed = _decimal_or_none(params.get("rebound_threshold"))
    ranked: list[tuple[Decimal, str, Mapping[str, Any]]] = []
    for instrument_id, raw in signals.items():
        if not isinstance(raw, Mapping):
            continue
        drawdown_key = "current_drawdown" if mode == "ladder" else "event_drawdown"
        drawdown = _decimal_or_none(raw.get(drawdown_key))
        rebound = _decimal_or_none(raw.get("rebound_from_low"))
        if drawdown is None or minimum in {None, Decimal("0")}:
            continue
        drawdown_progress = abs(drawdown) / minimum
        if mode == "ladder" or rebound_needed in {None, Decimal("0")}:
            progress = drawdown_progress
        else:
            rebound_progress = Decimal("0") if rebound is None else rebound / rebound_needed
            average_progress = (
                Decimal("1") if raw.get("above_confirmation_average") is True else Decimal("0")
            )
            progress = min(drawdown_progress, rebound_progress, average_progress)
        ranked.append((progress, str(instrument_id), raw))
    if not ranked:
        return None
    progress, instrument_id, raw = max(ranked, key=lambda item: (item[0], item[1]))
    return {
        "instrument_id": instrument_id,
        "instrument_name": (instrument_names or {}).get(instrument_id),
        "trigger_progress": str(progress),
        "minimum_drawdown": None if minimum is None else str(minimum),
        "rebound_threshold": None if rebound_needed is None else str(rebound_needed),
        **{str(key): to_primitive(value) for key, value in raw.items()},
    }


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _account_event_visible(event_type: str, payload: Any) -> bool:
    """Hide market-wide corporate-action notices with no account impact."""
    if event_type == "corporate_action_ex_date":
        # Economic account effects have their own explicit events (cash paid,
        # shares listed, split applied).  A bare market ex-date is not an
        # account event and previously flooded every account's recent history.
        return False
    if not isinstance(payload, Mapping):
        return True
    quantity_key = {
        "corporate_action_entitlement": "quantity",
        "cash_dividend_paid": "entitled_quantity",
        "rights_issue_declined": "entitled_quantity",
        "share_distribution_listed": "entitled_quantity",
        "share_cost_basis_adjusted": "entitled_quantity",
        "split_applied": "pre_event_quantity",
    }.get(event_type)
    if quantity_key is None or payload.get(quantity_key) is None:
        return True
    quantity = _decimal_or_none(payload.get(quantity_key))
    return quantity is None or quantity > 0


def _configured_instruments(
    config: DailyAccountSettings,
    params: Mapping[str, Any],
    instrument_names: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Return the fixed part of a strategy universe with human-readable roles."""

    roles: dict[str, list[str]] = {}

    def add(raw: Any, role: str) -> None:
        if not isinstance(raw, str) or not raw.strip():
            return
        instrument_id = raw.strip()
        found = roles.setdefault(instrument_id, [])
        if role not in found:
            found.append(role)

    if config.strategy == "static":
        for instrument_id in config.weights:
            add(instrument_id, "目标配置")
    for instrument_id in params.get("risk_instruments") or ():
        add(instrument_id, "风险池")
    sector_mapping = params.get("sector_mapping") or {}
    if isinstance(sector_mapping, Mapping):
        for sector, instrument_id in sector_mapping.items():
            add(instrument_id, f"行业池：{sector}")
    for instrument_id in params.get("instruments") or ():
        add(instrument_id, "配置池")
    add(params.get("risk_instrument"), "风险资产")
    add(params.get("instrument"), "网格标的")
    add(params.get("defensive_instrument"), "防守资产")
    add(params.get("benchmark_instrument"), "业绩基准")

    return [
        {
            "instrument_id": instrument_id,
            "instrument_name": instrument_names.get(instrument_id),
            "role": "／".join(item_roles),
        }
        for instrument_id, item_roles in roles.items()
    ]


@lru_cache(maxsize=8)
def _daily_report_summaries(
    root: str,
    files: tuple[tuple[str, int, int], ...],
) -> tuple[dict[str, Any], ...]:
    """Cache report summaries until a report file identity changes."""

    entries: list[dict[str, Any]] = []
    directory = Path(root)
    for file_name, _, _ in files:
        try:
            payload = json.loads((directory / file_name).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        stages = payload.get("stages") or []
        if not isinstance(stages, list):
            stages = []
        stage_items = [item for item in stages if isinstance(item, Mapping)]
        accounts = payload.get("accounts") or []
        if not isinstance(accounts, list):
            accounts = []
        account_items = [item for item in accounts if isinstance(item, Mapping)]
        entries.append({
            "file": file_name,
            "generated_at": payload.get("generated_at"),
            "status": payload.get("status"),
            "target_date": payload.get("target_date"),
            "snapshot_id": payload.get("snapshot_id"),
            "stage_names": [
                f"{item.get('name')}:{item.get('status')}" for item in stage_items
            ],
            "blocked_stage": next(
                (item.get("name") for item in stage_items if item.get("status") == "blocked"),
                None,
            ),
            "accounts": [
                {
                    "account_id": item.get("account_id"),
                    "status": item.get("status"),
                    "equity": item.get("equity"),
                }
                for item in account_items
            ],
        })
    entries.sort(key=lambda item: str(item.get("generated_at") or ""), reverse=True)
    return tuple(entries)


@lru_cache(maxsize=8)
def _market_summary_for_snapshot(root: str, snapshot_id: str) -> dict[str, Any]:
    """Cache immutable snapshot metadata while the canonical pointer is unchanged."""

    snapshot = MarketDataWarehouse(root).load_snapshot(snapshot_id)
    scope = snapshot.plan.universe_scope
    return {
        "snapshot_id": snapshot.snapshot_id,
        "published_end": None if scope is None else scope.history_end.isoformat(),
        "history_start": None if scope is None else scope.history_start.isoformat(),
        "instruments": 0 if scope is None else len(scope.instrument_ids),
    }


@lru_cache(maxsize=8)
def _instrument_names_for_snapshot(root: str, snapshot_id: str) -> dict[str, str]:
    """Cache immutable canonical instrument names for the active snapshot."""

    warehouse = MarketDataWarehouse(root)
    snapshot = warehouse.load_snapshot(snapshot_id)
    frame = warehouse.query_loaded_snapshot_table(snapshot, MarketTable.INSTRUMENTS)
    return {
        str(row.instrument_id): str(row.name)
        for row in frame[["instrument_id", "name"]].itertuples(index=False)
        if row.name is not None and str(row.name).strip()
    }


def _research_subject(payload: Mapping[str, Any]) -> str:
    question = payload.get("question")
    if isinstance(question, str) and question.strip():
        return re.sub(r"\s+", " ", question.strip())[:240]
    ids = set()
    for event in payload.get('tool_events', []):
        if event.get('event') == 'tool_call':
            ids.update(re.findall(r'\b\d{6}\.(?:SH|SZ)\b', json.dumps(event.get('arguments', {})).upper()))
    if ids:
        return '历史调试 · ' + ' / '.join(sorted(ids))
    report = payload.get("research_report")
    if not isinstance(report, Mapping):
        try:
            report = json.loads(str(payload.get("final_text") or ""))
        except json.JSONDecodeError:
            report = None
    thesis = report.get("thesis") if isinstance(report, Mapping) else payload.get("final_text")
    if isinstance(thesis, str) and thesis.strip():
        return re.sub(r"\s+", " ", thesis.strip())[:240]
    return "unknown"
