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
from pathlib import Path
from typing import Any, Mapping

from fundlab.marketdata import MarketDataWarehouse
from fundlab.settings import DailyAccountSettings, FoundationSettings
from fundlab.strategies import AgentDecisionError, load_agent_decision, write_agent_decision
from fundlab.trading import TradingRepository, build_simulation_feedback
from fundlab.trading.repository import RunRecord

_DECISION_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}\.json$")
_REPORT_NAME = re.compile(r"^daily-[A-Za-z0-9-]+\.json$")
_CHAIN_LIMIT = 20000


class DashboardError(ValueError):
    """User-facing request problem (bad account, invalid decision, ...)."""


@dataclass(frozen=True)
class DashboardService:
    settings: FoundationSettings

    # ------------------------------------------------------------- helpers

    def _repository(self) -> TradingRepository:
        return TradingRepository(self.settings.paths.trading_database)

    def _account_config(self, account_id: str) -> DailyAccountSettings | None:
        for item in self.settings.daily.accounts:
            if item.account_id == account_id:
                return item
        return None

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
        try:
            warehouse = MarketDataWarehouse(self.settings.paths.market_data)
            snapshot = warehouse.load_snapshot(warehouse.current_snapshot_id())
            scope = snapshot.plan.universe_scope
            return {
                "snapshot_id": snapshot.snapshot_id,
                "published_end": None if scope is None else scope.history_end.isoformat(),
                "history_start": None if scope is None else scope.history_start.isoformat(),
                "instruments": 0 if scope is None else len(scope.instrument_ids),
            }
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    # ------------------------------------------------------------ accounts

    def accounts(self) -> list[dict[str, Any]]:
        repository = self._repository()
        found = []
        for config in self.settings.daily.accounts:
            entry: dict[str, Any] = {
                "account_id": config.account_id,
                "name": config.name,
                "strategy": config.strategy,
                "initial_cash": str(config.initial_cash),
                "weights": {key: str(value) for key, value in config.weights.items()},
                "exists": False,
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
                return {
                    "account_id": account_id,
                    "name": config.name,
                    "exists": False,
                    "strategy": config.strategy,
                    "weights": {key: str(value) for key, value in config.weights.items()},
                    "cash": str(config.initial_cash),
                    "equity_curve": [],
                    "positions": [],
                    "pending_orders": [],
                    "recent_events": [],
                    "feedback": None,
                    "runs": [],
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
                    recent_events.append({
                        "run_id": record.run_id,
                        "session_date": event["session_date"],
                        "event_type": event["event_type"],
                        "entity_type": event["entity_type"],
                        "entity_id": event["entity_id"],
                        "payload": event["payload"],
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
            "side": order.side.value,
            "created_on": order.created_on.isoformat(),
            "execution_date": order.execution_date.isoformat(),
            "expiry_date": order.expiry_date.isoformat(),
            "requested_quantity": order.requested_quantity,
            "remaining_quantity": order.remaining_quantity,
            "status": order.status.value,
        } for order in state.pending_orders]

        return {
            "account_id": account_id,
            "name": account.name,
            "exists": True,
            "status": account.status.value,
            "strategy": None if config is None else config.strategy,
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
        }

    # -------------------------------------------------------- daily reports

    def daily_reports(self, *, limit: int = 60) -> list[dict[str, Any]]:
        root = Path(self.settings.daily.report_root)
        if not root.is_dir():
            return []
        entries = []
        for path in root.iterdir():
            if not path.is_file() or not _REPORT_NAME.match(path.name):
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            stages = payload.get("stages") or []
            entries.append({
                "file": path.name,
                "generated_at": payload.get("generated_at"),
                "status": payload.get("status"),
                "target_date": payload.get("target_date"),
                "snapshot_id": payload.get("snapshot_id"),
                "stage_names": [
                    f"{item.get('name')}:{item.get('status')}" for item in stages
                ],
                "blocked_stage": next(
                    (item.get("name") for item in stages if item.get("status") == "blocked"),
                    None,
                ),
                "accounts": [
                    {
                        "account_id": item.get("account_id"),
                        "status": item.get("status"),
                        "equity": item.get("equity"),
                    }
                    for item in payload.get("accounts") or []
                ],
            })
        entries.sort(key=lambda item: str(item.get("generated_at") or ""), reverse=True)
        return entries[:limit]

    def daily_report(self, file_name: str) -> dict[str, Any]:
        if (
            not _REPORT_NAME.match(file_name)
            or "/" in file_name or "\\" in file_name or ".." in file_name
        ):
            raise DashboardError(f"非法报告文件名: {file_name}")
        path = Path(self.settings.daily.report_root) / file_name
        if not path.is_file():
            raise DashboardError(f"报告不存在: {file_name}")
        return json.loads(path.read_text(encoding="utf-8"))

    # ------------------------------------------------------ agent decisions

    def agent_accounts(self) -> list[dict[str, Any]]:
        repository = self._repository()
        found = []
        for config in self.settings.daily.accounts:
            if config.strategy != "agent-file":
                continue
            entry: dict[str, Any] = {
                "account_id": config.account_id,
                "name": config.name,
                "head_date": None,
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
        self, account_id: str, *, overwrite: bool = False, dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the configured deterministic policy for one agent-file account.

        Same mechanism the scheduled task uses (`fundlab agent decide`); the
        dashboard only offers a button for it.
        """
        from fundlab.agent import AgentDecisionService, AgentPolicyError, AgentServiceError

        try:
            return AgentDecisionService(self.settings).decide(
                account_id, overwrite=overwrite, dry_run=dry_run,
            )
        except (AgentServiceError, AgentPolicyError, AgentDecisionError) as exc:
            raise DashboardError(str(exc)) from exc
