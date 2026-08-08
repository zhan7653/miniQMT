from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Iterable, Mapping

from fundlab.common.canonical import stable_digest
from fundlab.marketdata.portal import CorporateAction, DailyBar, Instrument, MarketSession
from fundlab.marketdata.contracts import (
    CorporateActionType,
    DATA_GAP_QUARANTINE_RULE_ID,
    EXECUTION_EVIDENCE_GAP_RULE_ID,
    PriceLimitState,
)
from fundlab.trading.fees import FeeBreakdown, FeeSchedule, money
from fundlab.trading.intent import PortfolioIntent, RiskPolicy, assess_intent, decimal_value
from fundlab.trading.state import (
    Entitlement,
    ExecutionPolicy,
    ExecutionStatus,
    Fill,
    IntentResult,
    LedgerEvent,
    Order,
    PortfolioState,
    PositionLot,
    SessionResult,
    Side,
    Valuation,
)


ZERO_FEES = FeeBreakdown(*(Decimal("0.00") for _ in range(5)))


class TradingKernel:
    """Pure daily trading/accounting kernel shared by historical and daily clocks."""

    def __init__(
        self,
        *,
        instruments: Mapping[str, Instrument],
        sessions: Iterable[date],
        execution_policy: ExecutionPolicy,
        risk_policy: RiskPolicy,
        fee_schedule: FeeSchedule,
    ) -> None:
        self.instruments = dict(instruments)
        self.sessions = tuple(sorted(set(sessions)))
        self._session_index = {day: index for index, day in enumerate(self.sessions)}
        self.execution_policy = execution_policy
        self.risk_policy = risk_policy
        self.fee_schedule = fee_schedule
        if not self.sessions:
            raise ValueError("Trading kernel requires at least one session")

    def process_session(self, state: PortfolioState, market: MarketSession) -> SessionResult:
        day = market.session_date
        if day not in self._session_index:
            raise ValueError(f"Session is not in the bound trading calendar: {day}")
        events: list[LedgerEvent] = []
        fills: list[Fill] = []
        current = state
        current, cleanup_events = self._prune_zero_entitlements(current, day)
        events.extend(cleanup_events)
        if not self.fee_schedule.trusted_for_simulation:
            current, event = _mark_incomplete(
                current, day, "untrusted_simulation_fee_schedule", self.fee_schedule.schedule_id,
            )
            if event is not None:
                events.append(event)

        current, action_events = self._apply_open_actions(current, market)
        events.extend(action_events)

        retained: list[Order] = []
        due: list[Order] = []
        for order in current.pending_orders:
            if order.expiry_date < day:
                events.append(_order_event(day, "order_expired", order, {
                    "remaining_quantity": order.remaining_quantity,
                    "reason": "clock_advanced_past_expiry",
                }))
            elif order.execution_date == day:
                due.append(order)
            else:
                retained.append(order)
        current = replace(current, pending_orders=tuple(retained))

        for order in sorted(due, key=lambda item: (item.side is Side.BUY, item.instrument_id, item.order_id)):
            bar = market.bars.get(order.instrument_id)
            if bar is not None and bar.trade_rule_id in {
                DATA_GAP_QUARANTINE_RULE_ID,
                EXECUTION_EVIDENCE_GAP_RULE_ID,
            }:
                next_session = self._advance_session(day, 1)
                if next_session != date.max:
                    deferred = replace(
                        order,
                        execution_date=next_session,
                        expiry_date=next_session,
                    )
                    current = replace(
                        current,
                        pending_orders=tuple((*current.pending_orders, deferred)),
                    )
                    events.append(_order_event(day, "order_deferred", order, {
                        "reason": (
                            "instrument_data_gap_quarantine"
                            if bar.trade_rule_id == DATA_GAP_QUARANTINE_RULE_ID
                            else "execution_evidence_gap"
                        ),
                        "previous_execution_date": day,
                        "next_execution_date": next_session,
                        "remaining_quantity": order.remaining_quantity,
                    }))
                else:
                    events.append(_order_event(day, "order_not_executed", order, {
                        "reason": (
                            "instrument_data_gap_quarantine"
                            if bar.trade_rule_id == DATA_GAP_QUARANTINE_RULE_ID
                            else "execution_evidence_gap"
                        ),
                        "previous_execution_date": day,
                        "next_execution_date": None,
                        "remaining_quantity": order.remaining_quantity,
                    }))
                # A no-execution rule is unconditional.  In particular, the
                # last calendar session must never fall through into the fill
                # path merely because there is no later session to defer to.
                continue
            current, fill, order_events = self._execute_order(current, order, bar)
            if fill is not None:
                fills.append(fill)
            events.extend(order_events)

        current, valuation, valuation_events = self._value_at_close(current, market)
        events.extend(valuation_events)
        current, entitlement_events = self._capture_entitlements(current, market)
        events.extend(entitlement_events)
        return SessionResult(current, tuple(events), tuple(fills), valuation)

    def submit_intent(
        self,
        state: PortfolioState,
        intent: PortfolioIntent,
        market: MarketSession,
        *,
        next_session: date | None,
    ) -> IntentResult:
        if intent.decision_date != market.session_date:
            raise ValueError("PortfolioIntent decision_date must match the processed close")
        events = [LedgerEvent(
            market.session_date,
            "portfolio_intent_received",
            "intent",
            intent.intent_id,
            {
                "account_id": intent.account_id,
                "snapshot_id": intent.snapshot_id,
                "strategy_id": intent.strategy_id,
                "strategy_version": intent.strategy_version,
                "strategy_config_hash": intent.strategy_config_hash,
                "observation_hash": intent.observation_hash,
                "target_weights": intent.target_weights,
                "reason": intent.reason,
                "metadata": intent.metadata,
            },
        )]
        assessment = assess_intent(intent, self.instruments, self.risk_policy)
        events.append(LedgerEvent(
            market.session_date,
            "risk_assessed",
            "intent",
            intent.intent_id,
            {
                "accepted": assessment.accepted,
                "requested_weights": assessment.requested_weights,
                "approved_weights": assessment.approved_weights,
                "codes": assessment.codes,
                "risk_policy_hash": self.risk_policy.config_hash,
            },
        ))
        if not assessment.accepted or next_session is None:
            if next_session is None:
                events.append(LedgerEvent(
                    market.session_date,
                    "intent_not_scheduled",
                    "intent",
                    intent.intent_id,
                    {"reason": "no_next_trading_session"},
                ))
            return IntentResult(state, assessment, (), tuple(events))
        if next_session <= market.session_date or next_session not in self._session_index:
            raise ValueError("next_session must be a later bound trading session")

        total_equity = self._equity(state, market.bars)
        symbols = set(assessment.approved_weights) | {lot.instrument_id for lot in state.lots}
        orders: list[Order] = []
        incomplete = state
        for symbol in sorted(symbols):
            bar = market.bars.get(symbol)
            instrument = self.instruments.get(symbol)
            if instrument is None or bar is None or bar.close is None or bar.close <= 0:
                incomplete, event = _mark_incomplete(incomplete, market.session_date, f"missing_close:{symbol}", symbol)
                if event is not None:
                    events.append(event)
                events.append(LedgerEvent(
                    market.session_date, "order_not_created", "intent", intent.intent_id,
                    {"instrument_id": symbol, "reason": "missing_raw_close"},
                ))
                continue
            close = decimal_value(bar.close)
            weight = assessment.approved_weights.get(symbol, Decimal("0"))
            target = _round_down_normal_quantity(
                total_equity * weight / close,
                bar.buy_lot,
                bar.quantity_step,
            )
            current_quantity = incomplete.quantity(symbol)
            delta = target - current_quantity
            if delta == 0:
                continue
            side = Side.BUY if delta > 0 else Side.SELL
            quantity = abs(delta)
            if side is Side.BUY:
                quantity = _round_down_normal_quantity(
                    Decimal(quantity), bar.buy_lot, bar.quantity_step,
                )
            else:
                quantity = _normalize_sell_declaration(
                    quantity,
                    current_quantity,
                    bar.buy_lot,
                    bar.quantity_step,
                    bar.odd_lot_sell_all,
                )
            if quantity <= 0:
                continue
            order_id = "order-" + stable_digest({
                'intent_id': intent.intent_id,
                'instrument_id': symbol,
                'side': side,
                'execution_date': next_session,
                'quantity': quantity,
            })[:24]
            order = Order(
                order_id,
                intent.intent_id,
                symbol,
                side,
                market.session_date,
                next_session,
                next_session,
                quantity,
                quantity,
            )
            orders.append(order)
            events.append(_order_event(market.session_date, "order_created", order, {
                "sizing_price": close,
                "target_weight": weight,
                "execution_policy_hash": self.execution_policy.config_hash,
            }))
        combined = tuple(sorted((*incomplete.pending_orders, *orders), key=lambda item: item.order_id))
        return IntentResult(replace(incomplete, pending_orders=combined), assessment, tuple(orders), tuple(events))

    def _execute_order(
        self, state: PortfolioState, order: Order, bar: DailyBar | None,
    ) -> tuple[PortfolioState, Fill | None, tuple[LedgerEvent, ...]]:
        day = order.execution_date
        instrument = self.instruments[order.instrument_id]
        blocked_reason = self._blocked_reason(order, instrument, bar)
        if blocked_reason is not None:
            event = _order_event(day, "order_rejected", order, {
                "status": ExecutionStatus.REJECTED,
                "reason": blocked_reason,
                "remaining_quantity": order.remaining_quantity,
            })
            expired = _order_event(day, "order_expired", order, {
                "remaining_quantity": order.remaining_quantity,
                "reason": "day_order_unfilled",
            })
            return state, None, (event, expired)
        assert bar is not None and bar.open is not None
        requested = order.remaining_quantity
        total_position = state.quantity(order.instrument_id)
        sellable = state.sellable_quantity(order.instrument_id, day)
        full_residual_sell = (
            order.side is Side.SELL
            and bar.odd_lot_sell_all
            and requested == total_position
            and requested == sellable
        )
        if (
            not _is_normal_quantity(requested, bar.buy_lot, bar.quantity_step)
            and not full_residual_sell
        ):
            return state, None, (
                _order_event(day, "order_rejected", order, {
                    "status": ExecutionStatus.REJECTED,
                    "reason": "invalid_order_quantity",
                    "remaining_quantity": requested,
                }),
                _order_event(day, "order_expired", order, {
                    "remaining_quantity": requested,
                    "reason": "day_order_unfilled",
                }),
            )
        raw_capacity = (
            Decimal(str(bar.volume)) * self.execution_policy.maximum_participation
        )
        capacity = _round_down_normal_quantity(
            raw_capacity,
            bar.buy_lot,
            bar.quantity_step,
        )
        if full_residual_sell and raw_capacity >= requested:
            capacity = max(capacity, requested)
        approved = min(requested, capacity)
        reasons: list[str] = []
        if approved < requested:
            reasons.append("liquidity_capacity")
        if order.side is Side.SELL:
            if approved > sellable:
                approved = sellable
                reasons.append("t_plus_sellable_capacity")
            if not (full_residual_sell and approved == requested):
                approved = _round_down_normal_quantity(
                    Decimal(approved), bar.buy_lot, bar.quantity_step,
                )
        if not self.execution_policy.allow_partial_fills and approved < requested:
            approved = 0
            reasons.append("partial_fills_disabled")
        if approved <= 0:
            reason = reasons[-1] if reasons else "zero_liquidity"
            return state, None, (
                _order_event(day, "order_rejected", order, {
                    "status": ExecutionStatus.REJECTED, "reason": reason,
                    "remaining_quantity": requested,
                }),
                _order_event(day, "order_expired", order, {
                    "remaining_quantity": requested, "reason": "day_order_unfilled",
                }),
            )

        if order.side is Side.BUY:
            approved = self._affordable_buy_quantity(state.cash, approved, instrument, bar, day)
            if approved < requested:
                reasons.append("cash_capacity")
        if approved <= 0:
            return state, None, (
                _order_event(day, "order_rejected", order, {
                    "status": ExecutionStatus.REJECTED, "reason": "insufficient_cash",
                    "remaining_quantity": requested,
                }),
                _order_event(day, "order_expired", order, {
                    "remaining_quantity": requested, "reason": "day_order_unfilled",
                }),
            )

        price = self._fill_price(order.side, approved, instrument, bar)
        amount = money(price * approved)
        fees = self.fee_schedule.calculate(
            day=day,
            asset_type=instrument.asset_type.value,
            exchange=instrument.exchange,
            side=order.side.value,
            amount=amount,
        )
        if order.side is Side.BUY:
            current = self._apply_buy(
                state, order, approved, price, amount, fees, bar.sell_delay_sessions,
            )
            realized = Decimal("0.00")
        else:
            current, cost = self._apply_sell(state, order, approved, amount, fees)
            realized = money(amount - fees.total - cost)
            current = replace(current, realized_pnl=money(current.realized_pnl + realized))
        fill_id = "fill-" + stable_digest({
            'order_id': order.order_id, 'date': day, 'quantity': approved, 'price': price,
        })[:24]
        reference_price = decimal_value(bar.open)
        slippage_amount = money(abs(price - reference_price) * approved)
        fill = Fill(fill_id, order.order_id, order.instrument_id, order.side, day,
                    approved, reference_price, price, amount, slippage_amount, fees, realized)
        remaining = requested - approved
        status = ExecutionStatus.FILLED if remaining == 0 else ExecutionStatus.PARTIAL
        events = [_order_event(day, "fill_created", order, {
            "fill_id": fill_id,
            "status": status,
            "quantity": approved,
            "reference_price": reference_price,
            "price": price,
            "amount": amount,
            "slippage_amount": slippage_amount,
            "fees": fees,
            "realized_pnl": realized,
            "reasons": tuple(sorted(set(reasons))),
        })]
        if remaining:
            events.append(_order_event(day, "order_expired", order, {
                "remaining_quantity": remaining,
                "reason": "day_order_partial_remainder",
            }))
        return current, fill, tuple(events)

    def _blocked_reason(self, order: Order, instrument: Instrument, bar: DailyBar | None) -> str | None:
        if bar is None:
            return "missing_raw_bar"
        if bar.suspended:
            return "suspended"
        if bar.open is None or bar.high is None or bar.low is None or bar.close is None:
            return "missing_raw_ohlc"
        if bar.volume <= 0:
            return "zero_volume"
        upper, lower = _price_limits(instrument, bar)
        if self.execution_policy.block_at_price_limit:
            tick = decimal_value(bar.price_tick)
            open_price = decimal_value(bar.open)
            if order.side is Side.BUY and upper is not None and open_price >= upper - tick / 2:
                return "limit_up_locked"
            if order.side is Side.SELL and lower is not None and open_price <= lower + tick / 2:
                return "limit_down_locked"
        return None

    def _fill_price(self, side: Side, quantity: int, instrument: Instrument, bar: DailyBar) -> Decimal:
        opening = decimal_value(bar.open)
        volume = decimal_value(bar.volume)
        participation_ratio = Decimal(quantity) / volume / self.execution_policy.maximum_participation
        participation_ratio = min(Decimal("1"), max(Decimal("0"), participation_ratio))
        bps = (
            self.execution_policy.base_slippage_bps
            + self.execution_policy.impact_bps_at_max_participation * participation_ratio * participation_ratio
        )
        direction = Decimal("1") if side is Side.BUY else Decimal("-1")
        candidate = opening * (Decimal("1") + direction * bps / Decimal("10000"))
        if side is Side.BUY:
            candidate = min(candidate, decimal_value(bar.high))
        else:
            candidate = max(candidate, decimal_value(bar.low))
        upper, lower = _price_limits(instrument, bar)
        if upper is not None:
            candidate = min(candidate, upper)
        if lower is not None:
            candidate = max(candidate, lower)
        return _round_price(candidate, decimal_value(bar.price_tick), side)

    def _affordable_buy_quantity(
        self, cash: Decimal, maximum: int, instrument: Instrument, bar: DailyBar, day: date,
    ) -> int:
        minimum = bar.buy_lot
        step = bar.quantity_step
        if maximum < minimum:
            return 0
        # Slot zero means no order; slot one is the minimum declaration and each
        # later slot advances by the independent quantity step.
        high = 1 + (maximum - minimum) // step
        low = 0
        while low < high:
            middle = (low + high + 1) // 2
            quantity = minimum + (middle - 1) * step
            price = self._fill_price(Side.BUY, quantity, instrument, bar)
            amount = money(price * quantity)
            fees = self.fee_schedule.calculate(
                day=day,
                asset_type=instrument.asset_type.value,
                exchange=instrument.exchange,
                side=Side.BUY.value,
                amount=amount,
            )
            if amount + fees.total <= cash:
                low = middle
            else:
                high = middle - 1
        return 0 if low == 0 else minimum + (low - 1) * step

    def _apply_buy(
        self,
        state: PortfolioState,
        order: Order,
        quantity: int,
        price: Decimal,
        amount: Decimal,
        fees: FeeBreakdown,
        sell_delay_sessions: int,
    ) -> PortfolioState:
        total = amount + fees.total
        if total > state.cash:
            raise ValueError("Buy fill exceeds available cash")
        sellable_on = self._advance_session(order.execution_date, sell_delay_sessions)
        lot_id = "lot-" + stable_digest({
            'order_id': order.order_id, 'date': order.execution_date, 'quantity': quantity, 'price': price,
        })[:24]
        lot = PositionLot(lot_id, order.instrument_id, quantity, order.execution_date, sellable_on, total)
        return replace(
            state,
            cash=money(state.cash - total),
            lots=tuple((*state.lots, lot)),
            fees_paid=money(state.fees_paid + fees.total),
        )

    def _apply_sell(
        self,
        state: PortfolioState,
        order: Order,
        quantity: int,
        amount: Decimal,
        fees: FeeBreakdown,
    ) -> tuple[PortfolioState, Decimal]:
        remaining = quantity
        cost = Decimal("0.00")
        lots: list[PositionLot] = []
        ordered = sorted(state.lots, key=lambda lot: (lot.instrument_id != order.instrument_id,
                                                       lot.sellable_on, lot.acquired_on, lot.lot_id))
        for lot in ordered:
            if lot.instrument_id != order.instrument_id or lot.sellable_on > order.execution_date or remaining == 0:
                lots.append(lot)
                continue
            consumed = min(lot.quantity, remaining)
            consumed_cost = lot.cost_amount if consumed == lot.quantity else money(
                lot.cost_amount * Decimal(consumed) / Decimal(lot.quantity)
            )
            cost += consumed_cost
            remaining -= consumed
            if consumed < lot.quantity:
                lots.append(replace(
                    lot,
                    quantity=lot.quantity - consumed,
                    cost_amount=money(lot.cost_amount - consumed_cost),
                ))
        if remaining:
            raise ValueError("Sell fill exceeds settled position")
        return replace(
            state,
            cash=money(state.cash + amount - fees.total),
            lots=tuple(sorted(lots, key=lambda lot: (lot.instrument_id, lot.acquired_on, lot.lot_id))),
            fees_paid=money(state.fees_paid + fees.total),
        ), money(cost)

    def _apply_open_actions(
        self, state: PortfolioState, market: MarketSession,
    ) -> tuple[PortfolioState, tuple[LedgerEvent, ...]]:
        current = state
        events: list[LedgerEvent] = []
        for action in market.ex_actions:
            if not self._action_is_relevant(current, action):
                continue
            events.append(_action_event(market.session_date, "corporate_action_ex_date", action, {}))
            if action.action_type is CorporateActionType.STOCK_DIVIDEND:
                current, allocation_events = self._allocate_share_action_cost(current, action, market.session_date)
                events.extend(allocation_events)
            elif action.action_type is CorporateActionType.SPLIT:
                current, split_events = self._apply_split(current, action, market.session_date)
                events.extend(split_events)
        for action in market.pay_actions:
            entitlement = next((item for item in current.entitlements if item.action_id == action.action_id), None)
            if action.action_type is CorporateActionType.RIGHTS_ISSUE:
                if entitlement is None or entitlement.quantity <= 0:
                    continue
                events.append(_action_event(market.session_date, "rights_issue_declined", action, {
                    "policy": "do_not_subscribe",
                    "entitled_quantity": 0 if entitlement is None else entitlement.quantity,
                }))
                current = replace(current, entitlements=tuple(
                    item for item in current.entitlements if item.action_id != action.action_id
                ))
                continue
            if action.action_type is not CorporateActionType.CASH_DIVIDEND:
                continue
            if entitlement is None:
                continue
            gross = money(Decimal(entitlement.quantity) * decimal_value(action.cash_per_share))
            current = replace(
                current,
                cash=money(current.cash + gross),
                dividend_income=money(current.dividend_income + gross),
                entitlements=tuple(item for item in current.entitlements if item.action_id != action.action_id),
            )
            events.append(_action_event(market.session_date, "cash_dividend_paid", action, {
                "entitled_quantity": entitlement.quantity,
                "gross_cash": gross,
                "tax_treatment": "not_withheld_by_kernel",
            }))
            current, event = _mark_incomplete(
                current, market.session_date, f"dividend_tax_unmodeled:{action.action_id}", action.action_id,
            )
            if event is not None:
                events.append(event)

        for action in market.listing_actions:
            if action.action_type is CorporateActionType.SPLIT:
                # A split/consolidation is applied atomically on its canonical
                # price-effect date; it does not create a second distribution lot.
                continue
            if action.action_type is not CorporateActionType.STOCK_DIVIDEND:
                continue
            entitlement = next((item for item in current.entitlements if item.action_id == action.action_id), None)
            if entitlement is None:
                continue
            quantity = entitlement.distributed_quantity
            if entitlement.adjusted_on is None:
                current, event = _mark_incomplete(
                    current, market.session_date,
                    f"share_cost_basis_not_adjusted:{action.action_id}", action.action_id,
                )
                if event is not None:
                    events.append(event)
            if quantity > 0:
                lot = PositionLot(
                    f"lot-{stable_digest({'action_id': action.action_id, 'date': market.session_date})[:24]}",
                    action.instrument_id,
                    quantity,
                    market.session_date,
                    market.session_date,
                    entitlement.allocated_cost,
                )
                current = replace(current, lots=tuple((*current.lots, lot)))
            current = replace(current, entitlements=tuple(
                item for item in current.entitlements if item.action_id != action.action_id
            ))
            events.append(_action_event(market.session_date, "share_distribution_listed", action, {
                "entitled_quantity": entitlement.quantity,
                "new_quantity": quantity,
                "allocated_cost": entitlement.allocated_cost,
                "fractional_quantity_policy": "discard",
            }))
            exact = Decimal(entitlement.quantity) * decimal_value(action.share_ratio)
            if exact != Decimal(quantity):
                current, event = _mark_incomplete(
                    current, market.session_date, f"fractional_share_unmodeled:{action.action_id}", action.action_id,
                )
                if event is not None:
                    events.append(event)
        return current, tuple(events)

    @staticmethod
    def _action_is_relevant(state: PortfolioState, action: CorporateAction) -> bool:
        """Whether an account can be economically affected by an action."""
        entitled = any(
            item.action_id == action.action_id and item.quantity > 0
            for item in state.entitlements
        )
        if entitled:
            return True
        # Same-session actions have no previously captured entitlement yet.
        # A position acquired after an earlier record date is deliberately not
        # considered entitled merely because it still exists on the ex-date.
        return (
            action.record_date == action.ex_date
            and state.quantity(action.instrument_id) > 0
        )

    @staticmethod
    def _prune_zero_entitlements(
        state: PortfolioState, day: date,
    ) -> tuple[PortfolioState, tuple[LedgerEvent, ...]]:
        """Remove zero-quantity records created by the former market-wide bug."""
        zero = tuple(item for item in state.entitlements if item.quantity <= 0)
        if not zero:
            return state, ()
        current = replace(
            state,
            entitlements=tuple(item for item in state.entitlements if item.quantity > 0),
        )
        event = LedgerEvent(
            day,
            "zero_entitlements_pruned",
            "portfolio_state",
            f"zero-entitlements-{stable_digest(tuple(item.action_id for item in zero))[:20]}",
            {
                "pruned_count": len(zero),
                "action_ids": tuple(item.action_id for item in zero),
            },
        )
        return current, (event,)

    def _apply_split(
        self, state: PortfolioState, action: CorporateAction, day: date,
    ) -> tuple[PortfolioState, tuple[LedgerEvent, ...]]:
        entitlement = next(
            (item for item in state.entitlements if item.action_id == action.action_id), None,
        )
        if entitlement is None:
            if not state.quantity(action.instrument_id):
                return state, ()
            if action.record_date != action.ex_date:
                # No positive record-date entitlement means the account bought
                # after the entitlement boundary.  It must not receive or be
                # marked incomplete for this action.
                return state, ()
        multiplier = action.quantity_multiplier
        if multiplier is None or multiplier <= 0:
            current, event = _mark_incomplete(
                state, day, f"invalid_split_multiplier:{action.action_id}", action.action_id,
            )
            return current, () if event is None else (event,)
        lots = [item for item in state.lots if item.instrument_id == action.instrument_id]
        existing_quantity = sum(item.quantity for item in lots)
        entitled_quantity = existing_quantity if entitlement is None else entitlement.quantity
        if existing_quantity != entitled_quantity:
            current, event = _mark_incomplete(
                state, day, f"split_entitlement_position_mismatch:{action.action_id}", action.action_id,
            )
            return current, () if event is None else (event,)

        exact_total = Decimal(existing_quantity) * decimal_value(multiplier)
        target_total = int(exact_total.to_integral_value(rounding=ROUND_FLOOR))
        ordered = sorted(lots, key=lambda item: (item.sellable_on, item.acquired_on, item.lot_id))
        exact_by_lot = [Decimal(item.quantity) * decimal_value(multiplier) for item in ordered]
        quantities = [int(item.to_integral_value(rounding=ROUND_FLOOR)) for item in exact_by_lot]
        remainder = target_total - sum(quantities)
        ranking = sorted(
            range(len(ordered)),
            key=lambda index: (-(exact_by_lot[index] - quantities[index]), ordered[index].lot_id),
        )
        for index in ranking[:remainder]:
            quantities[index] += 1

        total_cost = money(sum((item.cost_amount for item in ordered), Decimal("0")))
        replacements: list[PositionLot] = []
        allocated_cost = Decimal("0.00")
        positive = [index for index, quantity in enumerate(quantities) if quantity > 0]
        for ordinal, index in enumerate(positive):
            quantity = quantities[index]
            if ordinal + 1 == len(positive):
                cost = money(total_cost - allocated_cost)
            elif target_total:
                cost = money(total_cost * Decimal(quantity) / Decimal(target_total))
                allocated_cost = money(allocated_cost + cost)
            else:
                cost = Decimal("0.00")
            replacements.append(replace(ordered[index], quantity=quantity, cost_amount=cost))
        untouched = [item for item in state.lots if item.instrument_id != action.instrument_id]
        current = replace(
            state,
            lots=tuple((*untouched, *replacements)),
            entitlements=tuple(
                item for item in state.entitlements if item.action_id != action.action_id
            ),
        )
        events: list[LedgerEvent] = [_action_event(day, "split_applied", action, {
            "pre_event_quantity": existing_quantity,
            "post_event_quantity": target_total,
            "quantity_multiplier": multiplier,
            "total_cost_preserved": total_cost,
            "entitlement_mode": (
                "same_session_atomic" if entitlement is None else "record_date_snapshot"
            ),
        })]
        if exact_total != Decimal(target_total):
            current, event = _mark_incomplete(
                current, day, f"fractional_split_unmodeled:{action.action_id}", action.action_id,
            )
            if event is not None:
                events.append(event)
        return current, tuple(events)

    def _allocate_share_action_cost(
        self, state: PortfolioState, action: CorporateAction, day: date,
    ) -> tuple[PortfolioState, tuple[LedgerEvent, ...]]:
        entitlement = next((item for item in state.entitlements if item.action_id == action.action_id), None)
        if entitlement is None:
            return state, ()
        if entitlement.adjusted_on is not None:
            return state, ()
        existing_lots = [lot for lot in state.lots if lot.instrument_id == action.instrument_id]
        existing_quantity = sum(lot.quantity for lot in existing_lots)
        if existing_quantity != entitlement.quantity:
            current, event = _mark_incomplete(
                state, day, f"share_entitlement_position_mismatch:{action.action_id}", action.action_id,
            )
            return current, () if event is None else (event,)
        exact_new = Decimal(entitlement.quantity) * decimal_value(action.share_ratio)
        new_quantity = int(exact_new.to_integral_value(rounding=ROUND_FLOOR))
        total_cost = money(sum((lot.cost_amount for lot in existing_lots), Decimal("0")))
        if existing_quantity + new_quantity == 0:
            old_cost, new_cost = total_cost, Decimal("0.00")
        else:
            old_cost = money(
                total_cost * Decimal(existing_quantity) / Decimal(existing_quantity + new_quantity)
            )
            new_cost = money(total_cost - old_cost)
        adjusted_lots: list[PositionLot] = []
        eligible_seen = 0
        cost_assigned = Decimal("0.00")
        for lot in state.lots:
            if lot.instrument_id != action.instrument_id:
                adjusted_lots.append(lot)
                continue
            eligible_seen += 1
            if eligible_seen == len(existing_lots):
                allocated = money(old_cost - cost_assigned)
            elif total_cost == 0:
                allocated = Decimal("0.00")
            else:
                allocated = money(old_cost * lot.cost_amount / total_cost)
                cost_assigned = money(cost_assigned + allocated)
            adjusted_lots.append(replace(lot, cost_amount=allocated))
        adjusted_entitlement = replace(
            entitlement,
            distributed_quantity=new_quantity,
            allocated_cost=new_cost,
            adjusted_on=day,
        )
        current = replace(
            state,
            lots=tuple(adjusted_lots),
            entitlements=tuple(
                adjusted_entitlement if item.action_id == action.action_id else item
                for item in state.entitlements
            ),
        )
        event = _action_event(day, "share_cost_basis_adjusted", action, {
            "entitled_quantity": entitlement.quantity,
            "new_quantity": new_quantity,
            "existing_cost_after": old_cost,
            "new_share_cost": new_cost,
            "total_cost_preserved": total_cost,
        })
        return current, (event,)

    def _capture_entitlements(
        self, state: PortfolioState, market: MarketSession,
    ) -> tuple[PortfolioState, tuple[LedgerEvent, ...]]:
        existing = {item.action_id for item in state.entitlements}
        entitlements = list(state.entitlements)
        events: list[LedgerEvent] = []
        for action in market.record_actions:
            if action.action_id in existing:
                continue
            quantity = state.quantity(action.instrument_id)
            if quantity <= 0:
                continue
            entitlement = Entitlement(action.action_id, action.instrument_id, quantity, market.session_date)
            entitlements.append(entitlement)
            events.append(_action_event(market.session_date, "corporate_action_entitlement", action, {
                "quantity": quantity,
            }))
        return replace(state, entitlements=tuple(entitlements)), tuple(events)

    def _value_at_close(
        self, state: PortfolioState, market: MarketSession,
    ) -> tuple[PortfolioState, Valuation, tuple[LedgerEvent, ...]]:
        prices = dict(state.last_prices)
        for symbol, bar in market.bars.items():
            if bar.close is not None and bar.close > 0:
                prices[symbol] = decimal_value(bar.close)
        stale: list[str] = []
        value = Decimal("0.00")
        current = state
        events: list[LedgerEvent] = []
        for symbol in sorted({lot.instrument_id for lot in state.lots}):
            price = prices.get(symbol)
            if price is None:
                stale.append(symbol)
                current, event = _mark_incomplete(
                    current, market.session_date, f"missing_valuation_price:{symbol}", symbol,
                )
                if event is not None:
                    events.append(event)
                continue
            bar = market.bars.get(symbol)
            if bar is None or bar.close is None:
                stale.append(symbol)
            value += money(Decimal(current.quantity(symbol)) * price)
        value = money(value)
        equity = money(current.cash + value)
        nav = (equity / current.initial_cash).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        current = replace(current, last_prices=prices)
        valuation = Valuation(market.session_date, current.cash, value, equity, nav, tuple(stale))
        events.append(LedgerEvent(
            market.session_date,
            "portfolio_valued",
            "portfolio",
            "close",
            {
                "cash": current.cash,
                "market_value": value,
                "total_equity": equity,
                "nav": nav,
                "stale_instruments": tuple(stale),
            },
        ))
        return current, valuation, tuple(events)

    def _equity(self, state: PortfolioState, bars: Mapping[str, DailyBar]) -> Decimal:
        value = state.cash
        for symbol in {lot.instrument_id for lot in state.lots}:
            bar = bars.get(symbol)
            price = None if bar is None else bar.close
            if price is None:
                price = state.last_prices.get(symbol)
            if price is None:
                continue
            value += Decimal(state.quantity(symbol)) * decimal_value(price)
        return money(value)

    def _advance_session(self, day: date, delay: int) -> date:
        index = self._session_index[day] + delay
        if index >= len(self.sessions):
            return date.max
        return self.sessions[index]


def _price_limits(instrument: Instrument, bar: DailyBar) -> tuple[Decimal | None, Decimal | None]:
    if bar.price_limit_state is PriceLimitState.UNBOUNDED:
        return None, None
    if bar.price_limit_state is PriceLimitState.UNKNOWN:
        raise ValueError(f"Price-limit state is unknown for {bar.instrument_id} on {bar.session_date}")
    if bar.limit_up is not None and bar.limit_down is not None:
        return decimal_value(bar.limit_up), decimal_value(bar.limit_down)
    if bar.previous_close is None or bar.price_limit_ratio is None:
        return None, None
    previous = decimal_value(bar.previous_close)
    ratio = decimal_value(bar.price_limit_ratio)
    tick = decimal_value(bar.price_tick)
    upper = _round_nearest_tick(previous * (Decimal("1") + ratio), tick)
    lower = _round_nearest_tick(previous * (Decimal("1") - ratio), tick)
    return upper, lower


def _round_nearest_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


def _round_price(value: Decimal, tick: Decimal, side: Side) -> Decimal:
    rounding = ROUND_CEILING if side is Side.BUY else ROUND_FLOOR
    return (value / tick).to_integral_value(rounding=rounding) * tick


def _round_down_normal_quantity(value: Decimal, minimum: int, step: int) -> int:
    if value <= 0:
        return 0
    if minimum <= 0 or step <= 0:
        raise ValueError("Order quantity rule must be positive")
    maximum = int(value.to_integral_value(rounding=ROUND_FLOOR))
    if maximum < minimum:
        return 0
    return minimum + ((maximum - minimum) // step) * step


def _is_normal_quantity(quantity: int, minimum: int, step: int) -> bool:
    return quantity >= minimum and (quantity - minimum) % step == 0


def _normalize_sell_declaration(
    requested: int,
    total_position: int,
    minimum: int,
    step: int,
    odd_lot_sell_all: bool,
) -> int:
    if requested <= 0:
        return 0
    if odd_lot_sell_all and requested == total_position:
        return requested
    return _round_down_normal_quantity(Decimal(requested), minimum, step)


def _order_event(day: date, event_type: str, order: Order, payload: Mapping[str, object]) -> LedgerEvent:
    return LedgerEvent(day, event_type, "order", order.order_id, {
        "intent_id": order.intent_id,
        "instrument_id": order.instrument_id,
        "side": order.side,
        "requested_quantity": order.requested_quantity,
        **payload,
    })


def _action_event(
    day: date, event_type: str, action: CorporateAction, payload: Mapping[str, object],
) -> LedgerEvent:
    return LedgerEvent(day, event_type, "corporate_action", action.action_id, {
        "instrument_id": action.instrument_id,
        "action_type": action.action_type,
        "source_provider": action.source_provider,
        "source_observation_id": action.source_observation_id,
        **payload,
    })


def _mark_incomplete(
    state: PortfolioState, day: date, reason: str, entity_id: str,
) -> tuple[PortfolioState, LedgerEvent | None]:
    if reason in state.incomplete_reasons:
        return state, None
    current = replace(state, incomplete_reasons=tuple((*state.incomplete_reasons, reason)))
    return current, LedgerEvent(
        day, "simulation_marked_incomplete", "quality", entity_id, {"reason": reason},
    )
