from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Any, Iterable, Mapping

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest
from fundlab.marketdata.contracts import PriceLimitState, TradeRuleError


@dataclass(frozen=True)
class RuleDecision:
    rule_id: str
    known_date: date
    price_limit_state: PriceLimitState
    price_limit_ratio: Decimal | None
    sell_delay_sessions: int
    upper_multiplier: Decimal | None = None
    lower_multiplier: Decimal | None = None
    evidence: str = ""


@dataclass(frozen=True)
class OrderQuantityRule:
    minimum_buy_quantity: int
    quantity_step: int
    odd_lot_sell_all: bool
    rule_id: str
    known_date: date
    evidence: str


@dataclass(frozen=True)
class PriceLimitAudit:
    bounded_sessions: int
    provider_rows: int
    minimum_provider_observations_per_session: int
    direct_limit_values: int
    latest_bounded_sessions: int
    latest_direct_verified_sessions: int
    minimum_direct_limit_observations_latest_session: int
    observed_price_bound_checks: int
    conflicting_provider_rows: int
    conflicting_direct_limit_values: int
    providers: tuple[str, ...]
    source_observation_ids: tuple[str, ...]
    evidence_hash: str


def apply_corroborated_historical_limit_exceptions(
    derived_bars: pd.DataFrame,
    provider_bars: Mapping[str, pd.DataFrame],
    *,
    protected_dates: Iterable[date | str] = (),
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    """Resolve dated exceptional sessions after the ordinary rule is derived first.

    Historical recovery listings, share-reform resumptions, reorganizations, and
    special first-listing mechanisms are not identifiable from OHLC or the current
    instrument master alone.  An ordinary bound is rejected only when at least two
    independent backend groups report prices outside it.  No provider priority or
    single-source assertion can create an exception.
    """

    keys = ["instrument_id", "session_date", "price_mode"]
    protected = {
        item.isoformat() if isinstance(item, date) else str(item)[:10]
        for item in protected_dates
    }
    bounded = derived_bars.loc[
        derived_bars["price_limit_state"].eq(PriceLimitState.BOUNDED.value)
        & ~derived_bars["suspended"].fillna(False).astype(bool)
        & ~derived_bars["session_date"].astype(str).str[:10].isin(protected)
    ].copy()
    pieces: list[pd.DataFrame] = []
    for provider, frame in provider_bars.items():
        if frame.empty:
            continue
        piece = frame[keys + ["high", "low"]].copy()
        piece["provider"] = str(provider)
        pieces.append(piece)
    if bounded.empty or not pieces:
        payload = {
            "exception_count": 0,
            "exception_keys": (),
            "providers": tuple(sorted(provider_bars)),
            "source_observation_ids": _provider_observation_ids(provider_bars),
            "protected_dates": tuple(sorted(protected)),
            "policy": "ordinary-rule-first-two-independent-boundary-rejection-r2-v2",
        }
        return derived_bars.copy(deep=True), {
            **payload, "evidence_hash": stable_digest(payload),
        }
    observed = pd.concat(pieces, ignore_index=True)
    joined = bounded[keys + ["limit_up", "limit_down", "price_tick"]].merge(
        observed, on=keys, how="inner", validate="one_to_many",
    )
    tolerance = pd.to_numeric(joined["price_tick"], errors="coerce") / 2 + 1e-9
    high = pd.to_numeric(joined["high"], errors="coerce")
    low = pd.to_numeric(joined["low"], errors="coerce")
    outside = high.gt(pd.to_numeric(joined["limit_up"], errors="coerce") + tolerance) | low.lt(
        pd.to_numeric(joined["limit_down"], errors="coerce") - tolerance
    )
    rejected = joined.loc[outside].groupby(keys, sort=False)["provider"].nunique()
    exception_keys = tuple(sorted(
        tuple(map(str, key)) for key, count in rejected.items() if int(count) >= 2
    ))
    result = derived_bars.copy(deep=True)
    if exception_keys:
        index = pd.MultiIndex.from_frame(result[keys])
        applies = index.isin(exception_keys)
        result.loc[applies, "trade_rule_id"] = (
            "cn-historical-exchange-exception-corroborated-v1"
        )
        result.loc[applies, "trade_rule_known_date"] = result.loc[
            applies, "session_date"
        ].astype(str).str[:10]
        result.loc[applies, "price_limit_state"] = PriceLimitState.UNBOUNDED.value
        result.loc[applies, ["price_limit_ratio", "limit_up", "limit_down"]] = pd.NA
        providers_by_key = joined.loc[outside].groupby(keys, sort=False)["provider"].apply(
            lambda values: tuple(sorted(set(map(str, values))))
        ).to_dict()
        for row_index in result.index[applies]:
            key = tuple(map(str, result.loc[row_index, keys]))
            raw = result.at[row_index, "field_lineage"]
            try:
                lineage = json.loads(str(raw)) if raw is not None and not pd.isna(raw) else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                lineage = {"upstream_raw": None if raw is None else str(raw)}
            if not isinstance(lineage, dict):
                lineage = {"upstream": lineage}
            lineage["historical_price_limit_exception"] = {
                "method": "ordinary-rule-first-two-independent-boundary-rejection-r2-v2",
                "providers": providers_by_key[key],
                "official_rule": (
                    "SZSE Trading Rules (November 2013 revision), article 3.3.17; "
                    "historical exchange rules enumerate no-daily-limit exceptional sessions"
                ),
                "official_rule_url": (
                    "https://docs.static.szse.cn/www/disclosure/notice/"
                    "W020180328432928783546.pdf"
                ),
                "official_document_sha256": (
                    "4cf54339d81de549fda33e7d92c9e5ad4800d767d4b3c582ab9ad31e979c03af"
                ),
                "known_date_semantics": (
                    "session-date conservative boundary for the public listing/resumption event"
                ),
            }
            result.at[row_index, "field_lineage"] = canonical_json(lineage)
    payload = {
        "exception_count": len(exception_keys),
        "exception_keys": exception_keys,
        "providers": tuple(sorted(provider_bars)),
        "source_observation_ids": _provider_observation_ids(provider_bars),
        "protected_dates": tuple(sorted(protected)),
        "policy": "ordinary-rule-first-two-independent-boundary-rejection-r2-v2",
    }
    return result, {**payload, "evidence_hash": stable_digest(payload)}


def audit_provider_price_limits(
    derived_bars: pd.DataFrame,
    provider_bars: Mapping[str, pd.DataFrame],
    *,
    required_direct_limit_date: date | str | None = None,
) -> PriceLimitAudit:
    """Verify derived limits against independent upstream numeric observations.

    Provider-labelled limit values are checked when supplied.  Because the free
    historical channels generally omit those columns, every upstream high/low is
    also checked against the derived legal bounds.  Every active bounded session
    must have observations from at least two provider backend groups.
    """

    keys = ["instrument_id", "session_date", "price_mode"]
    bounded = derived_bars.loc[
        derived_bars["price_limit_state"].eq(PriceLimitState.BOUNDED.value)
        & ~derived_bars["suspended"].fillna(False).astype(bool)
    ].copy()
    if bounded.empty:
        source_ids = _provider_observation_ids(provider_bars)
        payload = {
            "bounded_sessions": 0, "provider_rows": 0,
            "minimum_provider_observations_per_session": 0,
            "direct_limit_values": 0, "observed_price_bound_checks": 0,
            "latest_bounded_sessions": 0,
            "latest_direct_verified_sessions": 0,
            "minimum_direct_limit_observations_latest_session": 0,
            "conflicting_provider_rows": 0,
            "conflicting_direct_limit_values": 0,
            "providers": tuple(sorted(provider_bars)),
            "source_observation_ids": source_ids,
        }
        return PriceLimitAudit(**payload, evidence_hash=stable_digest(payload))
    required = set(keys) | {"limit_up", "limit_down", "price_tick"}
    missing = sorted(required - set(bounded))
    if missing:
        raise TradeRuleError(f"Price-limit audit is missing derived columns: {', '.join(missing)}")
    pieces: list[pd.DataFrame] = []
    for provider, frame in provider_bars.items():
        if frame.empty:
            continue
        needed = set(keys) | {"high", "low", "limit_up", "limit_down"}
        missing = sorted(needed - set(frame))
        if missing:
            raise TradeRuleError(
                f"Price-limit provider {provider} is missing columns: {', '.join(missing)}"
            )
        piece = frame[list(needed)].copy()
        piece["provider"] = str(provider)
        pieces.append(piece)
    if not pieces:
        raise TradeRuleError("Price-limit audit has no provider observations")
    observed = pd.concat(pieces, ignore_index=True)
    joined = bounded[keys + ["limit_up", "limit_down", "price_tick"]].merge(
        observed,
        on=keys,
        how="left",
        suffixes=("__derived", "__provider"),
        validate="one_to_many",
    )
    tick = pd.to_numeric(joined["price_tick"], errors="coerce")
    upper = pd.to_numeric(joined["limit_up__derived"], errors="coerce")
    lower = pd.to_numeric(joined["limit_down__derived"], errors="coerce")
    high = pd.to_numeric(joined["high"], errors="coerce")
    low = pd.to_numeric(joined["low"], errors="coerce")
    tolerance = tick / 2 + 1e-9
    observed_bounds = high.notna() & low.notna()
    compliant = observed_bounds & high.le(upper + tolerance) & low.ge(lower - tolerance)
    counts = joined.loc[compliant].groupby(keys, sort=False)["provider"].nunique(
        dropna=True,
    ).reindex(pd.MultiIndex.from_frame(bounded[keys]), fill_value=0)
    minimum = int(counts.min()) if not counts.empty else 0
    if minimum < 2:
        key = counts.idxmin() if not counts.empty else ("unknown", "unknown", "raw")
        raise TradeRuleError(
            "Price-limit audit needs two independent provider observations for "
            f"{key[0]}/{key[1]}"
        )
    direct_checks = 0
    direct_conflicts = 0
    direct_pair_match = pd.Series(True, index=joined.index)
    direct_pair_supplied = pd.Series(True, index=joined.index)
    for side in ("up", "down"):
        provider_value = pd.to_numeric(joined[f"limit_{side}__provider"], errors="coerce")
        derived_value = pd.to_numeric(joined[f"limit_{side}__derived"], errors="coerce")
        supplied = provider_value.notna()
        mismatch = supplied & provider_value.sub(derived_value).abs().gt(tolerance)
        direct_pair_supplied &= supplied
        direct_pair_match &= supplied & ~mismatch
        direct_conflicts += int(mismatch.sum())
        direct_checks += int(supplied.sum())
    latest_bounded = 0
    latest_verified = 0
    latest_minimum = 0
    if required_direct_limit_date is not None:
        required_date = str(required_direct_limit_date)[:10]
        required_keys = bounded.loc[
            bounded["session_date"].astype(str).eq(required_date), keys
        ]
        latest_bounded = len(required_keys)
        direct_counts = joined.loc[
            joined["session_date"].astype(str).eq(required_date)
            & direct_pair_supplied & direct_pair_match
        ].groupby(keys, sort=False)["provider"].nunique(dropna=True).reindex(
            pd.MultiIndex.from_frame(required_keys), fill_value=0,
        )
        latest_verified = int(direct_counts.ge(2).sum())
        latest_minimum = int(direct_counts.min()) if not direct_counts.empty else 0
        if latest_bounded and latest_minimum < 2:
            key = direct_counts.idxmin()
            raise TradeRuleError(
                "Price-limit audit needs two direct provider limit values for "
                f"{key[0]}/{key[1]}"
            )
    payload = {
        "bounded_sessions": len(bounded),
        "provider_rows": int(joined["provider"].notna().sum()),
        "minimum_provider_observations_per_session": minimum,
        "direct_limit_values": direct_checks,
        "latest_bounded_sessions": latest_bounded,
        "latest_direct_verified_sessions": latest_verified,
        "minimum_direct_limit_observations_latest_session": latest_minimum,
        "observed_price_bound_checks": int((high.notna() & low.notna()).sum()),
        "conflicting_provider_rows": int((observed_bounds & ~compliant).sum()),
        "conflicting_direct_limit_values": direct_conflicts,
        "providers": tuple(sorted(provider_bars)),
        "source_observation_ids": _provider_observation_ids(provider_bars),
    }
    return PriceLimitAudit(**payload, evidence_hash=stable_digest(payload))


def _provider_observation_ids(
    provider_bars: Mapping[str, pd.DataFrame],
) -> tuple[str, ...]:
    return tuple(sorted({
        str(value)
        for frame in provider_bars.values()
        if "_audit_observation_id" in frame
        for value in frame["_audit_observation_id"].dropna().unique()
        if str(value).strip()
    }))


ETF_RULE_COLUMNS = (
    "instrument_id",
    "effective_from",
    "effective_to",
    "known_date",
    "price_limit_ratio",
    "sell_delay_sessions",
    "rule_id",
    "evidence",
)


def resolve_order_quantity_rule(
    instrument: Mapping[str, Any],
) -> OrderQuantityRule:
    """Resolve minimum order size separately from the quantity increment.

    The two values happen to both be 100 for ordinary SH/SZ stocks and ETFs,
    but they are deliberately separate: STAR purchases have a 200-share
    minimum and may then increase one share at a time.  An odd residual may be
    sold only as the complete residual position.
    """

    instrument_id = str(instrument.get("instrument_id") or "<unknown>")
    asset_type = str(instrument.get("asset_type") or "")
    board = str(instrument.get("board") or "")
    if asset_type == "stock" and board == "star":
        return OrderQuantityRule(
            200,
            1,
            True,
            "sse-star-minimum-200-step-1-v1",
            date(2019, 7, 22),
            (
                "SSE STAR trading rule: buy declarations have a 200-share minimum "
                "and may increase one share at a time; odd residual sales must be "
                "declared in full; https://edu.sse.com.cn/tib/"
            ),
        )
    raw_minimum = instrument.get("buy_lot")
    if raw_minimum is None or pd.isna(raw_minimum) or int(raw_minimum) <= 0:
        raise TradeRuleError(f"Invalid minimum buy quantity: {instrument_id}")
    minimum = int(raw_minimum)
    raw_step = instrument.get("quantity_step")
    step = minimum if raw_step is None or pd.isna(raw_step) else int(raw_step)
    if step <= 0:
        raise TradeRuleError(f"Invalid quantity step: {instrument_id}")
    raw_odd_lot = instrument.get("odd_lot_sell_all")
    odd_lot_sell_all = (
        True if raw_odd_lot is None or pd.isna(raw_odd_lot) else bool(raw_odd_lot)
    )
    return OrderQuantityRule(
        minimum,
        step,
        odd_lot_sell_all,
        "cn-sh-sz-board-lot-and-odd-residual-v1",
        date(2006, 7, 1),
        (
            "Ordinary SH/SZ stocks and ETFs use the provider-declared normal buy "
            "lot as both minimum and increment; odd residual sales must be declared in full"
        ),
    )


def materialize_order_quantity_rules(instruments: pd.DataFrame) -> pd.DataFrame:
    required = {"instrument_id", "asset_type", "board", "buy_lot"}
    missing = sorted(required - set(instruments))
    if missing:
        raise TradeRuleError(
            "Order-quantity materialization is missing instrument columns: "
            + ", ".join(missing)
        )
    result = instruments.copy(deep=True)
    if "field_lineage" not in result:
        result["field_lineage"] = pd.NA
    rules = {
        str(row["instrument_id"]): resolve_order_quantity_rule(row)
        for row in result.to_dict("records")
    }
    ids = result["instrument_id"].astype(str)
    result["buy_lot"] = ids.map({
        key: rule.minimum_buy_quantity for key, rule in rules.items()
    })
    result["quantity_step"] = ids.map({
        key: rule.quantity_step for key, rule in rules.items()
    })
    result["odd_lot_sell_all"] = ids.map({
        key: rule.odd_lot_sell_all for key, rule in rules.items()
    })
    result["field_lineage"] = [
        _append_order_quantity_lineage(raw, rules[instrument_id])
        for raw, instrument_id in zip(result["field_lineage"], ids, strict=True)
    ]
    return result


def materialize_daily_trade_rules(
    bars: pd.DataFrame,
    instruments: pd.DataFrame,
    calendar: pd.DataFrame,
    *,
    etf_rules: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Derive replayable daily trading rules without guessing ETF subclasses.

    Stock rules are deterministic exchange rules keyed by board, listing-session
    ordinal, date, and the already observed daily ST state.  ETF T+0/T+1 and 20%
    membership require an explicit dated rule row because neither can be inferred
    safely from a six-digit code or current fund name.
    """

    instruments = materialize_order_quantity_rules(instruments)
    required_bar_columns = {
        "instrument_id", "session_date", "price_mode", "previous_close",
        "suspended", "is_st", "field_lineage",
    }
    missing = sorted(required_bar_columns - set(bars))
    if missing:
        raise TradeRuleError(f"Daily rule materialization is missing bar columns: {', '.join(missing)}")
    required_instrument_columns = {
        "instrument_id", "exchange", "asset_type", "board", "listed_date",
        "buy_lot", "quantity_step", "odd_lot_sell_all", "price_tick",
    }
    missing = sorted(required_instrument_columns - set(instruments))
    if missing:
        raise TradeRuleError(
            f"Daily rule materialization is missing instrument columns: {', '.join(missing)}"
        )

    master = instruments.set_index("instrument_id").to_dict("index")
    sessions_by_exchange = {
        str(exchange): tuple(sorted(map(str, group.loc[group["is_open"], "session_date"])))
        for exchange, group in calendar.groupby("exchange")
    }
    session_ordinals: dict[tuple[str, str], int] = {}
    for instrument_id, item in master.items():
        listed = _required_date(item.get("listed_date"), f"listed_date:{instrument_id}")
        sessions = sessions_by_exchange.get(str(item["exchange"]), ())
        if sessions and listed.isoformat() < sessions[0]:
            # The snapshot starts after this instrument listed.  Its exact ordinal
            # is outside the declared history, but it is certainly past the IPO
            # first-five-session window.
            for session in sessions:
                session_ordinals[(str(instrument_id), session)] = 6
            continue
        ordinal = 0
        for session in sessions:
            if session < listed.isoformat():
                continue
            ordinal += 1
            session_ordinals[(str(instrument_id), session)] = ordinal

    result = bars.copy(deep=True)
    result["_session_text"] = result["session_date"].astype(str).str[:10]
    instrument_ids = result["instrument_id"].astype(str)
    missing_master = sorted(set(instrument_ids) - set(master))
    if missing_master:
        raise TradeRuleError(f"Rule materialization has no instrument: {missing_master[0]}")
    result["_asset_type"] = instrument_ids.map({
        instrument_id: str(item["asset_type"]) for instrument_id, item in master.items()
    })
    unsupported = ~result["_asset_type"].isin(("stock", "etf"))
    if unsupported.any():
        raise TradeRuleError(
            f"Unsupported rule asset type: {result.loc[unsupported, '_asset_type'].iloc[0]}"
        )
    result["_listing_ordinal"] = [
        session_ordinals.get((instrument_id, session))
        for instrument_id, session in zip(
            instrument_ids, result["_session_text"], strict=True,
        )
    ]
    suspended = result["suspended"].fillna(False).astype(bool)
    unknown_st = result["is_st"].isna()
    unknown_suspended = suspended & unknown_st
    invalid_unknown_st = ~suspended & unknown_st
    if invalid_unknown_st.any():
        row = result.loc[invalid_unknown_st].iloc[0]
        raise TradeRuleError(
            f"ST state is unknown: {row['instrument_id']}/{row['session_date']}"
        )

    prepared_etf = _prepare_etf_rules(etf_rules)
    result["_etf_rule_token"] = pd.Series(pd.NA, index=result.index, dtype="string")
    etf_decisions: dict[str, RuleDecision] = {}
    for instrument_id, rules in prepared_etf.items():
        instrument_mask = instrument_ids.eq(instrument_id)
        if not instrument_mask.any():
            continue
        for ordinal, row in enumerate(rules):
            token = f"{instrument_id}:{ordinal}"
            interval = (
                instrument_mask
                & result["_session_text"].ge(row["effective_from"].isoformat())
                & (
                    True if row["effective_to"] is None else
                    result["_session_text"].le(row["effective_to"].isoformat())
                )
            )
            if result.loc[interval, "_etf_rule_token"].notna().any():
                sample = result.loc[interval].iloc[0]
                raise TradeRuleError(
                    "ETF session requires exactly one explicit dated rule: "
                    f"{sample['instrument_id']}/{sample['session_date']}"
                )
            result.loc[interval, "_etf_rule_token"] = token
            etf_decisions[token] = RuleDecision(
                str(row["rule_id"]),
                row["known_date"],
                PriceLimitState.BOUNDED,
                row["price_limit_ratio"],
                row["sell_delay_sessions"],
                evidence=str(row.get("evidence") or ""),
            )
    missing_etf_rule = (
        result["_asset_type"].eq("etf")
        & ~unknown_suspended
        & result["_etf_rule_token"].isna()
    )
    if missing_etf_rule.any():
        sample = result.loc[missing_etf_rule].iloc[0]
        raise TradeRuleError(
            "ETF session requires exactly one explicit dated rule: "
            f"{sample['instrument_id']}/{sample['session_date']}"
        )

    result["_session_bucket"] = 0
    result.loc[result["_session_text"].ge("2020-08-24"), "_session_bucket"] = 1
    result.loc[result["_session_text"].ge("2026-07-06"), "_session_bucket"] = 2
    result["_ordinal_bucket"] = pd.to_numeric(
        result["_listing_ordinal"], errors="coerce",
    ).clip(upper=6).fillna(-1).astype(int)
    result["_decision_key"] = "cn-suspended-no-execution-v1"
    stock = result["_asset_type"].eq("stock") & ~unknown_suspended
    result.loc[stock, "_decision_key"] = (
        "stock|"
        + instrument_ids.loc[stock]
        + "|"
        + result.loc[stock, "is_st"].astype(bool).astype(str)
        + "|"
        + result.loc[stock, "_ordinal_bucket"].astype(str)
        + "|"
        + result.loc[stock, "_session_bucket"].astype(str)
    )
    etf = result["_asset_type"].eq("etf") & ~unknown_suspended
    result.loc[etf, "_decision_key"] = "etf|" + result.loc[etf, "_etf_rule_token"]

    decisions: dict[str, RuleDecision] = {
        "cn-suspended-no-execution-v1": RuleDecision(
            "cn-suspended-no-execution-v1",
            date(2006, 7, 1),
            PriceLimitState.UNKNOWN,
            None,
            1,
            evidence="nontradable session with unavailable point-in-time ST state",
        ),
    }
    representative_sessions = {
        0: date(2000, 1, 1),
        1: date(2020, 8, 24),
        2: date(2026, 7, 6),
    }
    for row in result.drop_duplicates("_decision_key").to_dict("records"):
        key = str(row["_decision_key"])
        if key in decisions:
            continue
        if row["_asset_type"] == "etf":
            decisions[key] = etf_decisions[str(row["_etf_rule_token"])]
            continue
        ordinal = int(row["_ordinal_bucket"])
        decisions[key] = resolve_stock_trade_rule(
            master[str(row["instrument_id"])],
            representative_sessions[int(row["_session_bucket"])],
            bool(row["is_st"]),
            None if ordinal < 0 else ordinal,
        )

    result["trade_rule_id"] = result["_decision_key"].map({
        key: item.rule_id for key, item in decisions.items()
    })
    result["trade_rule_known_date"] = result["_decision_key"].map({
        key: item.known_date.isoformat() for key, item in decisions.items()
    })
    result["buy_lot"] = instrument_ids.map({
        instrument_id: int(item["buy_lot"]) for instrument_id, item in master.items()
    })
    result["quantity_step"] = instrument_ids.map({
        instrument_id: int(item["quantity_step"])
        for instrument_id, item in master.items()
    })
    result["odd_lot_sell_all"] = instrument_ids.map({
        instrument_id: bool(item["odd_lot_sell_all"])
        for instrument_id, item in master.items()
    })
    result["price_tick"] = instrument_ids.map({
        instrument_id: float(item["price_tick"]) for instrument_id, item in master.items()
    })
    result["sell_delay_sessions"] = result["_decision_key"].map({
        key: item.sell_delay_sessions for key, item in decisions.items()
    })
    result["price_limit_state"] = result["_decision_key"].map({
        key: item.price_limit_state.value for key, item in decisions.items()
    })
    result["price_limit_ratio"] = result["_decision_key"].map({
        key: pd.NA if item.price_limit_ratio is None else float(item.price_limit_ratio)
        for key, item in decisions.items()
    })
    result["limit_up"] = pd.Series(pd.NA, index=result.index, dtype="Float64")
    result["limit_down"] = pd.Series(pd.NA, index=result.index, dtype="Float64")
    for key, decision in decisions.items():
        bounded = (
            result["_decision_key"].eq(key)
            & ~suspended
            & (decision.price_limit_state is PriceLimitState.BOUNDED)
        )
        if not bounded.any():
            continue
        if result.loc[bounded, "previous_close"].isna().any():
            row = result.loc[bounded & result["previous_close"].isna()].iloc[0]
            raise TradeRuleError(
                f"Bounded rule has no previous_close: {row['instrument_id']}/{row['session_date']}"
            )
        if decision.upper_multiplier is not None:
            upper_multiplier = decision.upper_multiplier
            assert decision.lower_multiplier is not None
            lower_multiplier = decision.lower_multiplier
        else:
            assert decision.price_limit_ratio is not None
            upper_multiplier = Decimal("1") + decision.price_limit_ratio
            lower_multiplier = Decimal("1") - decision.price_limit_ratio
        result.loc[bounded, "limit_up"] = _round_price_series_to_tick(
            result.loc[bounded, "previous_close"],
            result.loc[bounded, "price_tick"],
            upper_multiplier,
        )
        result.loc[bounded, "limit_down"] = _round_price_series_to_tick(
            result.loc[bounded, "previous_close"],
            result.loc[bounded, "price_tick"],
            lower_multiplier,
        )

    result.loc[suspended, ["open", "high", "low", "close"]] = pd.NA
    result.loc[suspended, "volume"] = 0
    lineage = result[["field_lineage", "_decision_key"]].astype("string").fillna("")
    unique_lineage = lineage.drop_duplicates(ignore_index=True)
    unique_lineage["_derived_lineage"] = [
        _append_lineage(item["field_lineage"], decisions[item["_decision_key"]])
        for item in unique_lineage.to_dict("records")
    ]
    result["field_lineage"] = lineage.merge(
        unique_lineage,
        on=["field_lineage", "_decision_key"],
        how="left",
        sort=False,
        validate="many_to_one",
    )["_derived_lineage"].array
    return result.drop(columns=[
        "_session_text",
        "_asset_type",
        "_listing_ordinal",
        "_etf_rule_token",
        "_session_bucket",
        "_ordinal_bucket",
        "_decision_key",
    ])


def resolve_stock_trade_rule(
    instrument: Mapping[str, Any],
    session: date,
    is_st: bool,
    listing_session_ordinal: int | None,
) -> RuleDecision:
    """Resolve one stock session from dated exchange rules and listing ordinal."""
    board = str(instrument.get("board") or "main")
    listed = _required_date(instrument.get("listed_date"), "listed_date")
    if listing_session_ordinal is None:
        raise TradeRuleError(
            f"Trading calendar does not cover stock session: {instrument.get('instrument_id')}/{session}"
        )

    registration_start = {
        "star": date(2019, 7, 22),
        "chinext": date(2020, 8, 24),
        "main": date(2023, 4, 10),
    }.get(board)
    if (
        registration_start is not None
        and listed >= registration_start
        and listing_session_ordinal <= 5
    ):
        return RuleDecision(
            f"cn-stock-{board}-ipo-first-five-unbounded-v1",
            registration_start,
            PriceLimitState.UNBOUNDED,
            None,
            1,
            evidence="exchange registration-based IPO first-five-session rule",
        )

    if listing_session_ordinal == 1 and listed >= date(2014, 1, 1):
        return RuleDecision(
            "cn-stock-legacy-ipo-first-session-44up-36down-v1",
            date(2014, 1, 1),
            PriceLimitState.BOUNDED,
            None,
            1,
            Decimal("1.44"),
            Decimal("0.64"),
            "pre-registration IPO first-session exchange rule",
        )
    if listing_session_ordinal == 1:
        return RuleDecision(
            "cn-stock-pre-2014-ipo-first-session-unbounded-v1",
            date(2006, 7, 1),
            PriceLimitState.UNBOUNDED,
            None,
            1,
            evidence="pre-2014 exchange IPO first-session rule",
        )

    name = str(instrument.get("name") or "").strip().upper()
    if (
        str(instrument.get("exchange")) == "SH"
        and name.startswith("S")
        and not name.startswith("ST")
    ):
        return RuleDecision(
            "sse-unreformed-s-share-5pct-v1",
            date(2007, 1, 4),
            PriceLimitState.BOUNDED,
            Decimal("0.05"),
            1,
            evidence=(
                "SSE 上证发〔2014〕3号: unreformed shares prefixed S remain at ±5%; "
                "supersedes the same 2007-01-04 differential rule; "
                "https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/reform/"
                "c/c_20250520_10779478.shtml"
            ),
        )

    if board == "star":
        ratio, rule_id, known = Decimal("0.20"), "cn-stock-star-20pct-v1", date(2019, 7, 22)
    elif board == "chinext" and session >= date(2020, 8, 24):
        ratio, rule_id, known = Decimal("0.20"), "cn-stock-chinext-20pct-v1", date(2020, 8, 24)
    elif board == "chinext" and is_st:
        ratio, rule_id, known = Decimal("0.05"), "cn-stock-chinext-st-5pct-legacy-v1", date(2006, 7, 1)
    elif board == "chinext":
        ratio, rule_id, known = Decimal("0.10"), "cn-stock-chinext-10pct-legacy-v1", date(2009, 10, 30)
    elif is_st and session < date(2026, 7, 6):
        ratio, rule_id, known = Decimal("0.05"), "cn-stock-main-st-5pct-through-2026-07-05-v1", date(2006, 7, 1)
    elif is_st:
        ratio, rule_id, known = Decimal("0.10"), "cn-stock-main-st-10pct-from-2026-07-06-v1", date(2026, 4, 24)
    else:
        ratio, rule_id, known = Decimal("0.10"), "cn-stock-main-10pct-v1", date(2006, 7, 1)
    return RuleDecision(rule_id, known, PriceLimitState.BOUNDED, ratio, 1)


def _prepare_etf_rules(frame: pd.DataFrame | None) -> dict[str, tuple[Mapping[str, Any], ...]]:
    if frame is None:
        frame = pd.DataFrame(columns=ETF_RULE_COLUMNS)
    missing = sorted(set(ETF_RULE_COLUMNS) - set(frame))
    if missing:
        raise TradeRuleError(f"ETF rule source is missing columns: {', '.join(missing)}")
    grouped: dict[str, tuple[Mapping[str, Any], ...]] = {}
    for instrument_id, rows in frame.groupby("instrument_id", sort=True):
        normalized = []
        for row in rows.to_dict("records"):
            start = _required_date(row["effective_from"], f"ETF effective_from:{instrument_id}")
            end = None if pd.isna(row["effective_to"]) else _required_date(
                row["effective_to"], f"ETF effective_to:{instrument_id}",
            )
            known = _required_date(row["known_date"], f"ETF known_date:{instrument_id}")
            if end is not None and start > end:
                raise TradeRuleError(f"ETF rule interval is inverted: {instrument_id}")
            if known > start:
                raise TradeRuleError(f"ETF rule becomes known after it is effective: {instrument_id}")
            delay = int(row["sell_delay_sessions"])
            ratio = Decimal(str(row["price_limit_ratio"]))
            if delay < 0 or not Decimal("0") < ratio < Decimal("1"):
                raise TradeRuleError(f"ETF rule values are invalid: {instrument_id}")
            normalized.append({
                **row,
                "effective_from": start,
                "effective_to": end,
                "known_date": known,
                "sell_delay_sessions": delay,
                "price_limit_ratio": ratio,
            })
        grouped[str(instrument_id)] = tuple(normalized)
    return grouped


def _etf_rule(
    instrument_id: str,
    session: date,
    rules: Mapping[str, tuple[Mapping[str, Any], ...]],
) -> RuleDecision:
    matches = [
        row for row in rules.get(instrument_id, ())
        if row["effective_from"] <= session
        and (row["effective_to"] is None or session <= row["effective_to"])
    ]
    if len(matches) != 1:
        raise TradeRuleError(
            f"ETF session requires exactly one explicit dated rule: {instrument_id}/{session}"
        )
    row = matches[0]
    return RuleDecision(
        str(row["rule_id"]),
        row["known_date"],
        PriceLimitState.BOUNDED,
        row["price_limit_ratio"],
        row["sell_delay_sessions"],
        evidence=str(row.get("evidence") or ""),
    )


def _append_lineage(raw: Any, decision: RuleDecision) -> str:
    payload: dict[str, Any]
    try:
        parsed = json.loads(str(raw)) if raw is not None and not pd.isna(raw) else {}
        payload = parsed if isinstance(parsed, dict) else {"upstream": parsed}
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {"upstream_raw": None if raw is None else str(raw)}
    payload["trade_rule_derivation"] = {
        "rule_id": decision.rule_id,
        "known_date": decision.known_date,
        "price_limit_state": decision.price_limit_state,
        "price_limit_ratio": decision.price_limit_ratio,
        "sell_delay_sessions": decision.sell_delay_sessions,
        "evidence": decision.evidence,
    }
    return canonical_json(payload)


def _append_order_quantity_lineage(raw: Any, rule: OrderQuantityRule) -> str:
    payload: dict[str, Any]
    try:
        parsed = json.loads(str(raw)) if raw is not None and not pd.isna(raw) else {}
        payload = parsed if isinstance(parsed, dict) else {"upstream": parsed}
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {"upstream_raw": None if raw is None else str(raw)}
    payload["order_quantity_rule"] = {
        "rule_id": rule.rule_id,
        "known_date": rule.known_date,
        "minimum_buy_quantity": rule.minimum_buy_quantity,
        "quantity_step": rule.quantity_step,
        "odd_lot_sell_all": rule.odd_lot_sell_all,
        "evidence": rule.evidence,
    }
    return canonical_json(payload)


def _round_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


def _round_price_series_to_tick(
    previous_close: pd.Series,
    price_tick: pd.Series,
    multiplier: Decimal,
) -> pd.Series:
    ticks = pd.to_numeric(price_tick, errors="coerce").astype(float)
    prices = pd.to_numeric(previous_close, errors="coerce").astype(float)
    if ticks.isna().any() or (ticks <= 0).any() or prices.isna().any():
        raise TradeRuleError("Bounded rule has invalid price/tick values")
    units_raw = prices / ticks
    units = units_raw.round().astype("int64")
    if (units_raw.sub(units).abs() > 1e-6).any():
        raise TradeRuleError("Previous close is not aligned to the instrument price tick")
    numerator, denominator = multiplier.as_integer_ratio()
    rounded_units = (2 * units * numerator + denominator) // (2 * denominator)
    return (rounded_units.astype(float) * ticks).round(8)


def _required_date(value: Any, field: str) -> date:
    if value is None or pd.isna(value):
        raise TradeRuleError(f"Required rule date is missing: {field}")
    return date.fromisoformat(str(value)[:10])
