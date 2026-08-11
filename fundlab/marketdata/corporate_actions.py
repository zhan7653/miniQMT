from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from types import MappingProxyType
from typing import Any, Mapping

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest
from fundlab.marketdata.contracts import (
    CorporateActionType,
    SnapshotNotReadyError,
    TradeRuleError,
    UniverseScope,
)


@dataclass(frozen=True)
class CorporateActionReconciliationResult:
    actions: pd.DataFrame
    factors: pd.DataFrame
    evidence_hash: str
    report: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "report", MappingProxyType(dict(self.report)))


def build_factor_audit_candidates(
    *,
    instruments: pd.DataFrame,
    actions: pd.DataFrame,
    factors: pd.DataFrame,
    universe_scope: UniverseScope,
    daily_bars: pd.DataFrame | None = None,
) -> Mapping[str, Mapping[str, float]]:
    """Return only unresolved material events that need an independent factor audit."""

    target = set(map(str, instruments["instrument_id"]))
    lifecycle = instruments.set_index("instrument_id")
    listed = {
        str(instrument_id): str(value)[:10]
        for instrument_id, value in lifecycle["listed_date"].items()
        if not pd.isna(value)
    }
    delisted = {
        str(instrument_id): None if pd.isna(value) else str(value)[:10]
        for instrument_id, value in lifecycle["delisted_date"].items()
    }

    def in_lifecycle(instrument_id: str, event_date: str) -> bool:
        return (
            instrument_id in target
            and listed.get(instrument_id, "9999-12-31") <= event_date
            and (
                delisted.get(instrument_id) is None
                or event_date <= str(delisted[instrument_id])
            )
            and universe_scope.history_start.isoformat() <= event_date
            <= universe_scope.history_end.isoformat()
        )

    selected_actions = actions.loc[[
        in_lifecycle(str(row["instrument_id"]), str(row["ex_date"]))
        for row in actions.to_dict("records")
    ]].copy()
    selected_factors = factors.loc[[
        in_lifecycle(str(row["instrument_id"]), str(row["effective_date"]))
        for row in factors.to_dict("records")
    ]].copy()
    action_groups = {
        (str(instrument_id), str(ex_date)): group
        for (instrument_id, ex_date), group in selected_actions.groupby(
            ["instrument_id", "ex_date"], sort=True,
        )
    }
    factor_rows = selected_factors.to_dict("records")
    action_groups_by_instrument: dict[
        str, list[tuple[tuple[str, str], pd.DataFrame]]
    ] = {}
    for key, group in action_groups.items():
        action_groups_by_instrument.setdefault(key[0], []).append((key, group))
    factor_rows_by_instrument: dict[str, list[dict[str, Any]]] = {}
    for row in factor_rows:
        factor_rows_by_instrument.setdefault(str(row["instrument_id"]), []).append(row)
    candidates: dict[str, dict[str, float]] = {}
    for row in factor_rows:
        raw = _factor_raw(
            row.get("source_payload"), instrument_id=str(row["instrument_id"]),
        )
        if not _has_position_economics(raw):
            continue
        instrument_id = str(row["instrument_id"])
        event_date = str(row["effective_date"])
        if any(
            _date_distance(key[1], event_date) <= 31
            and not _economic_mismatches(group, raw)
            for key, group in action_groups_by_instrument.get(instrument_id, ())
        ):
            continue
        candidates.setdefault(instrument_id, {})[event_date] = float(
            row["price_multiplier"]
        )

    previous_close = _previous_close_lookup(daily_bars)
    for key, group in action_groups.items():
        if any(
            _date_distance(str(row["effective_date"]), key[1]) <= 31
            and not _economic_mismatches(
                group,
                _factor_raw(
                    row.get("source_payload"),
                    instrument_id=str(row["instrument_id"]),
                ),
            )
            for row in factor_rows_by_instrument.get(key[0], ())
        ):
            continue
        expected = _expected_action_price_multiplier(group, previous_close.get(key))
        if expected is not None:
            candidates.setdefault(key[0], {})[key[1]] = expected
    return MappingProxyType({
        instrument_id: MappingProxyType(dict(sorted(values.items())))
        for instrument_id, values in sorted(candidates.items())
    })


def _scoped_reconciliation_error(
    message: str,
    instrument_ids: Any,
) -> TradeRuleError:
    """Represent a reconciliation failure whose affected partition is exact."""

    return TradeRuleError(
        message,
        instrument_ids=tuple(sorted({
            str(instrument_id) for instrument_id in instrument_ids
            if str(instrument_id).strip()
        })),
    )


def reconcile_corporate_action_factors(
    *,
    instruments: pd.DataFrame,
    actions: pd.DataFrame,
    factors: pd.DataFrame,
    universe_scope: UniverseScope,
    corroborating_factors: pd.DataFrame | None = None,
    daily_bars: pd.DataFrame | None = None,
) -> CorporateActionReconciliationResult:
    """Require every position-changing event to agree with one MiniQMT factor event.

    MiniQMT occasionally reports the same economic event on an announcement,
    suspension, or resume date rather than the exchange implementation date.  The
    official action lifecycle remains canonical; a unique economic match within 31
    civil days may therefore prove the event while retaining the raw factor date in
    lineage.  Factor rows with no holding economics are retained as technical price
    adjustments, but a material factor without an independently sourced action still
    blocks simulation readiness.
    """

    target = set(map(str, instruments["instrument_id"]))
    if not target or not target <= set(universe_scope.instrument_ids):
        raise SnapshotNotReadyError("Corporate-action partition is outside the pinned universe")
    lifecycle = instruments.set_index("instrument_id")
    if lifecycle["listed_date"].isna().any():
        raise _scoped_reconciliation_error(
            "Corporate-action partition has an unknown listed date",
            lifecycle.index[lifecycle["listed_date"].isna()],
        )
    listed_dates = {
        str(instrument_id): str(value)[:10]
        for instrument_id, value in lifecycle["listed_date"].items()
    }
    delisted_dates = {
        str(instrument_id): None if pd.isna(value) else str(value)[:10]
        for instrument_id, value in lifecycle["delisted_date"].items()
    }
    action_dates = actions["ex_date"].astype(str)
    action_ids = actions["instrument_id"].astype(str)
    action_lifecycle = pd.Series(
        (
            listed_dates.get(instrument_id, "9999-12-31") <= event_date
            and (
                delisted_dates.get(instrument_id) is None
                or event_date <= str(delisted_dates[instrument_id])
            )
            for instrument_id, event_date in zip(action_ids, action_dates)
        ),
        index=actions.index,
        dtype=bool,
    )
    factor_dates = factors["effective_date"].astype(str)
    factor_ids = factors["instrument_id"].astype(str)
    factor_lifecycle = pd.Series(
        (
            listed_dates.get(instrument_id, "9999-12-31") <= event_date
            and (
                delisted_dates.get(instrument_id) is None
                or event_date <= str(delisted_dates[instrument_id])
            )
            for instrument_id, event_date in zip(factor_ids, factor_dates)
        ),
        index=factors.index,
        dtype=bool,
    )
    out_of_lifecycle_factors = tuple(sorted(zip(
        factor_ids.loc[~factor_lifecycle], factor_dates.loc[~factor_lifecycle],
    )))
    selected_actions = actions.loc[
        actions["instrument_id"].astype(str).isin(target)
        & action_lifecycle
        & actions["ex_date"].astype(str).between(
            universe_scope.history_start.isoformat(), universe_scope.history_end.isoformat(),
        )
    ].copy()
    selected_factors = factors.loc[
        factors["instrument_id"].astype(str).isin(target)
        & factor_lifecycle
        & factors["effective_date"].astype(str).between(
            universe_scope.history_start.isoformat(), universe_scope.history_end.isoformat(),
        )
    ].copy()
    selected_corroborating = None
    if corroborating_factors is not None:
        secondary_dates = corroborating_factors["effective_date"].astype(str)
        secondary_ids = corroborating_factors["instrument_id"].astype(str)
        secondary_lifecycle = pd.Series(
            (
                listed_dates.get(instrument_id, "9999-12-31") <= event_date
                and (
                    delisted_dates.get(instrument_id) is None
                    or event_date <= str(delisted_dates[instrument_id])
                )
                for instrument_id, event_date in zip(secondary_ids, secondary_dates)
            ),
            index=corroborating_factors.index,
            dtype=bool,
        )
        selected_corroborating = corroborating_factors.loc[
            corroborating_factors["instrument_id"].astype(str).isin(target)
            & secondary_lifecycle
            & corroborating_factors["effective_date"].astype(str).between(
                universe_scope.history_start.isoformat(),
                universe_scope.history_end.isoformat(),
            )
        ].copy()
    derived_cash_pay_dates: list[str] = []
    derived_share_listing_dates: list[str] = []
    cash_without_pay = (
        selected_actions["action_type"].astype(str).eq(
            CorporateActionType.CASH_DIVIDEND.value
        )
        & selected_actions["pay_date"].isna()
    )
    for index in selected_actions.index[cash_without_pay]:
        action_id = str(selected_actions.at[index, "action_id"])
        ex_date = str(selected_actions.at[index, "ex_date"])
        selected_actions.at[index, "pay_date"] = ex_date
        upstream_lineage = _optional_text(
            selected_actions.at[index, "field_lineage"]
        ) if "field_lineage" in selected_actions else None
        upstream_payload = selected_actions.at[index, "source_payload"]
        selected_actions.at[index, "field_lineage"] = canonical_json({
            "kind": "cash_pay_date_completion_r2",
            "semantics": (
                "same-day settlement fallback when the official implementation "
                "row omits a noncritical payment date"
            ),
            "upstream_lineage": upstream_lineage,
        })
        selected_actions.at[index, "source_payload"] = canonical_json({
            "upstream_action_payload": upstream_payload,
            "derived_pay_date": ex_date,
            "derivation": "ex_date fallback; cash amount and effect independently factor-verified",
        })
        derived_cash_pay_dates.append(action_id)
    stock_distribution = selected_actions["action_type"].astype(str).eq(
        CorporateActionType.STOCK_DIVIDEND.value
    )
    share_listing_needs_completion = stock_distribution & (
        selected_actions["listing_date"].isna()
        | (
            selected_actions["listing_date"].notna()
            & selected_actions["listing_date"].astype(str).lt(
                selected_actions["ex_date"].astype(str)
            )
        )
    )
    for index in selected_actions.index[share_listing_needs_completion]:
        action_id = str(selected_actions.at[index, "action_id"])
        ex_date = str(selected_actions.at[index, "ex_date"])
        raw_listing_date = selected_actions.at[index, "listing_date"]
        upstream_lineage = _optional_text(
            selected_actions.at[index, "field_lineage"]
        ) if "field_lineage" in selected_actions else None
        upstream_payload = selected_actions.at[index, "source_payload"]
        selected_actions.at[index, "listing_date"] = ex_date
        selected_actions.at[index, "field_lineage"] = canonical_json({
            "kind": "share_listing_date_completion_r2",
            "semantics": (
                "first execution-safe session is no earlier than the ex-date; "
                "the official credit date is retained as upstream evidence"
            ),
            "upstream_lineage": upstream_lineage,
        })
        selected_actions.at[index, "source_payload"] = canonical_json({
            "upstream_action_payload": upstream_payload,
            "raw_listing_or_credit_date": (
                None if pd.isna(raw_listing_date) else str(raw_listing_date)
            ),
            "derived_listing_date": ex_date,
            "derivation": (
                "ex_date execution floor; share ratio and effect independently "
                "factor-verified"
            ),
        })
        derived_share_listing_dates.append(action_id)
    selected_factors, official_etf_recovery = _recover_official_etf_cash_factors(
        instruments=instruments,
        actions=selected_actions,
        factors=selected_factors,
        daily_bars=daily_bars,
    )
    selected_actions, selected_factors, corroboration_report = (
        _apply_corroborating_factor_evidence(
            instruments=instruments,
            actions=selected_actions,
            primary_factors=selected_factors,
            corroborating_factors=selected_corroborating,
            daily_bars=daily_bars,
        )
    )
    if selected_actions.duplicated(["instrument_id", "action_type", "ex_date"]).any():
        raise _scoped_reconciliation_error(
            "Corporate-action sources contain duplicate semantic events",
            selected_actions.loc[
                selected_actions.duplicated(
                    ["instrument_id", "action_type", "ex_date"], keep=False,
                ),
                "instrument_id",
            ],
        )
    if selected_factors.duplicated(["instrument_id", "effective_date"]).any():
        raise _scoped_reconciliation_error(
            "Adjustment-factor source contains duplicate events",
            selected_factors.loc[
                selected_factors.duplicated(
                    ["instrument_id", "effective_date"], keep=False,
                ),
                "instrument_id",
            ],
        )

    action_groups = {
        (str(instrument_id), str(ex_date)): group
        for (instrument_id, ex_date), group in selected_actions.groupby(
            ["instrument_id", "ex_date"], sort=True,
        )
    }
    factor_rows_source = selected_factors.to_dict("records")
    factor_groups = {
        (str(row["instrument_id"]), str(row["effective_date"])): index
        for index, row in enumerate(factor_rows_source)
    }
    action_keys = set(action_groups)
    matched: dict[tuple[str, str], int] = {}
    ignored_restructuring: list[tuple[str, str]] = []
    missing_factors: list[tuple[str, str]] = []
    for key in sorted(action_keys):
        group = action_groups[key]
        exact = factor_groups.get(key)
        candidate_indexes = [] if exact is None else [exact]
        candidate_indexes.extend(
            index for index, row in enumerate(factor_rows_source)
            if index != exact
            and str(row["instrument_id"]) == key[0]
            and _date_distance(str(row["effective_date"]), key[1]) <= 31
        )
        viable: list[tuple[int, int]] = []
        exact_mismatches: tuple[str, ...] = ()
        for index in candidate_indexes:
            raw = _factor_raw(
                factor_rows_source[index].get("source_payload"),
                instrument_id=str(factor_rows_source[index]["instrument_id"]),
            )
            mismatches = _economic_mismatches(group, raw)
            if index == exact:
                exact_mismatches = mismatches
            if not mismatches:
                viable.append((
                    _date_distance(
                        str(factor_rows_source[index]["effective_date"]), key[1],
                    ),
                    index,
                ))
        if viable:
            viable.sort()
            if len(viable) > 1 and viable[0][0] == viable[1][0]:
                raise _scoped_reconciliation_error(
                    f"Ambiguous corporate-action factor match at {key[0]}/{key[1]}",
                    (key[0],),
                )
            matched[key] = viable[0][1]
            continue
        if _is_nonholder_restructuring_candidate(group):
            ignored_restructuring.append(key)
            continue
        if exact is not None and exact_mismatches:
            raise _scoped_reconciliation_error(
                f"Corporate-action/factor conflict at {key[0]}/{key[1]}: "
                f"{','.join(exact_mismatches)}",
                (key[0],),
            )
        missing_factors.append(key)

    matched_factor_indexes = set(matched.values())
    duplicate_factor_indexes: set[int] = set()
    missing_actions: list[tuple[str, str]] = []
    technical_factor_indexes: set[int] = set()
    for index, factor in enumerate(factor_rows_source):
        if index in matched_factor_indexes:
            continue
        raw = _factor_raw(
            factor.get("source_payload"), instrument_id=str(factor["instrument_id"]),
        )
        if not _has_position_economics(raw):
            technical_factor_indexes.add(index)
            continue
        factor_key = (str(factor["instrument_id"]), str(factor["effective_date"]))
        duplicate = any(
            key[0] == factor_key[0]
            and _date_distance(key[1], factor_key[1]) <= 7
            and not _economic_mismatches(action_groups[key], raw)
            for key in matched
        )
        if duplicate:
            duplicate_factor_indexes.add(index)
            continue
        missing_actions.append(factor_key)

    missing_factors_tuple = tuple(sorted(missing_factors))
    missing_actions_tuple = tuple(sorted(missing_actions))
    if missing_factors_tuple or missing_actions_tuple:
        raise _scoped_reconciliation_error(
            "Corporate-action/factor event coverage mismatch: "
            f"missing_factors={len(missing_factors_tuple)} "
            f"missing_actions={len(missing_actions_tuple)} "
            f"factor_examples={missing_factors_tuple[:3]} "
            f"action_examples={missing_actions_tuple[:3]}",
            tuple(key[0] for key in (*missing_factors_tuple, *missing_actions_tuple)),
        )

    factor_rows: list[dict[str, Any]] = []
    verified: list[Mapping[str, Any]] = []
    retained_action_indexes: set[int] = set()
    for key in sorted(matched):
        group = action_groups[key]
        retained_action_indexes.update(map(int, group.index))
        factor = dict(factor_rows_source[matched[key]])
        raw = _factor_raw(
            factor.get("source_payload"), instrument_id=str(factor["instrument_id"]),
        )
        mismatches = _economic_mismatches(group, raw)
        if mismatches:
            raise _scoped_reconciliation_error(
                f"Corporate-action/factor conflict at {key[0]}/{key[1]}: {','.join(mismatches)}",
                (key[0],),
            )
        known_date_values = tuple(group["known_date"])
        if not known_date_values or any(
            pd.isna(item) or str(item) in {"", "<NA>", "None"}
            for item in known_date_values
        ):
            raise _scoped_reconciliation_error(
                f"Corporate action has no known date: {key}", (key[0],),
            )
        known_dates = tuple(sorted(map(str, known_date_values)))
        raw_effective_date = str(factor["effective_date"])
        factor["effective_date"] = key[1]
        factor["known_date"] = known_dates[0]
        factor["source_provider"] = "canonical-action-factor-r2"
        factor["field_lineage"] = canonical_json({
            "kind": "action_factor_reconciliation_r2",
            "canonical_effective_date": key[1],
            "raw_factor_effective_date": raw_effective_date,
            "factor_observation_id": _optional_text(
                factor.get("source_observation_id")
            ),
            "action_ids": tuple(sorted(map(str, group["action_id"]))),
            "action_observation_ids": tuple(sorted(set(map(
                str, group["source_observation_id"],
            )))),
            "known_date_semantics": "earliest implementation announcement for the event",
        })
        factor["source_payload"] = canonical_json({
            "factor_payload": factor.get("source_payload"),
            "action_payloads": tuple(map(str, group["source_payload"])),
        })
        factor_rows.append(factor)
        verified.append({
            "instrument_id": key[0],
            "effective_date": key[1],
            "action_types": tuple(sorted(map(str, group["action_type"]))),
            "known_date": known_dates[0],
            "factor_id": _optional_text(factor.get("factor_id")),
            "raw_factor_effective_date": raw_effective_date,
        })

    for index in sorted(technical_factor_indexes):
        factor = dict(factor_rows_source[index])
        upstream_lineage = _optional_text(factor.get("field_lineage"))
        factor["field_lineage"] = canonical_json({
            "kind": "non_position_adjustment_factor_r2",
            "semantics": "factor has no cash/share/rights holding economics",
            "upstream_lineage": upstream_lineage,
        })
        factor_rows.append(factor)

    reconciled_factors = pd.DataFrame(
        factor_rows, columns=selected_factors.columns,
    ).sort_values(["instrument_id", "effective_date"], kind="stable").reset_index(drop=True)
    selected_actions = selected_actions.loc[
        selected_actions.index.isin(retained_action_indexes)
    ].sort_values(
        ["instrument_id", "ex_date", "action_type"], kind="stable",
    ).reset_index(drop=True)
    report = {
        "policy": "corporate-action-factor-r2-v1",
        "instrument_ids": tuple(sorted(target)),
        "action_count": len(selected_actions),
        "factor_count": len(reconciled_factors),
        "verified_events": tuple(verified),
        "derived_cash_pay_date_action_ids": tuple(sorted(derived_cash_pay_dates)),
        "derived_share_listing_date_action_ids": tuple(
            sorted(derived_share_listing_dates)
        ),
        "ignored_nonholder_restructuring_candidates": tuple(sorted(ignored_restructuring)),
        "duplicate_factor_events": tuple(sorted(
            (
                str(factor_rows_source[index]["instrument_id"]),
                str(factor_rows_source[index]["effective_date"]),
            )
            for index in duplicate_factor_indexes
        )),
        "technical_factor_events": tuple(sorted(
            (
                str(factor_rows_source[index]["instrument_id"]),
                str(factor_rows_source[index]["effective_date"]),
            )
            for index in technical_factor_indexes
        )),
        "out_of_lifecycle_factor_events": out_of_lifecycle_factors,
        "official_etf_cash_factor_recovery": official_etf_recovery,
        "corroboration": corroboration_report,
        "missing_factors": missing_factors_tuple,
        "missing_actions": missing_actions_tuple,
    }
    return CorporateActionReconciliationResult(
        selected_actions,
        reconciled_factors,
        stable_digest(report),
        report,
    )


def _recover_official_etf_cash_factors(
    *,
    instruments: pd.DataFrame,
    actions: pd.DataFrame,
    factors: pd.DataFrame,
    daily_bars: pd.DataFrame | None,
) -> tuple[pd.DataFrame, tuple[Mapping[str, Any], ...]]:
    """Quarantine an exact conflicting ETF factor in favour of official cash terms."""

    if "asset_type" not in instruments:
        return factors, ()
    etfs = set(map(str, instruments.loc[
        instruments["asset_type"].astype(str).eq("etf"), "instrument_id",
    ]))
    if not etfs or actions.empty or factors.empty:
        return factors, ()
    working = factors.copy()
    previous_close = _previous_close_lookup(daily_bars)
    recovered: list[Mapping[str, Any]] = []
    for key, group in actions.groupby(["instrument_id", "ex_date"], sort=True):
        event_key = (str(key[0]), str(key[1]))
        if event_key[0] not in etfs or set(map(str, group["action_type"])) != {
            CorporateActionType.CASH_DIVIDEND.value
        }:
            continue
        exact_indexes = working.index[
            working["instrument_id"].astype(str).eq(event_key[0])
            & working["effective_date"].astype(str).eq(event_key[1])
        ].tolist()
        if len(exact_indexes) != 1:
            continue
        factor_index = exact_indexes[0]
        primary = working.loc[factor_index].to_dict()
        mismatches = _economic_mismatches(
            group,
            _factor_raw(
                primary.get("source_payload"), instrument_id=event_key[0],
            ),
        )
        if not mismatches:
            continue
        expected = _expected_action_price_multiplier(
            group, previous_close.get(event_key),
        )
        if expected is None:
            continue
        official_raw = _action_group_raw_economics(group)
        identity = {
            "kind": "official-etf-cash-factor-r2-v1",
            "instrument_id": event_key[0],
            "effective_date": event_key[1],
            "action_ids": tuple(sorted(map(str, group["action_id"]))),
            "quarantined_factor_id": _optional_text(primary.get("factor_id")),
        }
        working.at[factor_index, "factor_id"] = (
            f"factor-{stable_digest(identity)[:24]}"
        )
        working.at[factor_index, "known_date"] = min(map(str, group["known_date"]))
        working.at[factor_index, "price_multiplier"] = expected
        working.at[factor_index, "field_lineage"] = canonical_json({
            "kind": "official_etf_cash_factor_recovery_r2",
            "semantics": (
                "official implementation cash amount and previous raw close derive "
                "the canonical adjustment; the exact conflicting vendor factor is quarantined"
            ),
            "quarantined_factor_id": _optional_text(primary.get("factor_id")),
            "mismatches": mismatches,
        })
        working.at[factor_index, "source_payload"] = canonical_json({
            "raw": official_raw,
            "action_payloads": tuple(map(str, group["source_payload"])),
            "quarantined_factor_payload": _optional_text(primary.get("source_payload")),
        })
        working.at[factor_index, "source_provider"] = (
            "canonical-official-etf-cash-factor-r2-v1"
        )
        action_observation_ids = tuple(sorted(set(
            value for value in map(_optional_text, group["source_observation_id"])
            if value is not None
        )))
        working.at[factor_index, "source_observation_id"] = (
            action_observation_ids[0] if len(action_observation_ids) == 1 else None
        )
        observed = tuple(sorted(
            value for value in map(_optional_text, group["observed_at"])
            if value is not None
        ))
        if observed:
            working.at[factor_index, "observed_at"] = observed[-1]
        recovered.append({
            "instrument_id": event_key[0],
            "effective_date": event_key[1],
            "cash_per_share": official_raw.get("interest"),
            "price_multiplier": expected,
            "quarantined_factor_id": _optional_text(primary.get("factor_id")),
            "mismatches": mismatches,
        })
    return working, tuple(recovered)


def _factor_raw(
    value: Any, *, instrument_id: str | None = None,
) -> Mapping[str, Any]:
    if value is None or value is pd.NA or pd.isna(value):
        return {}
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError as exc:
        if instrument_id is not None:
            raise _scoped_reconciliation_error(
                "Adjustment factor has invalid source payload", (instrument_id,),
            ) from exc
        raise SnapshotNotReadyError("Adjustment factor has invalid source payload") from exc
    raw = payload.get("raw") if isinstance(payload, Mapping) else None
    return raw if isinstance(raw, Mapping) else {}


def _apply_corroborating_factor_evidence(
    *,
    instruments: pd.DataFrame,
    actions: pd.DataFrame,
    primary_factors: pd.DataFrame,
    corroborating_factors: pd.DataFrame | None,
    daily_bars: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame, Mapping[str, Any]]:
    if corroborating_factors is None:
        return actions, primary_factors, {
            "mode": "not_requested",
            "recovered_official_actions": (),
            "synthesized_actions": (),
            "quarantined_primary_factors": (),
        }

    working_actions = actions.copy()
    working_factors = primary_factors.copy()
    secondary_rows = corroborating_factors.to_dict("records")
    primary_rows = working_factors.to_dict("records")
    action_groups = {
        (str(instrument_id), str(ex_date)): group
        for (instrument_id, ex_date), group in working_actions.groupby(
            ["instrument_id", "ex_date"], sort=True,
        )
    }
    previous_close = _previous_close_lookup(daily_bars)
    recovered: list[Mapping[str, Any]] = []
    synthesized_factor_rows: list[dict[str, Any]] = []
    recovered_action_keys: set[tuple[str, str]] = set()

    for key, group in sorted(action_groups.items()):
        already_proven = any(
            str(row["instrument_id"]) == key[0]
            and _date_distance(str(row["effective_date"]), key[1]) <= 31
            and not _economic_mismatches(
                group,
                _factor_raw(
                    row.get("source_payload"),
                    instrument_id=str(row["instrument_id"]),
                ),
            )
            for row in primary_rows
        )
        if already_proven:
            continue
        expected = _expected_action_price_multiplier(
            group,
            previous_close.get(key),
        )
        if expected is None:
            continue
        matched = _matching_secondary_factor(
            secondary_rows,
            instrument_id=key[0],
            event_date=key[1],
            expected_multiplier=expected,
            max_days=31,
            relative_tolerance=0.03,
        )
        if matched is None:
            continue
        secondary, relative = matched
        nearby_material_primary = [
            row for row in primary_rows
            if str(row["instrument_id"]) == key[0]
            and _date_distance(str(row["effective_date"]), key[1]) <= 31
            and _has_position_economics(_factor_raw(
                row.get("source_payload"), instrument_id=str(row["instrument_id"]),
            ))
        ]
        primary_agrees = any(
            abs(float(row["price_multiplier"]) - float(secondary["price_multiplier"]))
            / float(secondary["price_multiplier"]) <= 0.02
            for row in nearby_material_primary
        )
        independent_secondary_agrees = any(
            str(row.get("source_provider")) != str(secondary.get("source_provider"))
            and str(row["instrument_id"]) == key[0]
            and _date_distance(str(row["effective_date"]), key[1]) <= 31
            and float(row["price_multiplier"]) > 0
            and abs(
                float(row["price_multiplier"])
                - float(secondary["price_multiplier"])
            ) / float(secondary["price_multiplier"]) <= 0.02
            for row in secondary_rows
        )
        official_raw = _action_group_raw_economics(group)
        primary_is_official_cash_subset = (
            set(map(str, group["action_type"]))
            == {CorporateActionType.CASH_DIVIDEND.value}
            and official_raw["interest"] > 0
            and any(
                str(row["effective_date"]) == key[1]
                and 0 < _number(_factor_raw(
                    row.get("source_payload"), instrument_id=str(row["instrument_id"]),
                ).get("interest"))
                < official_raw["interest"] - 1e-12
                and all(
                    abs(_number(_factor_raw(
                        row.get("source_payload"),
                        instrument_id=str(row["instrument_id"]),
                    ).get(field)))
                    <= 1e-12
                    for field in ("stockBonus", "stockGift", "allotNum", "allotPrice")
                )
                for row in nearby_material_primary
            )
        )
        if (
            nearby_material_primary
            and not primary_agrees
            and not independent_secondary_agrees
            and not primary_is_official_cash_subset
        ):
            # A relaxed official/theoretical price comparison is safe only when
            # two independent price-factor sources agree with each other.  The
            # second source may be another audit provider when the primary feed
            # omitted one component of a same-day official distribution.
            continue
        raw = _action_group_raw_economics(group)
        known_date = min(map(str, group["known_date"]))
        identity = {
            "kind": "official-action-secondary-factor-r2",
            "instrument_id": key[0],
            "effective_date": key[1],
            "action_ids": tuple(sorted(map(str, group["action_id"]))),
            "secondary_factor_id": _optional_text(secondary.get("factor_id")),
        }
        synthesized_factor_rows.append({
            "factor_id": f"factor-{stable_digest(identity)[:24]}",
            "instrument_id": key[0],
            "effective_date": key[1],
            "known_date": known_date,
            "price_multiplier": float(secondary["price_multiplier"]),
            "field_lineage": canonical_json({
                "kind": "official_action_secondary_factor_reconciliation_r2",
                "expected_price_multiplier": expected,
                "relative_difference": relative,
                "secondary_provider": _optional_text(secondary.get("source_provider")),
                "secondary_effective_date": str(secondary["effective_date"]),
            }),
            "source_payload": canonical_json({
                "raw": raw,
                "action_payloads": tuple(map(str, group["source_payload"])),
                "secondary_factor_payload": _optional_text(
                    secondary.get("source_payload")
                ),
            }),
            "source_provider": "canonical-action-secondary-factor-r2",
            "source_observation_id": _optional_text(
                secondary.get("source_observation_id")
            ),
            "observed_at": _optional_text(secondary.get("observed_at")),
        })
        recovered_action_keys.add(key)
        recovered.append({
            "instrument_id": key[0],
            "effective_date": key[1],
            "expected_price_multiplier": expected,
            "secondary_price_multiplier": float(secondary["price_multiplier"]),
            "secondary_provider": _optional_text(secondary.get("source_provider")),
            "secondary_effective_date": str(secondary["effective_date"]),
        })

    quarantined_indexes: set[int] = set()
    quarantine_evidence: list[Mapping[str, Any]] = []
    for index, row in enumerate(primary_rows):
        raw = _factor_raw(
            row.get("source_payload"), instrument_id=str(row["instrument_id"]),
        )
        if not _has_position_economics(raw):
            continue
        factor_key = (str(row["instrument_id"]), str(row["effective_date"]))
        if any(
            key[0] == factor_key[0]
            and _date_distance(key[1], factor_key[1]) <= 31
            and not _economic_mismatches(group, raw)
            for key, group in action_groups.items()
        ):
            continue
        if any(
            key[0] == factor_key[0]
            and _date_distance(key[1], factor_key[1]) <= 31
            for key in recovered_action_keys
        ):
            quarantined_indexes.add(index)
            quarantine_evidence.append({
                "instrument_id": factor_key[0],
                "effective_date": factor_key[1],
                "reason": "conflicts_with_official_action_confirmed_by_secondary_factor",
            })

    synthesized_action_rows: list[dict[str, Any]] = []
    replaced_action_indexes: set[int] = set()
    instrument_types = {
        str(row["instrument_id"]): str(row.get("asset_type") or "stock")
        for row in instruments.to_dict("records")
    }
    for index, row in enumerate(primary_rows):
        if index in quarantined_indexes:
            continue
        raw = _factor_raw(
            row.get("source_payload"), instrument_id=str(row["instrument_id"]),
        )
        if not _has_position_economics(raw):
            continue
        factor_key = (str(row["instrument_id"]), str(row["effective_date"]))
        if any(
            key[0] == factor_key[0]
            and _date_distance(key[1], factor_key[1]) <= 31
            and not _economic_mismatches(group, raw)
            for key, group in action_groups.items()
        ):
            continue
        nearby_conflicts = [
            group for key, group in action_groups.items()
            if key[0] == factor_key[0]
            and _date_distance(key[1], factor_key[1]) <= 31
        ]
        if nearby_conflicts and not all(
            _is_nonholder_restructuring_candidate(group) for group in nearby_conflicts
        ):
            continue
        matched = _matching_secondary_factor(
            secondary_rows,
            instrument_id=factor_key[0],
            event_date=factor_key[1],
            expected_multiplier=float(row["price_multiplier"]),
            max_days=60,
        )
        if matched is None:
            quarantined_indexes.add(index)
            quarantine_evidence.append({
                "instrument_id": factor_key[0],
                "effective_date": factor_key[1],
                "reason": "material_primary_factor_not_independently_corroborated",
            })
            continue
        secondary, relative = matched
        for group in nearby_conflicts:
            replaced_action_indexes.update(map(int, group.index))
        # MiniQMT supplies the canonical market-effect date.  Secondary cumulative
        # factor feeds can publish the same economics on a later bookkeeping date;
        # retain that raw date in lineage without moving the executable event.
        effect_date = factor_key[1]
        rows = _actions_from_factor_economics(
            instrument_id=factor_key[0],
            asset_type=instrument_types[factor_key[0]],
            effect_date=effect_date,
            raw=raw,
            primary=row,
            secondary=secondary,
            relative_difference=relative,
            columns=working_actions.columns,
        )
        if not rows:
            quarantined_indexes.add(index)
            quarantine_evidence.append({
                "instrument_id": factor_key[0],
                "effective_date": factor_key[1],
                "reason": "corroborated_factor_has_no_executable_holding_action",
            })
            continue
        synthesized_action_rows.extend(rows)

    if replaced_action_indexes:
        working_actions = working_actions.loc[
            ~working_actions.index.isin(replaced_action_indexes)
        ].copy()
    if synthesized_action_rows:
        working_actions = pd.concat((
            working_actions,
            pd.DataFrame(synthesized_action_rows, columns=working_actions.columns),
        ), ignore_index=True)
    if quarantined_indexes:
        working_factors = working_factors.iloc[
            [index for index in range(len(working_factors)) if index not in quarantined_indexes]
        ].copy()
    if synthesized_factor_rows:
        working_factors = pd.concat((
            working_factors,
            pd.DataFrame(synthesized_factor_rows, columns=working_factors.columns),
        ), ignore_index=True)
    synthesized_ids = tuple(sorted(map(
        str,
        (row["action_id"] for row in synthesized_action_rows),
    )))
    return working_actions, working_factors, {
        "mode": "complete_candidate_audit",
        "recovered_official_actions": tuple(recovered),
        "synthesized_actions": synthesized_ids,
        "replaced_unverified_action_ids": tuple(sorted(
            map(str, actions.loc[actions.index.isin(replaced_action_indexes), "action_id"])
        )),
        "quarantined_primary_factors": tuple(quarantine_evidence),
    }


def _previous_close_lookup(
    daily_bars: pd.DataFrame | None,
) -> dict[tuple[str, str], float]:
    if daily_bars is None or daily_bars.empty or "previous_close" not in daily_bars:
        return {}
    values = pd.to_numeric(daily_bars["previous_close"], errors="coerce")
    valid = values.notna() & values.gt(0)
    instruments = daily_bars.loc[valid, "instrument_id"].astype(str)
    sessions = daily_bars.loc[valid, "session_date"].astype(str)
    return {
        (instrument_id, session_date): float(value)
        for instrument_id, session_date, value in zip(
            instruments,
            sessions,
            values.loc[valid],
        )
    }


def _action_group_raw_economics(actions: pd.DataFrame) -> Mapping[str, float]:
    cash = sum(_number(value) for value in actions.loc[
        actions["action_type"].astype(str).eq(CorporateActionType.CASH_DIVIDEND.value),
        "cash_per_share",
    ])
    shares = sum(_number(value) for value in actions.loc[
        actions["action_type"].astype(str).eq(CorporateActionType.STOCK_DIVIDEND.value),
        "share_ratio",
    ])
    splits = actions.loc[
        actions["action_type"].astype(str).eq(CorporateActionType.SPLIT.value)
    ]
    if len(splits) == 1:
        shares = _number(splits.iloc[0]["quantity_multiplier"]) - 1.0
    rights = actions.loc[
        actions["action_type"].astype(str).eq(CorporateActionType.RIGHTS_ISSUE.value)
    ]
    return {
        "interest": cash,
        "stockBonus": shares,
        "stockGift": 0.0,
        "allotNum": 0.0 if rights.empty else _number(rights.iloc[0]["share_ratio"]),
        "allotPrice": 0.0 if rights.empty else _number(rights.iloc[0]["rights_price"]),
    }


def _expected_action_price_multiplier(
    actions: pd.DataFrame,
    previous_close: float | None,
) -> float | None:
    raw = _action_group_raw_economics(actions)
    cash = raw["interest"]
    shares = raw["stockBonus"] + raw["stockGift"]
    rights = raw["allotNum"]
    rights_price = raw["allotPrice"]
    denominator = 1.0 + shares + rights
    if denominator <= 0:
        return None
    if cash == 0 and rights == 0:
        return 1.0 / denominator
    if previous_close is None or previous_close <= 0:
        return None
    theoretical = previous_close - cash + rights * rights_price
    return None if theoretical <= 0 else theoretical / (previous_close * denominator)


def _matching_secondary_factor(
    rows: list[dict[str, Any]],
    *,
    instrument_id: str,
    event_date: str,
    expected_multiplier: float,
    max_days: int,
    relative_tolerance: float = 0.02,
) -> tuple[dict[str, Any], float] | None:
    candidates: list[tuple[int, float, str, str, int, dict[str, Any]]] = []
    for index, row in enumerate(rows):
        if str(row.get("instrument_id")) != instrument_id:
            continue
        distance = _date_distance(str(row.get("effective_date")), event_date)
        multiplier = _number(row.get("price_multiplier"))
        if distance > max_days or multiplier <= 0:
            continue
        relative = abs(multiplier - expected_multiplier) / expected_multiplier
        if relative <= relative_tolerance:
            candidates.append((
                distance,
                relative,
                str(row.get("source_provider")),
                str(row.get("effective_date")),
                index,
                row,
            ))
    if not candidates:
        return None
    _, relative, _, _, _, selected = min(candidates, key=lambda item: item[:-1])
    return selected, relative


def _actions_from_factor_economics(
    *,
    instrument_id: str,
    asset_type: str,
    effect_date: str,
    raw: Mapping[str, Any],
    primary: Mapping[str, Any],
    secondary: Mapping[str, Any],
    relative_difference: float,
    columns: pd.Index,
) -> list[dict[str, Any]]:
    cash = _number(raw.get("interest"))
    shares = _number(raw.get("stockBonus")) + _number(raw.get("stockGift"))
    rights = _number(raw.get("allotNum"))
    rights_price = _number(raw.get("allotPrice"))
    common = {
        "instrument_id": instrument_id,
        "known_date": effect_date,
        "record_date": effect_date,
        "ex_date": effect_date,
        "field_lineage": canonical_json({
            "kind": "dual_factor_holding_action_recovery_r2",
            "known_date_semantics": "conservative effective-date fallback",
            "primary_provider": _optional_text(primary.get("source_provider")),
            "secondary_provider": _optional_text(secondary.get("source_provider")),
            "secondary_effective_date": str(secondary.get("effective_date")),
            "relative_difference": relative_difference,
        }),
        "source_payload": canonical_json({
            "primary_factor_payload": _optional_text(primary.get("source_payload")),
            "primary_factor_observation_id": _optional_text(
                primary.get("source_observation_id")
            ),
            "secondary_factor_payload": _optional_text(secondary.get("source_payload")),
            "secondary_factor_observation_id": _optional_text(
                secondary.get("source_observation_id")
            ),
            "raw_economics": raw,
        }),
        "source_provider": "canonical-dual-factor-action-r2",
        "source_observation_id": pd.NA,
        "observed_at": max(filter(None, (
            _optional_text(primary.get("observed_at")),
            _optional_text(secondary.get("observed_at")),
        )), default=None),
    }
    rows: list[dict[str, Any]] = []

    def row(action_type: CorporateActionType, **values: Any) -> None:
        identity = {
            "kind": "dual-factor-action-r2",
            "instrument_id": instrument_id,
            "effect_date": effect_date,
            "action_type": action_type.value,
            "economics": values,
        }
        item = {column: pd.NA for column in columns}
        item.update(common)
        item.update({
            "action_id": f"act-{stable_digest(identity)[:24]}",
            "action_type": action_type.value,
            "pay_date": pd.NA,
            "listing_date": pd.NA,
            "cash_per_share": pd.NA,
            "share_ratio": pd.NA,
            "rights_price": pd.NA,
            "quantity_multiplier": pd.NA,
            **values,
        })
        rows.append(item)

    if cash > 0:
        row(
            CorporateActionType.CASH_DIVIDEND,
            pay_date=effect_date,
            cash_per_share=cash,
        )
    if abs(shares) > 1e-12:
        multiplier = 1.0 + shares
        if multiplier <= 0:
            return []
        if asset_type == "etf" or shares < 0:
            row(
                CorporateActionType.SPLIT,
                listing_date=effect_date,
                quantity_multiplier=multiplier,
            )
        else:
            row(
                CorporateActionType.STOCK_DIVIDEND,
                listing_date=effect_date,
                share_ratio=shares,
            )
    if rights > 0 and rights_price > 0:
        row(
            CorporateActionType.RIGHTS_ISSUE,
            pay_date=effect_date,
            listing_date=effect_date,
            share_ratio=rights,
            rights_price=rights_price,
        )
    return rows


def _economic_mismatches(actions: pd.DataFrame, raw: Mapping[str, Any]) -> tuple[str, ...]:
    mismatches: list[str] = []
    cash = sum(_number(item) for item in actions.loc[
        actions["action_type"].astype(str).eq(CorporateActionType.CASH_DIVIDEND.value),
        "cash_per_share",
    ])
    shares = sum(_number(item) for item in actions.loc[
        actions["action_type"].astype(str).eq(CorporateActionType.STOCK_DIVIDEND.value),
        "share_ratio",
    ])
    rights = actions.loc[
        actions["action_type"].astype(str).eq(CorporateActionType.RIGHTS_ISSUE.value)
    ]
    splits = actions.loc[
        actions["action_type"].astype(str).eq(CorporateActionType.SPLIT.value)
    ]
    raw_cash = _number(raw.get("interest"))
    if not _close(cash, raw_cash, absolute=1e-6, relative=1e-4):
        mismatches.append("cash_per_share")
    raw_shares = _number(raw.get("stockBonus")) + _number(raw.get("stockGift"))
    if splits.empty and not _close(shares, raw_shares, absolute=1e-6, relative=1e-4):
        mismatches.append("share_ratio")
    if len(rights) > 1:
        mismatches.append("multiple_rights_events")
    elif len(rights) == 1:
        row = rights.iloc[0]
        entitlement = _number(row["share_ratio"])
        allotted = _number(raw.get("allotNum"))
        # CNInfo reports the maximum subscription entitlement per holding;
        # MiniQMT reports the market-wide shares actually allotted.  The latter
        # may be lower after non-participation, but cannot exceed entitlement.
        if allotted <= 0 or allotted > entitlement + max(1e-6, entitlement * 1e-4):
            mismatches.append("rights_ratio")
        if not _close(_number(row["rights_price"]), _number(raw.get("allotPrice"))):
            mismatches.append("rights_price")
    elif not _close(_number(raw.get("allotNum")), 0.0):
        mismatches.append("rights_ratio")
    if len(splits) > 1:
        mismatches.append("multiple_split_events")
    elif len(splits) == 1:
        expected = _number(splits.iloc[0]["quantity_multiplier"])
        observed = 1.0 + raw_shares
        if not _close(expected, observed, absolute=1e-5, relative=0.005):
            mismatches.append("quantity_multiplier")
    if not raw:
        mismatches.append("missing_factor_raw_evidence")
    return tuple(sorted(set(mismatches)))


def _number(value: Any) -> float:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return 0.0 if pd.isna(parsed) else float(parsed)


def _close(left: float, right: float, *, absolute: float = 1e-6, relative: float = 1e-4) -> bool:
    return abs(left - right) <= max(absolute, relative * max(abs(left), abs(right), 1.0))


def _has_position_economics(raw: Mapping[str, Any]) -> bool:
    return any(
        abs(_number(raw.get(key))) > 1e-12
        for key in ("interest", "stockBonus", "stockGift", "allotNum")
    )


def _date_distance(left: str, right: str) -> int:
    try:
        return abs((date.fromisoformat(left[:10]) - date.fromisoformat(right[:10])).days)
    except ValueError:
        return 10 ** 9


def _is_nonholder_restructuring_candidate(actions: pd.DataFrame) -> bool:
    if actions.empty or not actions["action_type"].astype(str).eq(
        CorporateActionType.STOCK_DIVIDEND.value
    ).all():
        return False
    return all(
        any(marker in str(value) for marker in ("重整转增", "股改分红"))
        for value in actions["source_payload"]
    )


def _optional_text(value: Any) -> str | None:
    return None if value is None or value is pd.NA or pd.isna(value) else str(value)
