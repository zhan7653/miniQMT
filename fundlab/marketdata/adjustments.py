from __future__ import annotations

from datetime import date
from math import prod
import pandas as pd

from fundlab.common.canonical import canonical_json
from fundlab.marketdata.contracts import PriceMode


PRICE_COLUMNS = ("open", "high", "low", "close")


def derive_ratio_adjusted_bars(
    raw_bars: pd.DataFrame,
    factors: pd.DataFrame,
    *,
    as_of: date,
) -> pd.DataFrame:
    """Derive a forward-adjusted research view without using future factor knowledge.

    ``price_multiplier`` is an event ratio applied to prices strictly before the
    event's effective date.  Only events both effective and known by ``as_of``
    participate.  Raw volume and amount remain raw and are never rewritten.
    """

    result = raw_bars.copy(deep=True)
    if result.empty:
        return result
    if not set(result["price_mode"].astype(str)) <= {PriceMode.RAW.value}:
        raise ValueError("Ratio adjustment accepts raw bars only")

    visible = factors.copy(deep=True)
    if not visible.empty:
        visible = visible[
            (visible["effective_date"].astype(str) <= as_of.isoformat())
            & (visible["known_date"].astype(str) <= as_of.isoformat())
        ]
        if (pd.to_numeric(visible["price_multiplier"], errors="coerce") <= 0).any():
            raise ValueError("Adjustment factors must be positive")

    result["price_mode"] = PriceMode.ADJUSTED.value
    result["source_provider"] = "canonical-ratio-adjustment"
    # Previous-close and daily limit fields belong to the raw execution view.  Keeping
    # them in a research-adjusted row would suggest false cross-event comparability.
    for execution_only in ("previous_close", "limit_up", "limit_down"):
        if execution_only in result:
            result[execution_only] = pd.NA
    lineage_payloads: list[str] = []
    source_payloads: list[str] = []
    for index, row in result.iterrows():
        selected = visible[
            (visible["instrument_id"].astype(str) == str(row["instrument_id"]))
            & (visible["effective_date"].astype(str) > str(row["session_date"]))
        ]
        multiplier = prod(float(value) for value in selected["price_multiplier"]) if not selected.empty else 1.0
        for column in PRICE_COLUMNS:
            value = row.get(column)
            if value is not None and not pd.isna(value):
                result.at[index, column] = float(value) * multiplier
        factor_ids = tuple(sorted(map(str, selected.get("factor_id", ()))))
        factor_observations = tuple(
            sorted(set(map(str, selected.get("source_observation_id", ()))))
        )
        derivation = {
            "method": "point_in_time_ratio_v1",
            "as_of": as_of.isoformat(),
            "price_multiplier": multiplier,
            "factor_ids": factor_ids,
            "factor_observation_ids": factor_observations,
            "raw_observation_id": str(row.get("source_observation_id", "")),
        }
        lineage_payloads.append(canonical_json({"derived_price": derivation}))
        source_payloads.append(canonical_json(derivation))
    result["field_lineage"] = lineage_payloads
    result["source_payload"] = source_payloads
    return result.sort_values(["session_date", "instrument_id"], kind="stable").reset_index(drop=True)


def visible_factor_ids(factors: pd.DataFrame, *, as_of: date) -> tuple[str, ...]:
    """Return factor identities visible at a simulated point in time."""

    if factors.empty:
        return ()
    visible = factors[
        (factors["effective_date"].astype(str) <= as_of.isoformat())
        & (factors["known_date"].astype(str) <= as_of.isoformat())
    ]
    return tuple(sorted(map(str, visible["factor_id"])))
