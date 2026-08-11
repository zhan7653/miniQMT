from __future__ import annotations

from typing import Any

import pandas as pd

from fundlab.common.canonical import canonical_json


OFFICIAL_COMPONENT_SCOPES = {
    "sh-stock-main": {"exchange": "SH", "asset_type": "stock", "board": "main"},
    "sh-stock-star": {"exchange": "SH", "asset_type": "stock", "board": "star"},
    "sz-stock": {"exchange": "SZ", "asset_type": "stock"},
    "sh-etf": {"exchange": "SH", "asset_type": "etf"},
    "sz-etf": {"exchange": "SZ", "asset_type": "etf"},
}
OFFICIAL_COMPONENT_ENDPOINTS = {
    "sh-stock-main": ("sse-main-stock-list",),
    "sh-stock-star": ("sse-star-stock-list",),
    "sz-stock": ("szse-a-stock-list",),
    "sh-etf": ("sse-etf-scale-list", "sse-current-full-etf-list"),
    "sz-etf": ("szse-etf-scale-daily", "szse-current-etf-list"),
}


def component_for_row(row: dict[str, object]) -> str:
    exchange = str(row["exchange"])
    asset_type = str(row["asset_type"])
    if exchange == "SH" and asset_type == "stock":
        return f"sh-stock-{row['board']}"
    if exchange == "SZ" and asset_type == "stock":
        return "sz-stock"
    if exchange == "SH" and asset_type == "etf":
        return "sh-etf"
    if exchange == "SZ" and asset_type == "etf":
        return "sz-etf"
    raise AssertionError(f"unexpected fixture component row: {row}")


def with_component_closure(
    frame: pd.DataFrame,
    metadata: dict[str, Any],
    unavailable_components: tuple[str, ...],
) -> pd.DataFrame:
    """Attach a contract-valid closure to a small exchange fixture."""

    frame = frame.copy()
    successful = tuple(
        component for component in OFFICIAL_COMPONENT_SCOPES
        if component not in unavailable_components
    )
    admitted_by_component = {
        component: tuple(sorted(map(
            str,
            frame.loc[frame.apply(
                lambda row: component_for_row(row.to_dict()) == component,
                axis=1,
            ), "instrument_id"],
        )))
        for component in successful
    }
    authoritative_endpoint = {
        "sh-stock-main": "sse-main-stock-list",
        "sh-stock-star": "sse-star-stock-list",
        "sz-stock": "szse-a-stock-list",
        "sh-etf": "sse-current-full-etf-list",
        "sz-etf": "szse-etf-scale-daily",
    }
    response_hashes = metadata["response_sha256"]
    endpoint_counts = metadata["endpoint_counts"]
    response_counts = metadata["endpoint_response_counts"]
    assert isinstance(response_hashes, dict)
    assert isinstance(endpoint_counts, dict)
    assert isinstance(response_counts, dict)
    for index, row in frame.iterrows():
        component = component_for_row(row.to_dict())
        endpoint = authoritative_endpoint[component]
        frame.at[index, "field_lineage"] = canonical_json({
            "endpoint": endpoint,
            "upstream": "exchange-public",
            "response_sha256": response_hashes[endpoint],
        })
    closure: dict[str, Any] = {}
    partition_names = (
        "admitted", "pending_onboarding", "future_as_of", "duplicate_identity",
        "membership_conflict", "intentionally_out_of_component",
    )
    for component in successful:
        admitted_ids = admitted_by_component[component]
        endpoints = OFFICIAL_COMPONENT_ENDPOINTS[component]
        membership: dict[str, Any] = {}
        for endpoint in endpoints:
            partitions = {name: [] for name in partition_names}
            partitions["admitted"] = list(admitted_ids)
            product_values = (
                {instrument_id: "03" for instrument_id in admitted_ids}
                if endpoint == "sse-current-full-etf-list" else
                {instrument_id: "ETF" for instrument_id in admitted_ids}
                if endpoint == "szse-current-etf-list" else {}
            )
            membership[endpoint] = {
                "response_sha256": response_hashes[endpoint],
                "raw_row_count": response_counts[endpoint],
                "effective_row_count": endpoint_counts[endpoint],
                "raw_ids": list(admitted_ids),
                "duplicate_row_count": 0,
                "duplicate_occurrences": {},
                "authoritative_for_master": endpoint == authoritative_endpoint[component],
                "comparison_product_class_filtered_ids": [],
                "comparison_product_class_values": product_values,
                "admission_partitions": partitions,
            }
        record: dict[str, Any] = {
            "component": component,
            "scope": OFFICIAL_COMPONENT_SCOPES[component],
            "endpoints": list(endpoints),
            "admitted_ids": list(admitted_ids),
            "endpoint_membership": membership,
        }
        if component in {"sh-etf", "sz-etf"}:
            left, right = endpoints
            comparable = list(admitted_ids)
            record["intersection"] = {
                "left_endpoint": left,
                "right_endpoint": right,
                "left_comparable_ids": comparable,
                "right_comparable_ids": comparable,
                "intersection_ids": comparable,
                "membership_conflict_ids": [],
            }
        closure[component] = record
    metadata["component_closure"] = closure
    return frame
