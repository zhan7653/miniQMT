from __future__ import annotations

from datetime import date, datetime
from copy import deepcopy
from io import BytesIO
from importlib import import_module, util
import json
import re
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import warnings

import pandas as pd

from fundlab.marketdata.contracts import (
    CoverageClaim,
    MarketTable,
    ObservationError,
    ObservationPayload,
    ProviderCapability,
    ProviderUnavailableError,
    ProviderRequest,
)
from fundlab.marketdata.sources.base import frame_hash, now_utc, source_payload


_INSTRUMENT_COLUMNS = (
    "instrument_id", "exchange", "local_code", "asset_type", "name",
    "currency", "listed_date", "delisted_date", "board",
    "exchange_product_class", "buy_lot", "price_tick", "sell_delay_sessions",
    "price_limit_ratio", "field_lineage", "source_payload",
)


class ExchangePublicUniverseProvider:
    """Current SH/SZ stock and ETF membership from exchange-published lists."""

    name = "exchange-public"
    backend_group = "exchange-public"
    capabilities = frozenset({ProviderCapability.INSTRUMENTS})

    def __init__(self, *, client: Any | None = None) -> None:
        self._client = client

    @property
    def available(self) -> bool:
        return self._client is not None or util.find_spec("akshare") is not None

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        if request.capability is not ProviderCapability.INSTRUMENTS:
            raise ValueError(f"Exchange universe does not support {request.capability.value}")
        if request.instrument_ids:
            raise ValueError("Exchange current-universe observation does not accept instrument_ids")
        exchanges = tuple(sorted({
            str(item).upper()
            for item in request.parameters.get("exchanges", ("SH", "SZ"))
        }))
        assets = tuple(sorted({
            str(item).lower()
            for item in request.parameters.get("asset_types", ("stock", "etf"))
        }))
        if not set(exchanges) <= {"SH", "SZ"} or not set(assets) <= {"stock", "etf"}:
            raise ValueError("Exchange universe supports only SH/SZ stock/ETF")
        raw_as_of = request.parameters.get("as_of_date")
        if raw_as_of is None:
            raise ValueError("Exchange universe requires parameters.as_of_date")
        as_of = date.fromisoformat(str(raw_as_of)[:10])
        client = self._client or import_module("akshare")

        rows: list[dict[str, Any]] = []
        hashes: dict[str, str] = {}
        endpoint_counts: dict[str, int] = {}
        endpoint_response_counts: dict[str, int] = {}
        future_exclusions: dict[str, tuple[str, ...]] = {}
        pending_onboarding: dict[str, dict[str, Any]] = {}
        identity_evidence: dict[str, list[dict[str, Any]]] = {}
        unavailable_components: dict[str, dict[str, Any]] = {}
        successful_endpoint_membership: dict[str, dict[str, dict[str, Any]]] = {}
        component_intersections: dict[str, dict[str, Any]] = {}

        def merge_pending(instrument_id: str, entry: Mapping[str, Any]) -> None:
            current = pending_onboarding.get(instrument_id)
            if current is None:
                pending_onboarding[instrument_id] = dict(entry)
                return
            for field in ("missing_fields", "invalid_fields", "conflict_fields"):
                current[field] = tuple(sorted(set(current[field]) | set(entry[field])))
            if "membership_evidence" in entry:
                current["membership_evidence"] = entry["membership_evidence"]
            if "duplicate_identity" in entry["conflict_fields"]:
                current["raw_evidence"] = entry["raw_evidence"]

        def record_duplicate_code_rows(
            duplicates: Mapping[str, list[Mapping[str, Any]]],
            *,
            exchange: str,
            endpoint: str,
            digest: str,
        ) -> None:
            for local, raw_rows in duplicates.items():
                merge_pending(f"{local}.{exchange}", _pending_entry(
                    conflict_fields=("duplicate_identity", "master"),
                    endpoint=endpoint,
                    digest=digest,
                    raw={"records": raw_rows},
                ))

        def record_row(**kwargs: Any) -> None:
            admitted, pending = _row(**kwargs)
            instrument_id = admitted["instrument_id"] if admitted is not None else pending[0]
            identity_evidence.setdefault(instrument_id, []).append({
                "endpoint": kwargs["endpoint"],
                "response_sha256": kwargs["digest"],
                "row": _plain(kwargs["raw"]),
            })
            if len(identity_evidence[instrument_id]) > 1:
                merge_pending(instrument_id, _pending_entry(
                    conflict_fields=("duplicate_identity", "master"),
                    endpoint=kwargs["endpoint"],
                    digest=kwargs["digest"],
                    raw={"records": identity_evidence[instrument_id]},
                ))
                rows[:] = [row for row in rows if row["instrument_id"] != instrument_id]
            elif instrument_id not in pending_onboarding and admitted is not None:
                rows.append(admitted)
            elif pending is not None:
                assert pending is not None
                merge_pending(*pending)

        def component_state() -> tuple[Any, ...]:
            return (
                list(rows),
                dict(hashes),
                dict(endpoint_counts),
                dict(endpoint_response_counts),
                dict(future_exclusions),
                {key: dict(value) for key, value in pending_onboarding.items()},
                {key: list(value) for key, value in identity_evidence.items()},
                deepcopy(successful_endpoint_membership),
                deepcopy(component_intersections),
            )

        def restore_component_state(state: tuple[Any, ...]) -> None:
            (
                saved_rows,
                saved_hashes,
                saved_endpoint_counts,
                saved_endpoint_response_counts,
                saved_future_exclusions,
                saved_pending_onboarding,
                saved_identity_evidence,
                saved_successful_endpoint_membership,
                saved_component_intersections,
            ) = state
            rows[:] = saved_rows
            hashes.clear()
            hashes.update(saved_hashes)
            endpoint_counts.clear()
            endpoint_counts.update(saved_endpoint_counts)
            endpoint_response_counts.clear()
            endpoint_response_counts.update(saved_endpoint_response_counts)
            future_exclusions.clear()
            future_exclusions.update(saved_future_exclusions)
            pending_onboarding.clear()
            pending_onboarding.update(saved_pending_onboarding)
            identity_evidence.clear()
            identity_evidence.update(saved_identity_evidence)
            successful_endpoint_membership.clear()
            successful_endpoint_membership.update(saved_successful_endpoint_membership)
            component_intersections.clear()
            component_intersections.update(saved_component_intersections)

        def record_unavailable(
            component: str,
            *,
            scope: Mapping[str, Any],
            endpoints: tuple[str, ...],
            error: ProviderUnavailableError,
        ) -> None:
            assert error.endpoint in endpoints
            unavailable_components[component] = {
                "component": component,
                "scope": _plain(scope),
                "endpoints": list(endpoints),
                "failed_endpoint": error.endpoint,
                "error_type": type(error).__name__,
                "message": str(error),
            }

        def record_endpoint_membership(
            component: str,
            *,
            endpoint: str,
            frame: pd.DataFrame,
            code_column: str,
            exchange: str,
            authoritative: bool,
            comparison_product_class_column: str | None = None,
        ) -> None:
            occurrences: dict[str, int] = {}
            product_class_values: dict[str, Any] = {}
            for item in frame.to_dict("records"):
                instrument_id = f"{_canonical_local_code(item.get(code_column), endpoint=endpoint)}.{exchange}"
                occurrences[instrument_id] = occurrences.get(instrument_id, 0) + 1
                if comparison_product_class_column is not None:
                    product_class_values[instrument_id] = _plain(
                        item.get(comparison_product_class_column)
                    )
            successful_endpoint_membership.setdefault(component, {})[endpoint] = {
                "raw_row_count": len(frame),
                "effective_row_count": len(frame),
                "raw_ids": sorted(occurrences),
                "duplicate_row_count": sum(count - 1 for count in occurrences.values()),
                "duplicate_occurrences": {
                    instrument_id: count
                    for instrument_id, count in sorted(occurrences.items())
                    if count > 1
                },
                "authoritative_for_master": authoritative,
                "future_as_of_ids": [],
                "comparison_product_class_filtered_ids": [],
                "comparison_product_class_values": product_class_values,
            }

        def set_endpoint_roles(
            component: str,
            endpoint: str,
            *,
            future_as_of_ids: tuple[str, ...] = (),
            comparison_product_class_filtered_ids: tuple[str, ...] = (),
            effective_row_count: int | None = None,
        ) -> None:
            endpoint_membership = successful_endpoint_membership[component][endpoint]
            endpoint_membership["future_as_of_ids"] = list(sorted(future_as_of_ids))
            endpoint_membership["comparison_product_class_filtered_ids"] = list(sorted(
                comparison_product_class_filtered_ids
            ))
            if effective_row_count is not None:
                endpoint_membership["effective_row_count"] = effective_row_count

        if "stock" in assets and "SH" in exchanges:
            for component, endpoint, board, symbol in (
                ("sh-stock-main", "sse-main-stock-list", "main", "主板A股"),
                ("sh-stock-star", "sse-star-stock-list", "star", "科创板"),
            ):
                state = component_state()
                try:
                    frame = _call(
                        endpoint,
                        lambda symbol=symbol: client.stock_info_sh_name_code(symbol=symbol),
                        required_columns=("证券代码", "证券简称", "上市日期"),
                    )
                    hashes[endpoint] = frame_hash(frame)
                    endpoint_response_counts[endpoint] = len(frame)
                    endpoint_counts[endpoint] = len(frame)
                    record_endpoint_membership(
                        component,
                        endpoint=endpoint,
                        frame=frame,
                        code_column="证券代码",
                        exchange="SH",
                        authoritative=True,
                    )
                    for item in frame.to_dict("records"):
                        record_row(
                            code=item.get("证券代码"),
                            name=item.get("证券简称"),
                            exchange="SH",
                            asset_type="stock",
                            listed_date=item.get("上市日期"),
                            board=board,
                            product_class=None,
                            digest=hashes[endpoint],
                            endpoint=endpoint,
                            raw=item,
                        )
                except ProviderUnavailableError as error:
                    restore_component_state(state)
                    record_unavailable(
                        component,
                        scope={"exchange": "SH", "asset_type": "stock", "board": board},
                        endpoints=(endpoint,),
                        error=error,
                    )
        if "stock" in assets and "SZ" in exchanges:
            state = component_state()
            endpoint = "szse-a-stock-list"
            try:
                frame = _call(
                    endpoint,
                    lambda: client.stock_info_sz_name_code(symbol="A股列表"),
                    required_columns=("A股代码", "A股简称", "A股上市日期", "板块"),
                )
                hashes[endpoint] = frame_hash(frame)
                endpoint_response_counts[endpoint] = len(frame)
                endpoint_counts[endpoint] = len(frame)
                record_endpoint_membership(
                    "sz-stock",
                    endpoint=endpoint,
                    frame=frame,
                    code_column="A股代码",
                    exchange="SZ",
                    authoritative=True,
                )
                for item in frame.to_dict("records"):
                    panel = _required_text(item.get("板块"))
                    board = (
                        "chinext" if panel is not None and "创业" in panel
                        else "main" if panel is not None and "主板" in panel
                        else None if panel is None
                        else "unknown"
                    )
                    record_row(
                        code=item.get("A股代码"),
                        name=item.get("A股简称"),
                        exchange="SZ",
                        asset_type="stock",
                        listed_date=item.get("A股上市日期"),
                        board=board,
                        product_class=None,
                        digest=hashes[endpoint],
                        endpoint=endpoint,
                        raw=item,
                    )
            except ProviderUnavailableError as error:
                restore_component_state(state)
                record_unavailable(
                    "sz-stock",
                    scope={"exchange": "SZ", "asset_type": "stock"},
                    endpoints=(endpoint,),
                    error=error,
                )
        def collect_sh_etf() -> None:
            scale_endpoint = "sse-etf-scale-list"
            scale = _call(
                scale_endpoint,
                lambda: client.fund_etf_scale_sse(date=as_of.strftime("%Y%m%d")),
                required_columns=("基金代码", "基金简称"),
            )
            list_endpoint = "sse-current-full-etf-list"
            full = _call(
                list_endpoint,
                client.fund_etf_list_sse if self._client is not None else _fetch_sse_fund_list,
                required_columns=("fundCode", "secNameFull", "listingDate", "subClass"),
            )
            hashes[scale_endpoint] = frame_hash(scale)
            hashes[list_endpoint] = frame_hash(full)
            endpoint_response_counts[scale_endpoint] = len(scale)
            endpoint_response_counts[list_endpoint] = len(full)
            record_endpoint_membership(
                "sh-etf",
                endpoint=scale_endpoint,
                frame=scale,
                code_column="基金代码",
                exchange="SH",
                authoritative=False,
            )
            record_endpoint_membership(
                "sh-etf",
                endpoint=list_endpoint,
                frame=full,
                code_column="fundCode",
                exchange="SH",
                authoritative=True,
                comparison_product_class_column="subClass",
            )
            record_duplicate_code_rows(
                _duplicate_code_rows(scale, code_column="基金代码", endpoint=scale_endpoint),
                exchange="SH",
                endpoint=scale_endpoint,
                digest=hashes[scale_endpoint],
            )
            full, excluded, invalid_listings = _listed_as_of(
                full,
                date_column="listingDate",
                code_column="fundCode",
                exchange="SH",
                as_of=as_of,
                endpoint=list_endpoint,
            )
            if excluded:
                future_exclusions[list_endpoint] = excluded
            set_endpoint_roles(
                "sh-etf",
                list_endpoint,
                future_as_of_ids=excluded,
                comparison_product_class_filtered_ids=tuple(sorted(
                    f"{_canonical_local_code(item.get('fundCode'), endpoint=list_endpoint)}.SH"
                    for item in full.loc[
                        full["subClass"].astype(str).isin(("05", "07"))
                    ].to_dict("records")
                )),
                effective_row_count=len(full),
            )
            record_duplicate_code_rows(
                _duplicate_code_rows(full, code_column="fundCode", endpoint=list_endpoint),
                exchange="SH",
                endpoint=list_endpoint,
                digest=hashes[list_endpoint],
            )
            for instrument_id, raw in invalid_listings.items():
                merge_pending(instrument_id, _pending_entry(
                    invalid_fields=("listed_date",),
                    endpoint=list_endpoint,
                    digest=hashes[list_endpoint],
                    raw=raw,
                ))
            endpoint_counts[scale_endpoint] = len(scale)
            endpoint_counts[list_endpoint] = len(full)
            for item in full.to_dict("records"):
                record_row(
                    code=item.get("fundCode"),
                    name=item.get("secNameFull") or item.get("fundAbbr"),
                    exchange="SH",
                    asset_type="etf",
                    listed_date=item.get("listingDate"),
                    board="main",
                    product_class=_sse_product_class(item.get("subClass")),
                    digest=hashes[list_endpoint],
                    endpoint=list_endpoint,
                    raw=item,
                )
            scale_by_code = _rows_by_code(scale, code_column="基金代码", endpoint=scale_endpoint)
            full_by_code = _rows_by_code(full, code_column="fundCode", endpoint=list_endpoint)
            pending_sse_ids = {
                instrument_id.rsplit(".", 1)[0]
                for instrument_id in pending_onboarding
                if instrument_id.endswith(".SH")
            }
            scale_ids = set(scale_by_code)
            full_non_money = set(
                _canonical_local_code(item.get("fundCode"), endpoint=list_endpoint)
                for item in full.loc[
                    ~full["subClass"].astype(str).isin(("05", "07"))
                ].to_dict("records")
            )
            scale_ids -= pending_sse_ids
            full_non_money -= pending_sse_ids
            membership_conflicts = scale_ids ^ full_non_money
            component_intersections["sh-etf"] = {
                "left_endpoint": scale_endpoint,
                "right_endpoint": list_endpoint,
                "left_comparable_ids": sorted(f"{local}.SH" for local in scale_ids),
                "right_comparable_ids": sorted(f"{local}.SH" for local in full_non_money),
                "intersection_ids": sorted(f"{local}.SH" for local in scale_ids & full_non_money),
                "membership_conflict_ids": sorted(
                    f"{local}.SH" for local in membership_conflicts
                ),
            }
            for local in membership_conflicts:
                instrument_id = f"{local}.SH"
                merge_pending(instrument_id, _pending_entry(
                    conflict_fields=("membership",),
                    endpoint=scale_endpoint,
                    digest=hashes[scale_endpoint],
                    raw={
                        scale_endpoint: scale_by_code.get(local),
                        list_endpoint: full_by_code.get(local),
                    },
                    membership_evidence=_membership_evidence(
                        left_endpoint=scale_endpoint,
                        left_digest=hashes[scale_endpoint],
                        left_row=scale_by_code.get(local),
                        right_endpoint=list_endpoint,
                        right_digest=hashes[list_endpoint],
                        right_row=full_by_code.get(local),
                    ),
                ))
            if membership_conflicts:
                rows[:] = [
                    row for row in rows
                    if row["instrument_id"] not in {f"{local}.SH" for local in membership_conflicts}
                ]
        if "etf" in assets and "SH" in exchanges:
            state = component_state()
            try:
                collect_sh_etf()
            except ProviderUnavailableError as error:
                restore_component_state(state)
                record_unavailable(
                    "sh-etf",
                    scope={"exchange": "SH", "asset_type": "etf"},
                    endpoints=("sse-etf-scale-list", "sse-current-full-etf-list"),
                    error=error,
                )

        def collect_sz_etf() -> None:
            daily_endpoint = "szse-etf-scale-daily"
            daily = _call(daily_endpoint, lambda: client.fund_scale_daily_szse(
                start_date=as_of.strftime("%Y%m%d"),
                end_date=as_of.strftime("%Y%m%d"),
                symbol="ETF",
            ), required_columns=("基金代码", "基金简称"))
            detail_endpoint = "szse-current-etf-list"
            detail = _call(
                detail_endpoint,
                client.fund_etf_scale_szse if self._client is not None else _fetch_szse_etf_list,
                required_columns=("基金代码", "基金简称", "上市日期", "基金类别", "投资类别"),
            )
            hashes[daily_endpoint] = frame_hash(daily)
            hashes[detail_endpoint] = frame_hash(detail)
            endpoint_response_counts[daily_endpoint] = len(daily)
            endpoint_response_counts[detail_endpoint] = len(detail)
            record_endpoint_membership(
                "sz-etf",
                endpoint=daily_endpoint,
                frame=daily,
                code_column="基金代码",
                exchange="SZ",
                authoritative=True,
            )
            record_endpoint_membership(
                "sz-etf",
                endpoint=detail_endpoint,
                frame=detail,
                code_column="基金代码",
                exchange="SZ",
                authoritative=False,
                comparison_product_class_column="基金类别",
            )
            record_duplicate_code_rows(
                _duplicate_code_rows(daily, code_column="基金代码", endpoint=daily_endpoint),
                exchange="SZ",
                endpoint=daily_endpoint,
                digest=hashes[daily_endpoint],
            )
            detail, excluded, invalid_listings = _listed_as_of(
                detail,
                date_column="上市日期",
                code_column="基金代码",
                exchange="SZ",
                as_of=as_of,
                endpoint=detail_endpoint,
            )
            if excluded:
                future_exclusions[detail_endpoint] = excluded
            set_endpoint_roles(
                "sz-etf",
                detail_endpoint,
                future_as_of_ids=excluded,
                comparison_product_class_filtered_ids=tuple(sorted(
                    f"{_canonical_local_code(item.get('基金代码'), endpoint=detail_endpoint)}.SZ"
                    for item in detail.loc[
                        ~detail["基金类别"].astype(str).eq("ETF")
                    ].to_dict("records")
                )),
                effective_row_count=len(detail),
            )
            record_duplicate_code_rows(
                _duplicate_code_rows(detail, code_column="基金代码", endpoint=detail_endpoint),
                exchange="SZ",
                endpoint=detail_endpoint,
                digest=hashes[detail_endpoint],
            )
            for instrument_id, raw in invalid_listings.items():
                merge_pending(instrument_id, _pending_entry(
                    invalid_fields=("listed_date",),
                    endpoint=detail_endpoint,
                    digest=hashes[detail_endpoint],
                    raw=raw,
                ))
            endpoint_counts[daily_endpoint] = len(daily)
            endpoint_counts[detail_endpoint] = len(detail)
            detail_by_code = {
                _canonical_local_code(item.get("基金代码"), endpoint=detail_endpoint): item
                for item in detail.loc[
                    detail["基金类别"].astype(str).eq("ETF")
                ].to_dict("records")
            }
            for item in daily.to_dict("records"):
                code = _canonical_local_code(item.get("基金代码"), endpoint=daily_endpoint)
                metadata = detail_by_code.get(code, {})
                record_row(
                    code=code,
                    name=item.get("基金简称") or metadata.get("基金简称"),
                    exchange="SZ",
                    asset_type="etf",
                    listed_date=metadata.get("上市日期"),
                    board="main",
                    product_class=_szse_product_class(
                        metadata.get("基金类别"), metadata.get("投资类别"),
                    ),
                    digest=hashes[daily_endpoint],
                    endpoint=daily_endpoint,
                    raw={"daily": item, "detail": metadata},
                )
            daily_by_code = _rows_by_code(daily, code_column="基金代码", endpoint=daily_endpoint)
            pending_szse_ids = {
                instrument_id.rsplit(".", 1)[0]
                for instrument_id in pending_onboarding
                if instrument_id.endswith(".SZ")
            }
            daily_ids = set(daily_by_code)
            detail_ids = set(detail_by_code)
            daily_ids -= pending_szse_ids
            detail_ids -= pending_szse_ids
            membership_conflicts = daily_ids ^ detail_ids
            component_intersections["sz-etf"] = {
                "left_endpoint": daily_endpoint,
                "right_endpoint": detail_endpoint,
                "left_comparable_ids": sorted(f"{local}.SZ" for local in daily_ids),
                "right_comparable_ids": sorted(f"{local}.SZ" for local in detail_ids),
                "intersection_ids": sorted(f"{local}.SZ" for local in daily_ids & detail_ids),
                "membership_conflict_ids": sorted(
                    f"{local}.SZ" for local in membership_conflicts
                ),
            }
            for local in membership_conflicts:
                instrument_id = f"{local}.SZ"
                merge_pending(instrument_id, _pending_entry(
                    conflict_fields=("membership",),
                    endpoint=daily_endpoint,
                    digest=hashes[daily_endpoint],
                    raw={
                        daily_endpoint: daily_by_code.get(local),
                        detail_endpoint: detail_by_code.get(local),
                    },
                    membership_evidence=_membership_evidence(
                        left_endpoint=daily_endpoint,
                        left_digest=hashes[daily_endpoint],
                        left_row=daily_by_code.get(local),
                        right_endpoint=detail_endpoint,
                        right_digest=hashes[detail_endpoint],
                        right_row=detail_by_code.get(local),
                    ),
                ))
            if membership_conflicts:
                rows[:] = [
                    row for row in rows
                    if row["instrument_id"] not in {f"{local}.SZ" for local in membership_conflicts}
                ]
        if "etf" in assets and "SZ" in exchanges:
            state = component_state()
            try:
                collect_sz_etf()
            except ProviderUnavailableError as error:
                restore_component_state(state)
                record_unavailable(
                    "sz-etf",
                    scope={"exchange": "SZ", "asset_type": "etf"},
                    endpoints=("szse-etf-scale-daily", "szse-current-etf-list"),
                    error=error,
                )
        frame = pd.DataFrame(rows, columns=_INSTRUMENT_COLUMNS)
        if not frame.empty:
            frame = frame.drop_duplicates("instrument_id", keep="last").sort_values(
                "instrument_id", kind="stable",
            ).reset_index(drop=True)
        component_closure = _build_component_closure(
            successful_endpoint_membership=successful_endpoint_membership,
            component_intersections=component_intersections,
            frame=frame,
            pending_onboarding=pending_onboarding,
            endpoint_response_counts=endpoint_response_counts,
            endpoint_hashes=hashes,
        )
        complete = (
            all(count > 0 for count in endpoint_counts.values())
            and not pending_onboarding
            and not unavailable_components
        )
        instrument_ids = tuple(map(str, frame["instrument_id"]))
        source_metadata = {
            "upstream": "Shanghai Stock Exchange / Shenzhen Stock Exchange",
            "backend_group": self.backend_group,
            "transport": "AKShare clients over SSE/SZSE public HTTP endpoints",
            "client": "akshare",
            "client_version": getattr(client, "__version__", None),
            "as_of_date": as_of,
            "response_sha256": hashes,
            "endpoint_counts": endpoint_counts,
            "endpoint_response_counts": endpoint_response_counts,
            "as_of_excluded_future_instrument_ids": future_exclusions,
            "pending_onboarding": pending_onboarding,
            "unavailable_components": unavailable_components,
            "component_closure": component_closure,
            "requested_scope": {"exchanges": exchanges, "asset_types": assets},
        }
        validate_exchange_component_closure(metadata=source_metadata, frame=frame)
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {MarketTable.INSTRUMENTS: frame},
            (CoverageClaim(
                MarketTable.INSTRUMENTS,
                complete,
                instrument_ids=instrument_ids,
                detail=(
                    f"Exchange-published current universe as of {as_of.isoformat()}; "
                    f"endpoint_counts={endpoint_counts}"
                ),
            ),),
            source_metadata,
        )


def _call(
    name: str,
    function: Callable[[], Any],
    *,
    required_columns: tuple[str, ...],
) -> pd.DataFrame:
    try:
        value = function()
    except ProviderUnavailableError as exc:
        raise ProviderUnavailableError(str(exc), endpoint=name) from exc
    except Exception as exc:
        if _is_provider_transport_error(exc):
            raise ProviderUnavailableError(
                f"Exchange endpoint failed ({name}): {exc}", endpoint=name,
            ) from exc
        raise ObservationError(f"Exchange endpoint failed ({name}): {exc}") from exc
    try:
        frame = pd.DataFrame(value)
    except Exception as exc:
        raise ObservationError(f"Exchange endpoint returned an invalid table ({name}): {exc}") from exc
    if frame.empty:
        raise ProviderUnavailableError(
            f"Exchange endpoint returned no rows: {name}", endpoint=name,
        )
    missing = tuple(column for column in required_columns if column not in frame.columns)
    if missing:
        raise ObservationError(
            f"Exchange endpoint returned malformed schema ({name}): missing={missing}"
        )
    return frame


def _listed_as_of(
    frame: pd.DataFrame,
    *,
    date_column: str,
    code_column: str,
    exchange: str,
    as_of: date,
    endpoint: str,
) -> tuple[pd.DataFrame, tuple[str, ...], dict[str, Mapping[str, Any]]]:
    """Select an official current list at one exact date, preserving future audit ids."""

    listed_dates: list[date] = []
    valid_indices: list[Any] = []
    invalid: dict[str, Mapping[str, Any]] = {}
    for row in frame.to_dict("records"):
        code = _canonical_local_code(row.get(code_column), endpoint=endpoint)
        try:
            listed_dates.append(_required_exchange_date(row.get(date_column)))
            valid_indices.append(row)
        except (TypeError, ValueError):
            invalid[f"{code}.{exchange}"] = row
    frame = pd.DataFrame(valid_indices, columns=frame.columns)
    mask = pd.Series(
        (listed <= as_of for listed in listed_dates),
        index=frame.index,
        dtype="boolean",
    )
    excluded = tuple(sorted(
        f"{_canonical_local_code(code, endpoint=endpoint)}.{exchange}"
        for code in frame.loc[~mask, code_column]
    ))
    return frame.loc[mask].reset_index(drop=True), excluded, invalid


def _required_exchange_date(value: Any) -> date:
    if value is None or pd.isna(value):
        raise ValueError("missing exchange date")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    if len(raw) >= 10 and raw[4] in {"-", "/"}:
        return date.fromisoformat(raw[:10].replace("/", "-"))
    compact = raw[:8]
    if len(compact) == 8 and compact.isdigit():
        return date(int(compact[:4]), int(compact[4:6]), int(compact[6:8]))
    raise ValueError(f"invalid exchange date: {value!r}")


def _fetch_sse_fund_list() -> pd.DataFrame:
    raw = _http_bytes(
        "https://query.sse.com.cn/commonSoaQuery.do",
        {
            "isPagination": "true",
            "pageHelp.pageSize": "10000",
            "pageHelp.pageNo": "1",
            "pageHelp.beginPage": "1",
            "pageHelp.cacheSize": "1",
            "pageHelp.endPage": "1",
            "pagecache": "false",
            "sqlId": "FUND_LIST",
            "fundType": "00",
            "subClass": "01,02,03,04,05,06,07,08,09,31,32,33,34,35,36,37,38",
            "order": "",
        },
        {"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"},
    )
    try:
        payload = json.loads(raw.decode("utf-8"))
        frame = pd.DataFrame(payload["result"])
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ObservationError("SSE full ETF list returned invalid UTF-8 JSON") from exc
    required = {"fundCode", "secNameFull", "listingDate", "subClass"}
    if not required <= set(frame):
        raise ObservationError("SSE full ETF list is missing required fields")
    return frame


def _fetch_szse_etf_list() -> pd.DataFrame:
    raw = _http_bytes(
        "https://fund.szse.cn/api/report/ShowReport",
        {
            "SHOWTYPE": "xlsx",
            "CATALOGID": "1000_lf",
            "TABKEY": "tab1",
            "random": "0.07610353191740105",
        },
        {
            "Referer": "https://fund.szse.cn/marketdata/fundslist/index.html",
            "User-Agent": "Mozilla/5.0",
        },
    )
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            frame = pd.read_excel(BytesIO(raw), engine="openpyxl", dtype={"基金代码": str})
    except Exception as exc:
        raise ObservationError(f"SZSE ETF list workbook is invalid: {exc}") from exc
    required = {"基金代码", "基金简称", "基金类别", "投资类别", "上市日期"}
    if not required <= set(frame):
        raise ObservationError("SZSE ETF list is missing required fields")
    return frame


def _http_bytes(url: str, parameters: Mapping[str, Any], headers: Mapping[str, str]) -> bytes:
    request = Request(
        f"{url}?{urlencode(parameters)}",
        headers=dict(headers),
        method="GET",
    )
    try:
        with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed official URLs
            payload = response.read()
    except Exception as exc:
        if _is_provider_transport_error(exc, allow_os_error=True):
            raise ProviderUnavailableError(
                f"Official exchange request failed: {url}: {exc}", endpoint=url,
            ) from exc
        raise ObservationError(f"Official exchange request failed: {url}: {exc}") from exc
    if not payload:
        raise ProviderUnavailableError(
            f"Official exchange request returned no response: {url}", endpoint=url,
        )
    return payload


def _is_provider_transport_error(error: Exception, *, allow_os_error: bool = False) -> bool:
    if isinstance(error, (TimeoutError, ConnectionError, URLError, HTTPError)):
        return True
    if allow_os_error and isinstance(error, OSError):
        return True
    try:
        from requests.exceptions import ConnectionError as RequestsConnectionError
        from requests.exceptions import HTTPError as RequestsHTTPError
        from requests.exceptions import Timeout as RequestsTimeout
    except ImportError:
        return False
    return isinstance(error, (RequestsTimeout, RequestsConnectionError, RequestsHTTPError))


def _build_component_closure(
    *,
    successful_endpoint_membership: Mapping[str, Mapping[str, Mapping[str, Any]]],
    component_intersections: Mapping[str, Mapping[str, Any]],
    frame: pd.DataFrame,
    pending_onboarding: Mapping[str, Mapping[str, Any]],
    endpoint_response_counts: Mapping[str, int],
    endpoint_hashes: Mapping[str, str],
) -> dict[str, dict[str, Any]]:
    """Return a JSON-safe, independently checkable closure for successful components."""

    endpoint_to_component = {
        endpoint: component
        for component, endpoints in successful_endpoint_membership.items()
        for endpoint in endpoints
    }
    authoritative_endpoints = {
        endpoint
        for endpoints in successful_endpoint_membership.values()
        for endpoint, evidence in endpoints.items()
        if evidence["authoritative_for_master"]
    }
    admitted_by_component: dict[str, set[str]] = {
        component: set() for component in successful_endpoint_membership
    }
    for row in frame.to_dict("records"):
        lineage = json.loads(str(row["field_lineage"]))
        endpoint = lineage["endpoint"]
        if endpoint not in endpoint_to_component:
            raise ValueError(f"Admitted instrument has no successful component endpoint: {endpoint}")
        if endpoint not in authoritative_endpoints:
            raise ValueError(f"Admitted instrument uses a non-authoritative endpoint: {endpoint}")
        admitted_by_component[endpoint_to_component[endpoint]].add(str(row["instrument_id"]))

    scope_by_component = {
        "sh-stock-main": {"exchange": "SH", "asset_type": "stock", "board": "main"},
        "sh-stock-star": {"exchange": "SH", "asset_type": "stock", "board": "star"},
        "sz-stock": {"exchange": "SZ", "asset_type": "stock"},
        "sh-etf": {"exchange": "SH", "asset_type": "etf"},
        "sz-etf": {"exchange": "SZ", "asset_type": "etf"},
    }
    closure: dict[str, dict[str, Any]] = {}
    for component, endpoints in successful_endpoint_membership.items():
        endpoint_closure: dict[str, dict[str, Any]] = {}
        component_admitted = admitted_by_component[component]
        for endpoint, evidence in endpoints.items():
            raw_ids = set(map(str, evidence["raw_ids"]))
            duplicate_ids = set(map(str, evidence["duplicate_occurrences"])) | {
                instrument_id for instrument_id in raw_ids
                if "duplicate_identity" in pending_onboarding.get(
                    instrument_id, {}
                ).get("conflict_fields", ())
            }
            future_ids = set(map(str, evidence["future_as_of_ids"]))
            product_filtered_ids = set(map(
                str, evidence["comparison_product_class_filtered_ids"],
            ))
            membership_conflict_ids = {
                instrument_id for instrument_id in raw_ids
                if "membership" in pending_onboarding.get(instrument_id, {}).get("conflict_fields", ())
            }
            pending_ids = {
                instrument_id for instrument_id in raw_ids
                if instrument_id in pending_onboarding
                and instrument_id not in duplicate_ids
                and instrument_id not in membership_conflict_ids
            }
            admitted_ids = raw_ids & component_admitted
            intentional_out_of_component_ids = (
                product_filtered_ids
                - admitted_ids
                - pending_ids
                - membership_conflict_ids
                - duplicate_ids
                - future_ids
            )
            partitions = {
                "admitted": sorted(admitted_ids - duplicate_ids - future_ids - membership_conflict_ids - pending_ids),
                "pending_onboarding": sorted(pending_ids),
                "future_as_of": sorted(future_ids - duplicate_ids),
                "duplicate_identity": sorted(duplicate_ids),
                "membership_conflict": sorted(membership_conflict_ids - duplicate_ids),
                "intentionally_out_of_component": sorted(intentional_out_of_component_ids),
            }
            endpoint_closure[endpoint] = {
                "response_sha256": endpoint_hashes[endpoint],
                "raw_row_count": evidence["raw_row_count"],
                "effective_row_count": evidence["effective_row_count"],
                "raw_ids": sorted(raw_ids),
                "duplicate_row_count": evidence["duplicate_row_count"],
                "duplicate_occurrences": evidence["duplicate_occurrences"],
                "authoritative_for_master": evidence["authoritative_for_master"],
                "comparison_product_class_filtered_ids": sorted(product_filtered_ids),
                "comparison_product_class_values": evidence["comparison_product_class_values"],
                "admission_partitions": partitions,
            }
        component_record: dict[str, Any] = {
            "component": component,
            "scope": scope_by_component[component],
            "endpoints": list(endpoint_closure),
            "admitted_ids": sorted(component_admitted),
            "endpoint_membership": endpoint_closure,
        }
        if component in component_intersections:
            component_record["intersection"] = dict(component_intersections[component])
        closure[component] = component_record
    _validate_component_closure(closure, endpoint_response_counts)
    return closure


def _validate_component_closure(
    closure: Mapping[str, Mapping[str, Any]],
    endpoint_response_counts: Mapping[str, int],
) -> None:
    """Fail closed when component membership evidence is not an exact closure."""

    partition_names = (
        "admitted", "pending_onboarding", "future_as_of", "duplicate_identity",
        "membership_conflict", "intentionally_out_of_component",
    )
    for component, record in closure.items():
        endpoints = record["endpoint_membership"]
        if set(endpoints) != set(record["endpoints"]):
            raise ValueError(f"Component closure endpoint order mismatch: {component}")
        component_admitted = set(record["admitted_ids"])
        authoritative_admitted: set[str] = set()
        for endpoint, evidence in endpoints.items():
            for field_name in ("raw_row_count", "effective_row_count", "duplicate_row_count"):
                value = evidence[field_name]
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError(f"Endpoint closure count is invalid: {endpoint}/{field_name}")
            raw_ids = list(evidence["raw_ids"])
            raw_set = set(raw_ids)
            if raw_ids != sorted(raw_set):
                raise ValueError(f"Endpoint raw ids are not sorted unique: {endpoint}")
            if evidence["raw_row_count"] != endpoint_response_counts[endpoint]:
                raise ValueError(f"Endpoint raw row count mismatch: {endpoint}")
            occurrences = evidence["duplicate_occurrences"]
            if any(
                isinstance(count, bool) or not isinstance(count, int) or count < 2
                for count in occurrences.values()
            ):
                raise ValueError(f"Endpoint duplicate occurrences are invalid: {endpoint}")
            duplicate_row_count = sum(count - 1 for count in occurrences.values())
            if duplicate_row_count != evidence["duplicate_row_count"]:
                raise ValueError(f"Endpoint duplicate row count mismatch: {endpoint}")
            if evidence["raw_row_count"] != len(raw_set) + duplicate_row_count:
                raise ValueError(f"Endpoint raw row closure mismatch: {endpoint}")
            if not set(occurrences) <= set(evidence["admission_partitions"]["duplicate_identity"]):
                raise ValueError(f"Endpoint duplicate identity partition mismatch: {endpoint}")
            partitions = evidence["admission_partitions"]
            if set(partitions) != set(partition_names):
                raise ValueError(f"Endpoint partition schema mismatch: {endpoint}")
            seen: set[str] = set()
            for partition_name in partition_names:
                values = list(partitions[partition_name])
                value_set = set(values)
                if values != sorted(value_set):
                    raise ValueError(f"Endpoint partition is not sorted unique: {endpoint}/{partition_name}")
                if seen & value_set:
                    raise ValueError(f"Endpoint partitions overlap: {endpoint}/{partition_name}")
                seen |= value_set
            if seen != raw_set:
                raise ValueError(f"Endpoint partitions do not close raw ids: {endpoint}")
            if not set(evidence["comparison_product_class_filtered_ids"]) <= raw_set:
                raise ValueError(f"Endpoint product-class role escapes raw ids: {endpoint}")
            if evidence["authoritative_for_master"]:
                authoritative_admitted |= set(partitions["admitted"])
        if authoritative_admitted != component_admitted:
            raise ValueError(f"Component admitted ids do not match authoritative endpoint: {component}")


def validate_exchange_component_closure(
    *,
    metadata: Mapping[str, Any],
    frame: pd.DataFrame,
) -> Mapping[str, Any]:
    """Validate an exchange-public component closure without mutating its inputs.

    Daily and history callers can use this to establish that an incomplete
    observation is an exact partial closure rather than an unbounded fallback.
    """

    try:
        requested_scope = metadata["requested_scope"]
        raw_exchanges = requested_scope["exchanges"]
        raw_assets = requested_scope["asset_types"]
        if (
            isinstance(raw_exchanges, str)
            or isinstance(raw_assets, str)
            or not isinstance(raw_exchanges, (list, tuple, set, frozenset))
            or not isinstance(raw_assets, (list, tuple, set, frozenset))
        ):
            raise ValueError("Requested scope must use exchange and asset-type collections")
        requested_exchanges = set(raw_exchanges)
        requested_assets = set(raw_assets)
        if not requested_exchanges <= {"SH", "SZ"} or not requested_assets <= {"stock", "etf"}:
            raise ValueError("Requested scope contains unsupported exchange or asset type")
        component_specs = {
            "sh-stock-main": ({"exchange": "SH", "asset_type": "stock", "board": "main"}, ("sse-main-stock-list",)),
            "sh-stock-star": ({"exchange": "SH", "asset_type": "stock", "board": "star"}, ("sse-star-stock-list",)),
            "sz-stock": ({"exchange": "SZ", "asset_type": "stock"}, ("szse-a-stock-list",)),
            "sh-etf": ({"exchange": "SH", "asset_type": "etf"}, ("sse-etf-scale-list", "sse-current-full-etf-list")),
            "sz-etf": ({"exchange": "SZ", "asset_type": "etf"}, ("szse-etf-scale-daily", "szse-current-etf-list")),
        }
        authority_by_endpoint = {
            "sse-main-stock-list": True,
            "sse-star-stock-list": True,
            "szse-a-stock-list": True,
            "sse-etf-scale-list": False,
            "sse-current-full-etf-list": True,
            "szse-etf-scale-daily": True,
            "szse-current-etf-list": False,
        }
        expected_components = {
            component
            for component, (scope, _) in component_specs.items()
            if scope["exchange"] in requested_exchanges and scope["asset_type"] in requested_assets
        }
        unavailable = metadata["unavailable_components"]
        if not isinstance(unavailable, Mapping) or not set(unavailable) <= expected_components:
            raise ValueError("Unavailable components escape the requested scope")
        closure = metadata["component_closure"]
        if not isinstance(closure, Mapping) or set(closure) != expected_components - set(unavailable):
            raise ValueError("Successful component closure does not match unavailable components")
        expected_endpoints = {
            endpoint
            for component in closure
            for endpoint in component_specs[component][1]
        }
        response_hashes = metadata["response_sha256"]
        endpoint_counts = metadata["endpoint_counts"]
        endpoint_response_counts = metadata["endpoint_response_counts"]
        if (
            set(response_hashes) != expected_endpoints
            or set(endpoint_counts) != expected_endpoints
            or set(endpoint_response_counts) != expected_endpoints
        ):
            raise ValueError("Top-level endpoint evidence does not equal successful endpoints")
        for endpoint in expected_endpoints:
            response_hash = response_hashes[endpoint]
            if not isinstance(response_hash, str) or re.fullmatch(r"[0-9a-f]{64}", response_hash) is None:
                raise ValueError(f"Endpoint response hash is not a SHA-256 hex string: {endpoint}")
            for field_name, values in (
                ("endpoint_counts", endpoint_counts),
                ("endpoint_response_counts", endpoint_response_counts),
            ):
                value = values[endpoint]
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError(f"Endpoint count is not a non-negative integer: {field_name}/{endpoint}")
        for component, record in closure.items():
            expected_scope, endpoints = component_specs[component]
            if record["component"] != component or record["scope"] != expected_scope:
                raise ValueError(f"Component identity or scope mismatch: {component}")
            if tuple(record["endpoints"]) != endpoints:
                raise ValueError(f"Component endpoint contract mismatch: {component}")
            if set(record["endpoint_membership"]) != set(endpoints):
                raise ValueError(f"Component endpoint membership mismatch: {component}")
            for endpoint in endpoints:
                evidence = record["endpoint_membership"][endpoint]
                if evidence["response_sha256"] != response_hashes[endpoint]:
                    raise ValueError(f"Endpoint hash mismatch: {endpoint}")
                if evidence["raw_row_count"] != endpoint_response_counts[endpoint]:
                    raise ValueError(f"Endpoint response count mismatch: {endpoint}")
                if evidence["effective_row_count"] != endpoint_counts[endpoint]:
                    raise ValueError(f"Endpoint effective count mismatch: {endpoint}")
                if evidence["authoritative_for_master"] is not authority_by_endpoint[endpoint]:
                    raise ValueError(f"Endpoint authority contract mismatch: {endpoint}")
        _validate_component_closure(closure, endpoint_response_counts)
        _validate_exchange_component_comparators(closure)
        _validate_exchange_closure_frame(closure, frame)
        _validate_exchange_closure_top_level_partitions(
            metadata, closure, response_hashes,
        )
        for component, unavailable_record in unavailable.items():
            expected_scope, endpoints = component_specs[component]
            if set(unavailable_record) != {
                "component", "scope", "endpoints", "failed_endpoint", "error_type", "message",
            }:
                raise ValueError(f"Unavailable component shape mismatch: {component}")
            if (
                unavailable_record["component"] != component
                or unavailable_record["scope"] != expected_scope
                or tuple(unavailable_record["endpoints"]) != endpoints
                or unavailable_record["failed_endpoint"] not in endpoints
                or unavailable_record["error_type"] != "ProviderUnavailableError"
                or not isinstance(unavailable_record["message"], str)
                or not unavailable_record["message"].strip()
            ):
                raise ValueError(f"Unavailable component contract mismatch: {component}")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ObservationError(f"Invalid exchange component closure: {exc}") from exc
    return metadata["component_closure"]


def _validate_exchange_closure_frame(
    closure: Mapping[str, Mapping[str, Any]],
    frame: pd.DataFrame,
) -> None:
    admitted_by_component = {
        component: set(map(str, record["admitted_ids"]))
        for component, record in closure.items()
    }
    authoritative_endpoints = {
        endpoint: (component, evidence["response_sha256"])
        for component, record in closure.items()
        for endpoint, evidence in record["endpoint_membership"].items()
        if evidence["authoritative_for_master"]
    }
    actual_by_component = {component: set() for component in closure}
    seen_instrument_ids: set[str] = set()
    for row in frame.to_dict("records"):
        instrument_id = str(row["instrument_id"])
        if instrument_id in seen_instrument_ids:
            raise ValueError(f"Frame has duplicate instrument id: {instrument_id}")
        seen_instrument_ids.add(instrument_id)
        lineage = json.loads(str(row["field_lineage"]))
        endpoint = lineage.get("endpoint")
        if endpoint not in authoritative_endpoints:
            raise ValueError(f"Frame row has no authoritative successful endpoint: {instrument_id}")
        component, response_sha256 = authoritative_endpoints[endpoint]
        if (
            lineage.get("upstream") != "exchange-public"
            or lineage.get("response_sha256") != response_sha256
        ):
            raise ValueError(f"Frame row lineage does not match endpoint evidence: {instrument_id}")
        _validate_frame_identity_for_component(row, component, instrument_id)
        if instrument_id in actual_by_component[component]:
            raise ValueError(f"Frame id overlaps a component: {instrument_id}")
        actual_by_component[component].add(instrument_id)
    if actual_by_component != admitted_by_component:
        raise ValueError("Frame ids do not equal component admitted ids")


def _validate_frame_identity_for_component(
    row: Mapping[str, Any],
    component: str,
    instrument_id: str,
) -> None:
    local, separator, exchange = instrument_id.partition(".")
    if not separator or _canonical_local_code(local, endpoint="frame") != local:
        raise ValueError(f"Frame instrument id is not canonical: {instrument_id}")
    if row.get("local_code") != local or row.get("exchange") != exchange:
        raise ValueError(f"Frame identity fields do not match instrument id: {instrument_id}")
    expected = {
        "sh-stock-main": ("SH", "stock", {"main"}),
        "sh-stock-star": ("SH", "stock", {"star"}),
        "sz-stock": ("SZ", "stock", {"main", "chinext"}),
        "sh-etf": ("SH", "etf", {"main"}),
        "sz-etf": ("SZ", "etf", {"main"}),
    }[component]
    if (
        exchange != expected[0]
        or row.get("asset_type") != expected[1]
        or row.get("board") not in expected[2]
    ):
        raise ValueError(f"Frame component identity mismatch: {instrument_id}/{component}")


def _validate_exchange_component_comparators(
    closure: Mapping[str, Mapping[str, Any]],
) -> None:
    for component, record in closure.items():
        endpoints = record["endpoint_membership"]
        for endpoint, evidence in endpoints.items():
            raw_ids = set(evidence["raw_ids"])
            values = evidence["comparison_product_class_values"]
            if component == "sh-etf" and endpoint == "sse-current-full-etf-list":
                if set(values) != raw_ids:
                    raise ValueError(f"SSE ETF product classes do not close raw ids: {endpoint}")
                expected_filter = {
                    instrument_id for instrument_id, value in values.items()
                    if str(value) in {"05", "07"}
                }
            elif component == "sz-etf" and endpoint == "szse-current-etf-list":
                if set(values) != raw_ids:
                    raise ValueError(f"SZSE ETF product classes do not close raw ids: {endpoint}")
                expected_filter = {
                    instrument_id for instrument_id, value in values.items()
                    if str(value) != "ETF"
                }
            else:
                if values:
                    raise ValueError(f"Unexpected comparator product classes: {endpoint}")
                expected_filter = set()
            actual_filter = set(evidence["comparison_product_class_filtered_ids"])
            if actual_filter != expected_filter:
                raise ValueError(f"Endpoint product-class filter mismatch: {endpoint}")
            partitions = evidence["admission_partitions"]
            expected_intentional = (
                expected_filter
                - set(partitions["admitted"])
                - set(partitions["pending_onboarding"])
                - set(partitions["membership_conflict"])
                - set(partitions["duplicate_identity"])
                - set(partitions["future_as_of"])
            )
            if set(partitions["intentionally_out_of_component"]) != expected_intentional:
                raise ValueError(f"Endpoint intentional filter partition mismatch: {endpoint}")
        intersection = record.get("intersection")
        if component in {"sh-etf", "sz-etf"}:
            if not isinstance(intersection, Mapping):
                raise ValueError(f"ETF component is missing comparator evidence: {component}")
            left_endpoint = intersection["left_endpoint"]
            right_endpoint = intersection["right_endpoint"]
            if component == "sh-etf":
                expected_endpoints = ("sse-etf-scale-list", "sse-current-full-etf-list")
            else:
                expected_endpoints = ("szse-etf-scale-daily", "szse-current-etf-list")
            if (left_endpoint, right_endpoint) != expected_endpoints:
                raise ValueError(f"ETF comparator endpoint contract mismatch: {component}")
            left = _comparable_ids(endpoints[left_endpoint])
            right = _comparable_ids(endpoints[right_endpoint])
            if set(intersection["left_comparable_ids"]) != left:
                raise ValueError(f"ETF left comparable ids mismatch: {component}")
            if set(intersection["right_comparable_ids"]) != right:
                raise ValueError(f"ETF right comparable ids mismatch: {component}")
            if set(intersection["intersection_ids"]) != left & right:
                raise ValueError(f"ETF intersection ids mismatch: {component}")
            if set(intersection["membership_conflict_ids"]) != left ^ right:
                raise ValueError(f"ETF membership conflict ids mismatch: {component}")
        elif intersection is not None:
            raise ValueError(f"Non-ETF component has comparator evidence: {component}")


def _comparable_ids(evidence: Mapping[str, Any]) -> set[str]:
    partitions = evidence["admission_partitions"]
    return (
        set(evidence["raw_ids"])
        - set(partitions["duplicate_identity"])
        - set(partitions["pending_onboarding"])
        - set(partitions["future_as_of"])
        - set(evidence["comparison_product_class_filtered_ids"])
    )


def _validate_exchange_closure_top_level_partitions(
    metadata: Mapping[str, Any],
    closure: Mapping[str, Mapping[str, Any]],
    response_hashes: Mapping[str, str],
) -> None:
    admitted_ids = {
        instrument_id
        for record in closure.values()
        for instrument_id in record["admitted_ids"]
    }
    pending = metadata["pending_onboarding"]
    future = metadata["as_of_excluded_future_instrument_ids"]
    endpoint_membership = {
        endpoint: evidence
        for record in closure.values()
        for endpoint, evidence in record["endpoint_membership"].items()
    }
    if set(future) - set(endpoint_membership):
        raise ValueError("Future-as-of evidence names an unsuccessful endpoint")
    for endpoint, evidence in endpoint_membership.items():
        partition_ids = set(evidence["admission_partitions"]["future_as_of"])
        duplicate_ids = set(evidence["admission_partitions"]["duplicate_identity"])
        top_level_ids = set(future.get(endpoint, ()))
        if endpoint not in {"sse-current-full-etf-list", "szse-current-etf-list"} and (
            partition_ids or top_level_ids
        ):
            raise ValueError(f"Future-as-of is not allowed for endpoint: {endpoint}")
        for instrument_id in partition_ids | top_level_ids:
            _validate_endpoint_instrument_identity(instrument_id, endpoint)
            if instrument_id not in set(evidence["raw_ids"]):
                raise ValueError(f"Future-as-of id escapes endpoint raw membership: {endpoint}")
        if not partition_ids <= top_level_ids or not top_level_ids <= partition_ids | duplicate_ids:
            raise ValueError(f"Future-as-of ids escape endpoint partitions: {endpoint}")
        if not set(evidence["admission_partitions"]["pending_onboarding"]) <= set(pending):
            raise ValueError(f"Endpoint pending ids escape top-level metadata: {endpoint}")
        if not set(evidence["admission_partitions"]["membership_conflict"]) <= set(pending):
            raise ValueError(f"Endpoint conflict ids escape top-level metadata: {endpoint}")
    for instrument_id, entry in pending.items():
        if instrument_id in admitted_ids:
            raise ValueError(f"Pending onboarding id was admitted: {instrument_id}")
        covered = False
        for evidence in endpoint_membership.values():
            partitions = evidence["admission_partitions"]
            if instrument_id in (
                set(partitions["pending_onboarding"])
                | set(partitions["duplicate_identity"])
                | set(partitions["membership_conflict"])
            ):
                covered = True
                break
        if not covered:
            raise ValueError(f"Pending onboarding id escapes endpoint partitions: {instrument_id}")
        _validate_pending_onboarding_entry(
            instrument_id=instrument_id,
            entry=entry,
            endpoint_membership=endpoint_membership,
            response_hashes=response_hashes,
        )


def _validate_pending_onboarding_entry(
    *,
    instrument_id: str,
    entry: Mapping[str, Any],
    endpoint_membership: Mapping[str, Mapping[str, Any]],
    response_hashes: Mapping[str, str],
) -> None:
    allowed_keys = {
        "missing_fields", "invalid_fields", "conflict_fields", "endpoint", "raw_evidence",
        "membership_evidence",
    }
    required_keys = allowed_keys - {"membership_evidence"}
    if not isinstance(entry, Mapping) or not required_keys <= set(entry) <= allowed_keys:
        raise ValueError(f"Pending onboarding entry shape mismatch: {instrument_id}")
    endpoint = entry["endpoint"]
    if endpoint not in endpoint_membership:
        raise ValueError(f"Pending onboarding endpoint is not successful: {instrument_id}")
    for field_name, allowed_values in (
        ("missing_fields", {"name", "listed_date", "board", "exchange_product_class"}),
        ("invalid_fields", {"listed_date", "board", "exchange_product_class"}),
        ("conflict_fields", {"membership", "duplicate_identity", "master"}),
    ):
        values = entry[field_name]
        if isinstance(values, str) or not isinstance(values, (list, tuple)):
            raise ValueError(f"Pending onboarding fields are not a collection: {instrument_id}/{field_name}")
        normalized = list(values)
        if (
            any(not isinstance(value, str) or value not in allowed_values for value in normalized)
            or normalized != sorted(set(normalized))
        ):
            raise ValueError(f"Pending onboarding fields are invalid: {instrument_id}/{field_name}")
    if not any(entry[field_name] for field_name in (
        "missing_fields", "invalid_fields", "conflict_fields",
    )):
        raise ValueError(f"Pending onboarding entry has no disposition: {instrument_id}")
    raw_evidence = entry["raw_evidence"]
    if not isinstance(raw_evidence, Mapping) or set(raw_evidence) != {"response_sha256", "row"}:
        raise ValueError(f"Pending onboarding raw evidence shape mismatch: {instrument_id}")
    if raw_evidence["response_sha256"] != response_hashes[endpoint]:
        raise ValueError(f"Pending onboarding raw evidence hash mismatch: {instrument_id}")
    _require_json_safe(raw_evidence["row"], context=f"pending raw evidence {instrument_id}")
    _validate_pending_raw_evidence_identity(
        instrument_id=instrument_id,
        endpoint=endpoint,
        row=raw_evidence["row"],
        response_hashes=response_hashes,
        endpoint_membership=endpoint_membership,
    )
    is_membership_conflict = "membership" in entry["conflict_fields"]
    membership_evidence = entry.get("membership_evidence")
    if not is_membership_conflict and membership_evidence is not None:
        raise ValueError(f"Unexpected pending membership evidence: {instrument_id}")
    if not is_membership_conflict:
        return
    if not isinstance(membership_evidence, Mapping) or set(membership_evidence) != {"endpoints"}:
        raise ValueError(f"Membership evidence shape mismatch: {instrument_id}")
    component_endpoints = {
        "sse-etf-scale-list": {"sse-etf-scale-list", "sse-current-full-etf-list"},
        "sse-current-full-etf-list": {"sse-etf-scale-list", "sse-current-full-etf-list"},
        "szse-etf-scale-daily": {"szse-etf-scale-daily", "szse-current-etf-list"},
        "szse-current-etf-list": {"szse-etf-scale-daily", "szse-current-etf-list"},
    }.get(endpoint)
    if component_endpoints is None:
        raise ValueError(f"Membership conflict is not an ETF component: {instrument_id}")
    evidence_endpoints = membership_evidence["endpoints"]
    if not isinstance(evidence_endpoints, Mapping) or set(evidence_endpoints) != component_endpoints:
        raise ValueError(f"Membership evidence endpoint mismatch: {instrument_id}")
    for evidence_endpoint, evidence in evidence_endpoints.items():
        if not isinstance(evidence, Mapping) or set(evidence) != {"response_sha256", "row"}:
            raise ValueError(f"Membership evidence entry shape mismatch: {instrument_id}/{evidence_endpoint}")
        if evidence["response_sha256"] != response_hashes[evidence_endpoint]:
            raise ValueError(f"Membership evidence hash mismatch: {instrument_id}/{evidence_endpoint}")
        _require_json_safe(evidence["row"], context=f"membership raw evidence {instrument_id}")
        if evidence["row"] is not None and not _raw_membership_row_matches_id(
            evidence["row"], instrument_id, evidence_endpoint,
        ):
            raise ValueError(f"Membership evidence row does not match id: {instrument_id}/{evidence_endpoint}")


def _raw_membership_row_matches_id(
    row: Any,
    instrument_id: str,
    endpoint: str,
) -> bool:
    if not isinstance(row, Mapping):
        return False
    code_column = {
        "sse-etf-scale-list": "基金代码",
        "sse-current-full-etf-list": "fundCode",
        "szse-etf-scale-daily": "基金代码",
        "szse-current-etf-list": "基金代码",
    }.get(endpoint)
    if code_column is None:
        return False
    try:
        local = _canonical_local_code(row.get(code_column), endpoint=endpoint)
    except (TypeError, ValueError, ObservationError):
        return False
    return instrument_id.startswith(f"{local}.")


def _validate_pending_raw_evidence_identity(
    *,
    instrument_id: str,
    endpoint: str,
    row: Any,
    response_hashes: Mapping[str, str],
    endpoint_membership: Mapping[str, Mapping[str, Any]],
) -> None:
    _validate_endpoint_instrument_identity(instrument_id, endpoint)
    if not isinstance(row, Mapping):
        raise ValueError(f"Pending raw evidence row is not an object: {instrument_id}")
    if set(row) == {"records"}:
        records = row["records"]
        if not isinstance(records, (list, tuple)) or not records:
            raise ValueError(f"Pending duplicate records are invalid: {instrument_id}")
        wrapper_records = [
            isinstance(record, Mapping) and set(record) == {"endpoint", "response_sha256", "row"}
            for record in records
        ]
        if all(wrapper_records):
            for record in records:
                record_endpoint = record["endpoint"]
                if record_endpoint not in response_hashes or record["response_sha256"] != response_hashes[record_endpoint]:
                    raise ValueError(f"Pending duplicate record hash mismatch: {instrument_id}")
                if not _raw_endpoint_row_matches_id(record["row"], instrument_id, record_endpoint):
                    raise ValueError(f"Pending duplicate record id mismatch: {instrument_id}")
        elif not any(wrapper_records):
            if endpoint not in {"sse-etf-scale-list", "szse-current-etf-list"} or len(records) < 2:
                raise ValueError(f"Pending duplicate raw records are invalid: {instrument_id}")
            if not all(_raw_endpoint_row_matches_id(record, instrument_id, endpoint) for record in records):
                raise ValueError(f"Pending duplicate raw record id mismatch: {instrument_id}")
            occurrences = endpoint_membership[endpoint]["duplicate_occurrences"]
            if occurrences.get(instrument_id) != len(records):
                raise ValueError(f"Pending duplicate raw record count mismatch: {instrument_id}")
        else:
            raise ValueError(f"Pending duplicate record shape mismatch: {instrument_id}")
        return
    paired_endpoints = _paired_etf_endpoints(endpoint)
    if paired_endpoints is not None and set(row) == paired_endpoints:
        for evidence_endpoint, evidence_row in row.items():
            if evidence_row is not None and not _raw_endpoint_row_matches_id(
                evidence_row, instrument_id, evidence_endpoint,
            ):
                raise ValueError(f"Pending membership raw id mismatch: {instrument_id}/{evidence_endpoint}")
        return
    if not _raw_endpoint_row_matches_id(row, instrument_id, endpoint):
        raise ValueError(f"Pending raw evidence id mismatch: {instrument_id}")


def _endpoint_identity(endpoint: str) -> tuple[str, str, str | None, str]:
    identities = {
        "sse-main-stock-list": ("SH", "stock", "main", "证券代码"),
        "sse-star-stock-list": ("SH", "stock", "star", "证券代码"),
        "szse-a-stock-list": ("SZ", "stock", None, "A股代码"),
        "sse-etf-scale-list": ("SH", "etf", "main", "基金代码"),
        "sse-current-full-etf-list": ("SH", "etf", "main", "fundCode"),
        "szse-etf-scale-daily": ("SZ", "etf", "main", "基金代码"),
        "szse-current-etf-list": ("SZ", "etf", "main", "基金代码"),
    }
    try:
        return identities[endpoint]
    except KeyError as exc:
        raise ValueError(f"Unknown exchange endpoint: {endpoint}") from exc


def _validate_endpoint_instrument_identity(instrument_id: str, endpoint: str) -> None:
    local, separator, exchange = instrument_id.partition(".")
    expected_exchange, _, _, _ = _endpoint_identity(endpoint)
    if (
        not separator
        or exchange != expected_exchange
        or _canonical_local_code(local, endpoint=endpoint) != local
    ):
        raise ValueError(f"Instrument id does not match endpoint identity: {instrument_id}/{endpoint}")


def _raw_endpoint_row_matches_id(row: Any, instrument_id: str, endpoint: str) -> bool:
    if not isinstance(row, Mapping):
        return False
    _, _, _, code_column = _endpoint_identity(endpoint)
    raw_row = row.get("daily") if endpoint == "szse-etf-scale-daily" and "daily" in row else row
    if not isinstance(raw_row, Mapping):
        return False
    try:
        local = _canonical_local_code(raw_row.get(code_column), endpoint=endpoint)
    except (TypeError, ValueError, ObservationError):
        return False
    return instrument_id == f"{local}.{_endpoint_identity(endpoint)[0]}"


def _paired_etf_endpoints(endpoint: str) -> set[str] | None:
    return {
        "sse-etf-scale-list": {"sse-etf-scale-list", "sse-current-full-etf-list"},
        "sse-current-full-etf-list": {"sse-etf-scale-list", "sse-current-full-etf-list"},
        "szse-etf-scale-daily": {"szse-etf-scale-daily", "szse-current-etf-list"},
        "szse-current-etf-list": {"szse-etf-scale-daily", "szse-current-etf-list"},
    }.get(endpoint)


def _require_json_safe(value: Any, *, context: str) -> None:
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{context} is not JSON-safe") from exc


def _row(
    *,
    code: Any,
    name: Any,
    exchange: str,
    asset_type: str,
    listed_date: Any,
    board: str,
    product_class: str | None,
    digest: str,
    endpoint: str,
    raw: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, tuple[str, dict[str, Any]] | None]:
    local = _canonical_local_code(code, endpoint=endpoint)
    instrument_id = f"{local}.{exchange}"
    missing_fields: list[str] = []
    invalid_fields: list[str] = []

    normalized_name = _required_text(name)
    if normalized_name is None:
        missing_fields.append("name")
    try:
        listed = _required_exchange_date(listed_date).isoformat()
    except (TypeError, ValueError):
        if _required_text(listed_date) is None:
            missing_fields.append("listed_date")
        else:
            invalid_fields.append("listed_date")
        listed = None
    normalized_board = _required_text(board)
    if normalized_board is None:
        missing_fields.append("board")
    elif normalized_board not in {"main", "star", "chinext"}:
        invalid_fields.append("board")
    normalized_product_class = _required_text(product_class)
    if asset_type == "etf" and normalized_product_class is None:
        missing_fields.append("exchange_product_class")
    elif asset_type != "etf" and normalized_product_class is not None:
        invalid_fields.append("exchange_product_class")

    if missing_fields or invalid_fields:
        return None, (
            instrument_id,
            _pending_entry(
                missing_fields=tuple(sorted(missing_fields)),
                invalid_fields=tuple(sorted(invalid_fields)),
                endpoint=endpoint,
                digest=digest,
                raw=raw,
            ),
        )

    return {
        "instrument_id": instrument_id,
        "exchange": exchange,
        "local_code": local,
        "asset_type": asset_type,
        "name": normalized_name,
        "currency": "CNY",
        "listed_date": listed,
        "delisted_date": None,
        "board": normalized_board,
        "exchange_product_class": normalized_product_class,
        "buy_lot": 100,
        "price_tick": 0.001 if asset_type == "etf" else 0.01,
        "sell_delay_sessions": pd.NA,
        "price_limit_ratio": pd.NA,
        "field_lineage": source_payload(
            upstream="exchange-public",
            endpoint=endpoint,
            response_sha256=digest,
        ),
        "source_payload": source_payload(
            upstream="exchange-public",
            endpoint=endpoint,
            response_sha256=digest,
            raw=_plain(raw),
        ),
    }, None


def _pending_entry(
    *,
    endpoint: str,
    digest: str,
    raw: Mapping[str, Any],
    missing_fields: tuple[str, ...] = (),
    invalid_fields: tuple[str, ...] = (),
    conflict_fields: tuple[str, ...] = (),
    membership_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "missing_fields": tuple(sorted(set(missing_fields))),
        "invalid_fields": tuple(sorted(set(invalid_fields))),
        "conflict_fields": tuple(sorted(set(conflict_fields))),
        "endpoint": endpoint,
        "raw_evidence": {
            "response_sha256": digest,
            "row": _plain(raw),
        },
    }
    if membership_evidence is not None:
        entry["membership_evidence"] = _plain(membership_evidence)
    return entry


def _rows_by_code(
    frame: pd.DataFrame,
    *,
    code_column: str,
    endpoint: str,
) -> dict[str, Mapping[str, Any]]:
    return {
        _canonical_local_code(row.get(code_column), endpoint=endpoint): row
        for row in frame.to_dict("records")
    }


def _duplicate_code_rows(
    frame: pd.DataFrame,
    *,
    code_column: str,
    endpoint: str,
) -> dict[str, list[Mapping[str, Any]]]:
    rows_by_code: dict[str, list[Mapping[str, Any]]] = {}
    for row in frame.to_dict("records"):
        local = _canonical_local_code(row.get(code_column), endpoint=endpoint)
        rows_by_code.setdefault(local, []).append(row)
    return {
        local: rows
        for local, rows in rows_by_code.items()
        if len(rows) > 1
    }


def _membership_evidence(
    *,
    left_endpoint: str,
    left_digest: str,
    left_row: Mapping[str, Any] | None,
    right_endpoint: str,
    right_digest: str,
    right_row: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "endpoints": {
            left_endpoint: {"response_sha256": left_digest, "row": _plain(left_row)},
            right_endpoint: {"response_sha256": right_digest, "row": _plain(right_row)},
        },
    }


def _canonical_local_code(value: Any, *, endpoint: str) -> str:
    if _required_text(value) is None:
        raise ObservationError(f"Exchange endpoint has invalid instrument code ({endpoint}): missing")
    raw = str(value).strip().upper().split(".", 1)[0]
    if not re.fullmatch(r"\d{1,6}", raw):
        raise ObservationError(
            f"Exchange endpoint has invalid instrument code ({endpoint}): {value!r}"
        )
    return raw.zfill(6)


def _required_text(value: Any) -> str | None:
    if value is None or value is pd.NA:
        return None
    missing = pd.isna(value)
    if not hasattr(missing, "__len__") and bool(missing):
        return None
    text = str(value).strip()
    if not text or text.casefold() in {"none", "nan", "<na>"}:
        return None
    return text


def _sse_product_class(subclass: Any) -> str | None:
    value = _required_text(subclass)
    return None if value is None else f"sse-fund-subclass-{value}"


def _szse_product_class(fund_class: Any, investment_class: Any) -> str | None:
    normalized_fund_class = _required_text(fund_class)
    normalized_investment_class = _required_text(investment_class)
    if normalized_fund_class is None or normalized_investment_class is None:
        return None
    return f"szse-{normalized_fund_class}|{normalized_investment_class}"


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if value is None or value is pd.NA:
        return None
    missing = pd.isna(value)
    if not hasattr(missing, "__len__") and bool(missing):
        return None
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, (date, pd.Timestamp)):
        return str(value)[:10]
    return value
