from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from statistics import median
from time import sleep
from typing import Any, Mapping

import pandas as pd

from fundlab.common.canonical import stable_digest
from fundlab.marketdata.contracts import (
    CoverageClaim,
    MarketTable,
    ObservationError,
    ObservationPayload,
    ProviderCapability,
    ProviderRequest,
)
from fundlab.marketdata.sources.base import (
    JsonTransport,
    UrllibJsonTransport,
    now_utc,
    payload_hash,
    require_daily_scope,
    source_payload,
    split_instrument_id,
)


_KLINE_ENDPOINT = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"


class TencentAdjustmentFactorProvider:
    """Targeted raw/QFQ ratio audit for candidate adjustment events.

    This provider deliberately does not pretend to discover a complete corporate-
    action history.  A caller supplies candidate dates and expected event
    multipliers from another source; Tencent raw and forward-adjusted OHLC values
    independently confirm or reject each candidate.  The selected before/after rows
    and complete response hashes are retained for deterministic replay.
    """

    name = "tencent-public"
    backend_group = "tencent"
    capabilities = frozenset({ProviderCapability.ADJUSTMENT_FACTORS})

    def __init__(self, *, transport: JsonTransport | None = None) -> None:
        self._transport = transport or UrllibJsonTransport()

    @property
    def available(self) -> bool:
        return True

    def observe(self, request: ProviderRequest) -> ObservationPayload:
        require_daily_scope(request)
        if request.capability is not ProviderCapability.ADJUSTMENT_FACTORS:
            raise ValueError(f"Tencent factor audit does not support {request.capability.value}")
        raw_candidates = request.parameters.get("candidate_multipliers")
        if not isinstance(raw_candidates, Mapping):
            raise ValueError("Tencent factor audit requires candidate_multipliers")
        if set(map(str, raw_candidates)) != set(request.instrument_ids):
            raise ValueError("Tencent factor candidates must match requested instruments")
        candidates: dict[str, dict[date, float]] = {}
        for instrument_id in request.instrument_ids:
            values = raw_candidates.get(instrument_id)
            if not isinstance(values, Mapping) or not values:
                raise ValueError(f"Tencent factor candidates are empty: {instrument_id}")
            parsed: dict[date, float] = {}
            for raw_date, raw_multiplier in values.items():
                candidate_date = date.fromisoformat(str(raw_date)[:10])
                multiplier = float(raw_multiplier)
                if multiplier <= 0:
                    raise ValueError(f"Tencent factor candidate is invalid: {instrument_id}")
                parsed[candidate_date] = multiplier
            candidates[instrument_id] = parsed

        workers = int(request.parameters.get("max_workers", 4))
        retries = int(request.parameters.get("retries", 3))
        backoff = float(request.parameters.get("retry_backoff_seconds", 0.5))
        timeout = float(request.parameters.get("timeout_seconds", 20))
        relative_tolerance = float(request.parameters.get("relative_tolerance", 0.02))
        if (
            workers < 1 or workers > 8 or retries < 1 or backoff < 0
            or timeout <= 0 or relative_tolerance <= 0 or relative_tolerance > 0.1
        ):
            raise ValueError("Invalid Tencent factor audit parameters")

        completed: set[str] = set()
        errors: dict[str, str] = {}
        hashes: dict[str, Any] = {}
        outcomes: dict[str, Any] = {}
        rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}

        def fetch(instrument_id: str):
            return self._fetch_instrument(
                instrument_id,
                candidates[instrument_id],
                request=request,
                retries=retries,
                backoff=backoff,
                timeout=timeout,
                relative_tolerance=relative_tolerance,
            )

        with ThreadPoolExecutor(
            max_workers=min(workers, len(request.instrument_ids)),
            thread_name_prefix="tencent-factor-audit",
        ) as executor:
            futures = {
                executor.submit(fetch, instrument_id): instrument_id
                for instrument_id in request.instrument_ids
            }
            for future in as_completed(futures):
                instrument_id = futures[future]
                try:
                    found_rows, found_hashes, found_outcomes = future.result()
                except Exception as exc:
                    errors[instrument_id] = f"{type(exc).__name__}: {str(exc)[:500]}"
                    continue
                completed.add(instrument_id)
                hashes[instrument_id] = found_hashes
                outcomes[instrument_id] = found_outcomes
                for row in found_rows:
                    key = (instrument_id, str(row["effective_date"]))
                    previous = rows_by_key.get(key)
                    if previous is None or row["_relative_difference"] < previous[
                        "_relative_difference"
                    ]:
                        rows_by_key[key] = row

        if errors and not completed:
            raise ObservationError(
                "Tencent factor audit failed for every instrument: "
                + "; ".join(f"{key}={value}" for key, value in sorted(errors.items()))
            )
        rows = []
        for row in rows_by_key.values():
            item = dict(row)
            item.pop("_relative_difference", None)
            rows.append(item)
        frame = pd.DataFrame(rows, columns=(
            "factor_id", "instrument_id", "effective_date", "known_date",
            "price_multiplier", "source_payload",
        ))
        complete = completed == set(request.instrument_ids)
        return ObservationPayload(
            self.name,
            now_utc(),
            request,
            {MarketTable.ADJUSTMENT_FACTORS: frame},
            (CoverageClaim(
                MarketTable.ADJUSTMENT_FACTORS,
                complete,
                request.start_date,
                request.end_date,
                request.instrument_ids,
                (
                    "Tencent raw/QFQ candidate audit completed for every requested instrument"
                    if complete else
                    f"Tencent factor audit completed for {len(completed)}/{len(request.instrument_ids)}"
                ),
            ),),
            {
                "upstream": "Tencent Finance",
                "backend_group": self.backend_group,
                "transport": "web.ifzq.gtimg.cn raw and qfq daily K-line JSON",
                "response_sha256": hashes,
                "candidate_outcomes": outcomes,
                "request_errors": errors,
                "candidate_semantics": (
                    "targeted independent confirmation; absence is evidence, not a complete "
                    "corporate-action history claim"
                ),
            },
        )

    def _fetch_instrument(
        self,
        instrument_id: str,
        candidates: Mapping[date, float],
        *,
        request: ProviderRequest,
        retries: int,
        backoff: float,
        timeout: float,
        relative_tolerance: float,
    ) -> tuple[list[dict[str, Any]], Mapping[str, Any], Mapping[str, Any]]:
        local, exchange = split_instrument_id(instrument_id)
        symbol = ("sh" if exchange == "SH" else "sz") + local
        rows: list[dict[str, Any]] = []
        hashes: dict[str, Any] = {}
        outcomes: dict[str, Any] = {}
        for candidate_date, expected in sorted(candidates.items()):
            start = max(request.start_date, candidate_date - timedelta(days=40))
            end = min(request.end_date, candidate_date + timedelta(days=40))
            raw_payload = self._retry_json(
                symbol, start, end, adjusted=False,
                retries=retries, backoff=backoff, timeout=timeout,
            )
            qfq_payload = self._retry_json(
                symbol, start, end, adjusted=True,
                retries=retries, backoff=backoff, timeout=timeout,
            )
            key = candidate_date.isoformat()
            hashes[key] = {
                "raw": payload_hash(raw_payload),
                "qfq": payload_hash(qfq_payload),
            }
            raw_rows = _kline_rows(raw_payload, symbol, "day")
            qfq_rows = _kline_rows(qfq_payload, symbol, "qfqday")
            common = sorted(set(raw_rows) & set(qfq_rows))
            ratios = {
                day: _ohlc_ratio(qfq_rows[day], raw_rows[day]) for day in common
            }
            candidate_events: list[tuple[int, float, str, str, float]] = []
            previous_day: str | None = None
            for day in common:
                if previous_day is None:
                    previous_day = day
                    continue
                before = ratios.get(previous_day)
                after = ratios.get(day)
                if before is None or after is None or after <= 0:
                    previous_day = day
                    continue
                multiplier = before / after
                delta = abs((date.fromisoformat(day) - candidate_date).days)
                relative = abs(multiplier - expected) / expected
                if delta <= 31 and relative <= relative_tolerance:
                    candidate_events.append((delta, relative, previous_day, day, multiplier))
                previous_day = day
            if not candidate_events:
                outcomes[key] = {"status": "no_matching_ratio_transition"}
                continue
            delta, relative, before_day, effect_day, multiplier = min(candidate_events)
            identity = {
                "source": self.name,
                "instrument_id": instrument_id,
                "candidate_date": key,
                "effective_date": effect_day,
                "price_multiplier": round(multiplier, 12),
            }
            row = {
                "factor_id": f"factor-{stable_digest(identity)[:24]}",
                "instrument_id": instrument_id,
                "effective_date": effect_day,
                "known_date": effect_day,
                "price_multiplier": multiplier,
                "source_payload": source_payload(
                    candidate_date=key,
                    expected_price_multiplier=expected,
                    relative_difference=relative,
                    raw_response_sha256=hashes[key]["raw"],
                    qfq_response_sha256=hashes[key]["qfq"],
                    previous_session=before_day,
                    effective_session=effect_day,
                    raw_previous=raw_rows[before_day],
                    raw_effective=raw_rows[effect_day],
                    qfq_previous=qfq_rows[before_day],
                    qfq_effective=qfq_rows[effect_day],
                    derivation="median(qfq_ohlc/raw_ohlc)[t-1] / median(...)[t]",
                ),
                "_relative_difference": relative,
            }
            rows.append(row)
            outcomes[key] = {
                "status": "confirmed",
                "effective_date": effect_day,
                "price_multiplier": multiplier,
                "relative_difference": relative,
            }
        return rows, hashes, outcomes

    def _retry_json(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjusted: bool,
        retries: int,
        backoff: float,
        timeout: float,
    ) -> Mapping[str, Any]:
        last: Exception | None = None
        mode = "qfq" if adjusted else ""
        for attempt in range(retries):
            try:
                return self._transport.get_json(
                    _KLINE_ENDPOINT,
                    parameters={
                        "param": (
                            f"{symbol},day,{start.isoformat()},{end.isoformat()},640,{mode}"
                        ),
                    },
                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"},
                    timeout=timeout,
                )
            except Exception as exc:
                last = exc
                if attempt + 1 < retries and backoff:
                    sleep(backoff * (2 ** attempt))
        assert last is not None
        raise last


def _kline_rows(
    payload: Mapping[str, Any], symbol: str, key: str,
) -> dict[str, tuple[float, float, float, float]]:
    root = payload.get("data")
    item = root.get(symbol) if isinstance(root, Mapping) else None
    values = item.get(key) if isinstance(item, Mapping) else None
    if not isinstance(values, list):
        raise ObservationError(f"Tencent K-line response is missing {symbol}/{key}")
    rows: dict[str, tuple[float, float, float, float]] = {}
    for value in values:
        if not isinstance(value, list) or len(value) < 5:
            continue
        try:
            ohlc = tuple(float(value[index]) for index in (1, 2, 3, 4))
            date.fromisoformat(str(value[0])[:10])
        except (TypeError, ValueError):
            continue
        if all(number > 0 for number in ohlc):
            rows[str(value[0])[:10]] = ohlc  # type: ignore[assignment]
    return rows


def _ohlc_ratio(
    adjusted: tuple[float, float, float, float],
    raw: tuple[float, float, float, float],
) -> float | None:
    ratios = [left / right for left, right in zip(adjusted, raw) if right > 0]
    return median(ratios) if ratios else None


__all__ = ["TencentAdjustmentFactorProvider"]
