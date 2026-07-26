from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from importlib import import_module
import json
import math
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from fundlab.common.canonical import canonical_json, stable_digest, to_primitive
from fundlab.marketdata.contracts import TradeRuleError
from fundlab.marketdata.trade_rules import ETF_RULE_COLUMNS


CROSS_BORDER_T0_EFFECTIVE = date(2015, 1, 19)
CROSS_BORDER_T0_KNOWN = date(2015, 1, 9)
ETF_20_PERCENT_EFFECTIVE = date(2020, 8, 24)
ETF_20_PERCENT_KNOWN = date(2020, 6, 12)

_SSE_T0_SUBCLASSES = {"02", "04", "05", "06", "07", "32", "33", "34", "36", "37", "38"}
_SSE_CROSS_BORDER_SUBCLASSES = {"04", "33", "34"}
_SSE_20_PERCENT_SUBCLASSES = {"09", "31"}
_SZ_T0_CATEGORIES = {3211264, 3215360, 3219456, 3223552, 3227648, 3235840}
_SZ_T1_CATEGORIES = {3203072, 3207168}


@dataclass(frozen=True)
class EtfRuleEvidenceResult:
    rules: pd.DataFrame
    evidence_hash: str
    report: Path
    instrument_count: int
    rule_interval_count: int


class EtfRuleEvidenceBuilder:
    """Build dated ETF execution rules from exchange classes and current limit evidence."""

    def __init__(self, report_root: str | Path, *, client: Any | None = None) -> None:
        self.report_root = Path(report_root).resolve()
        self._client = client

    def build(
        self,
        instruments: pd.DataFrame,
        *,
        universe_as_of: date,
        universe_observation_id: str,
    ) -> EtfRuleEvidenceResult:
        etfs = instruments.loc[
            instruments["asset_type"].astype(str).eq("etf")
        ].sort_values("instrument_id", kind="stable").reset_index(drop=True)
        if etfs.empty or etfs["exchange_product_class"].isna().any():
            raise TradeRuleError("ETF rules require exchange product classes for every ETF")
        source_identity = {
            "membership_and_classification": "SSE/SZSE public fund lists",
            "current_limit_calibration": "MiniQMT/xtquant get_instrument_detail",
            "cross_border_t0_rule": (
                "https://english.sse.com.cn/news/newsrelease/c/4947611.shtml"
            ),
            "sse_etf_rule_faq": (
                "https://www.sse.com.cn/assortment/fund/etf/question/c/c_20240118_5734755.shtml"
            ),
        }
        instrument_ids = tuple(map(str, etfs["instrument_id"]))
        classifications = Counter(map(str, etfs["exchange_product_class"]))
        # The universe observation is immutable.  Once its complete ETF rule
        # evidence exists, reuse it before making 1,602 local client calls.
        candidates = sorted(
            self.report_root.glob(f"etf-rules-{universe_as_of.isoformat()}-*.json"),
            key=lambda item: item.stat().st_mtime_ns,
            reverse=True,
        ) if self.report_root.exists() else []
        for candidate in candidates:
            verified = _verified_etf_rule_report(candidate, universe_as_of, etfs)
            if verified is None:
                continue
            existing, frame, evidence_hash = verified
            if (
                existing.get("decision_source")
                == "https://github.com/zhan7653/miniQMT/issues/7"
                and existing.get("decision_revision") == 2
                and existing.get("kind") == "etf_trade_rule_evidence"
                and str(existing.get("universe_as_of"))[:10] == universe_as_of.isoformat()
                and existing.get("universe_observation_id") == universe_observation_id
                and tuple(map(str, existing.get("instrument_ids", ()))) == instrument_ids
                and existing.get("exchange_class_counts")
                == dict(sorted(classifications.items()))
                and existing.get("source_identity") == source_identity
            ):
                return EtfRuleEvidenceResult(
                    frame,
                    evidence_hash,
                    candidate,
                    len(etfs),
                    len(frame),
                )
        client = self._client or import_module("xtquant.xtdata")
        if hasattr(client, "enable_hello"):
            client.enable_hello = False

        rules: list[dict[str, Any]] = []
        detail_hashes: dict[str, str] = {}
        listing_date_conflicts: dict[str, dict[str, str]] = {}
        for instrument in etfs.to_dict("records"):
            instrument_id = str(instrument["instrument_id"])
            detail = client.get_instrument_detail(instrument_id, iscomplete=True)
            if not isinstance(detail, Mapping) or not detail:
                raise TradeRuleError(f"MiniQMT has no current ETF rule evidence: {instrument_id}")
            detail_hashes[instrument_id] = stable_digest(_replace_non_finite(detail))
            listed = _required_date(instrument.get("listed_date"), f"listed_date:{instrument_id}")
            observed_open = _compact_date(detail.get("OpenDate"))
            if observed_open is not None and observed_open != listed:
                # The exchange list owns the listing lifecycle.  MiniQMT's OpenDate
                # is only current instrument-detail evidence and is known to mean
                # data availability/creation date for some old and money-market
                # ETFs.  Preserve the disagreement instead of changing history or
                # blocking otherwise usable rule evidence.
                listing_date_conflicts[instrument_id] = {
                    "exchange_listed_date": listed.isoformat(),
                    "xtquant_open_date": observed_open.isoformat(),
                }
            current_ratio = _observed_limit_ratio(detail, instrument_id)
            product_class = str(instrument["exchange_product_class"])
            settlement, cross_border = _settlement_class(
                str(instrument["exchange"]), product_class, detail, instrument_id,
            )
            if str(instrument["exchange"]) == "SH":
                subclass = product_class.rsplit("-", 1)[-1]
                expected_ratio = 0.20 if subclass in _SSE_20_PERCENT_SUBCLASSES else 0.10
                if abs(current_ratio - expected_ratio) > 1e-9:
                    raise TradeRuleError(
                        f"SSE class/current limit conflict: {instrument_id}/{subclass}"
                    )
            breakpoints = {listed}
            if current_ratio == 0.20 and listed < ETF_20_PERCENT_EFFECTIVE:
                breakpoints.add(ETF_20_PERCENT_EFFECTIVE)
            if cross_border and listed < CROSS_BORDER_T0_EFFECTIVE:
                breakpoints.add(CROSS_BORDER_T0_EFFECTIVE)
            starts = sorted(breakpoints)
            for index, effective_from in enumerate(starts):
                effective_to = (
                    None if index + 1 == len(starts)
                    else date.fromordinal(starts[index + 1].toordinal() - 1)
                )
                ratio = (
                    0.10
                    if current_ratio == 0.20 and effective_from < ETF_20_PERCENT_EFFECTIVE
                    else current_ratio
                )
                delay = (
                    1
                    if cross_border and effective_from < CROSS_BORDER_T0_EFFECTIVE
                    else settlement
                )
                known = listed
                evidence_parts = [
                    f"exchange_product_class={product_class}",
                    f"xtquant_current_limit_ratio={current_ratio:.2f}",
                    f"xtquant_secuCategory={detail.get('secuCategory')}",
                ]
                if effective_from == CROSS_BORDER_T0_EFFECTIVE:
                    known = CROSS_BORDER_T0_KNOWN
                    evidence_parts.append("SSE/SZSE cross-border T+0 notice effective 2015-01-19")
                if effective_from == ETF_20_PERCENT_EFFECTIVE:
                    known = ETF_20_PERCENT_KNOWN
                    evidence_parts.append("ChiNext reform ETF 20% rule effective 2020-08-24")
                identity = {
                    "instrument_id": instrument_id,
                    "effective_from": effective_from,
                    "effective_to": effective_to,
                    "ratio": ratio,
                    "delay": delay,
                    "class": product_class,
                    "version": 1,
                }
                rules.append({
                    "instrument_id": instrument_id,
                    "effective_from": effective_from,
                    "effective_to": effective_to,
                    "known_date": known,
                    "price_limit_ratio": ratio,
                    "sell_delay_sessions": delay,
                    "rule_id": f"cn-etf-{stable_digest(identity)[:20]}-v1",
                    "evidence": "; ".join(evidence_parts),
                })
        frame = pd.DataFrame(rules, columns=ETF_RULE_COLUMNS).sort_values(
            ["instrument_id", "effective_from"], kind="stable",
        ).reset_index(drop=True)
        semantic_evidence = {
            "decision_source": "https://github.com/zhan7653/miniQMT/issues/7",
            "decision_revision": 2,
            "kind": "etf_trade_rule_evidence",
            "universe_as_of": universe_as_of,
            "universe_observation_id": universe_observation_id,
            "instrument_ids": instrument_ids,
            "exchange_class_counts": dict(sorted(classifications.items())),
            "rule_intervals": frame.to_dict("records"),
            "source_identity": source_identity,
        }
        # Reuse immutable evidence when the dated rules are unchanged.  The
        # raw report still pins the exact MiniQMT calibration that first proved
        # those rules, while today's nominal quote cannot invalidate completed
        # foundation checkpoints merely because PreClose moved.
        semantic_primitive = to_primitive(semantic_evidence)
        candidates = sorted(
            self.report_root.glob(f"etf-rules-{universe_as_of.isoformat()}-*.json"),
            key=lambda item: item.stat().st_mtime_ns,
            reverse=True,
        ) if self.report_root.exists() else []
        for candidate in candidates:
            verified = _verified_etf_rule_report(candidate, universe_as_of, etfs)
            if verified is None:
                continue
            existing, _, evidence_hash = verified
            existing_semantic = {
                key: existing.get(key) for key in semantic_primitive
            }
            if existing_semantic == semantic_primitive:
                return EtfRuleEvidenceResult(
                    frame,
                    evidence_hash,
                    candidate,
                    len(etfs),
                    len(frame),
                )
        evidence_payload = {
            **semantic_evidence,
            "xtquant_instrument_detail_sha256": detail_hashes,
            "listing_date_conflicts": listing_date_conflicts,
            "observed_at": datetime.now(timezone.utc),
        }
        evidence_hash = stable_digest(evidence_payload)
        self.report_root.mkdir(parents=True, exist_ok=True)
        report = self.report_root / (
            f"etf-rules-{universe_as_of.isoformat()}-{evidence_hash[:16]}.json"
        )
        primitive = to_primitive(evidence_payload)
        if report.exists():
            if json.loads(report.read_text(encoding="utf-8")) != primitive:
                raise ValueError(f"Immutable ETF rule report collision: {report}")
        else:
            report.write_text(canonical_json(evidence_payload), encoding="utf-8", newline="\n")
        return EtfRuleEvidenceResult(
            frame,
            evidence_hash,
            report,
            len(etfs),
            len(frame),
        )


def _verified_etf_rule_report(
    path: Path,
    universe_as_of: date,
    etfs: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame, str] | None:
    """Verify immutable report identity and executable rule completeness before reuse."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    evidence_hash = stable_digest(payload)
    if path.name != (
        f"etf-rules-{universe_as_of.isoformat()}-{evidence_hash[:16]}.json"
    ):
        return None
    instrument_ids = tuple(map(str, etfs["instrument_id"]))
    detail_hashes = payload.get("xtquant_instrument_detail_sha256")
    if (
        not isinstance(detail_hashes, Mapping)
        or set(map(str, detail_hashes)) != set(instrument_ids)
        or any(
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value.lower())
            for value in detail_hashes.values()
        )
    ):
        return None
    intervals = payload.get("rule_intervals")
    if not isinstance(intervals, list) or any(
        not isinstance(row, Mapping) or not set(ETF_RULE_COLUMNS) <= set(row)
        for row in intervals
    ):
        return None
    frame = pd.DataFrame(intervals, columns=ETF_RULE_COLUMNS)
    try:
        for column in ("effective_from", "effective_to", "known_date"):
            frame[column] = [
                None if pd.isna(value) else pd.Timestamp(value).date()
                for value in frame[column]
            ]
        _validate_reused_rules(frame, etfs)
    except (TypeError, ValueError, TradeRuleError):
        return None
    conflicts = payload.get("listing_date_conflicts", {})
    if not isinstance(conflicts, Mapping) or not set(map(str, conflicts)) <= set(instrument_ids):
        return None
    return payload, frame, evidence_hash


def _validate_reused_rules(frame: pd.DataFrame, etfs: pd.DataFrame) -> None:
    expected = set(map(str, etfs["instrument_id"]))
    if frame.empty or set(map(str, frame["instrument_id"])) != expected:
        raise TradeRuleError("Reused ETF rules do not cover the exact universe")
    if frame["rule_id"].astype(str).duplicated().any():
        raise TradeRuleError("Reused ETF rule IDs are not unique")
    listed_dates = {
        str(row["instrument_id"]): _required_date(
            row["listed_date"], f"listed_date:{row['instrument_id']}",
        )
        for row in etfs.to_dict("records")
    }
    for instrument_id, rows in frame.groupby("instrument_id", sort=True):
        ordered = rows.sort_values("effective_from", kind="stable").to_dict("records")
        if ordered[0]["effective_from"] != listed_dates[str(instrument_id)]:
            raise TradeRuleError(f"Reused ETF rule starts after listing: {instrument_id}")
        for index, row in enumerate(ordered):
            start = row["effective_from"]
            end = row["effective_to"]
            known = row["known_date"]
            ratio = float(row["price_limit_ratio"])
            delay = int(row["sell_delay_sessions"])
            if (
                start is None
                or known is None
                or known > start
                or ratio not in {0.10, 0.20}
                or delay not in {0, 1}
                or not str(row["rule_id"]).strip()
                or not str(row["evidence"]).strip()
            ):
                raise TradeRuleError(f"Reused ETF rule is invalid: {instrument_id}")
            if index + 1 == len(ordered):
                if end is not None:
                    raise TradeRuleError(f"Reused ETF rule has no open interval: {instrument_id}")
            else:
                next_start = ordered[index + 1]["effective_from"]
                if end is None or end.toordinal() + 1 != next_start.toordinal():
                    raise TradeRuleError(f"Reused ETF rule intervals are not contiguous: {instrument_id}")


def _settlement_class(
    exchange: str,
    product_class: str,
    detail: Mapping[str, Any],
    instrument_id: str,
) -> tuple[int, bool]:
    if exchange == "SH":
        if not product_class.startswith("sse-fund-subclass-"):
            raise TradeRuleError(f"Unknown SSE ETF product class: {instrument_id}")
        subclass = product_class.rsplit("-", 1)[-1]
        return (0 if subclass in _SSE_T0_SUBCLASSES else 1), (
            subclass in _SSE_CROSS_BORDER_SUBCLASSES
        )
    category = int(detail.get("secuCategory", -1))
    if category in _SZ_T0_CATEGORIES:
        return 0, category == 3211264
    if category in _SZ_T1_CATEGORIES:
        return 1, False
    raise TradeRuleError(
        f"Unknown SZSE ETF settlement category: {instrument_id}/{category}"
    )


def _observed_limit_ratio(detail: Mapping[str, Any], instrument_id: str) -> float:
    try:
        previous = float(detail["PreClose"])
        upper = float(detail["UpStopPrice"])
        lower = float(detail["DownStopPrice"])
        tick = float(detail["PriceTick"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TradeRuleError(f"ETF current limit evidence is incomplete: {instrument_id}") from exc
    if previous <= 0 or upper <= previous or lower >= previous or tick <= 0:
        raise TradeRuleError(f"ETF current limit evidence is invalid: {instrument_id}")
    candidates = (0.10, 0.20)
    observed_up = upper / previous - 1
    observed_down = 1 - lower / previous
    ratio = min(candidates, key=lambda item: abs(observed_up - item) + abs(observed_down - item))
    tolerance = 2 * tick / previous + 0.001
    if abs(observed_up - ratio) > tolerance or abs(observed_down - ratio) > tolerance:
        raise TradeRuleError(f"ETF current limit ratio is neither 10% nor 20%: {instrument_id}")
    return ratio


def _compact_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text or text in {"0", "99999999"}:
        return None
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _required_date(value: Any, field: str) -> date:
    if value is None or pd.isna(value):
        raise TradeRuleError(f"Required ETF rule date is missing: {field}")
    return date.fromisoformat(str(value)[:10])


def _replace_non_finite(value: Any) -> Any:
    """Keep vendor sentinel values hashable without treating them as facts."""

    if isinstance(value, Mapping):
        return {str(key): _replace_non_finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_replace_non_finite(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return {"vendor_non_finite": repr(value)}
    return value
