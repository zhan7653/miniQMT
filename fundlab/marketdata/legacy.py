from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping

import pandas as pd
import pyarrow.parquet as pq

from fundlab.common.canonical import canonical_json, stable_digest
from fundlab.marketdata.contracts import (
    CoverageClaim,
    MarketTable,
    ObservationManifest,
    ProviderCapability,
    ProviderRequest,
)
from fundlab.marketdata.warehouse import MarketDataWarehouse


@dataclass(frozen=True)
class ProtectedArtifact:
    path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class LegacyAuditReport:
    audited_at: datetime
    legacy_root: str
    authoritative_report: str | None
    authoritative_report_sha256: str | None
    source_run_id: str | None
    partition_counts: Mapping[str, int]
    complete_rows: int
    raw_adjusted_pairs: int
    unpaired_scopes: int
    unbound_complete_partitions: int
    first_date: str | None
    last_date: str | None
    discovered_instruments: int
    selected_instruments: int
    instrument_coverage: float | None
    expected_day_coverage: float | None
    coverage_gates_passed: bool
    corrupted_partitions: tuple[str, ...]
    blockers: tuple[str, ...]
    protected_before: tuple[ProtectedArtifact, ...]
    protected_after: tuple[ProtectedArtifact, ...]
    protected_unchanged: bool
    source_import_ready: bool
    canonical_ready: bool
    notes: Mapping[str, Any] = field(default_factory=dict)

    @property
    def audit_hash(self) -> str:
        return stable_digest(self)

    def write(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(canonical_json(self), encoding="utf-8", newline="\n")
        return target


@dataclass(frozen=True)
class _Partition:
    partition_id: str
    symbol: str
    price_mode: str
    start_date: str
    end_date: str
    status: str
    row_count: int
    checksum: str | None
    storage_path: str | None


class LegacyV2Importer:
    """One-time, read-only adapter for the existing v2 collector artifacts."""

    def __init__(
        self,
        *,
        legacy_root: str | Path,
        legacy_report_root: str | Path,
        protected_v1_db: str | Path | None = None,
        protected_v1_parquet_root: str | Path | None = None,
    ) -> None:
        self.legacy_root = Path(legacy_root).resolve()
        self.report_root = Path(legacy_report_root).resolve()
        self.catalog_path = self.legacy_root / "catalog.sqlite3"
        self.protected_v1_db = None if protected_v1_db is None else Path(protected_v1_db).resolve()
        self.protected_v1_parquet_root = (
            None if protected_v1_parquet_root is None else Path(protected_v1_parquet_root).resolve()
        )

    def audit(self) -> LegacyAuditReport:
        protected_paths = self._protected_paths()
        protected_before = tuple(_artifact(path) for path in protected_paths)
        report_path, report = self._authoritative_report()
        all_partitions = self._partitions()
        bound_ids, evidence_digest_valid = _authoritative_partition_ids(report)
        evidence_errors = self._evidence_errors(report)
        partition_by_id = {item.partition_id: item for item in all_partitions}
        missing_bound = sorted(bound_ids - set(partition_by_id))
        partitions = [partition_by_id[item] for item in sorted(bound_ids & set(partition_by_id))]
        unbound_complete = sum(
            item.status == "complete" and item.partition_id not in bound_ids for item in all_partitions
        )
        counts: dict[str, int] = {}
        for item in partitions:
            counts[item.status] = counts.get(item.status, 0) + 1
        complete = [item for item in partitions if item.status == "complete"]
        corrupted: list[str] = []
        complete_rows = 0
        first_date: str | None = None
        last_date: str | None = None
        for item in complete:
            try:
                self._verify_partition(item)
            except Exception as exc:
                corrupted.append(f"{item.partition_id}:{type(exc).__name__}:{exc}")
                continue
            complete_rows += item.row_count
            first_date = item.start_date if first_date is None else min(first_date, item.start_date)
            last_date = item.end_date if last_date is None else max(last_date, item.end_date)

        modes_by_scope: dict[tuple[str, str, str], set[str]] = {}
        for item in complete:
            modes_by_scope.setdefault((item.symbol, item.start_date, item.end_date), set()).add(item.price_mode)
        paired = sum(modes >= {"raw", "adjusted"} for modes in modes_by_scope.values())
        unpaired = sum(modes != {"raw", "adjusted"} for modes in modes_by_scope.values())

        gates = bool(report.get("coverage_gates_passed", False)) if report else False
        blockers: list[str] = []
        if report_path is None:
            blockers.append("authoritative_collect_report_missing")
        elif report.get("status") != "complete":
            blockers.append("authoritative_collect_run_not_complete")
        if not bound_ids:
            blockers.append("partition_scope_not_bound_by_report")
        if not evidence_digest_valid:
            blockers.append("collect_evidence_digest_mismatch")
        if missing_bound:
            blockers.append("collect_evidence_partition_missing_from_catalog")
        blockers.extend(evidence_errors)
        if not gates:
            blockers.append("legacy_coverage_gates_failed")
        if corrupted:
            blockers.append("legacy_partition_integrity_failed")
        if unpaired:
            blockers.append("raw_adjusted_partition_scope_mismatch")
        if not complete:
            blockers.append("no_complete_market_partitions")
        # These are known capability gaps, not guesses inferred from row counts.
        blockers.extend((
            "ordinary_stock_universe_not_collected",
            "corporate_actions_not_collected",
            "price_limit_rules_not_collected",
            "instrument_settlement_rules_not_collected",
        ))
        protected_after = tuple(_artifact(path) for path in protected_paths)
        protected_unchanged = protected_before == protected_after
        if not protected_unchanged:
            blockers.append("protected_legacy_artifact_changed_during_audit")
        source_import_ready = (
            report_path is not None
            and report.get("status") == "complete"
            and bool(bound_ids)
            and evidence_digest_valid
            and not missing_bound
            and not evidence_errors
            and bool(complete)
            and not corrupted
            and protected_unchanged
        )
        canonical_ready = source_import_ready and not blockers
        selected = tuple(report.get("selected_symbols", ())) if report else ()
        evidence = report.get("evidence", {}) if report else {}
        fund_master = evidence.get("fund_master", ())
        return LegacyAuditReport(
            datetime.now(timezone.utc),
            self.legacy_root.as_posix(),
            None if report_path is None else report_path.as_posix(),
            None if report_path is None else _hash_file(report_path),
            report.get("run_id") if report else None,
            dict(sorted(counts.items())),
            complete_rows,
            paired,
            unpaired,
            unbound_complete,
            first_date,
            last_date,
            int(report.get("discovered_count", len(fund_master))) if report else 0,
            len(selected),
            _optional_float(report.get("instrument_coverage")) if report else None,
            _optional_float(report.get("expected_day_coverage")) if report else None,
            gates,
            tuple(corrupted),
            tuple(sorted(set(blockers))),
            protected_before,
            protected_after,
            protected_unchanged,
            source_import_ready,
            canonical_ready,
            {
                "legacy_scope": "exchange-traded fund discovery only",
                "unbound_complete_partitions": (
                    "kept read-only and excluded; only fingerprints anchored by the authoritative report are importable"
                ),
                "source_observation_import": "allowed only with explicit acceptance of incomplete coverage",
                "canonical_publication": "blocked until every blocker is resolved",
            },
        )

    def import_source_observation(
        self,
        warehouse: MarketDataWarehouse,
        *,
        audit: LegacyAuditReport,
        allow_incomplete_source: bool = False,
    ) -> ObservationManifest:
        if not audit.source_import_ready:
            raise RuntimeError("Legacy source files failed the read-only import preconditions")
        if not audit.canonical_ready and not allow_incomplete_source:
            raise RuntimeError(
                "Legacy data is not canonical-ready; pass allow_incomplete_source only to preserve it as an observation"
            )
        report_path, report = self._authoritative_report()
        if report_path is None or _hash_file(report_path) != audit.authoritative_report_sha256:
            raise RuntimeError("Authoritative legacy report changed after the audit")
        evidence = report.get("evidence", {})
        master = tuple(evidence.get("fund_master", ()))
        calendar = tuple(str(item)[:10] for item in evidence.get("calendar", ()))
        if not master or not calendar:
            raise RuntimeError("Legacy collect evidence has no fund master or calendar")
        instruments = _legacy_instruments(master)
        calendar_frame = _legacy_calendar(calendar)
        bound_ids, digest_valid = _authoritative_partition_ids(report)
        if not digest_valid:
            raise RuntimeError("Authoritative collect evidence digest is invalid")
        complete = tuple(
            item for item in self._partitions()
            if item.partition_id in bound_ids and item.status == "complete"
        )
        request = ProviderRequest(
            ProviderCapability.DAILY_BARS_RAW,
            date.fromisoformat(min(calendar)),
            date.fromisoformat(max(calendar)),
            tuple(sorted(set(report.get("selected_symbols", ())))),
            {"legacy_run_id": report.get("run_id")},
        )
        complete_claim = audit.canonical_ready
        coverage = (
            CoverageClaim(MarketTable.INSTRUMENTS, complete_claim, detail="legacy fund discovery"),
            CoverageClaim(
                MarketTable.CALENDAR, complete_claim, request.start_date, request.end_date,
                detail="legacy xtquant trading calendar",
            ),
            CoverageClaim(
                MarketTable.DAILY_BARS, complete_claim, request.start_date, request.end_date,
                request.instrument_ids, "legacy raw/front-adjusted partitions",
            ),
        )
        return warehouse.record_streamed_observation(
            provider="xtquant-legacy-import",
            observed_at=datetime.fromtimestamp(report_path.stat().st_mtime, tz=timezone.utc),
            request=request,
            table_batches={
                MarketTable.INSTRUMENTS: (instruments,),
                MarketTable.CALENDAR: (calendar_frame,),
                MarketTable.DAILY_BARS: self._bar_batches(complete),
            },
            coverage=coverage,
            source_metadata={
                "legacy_audit_hash": audit.audit_hash,
                "legacy_report": report_path.as_posix(),
                "legacy_report_sha256": audit.authoritative_report_sha256,
                "legacy_run_id": audit.source_run_id,
                "legacy_blockers": audit.blockers,
                "migration_kind": "one_time_read_only_import",
            },
        )

    def _bar_batches(self, partitions: Iterable[_Partition]) -> Iterable[pd.DataFrame]:
        for item in sorted(partitions, key=lambda value: (value.symbol, value.start_date, value.price_mode)):
            self._verify_partition(item)
            path = Path(item.storage_path).resolve() / "part-0.parquet"
            frame = pd.read_parquet(path)
            source_payload = frame.apply(
                lambda row: canonical_json({
                    "source": row.get("source"), "updated_at": row.get("updated_at"),
                }),
                axis=1,
            )
            yield pd.DataFrame({
                "instrument_id": frame["symbol"].astype(str),
                "session_date": frame["date"].astype(str).str[:10],
                "price_mode": frame["price_mode"].astype(str),
                "open": frame["open"],
                "high": frame["high"],
                "low": frame["low"],
                "close": frame["close"],
                "volume": frame["volume"],
                "amount": frame["amount"],
                "suspended": frame["suspended"],
                "price_limit_state": "unknown",
                "previous_close": frame.get("pre_close"),
                "limit_up": frame.get("limit_up"),
                "limit_down": frame.get("limit_down"),
                "source_payload": source_payload,
            })

    def _authoritative_report(self) -> tuple[Path | None, Mapping[str, Any]]:
        candidates = sorted(
            self.report_root.glob("full-market-collect-*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            return None, {}
        path = candidates[0]
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError("Legacy collect report root must be an object")
        return path, payload

    def _partitions(self) -> tuple[_Partition, ...]:
        if not self.catalog_path.is_file():
            return ()
        connection = sqlite3.connect(f"file:{self.catalog_path.as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute("SELECT * FROM collection_partitions ORDER BY partition_id").fetchall()
        finally:
            connection.close()
        return tuple(_Partition(
            row["partition_id"], row["symbol"], row["price_mode"], row["start_date"], row["end_date"],
            row["status"], int(row["row_count"]), row["checksum"], row["storage_path"],
        ) for row in rows)

    def _evidence_errors(self, report: Mapping[str, Any]) -> list[str]:
        wrapper = report.get("evidence", {}) if report else {}
        evidence = wrapper.get("collect_evidence", {}) if isinstance(wrapper, dict) else {}
        if not isinstance(evidence, dict):
            return ["collect_evidence_missing"]
        errors: list[str] = []
        for field in (
            "run_id", "target_date", "selected_symbols", "discovered_count", "quarantined_symbols",
            "missing_dates", "instrument_coverage", "expected_day_coverage", "coverage_gates_passed",
            "legacy_manifest_before", "legacy_manifest_after",
        ):
            if canonical_json(report.get(field)) != canonical_json(evidence.get(field)):
                errors.append(f"collect_evidence_report_field_mismatch:{field}")
        if canonical_json(wrapper.get("fund_master")) != canonical_json(evidence.get("fund_master")):
            errors.append("collect_evidence_report_field_mismatch:fund_master")
        if canonical_json(wrapper.get("calendar")) != canonical_json(evidence.get("calendar")):
            errors.append("collect_evidence_report_field_mismatch:calendar")
        if not self.catalog_path.is_file():
            return errors + ["collect_evidence_catalog_missing"]
        connection = sqlite3.connect(f"file:{self.catalog_path.as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            if not {"collection_runs", "throttle_events"} <= tables:
                return errors + ["collect_evidence_catalog_anchor_tables_missing"]
            run = connection.execute(
                "SELECT * FROM collection_runs WHERE run_id=?", (evidence.get("run_id"),),
            ).fetchone()
            if run is None:
                errors.append("collect_evidence_run_missing")
            elif (
                run["phase"] != "collect" or run["status"] != "complete"
                or run["provider"] != evidence.get("provider")
                or run["target_date"] != evidence.get("target_date")
                or run["config_hash"] != evidence.get("config_hash")
            ):
                errors.append("collect_evidence_run_identity_mismatch")
            digest = wrapper.get("collect_evidence_sha256")
            anchors = [row[0] for row in connection.execute(
                "SELECT reason FROM throttle_events WHERE run_id=? AND reason LIKE 'collect_evidence_sha256:%'",
                (evidence.get("run_id"),),
            )]
            if anchors != [f"collect_evidence_sha256:{digest}"]:
                errors.append("collect_evidence_catalog_anchor_mismatch")
        finally:
            connection.close()
        return errors

    def _verify_partition(self, item: _Partition) -> None:
        if item.storage_path is None or item.checksum is None:
            raise RuntimeError("complete partition has no storage path or checksum")
        directory = Path(item.storage_path).resolve()
        if self.legacy_root not in directory.parents:
            raise RuntimeError("partition storage path escapes the legacy root")
        metadata_path = directory / "partition.json"
        parquet_path = directory / "part-0.parquet"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        identity = metadata.get("identity", {})
        if identity.get("fingerprint") != item.partition_id:
            raise RuntimeError("partition fingerprint differs from catalog")
        if metadata.get("checksum") != item.checksum or _hash_file(parquet_path) != item.checksum:
            raise RuntimeError("partition checksum mismatch")
        if metadata.get("row_count") != item.row_count or pq.read_metadata(parquet_path).num_rows != item.row_count:
            raise RuntimeError("partition row count mismatch")

    def _protected_paths(self) -> tuple[Path, ...]:
        found: list[Path] = []
        if self.protected_v1_db is not None and self.protected_v1_db.is_file():
            found.append(self.protected_v1_db)
        if self.protected_v1_parquet_root is not None and self.protected_v1_parquet_root.is_dir():
            found.extend(sorted(path.resolve() for path in self.protected_v1_parquet_root.rglob("*") if path.is_file()))
        return tuple(found)


def _legacy_instruments(rows: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    found = []
    for row in rows:
        symbol = str(row["symbol"])
        code, _, suffix = symbol.partition(".")
        product_type = str(row.get("product_type") or "unknown")
        asset_type = "etf" if "ETF" in product_type.upper() else "fund"
        found.append({
            "instrument_id": symbol,
            "exchange": str(row.get("exchange") or suffix),
            "local_code": code,
            "asset_type": asset_type,
            "name": str(row.get("name") or symbol),
            "currency": "CNY",
            "listed_date": row.get("listed_date"),
            "delisted_date": row.get("delisted_date"),
            "board": row.get("management_type"),
            "buy_lot": int(row.get("lot_size") or 100),
            "price_tick": float(row.get("price_tick") or 0.01),
            "sell_delay_sessions": 1,
            "price_limit_ratio": None,
            "source_payload": canonical_json(row),
        })
    return pd.DataFrame(found)


def _legacy_calendar(days: Iterable[str]) -> pd.DataFrame:
    return pd.DataFrame([
        {"exchange": exchange, "session_date": day, "is_open": True,
         "source_payload": canonical_json({"legacy_calendar": True})}
        for day in sorted(set(days)) for exchange in ("SH", "SZ")
    ])


def _artifact(path: Path) -> ProtectedArtifact:
    return ProtectedArtifact(path.as_posix(), path.stat().st_size, _hash_file(path))


def _hash_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _authoritative_partition_ids(report: Mapping[str, Any]) -> tuple[set[str], bool]:
    wrapper = report.get("evidence", {}) if report else {}
    evidence = wrapper.get("collect_evidence", {}) if isinstance(wrapper, dict) else {}
    if not isinstance(evidence, dict):
        return set(), False
    fingerprints = {str(item) for item in evidence.get("partition_fingerprints", ())}
    expected = wrapper.get("collect_evidence_sha256")
    return fingerprints, bool(expected) and stable_digest(evidence) == expected
