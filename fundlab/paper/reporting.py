from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from fundlab.paper.metrics import (
    available,
    benchmark_and_excess_metrics,
    metric_dict,
    performance_metrics,
    unavailable,
)
from fundlab.paper.repository import PaperLedgerRepository


CORPORATE_ACTION_DISCLOSURE = (
    "ETF corporate-action and dividend data is incomplete; account and benchmark results are "
    "price-return estimates, not complete total returns."
)


@dataclass(frozen=True)
class PaperReportBundle:
    payload: Mapping[str, Any]

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.payload, ensure_ascii=False, sort_keys=True, indent=indent)

    def to_csv(self) -> str:
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(("path", "value", "reason"))
        for path, value, reason in _metric_rows(self.payload):
            writer.writerow((path, "" if value is None else value, "" if reason is None else reason))
        return output.getvalue()

    def to_markdown(self) -> str:
        lines = [f"# Paper account report: {self.payload['account']['account_id']}", "", CORPORATE_ACTION_DISCLOSURE,
                 "", "| Metric | Value | Unavailable reason |", "|---|---:|---|"]
        for path, value, reason in _metric_rows(self.payload):
            rendered = "null" if value is None else str(value)
            lines.append(f"| `{path}` | {rendered} | {reason or ''} |")
        return "\n".join(lines) + "\n"


def build_paper_report(
    repository: PaperLedgerRepository,
    account_id: str,
    *,
    benchmark_closes: Mapping[str, float] | None = None,
    risk_free_rate: float = 0.0,
    rolling_windows: Sequence[int] = (5, 20, 60),
) -> PaperReportBundle:
    account = repository.get_account(account_id)
    version = account.selected_ledger_version
    snapshots = repository.account_snapshots(account_id, version)
    nav = [float(row["total_asset"]) for row in snapshots]
    dates = [str(row["trade_date"]) for row in snapshots]
    aligned_benchmark = None
    if benchmark_closes is not None and dates and all(day in benchmark_closes for day in dates):
        aligned_benchmark = [float(benchmark_closes[day]) for day in dates]

    account_metrics = performance_metrics(nav, risk_free_rate=risk_free_rate,
                                          rolling_windows=rolling_windows)
    benchmark_metrics, excess_metrics = benchmark_and_excess_metrics(
        nav, aligned_benchmark, risk_free_rate=risk_free_rate, rolling_windows=rolling_windows,
    )
    fills = repository.table_rows("paper_fills", where="account_id=? AND ledger_version=?",
                                  parameters=(account_id, version))
    positions = repository.table_rows("paper_daily_positions", where="account_id=? AND ledger_version=?",
                                      parameters=(account_id, version))
    latest = snapshots[-1] if snapshots else None
    average_assets = sum(nav) / len(nav) if nav else 0.0
    traded_value = sum(abs(float(row["price"]) * int(row["quantity"])) for row in fills)
    commissions = sum(float(row["commission"]) for row in fills)
    slippage = sum(float(row["slippage"]) for row in fills)
    latest_positions = [row for row in positions if latest and row["trade_date"] == latest["trade_date"]]
    data_versions = sorted({str(row["data_version"]) for row in snapshots})
    missing_benchmark = benchmark_closes is None or aligned_benchmark is None

    payload = metric_dict({
        "schema_version": "paper-report-v1",
        "account": {
            "account_id": account.account_id, "name": account.name, "status": account.status.value,
            "ledger_version": version, "benchmark_symbol": account.bindings.benchmark_symbol,
        },
        "period": {"start": dates[0] if dates else None, "end": dates[-1] if dates else None,
                   "observations": len(dates)},
        "returns_and_risk": account_metrics,
        "benchmark": {"return_type": "raw_close_price_return", "metrics": benchmark_metrics},
        "excess": excess_metrics,
        "costs": {
            "commission": available(commissions), "slippage": available(slippage),
            "total": available(commissions + slippage), "fill_count": available(len(fills)),
        },
        "turnover": unavailable("account NAV history is unavailable") if average_assets <= 0
                    else available(traded_value / average_assets),
        "exposure": {
            "cash_weight": unavailable("account snapshot is unavailable") if latest is None
                           else available(float(latest["cash"]) / float(latest["total_asset"])),
            "gross_weight": unavailable("account snapshot is unavailable") if latest is None
                            else available(float(latest["market_value"]) / float(latest["total_asset"])),
            "net_weight": unavailable("account snapshot is unavailable") if latest is None
                          else available(float(latest["market_value"]) / float(latest["total_asset"])),
            "position_count": available(len(latest_positions)),
        },
        "data_health": {
            "status": "incomplete" if missing_benchmark or not snapshots else "complete_with_disclosures",
            "data_versions": data_versions,
            "snapshot_count": len(snapshots),
            "benchmark_complete": not missing_benchmark,
            "corporate_actions_complete": False,
            "total_return_complete": False,
        },
        "disclosures": {
            "risk_free_rate": f"Risk-free rate assumption is {risk_free_rate}.",
            "benchmark_method": "Benchmark returns use raw, unadjusted close prices.",
            "corporate_actions": CORPORATE_ACTION_DISCLOSURE,
        },
    })
    return PaperReportBundle(payload)


def _metric_rows(value: Any, prefix: str = ""):
    if isinstance(value, Mapping):
        if set(value) == {"value", "reason"}:
            yield prefix, value["value"], value["reason"]
            return
        for key in sorted(value):
            path = f"{prefix}.{key}" if prefix else str(key)
            yield from _metric_rows(value[key], path)
        return
    if isinstance(value, (list, tuple)):
        yield prefix, json.dumps(value, ensure_ascii=False, sort_keys=True), None
        return
    yield prefix, value, None
