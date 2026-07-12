from __future__ import annotations

import csv
import io
import json

from fundlab.paper.lifecycle import CreateAccountRequest, PaperAccountLifecycle
from fundlab.paper.reporting import CORPORATE_ACTION_DISCLOSURE, build_paper_report
from fundlab.paper.repository import PaperLedgerRepository
from fundlab.trading import AccountBindings, ExecutionProfile, ResearchRiskProfile


def _repository(tmp_path):
    repository = PaperLedgerRepository(tmp_path / "paper.db")
    lifecycle = PaperAccountLifecycle(repository)
    lifecycle.register_profiles(ExecutionProfile("execution", "v1"), ResearchRiskProfile("risk", "v1"))
    lifecycle.create(CreateAccountRequest(
        account_id="alpha", name="Alpha", initial_cash=100_000.0,
        bindings=AccountBindings("equal_weight", "v1", "universe-v1", "510300.SH", "v1", "v1"),
        execution_profile_id="execution", risk_profile_id="risk",
    ))
    for day, asset, benchmark in (("2024-01-02", 100_000.0, 10.0),
                                  ("2024-01-03", 101_000.0, 10.1),
                                  ("2024-01-04", 99_000.0, 9.9)):
        repository.record_daily_state(account_id="alpha", ledger_version=1, trade_date=day,
                                      cash=asset, positions={}, data_version="complete-v1",
                                      benchmark_value=benchmark)
    return repository


def test_json_csv_and_markdown_share_one_canonical_metric_payload(tmp_path):
    repository = _repository(tmp_path)
    bundle = build_paper_report(repository, "alpha",
                                benchmark_closes={"2024-01-02": 10, "2024-01-03": 10.1,
                                                  "2024-01-04": 9.9}, rolling_windows=(2,))
    payload = json.loads(bundle.to_json())
    rows = {row["path"]: row for row in csv.DictReader(io.StringIO(bundle.to_csv()))}
    path = "returns_and_risk.total_return"
    assert float(rows[path]["value"]) == payload["returns_and_risk"]["total_return"]["value"]
    assert f"`{path}`" in bundle.to_markdown()
    assert str(payload["returns_and_risk"]["total_return"]["value"]) in bundle.to_markdown()
    assert CORPORATE_ACTION_DISCLOSURE in bundle.to_markdown()
    assert rows["disclosures.corporate_actions"]["value"] == CORPORATE_ACTION_DISCLOSURE
    assert payload["benchmark"]["return_type"] == "raw_close_price_return"
    assert payload["data_health"]["corporate_actions_complete"] is False
    assert payload["data_health"]["total_return_complete"] is False


def test_unavailable_values_and_reasons_agree_in_every_format(tmp_path):
    repository = _repository(tmp_path)
    bundle = build_paper_report(repository, "alpha")
    payload = json.loads(bundle.to_json())
    metric = payload["benchmark"]["metrics"]["total_return"]
    rows = {row["path"]: row for row in csv.DictReader(io.StringIO(bundle.to_csv()))}
    row = rows["benchmark.metrics.total_return"]
    assert metric["value"] is None and metric["reason"]
    assert row["value"] == "" and row["reason"] == metric["reason"]
    assert metric["reason"] in bundle.to_markdown()
    assert payload["disclosures"]["risk_free_rate"] == "Risk-free rate assumption is 0.0."
