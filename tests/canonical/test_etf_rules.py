from __future__ import annotations

from datetime import date
import json

import pandas as pd
import pytest

from fundlab.common.canonical import canonical_json, stable_digest
from fundlab.marketdata import EtfRuleEvidenceBuilder, TradeRuleError


def _instrument(instrument_id, listed_date, product_class):
    local, exchange = instrument_id.split(".")
    return {
        "instrument_id": instrument_id,
        "exchange": exchange,
        "local_code": local,
        "asset_type": "etf",
        "listed_date": listed_date,
        "exchange_product_class": product_class,
    }


class _Client:
    def get_instrument_detail(self, instrument_id, *, iscomplete):
        assert iscomplete
        values = {
            "510300.SH": ("20120528", 70283376, 0.10),
            "513100.SH": ("20130515", 70283488, 0.10),
            "159915.SZ": ("20111209", 3203072, 0.20),
        }
        opened, category, ratio = values[instrument_id]
        previous = 10.0
        return {
            "OpenDate": opened,
            "secuCategory": category,
            "PreClose": previous,
            "UpStopPrice": previous * (1 + ratio),
            "DownStopPrice": previous * (1 - ratio),
            "PriceTick": 0.001,
        }


def test_etf_rule_evidence_reports_typed_exact_instrument_failures(tmp_path):
    class MissingDetailClient(_Client):
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            if instrument_id == "510300.SH":
                return {}
            return super().get_instrument_detail(instrument_id, iscomplete=iscomplete)

    instruments = pd.DataFrame([
        _instrument("510300.SH", "2012-05-28", "sse-fund-subclass-03"),
        _instrument("513100.SH", "2013-05-15", "sse-fund-subclass-33"),
    ])
    with pytest.raises(TradeRuleError) as failure:
        EtfRuleEvidenceBuilder(tmp_path, client=MissingDetailClient()).build(
            instruments,
            universe_as_of=date(2026, 7, 17),
            universe_observation_id="obs-official",
        )
    assert failure.value.instrument_ids == ("510300.SH",)

    missing_classes = instruments.copy()
    missing_classes.loc[:, "exchange_product_class"] = pd.NA
    with pytest.raises(TradeRuleError) as failure:
        EtfRuleEvidenceBuilder(tmp_path, client=_Client()).build(
            missing_classes,
            universe_as_of=date(2026, 7, 17),
            universe_observation_id="obs-official",
        )
    assert failure.value.instrument_ids == ("510300.SH", "513100.SH")


def test_etf_rule_evidence_materializes_historical_ratio_and_t0_transitions(tmp_path):
    instruments = pd.DataFrame([
        _instrument("510300.SH", "2012-05-28", "sse-fund-subclass-03"),
        _instrument("513100.SH", "2013-05-15", "sse-fund-subclass-33"),
        _instrument("159915.SZ", "2011-12-09", "szse-ETF|股票基金"),
    ])

    result = EtfRuleEvidenceBuilder(
        tmp_path, client=_Client(),
    ).build(
        instruments,
        universe_as_of=date(2026, 7, 17),
        universe_observation_id="obs-official",
    )

    rules = result.rules
    domestic = rules.loc[rules["instrument_id"].eq("510300.SH")]
    cross_border = rules.loc[rules["instrument_id"].eq("513100.SH")]
    chinext = rules.loc[rules["instrument_id"].eq("159915.SZ")]
    assert len(domestic) == 1 and domestic.iloc[0]["sell_delay_sessions"] == 1
    assert cross_border["sell_delay_sessions"].tolist() == [1, 0]
    assert cross_border.iloc[1]["effective_from"] == date(2015, 1, 19)
    assert chinext["price_limit_ratio"].tolist() == [0.10, 0.20]
    assert chinext.iloc[1]["effective_from"] == date(2020, 8, 24)
    assert result.instrument_count == 3 and result.rule_interval_count == 5
    assert result.report.is_file() and len(result.evidence_hash) == 64


def test_exchange_listing_date_remains_authoritative_when_detail_open_date_differs(tmp_path):
    class ConflictingOpenDateClient(_Client):
        def get_instrument_detail(self, instrument_id, *, iscomplete):
            detail = super().get_instrument_detail(instrument_id, iscomplete=iscomplete)
            detail["OpenDate"] = "20120529"
            return detail

    result = EtfRuleEvidenceBuilder(
        tmp_path, client=ConflictingOpenDateClient(),
    ).build(
        pd.DataFrame([
            _instrument("510300.SH", "2012-05-28", "sse-fund-subclass-03"),
        ]),
        universe_as_of=date(2026, 7, 17),
        universe_observation_id="obs-official",
    )

    payload = __import__("json").loads(result.report.read_text(encoding="utf-8"))
    assert result.rules.iloc[0]["effective_from"] == date(2012, 5, 28)
    assert payload["listing_date_conflicts"] == {
        "510300.SH": {
            "exchange_listed_date": "2012-05-28",
            "detail_open_date": "2012-05-29",
        },
    }


def test_semantically_identical_etf_rules_keep_stable_foundation_evidence_hash(tmp_path):
    class RepricedClient(_Client):
        def __init__(self):
            self.calls = 0

        def get_instrument_detail(self, instrument_id, *, iscomplete):
            self.calls += 1
            detail = super().get_instrument_detail(instrument_id, iscomplete=iscomplete)
            detail["PreClose"] *= 2
            detail["UpStopPrice"] *= 2
            detail["DownStopPrice"] *= 2
            return detail

    instruments = pd.DataFrame([
        _instrument("510300.SH", "2012-05-28", "sse-fund-subclass-03"),
    ])
    first = EtfRuleEvidenceBuilder(tmp_path, client=_Client()).build(
        instruments,
        universe_as_of=date(2026, 7, 17),
        universe_observation_id="obs-official",
    )
    client = RepricedClient()
    repriced = EtfRuleEvidenceBuilder(tmp_path, client=client).build(
        instruments,
        universe_as_of=date(2026, 7, 17),
        universe_observation_id="obs-official",
    )

    assert repriced.evidence_hash == first.evidence_hash
    assert repriced.rules.to_dict("records") == first.rules.to_dict("records")
    assert client.calls == 0


def test_modified_etf_rule_report_is_not_reused(tmp_path):
    instruments = pd.DataFrame([
        _instrument("510300.SH", "2012-05-28", "sse-fund-subclass-03"),
    ])
    first = EtfRuleEvidenceBuilder(tmp_path, client=_Client()).build(
        instruments,
        universe_as_of=date(2026, 7, 17),
        universe_observation_id="obs-official",
    )
    payload = json.loads(first.report.read_text(encoding="utf-8"))
    payload["rule_intervals"][0]["price_limit_ratio"] = 0.20
    first.report.write_text(canonical_json(payload), encoding="utf-8", newline="\n")

    class CountingClient(_Client):
        def __init__(self):
            self.calls = 0

        def get_instrument_detail(self, instrument_id, *, iscomplete):
            self.calls += 1
            return super().get_instrument_detail(instrument_id, iscomplete=iscomplete)

    client = CountingClient()
    rebuilt = EtfRuleEvidenceBuilder(tmp_path, client=client).build(
        instruments,
        universe_as_of=date(2026, 7, 17),
        universe_observation_id="obs-official",
    )

    assert client.calls == 1
    assert rebuilt.rules.iloc[0]["price_limit_ratio"] == 0.10


def test_etf_report_with_incomplete_raw_detail_hashes_is_not_reused(tmp_path):
    instruments = pd.DataFrame([
        _instrument("510300.SH", "2012-05-28", "sse-fund-subclass-03"),
    ])
    first = EtfRuleEvidenceBuilder(tmp_path, client=_Client()).build(
        instruments,
        universe_as_of=date(2026, 7, 17),
        universe_observation_id="obs-official",
    )
    payload = json.loads(first.report.read_text(encoding="utf-8"))
    payload["instrument_detail_sha256"] = {}
    invalid_hash = stable_digest(payload)
    invalid = tmp_path / f"etf-rules-2026-07-17-{invalid_hash[:16]}.json"
    invalid.write_text(canonical_json(payload), encoding="utf-8", newline="\n")
    first.report.unlink()

    class CountingClient(_Client):
        def __init__(self):
            self.calls = 0

        def get_instrument_detail(self, instrument_id, *, iscomplete):
            self.calls += 1
            return super().get_instrument_detail(instrument_id, iscomplete=iscomplete)

    client = CountingClient()
    EtfRuleEvidenceBuilder(tmp_path, client=client).build(
        instruments,
        universe_as_of=date(2026, 7, 17),
        universe_observation_id="obs-official",
    )

    assert client.calls == 1
