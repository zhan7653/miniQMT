import json

import pytest

from fundlab.agent.report import ResearchReportError, parse_research_report, research_report_schema
from fundlab.trading import TradingRepository


def _report(source_ref="snap-1"):
    return {
        "thesis": "evidence is incomplete",
        "recommendation": "insufficient_evidence",
        "evidence": [{"claim": "price", "source_type": "canonical", "source_ref": source_ref, "as_of": "2026-09-07", "value": "1.2"}],
        "counter_evidence": [], "risks": [], "catalysts": [],
        "invalidation_conditions": [], "time_horizon": "one week",
        "confidence": 0.2, "recommended_intent": None,
    }


def test_research_report_requires_observed_sources():
    assert parse_research_report(json.dumps(_report()), known_sources={"snap-1"})["recommendation"] == "insufficient_evidence"
    with pytest.raises(ResearchReportError, match="unobserved source"):
        parse_research_report(json.dumps(_report("snap-missing")), known_sources={"snap-1"})


def test_research_report_schema_has_strict_nested_intent():
    intent = research_report_schema()["properties"]["recommended_intent"]
    assert intent["properties"]["target_weights"]["items"]["additionalProperties"] is False


def test_research_repository_never_creates_missing_database(tmp_path):
    path = tmp_path / "missing.sqlite3"
    with pytest.raises(FileNotFoundError):
        TradingRepository(path, read_only=True)
    assert not path.exists()
