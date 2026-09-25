"""Stable, validated structured output contract for research Agents."""

from __future__ import annotations

import json
from typing import Any


class ResearchReportError(ValueError):
    """The model report is not a valid research result."""


def research_report_schema() -> dict[str, object]:
    evidence = {
        "type": "object", "properties": {
            "claim": {"type": "string", "minLength": 1},
            "source_type": {"type": "string", "enum": ["canonical", "web", "opinion", "inference"]},
            "source_ref": {"type": "string", "minLength": 1},
            "as_of": {"type": ["string", "null"]},
            "value": {"type": ["string", "number", "null"]},
        }, "required": ["claim", "source_type", "source_ref", "as_of", "value"],
        "additionalProperties": False,
    }
    intent = {
        "type": ["object", "null"], "properties": {
            "target_weights": {"type": "array", "items": {
                "type": "object", "properties": {
                    "instrument_id": {"type": "string"}, "weight": {"type": "string"},
                }, "required": ["instrument_id", "weight"], "additionalProperties": False,
            }, "maxItems": 100},
            "reason": {"type": "string"}, "effective_date": {"type": ["string", "null"]},
        }, "required": ["target_weights", "reason", "effective_date"],
        "additionalProperties": False,
    }
    return {"type": "object", "properties": {
        "thesis": {"type": "string", "minLength": 1, "maxLength": 4000},
        "recommendation": {"type": "string", "enum": ["watch", "hold", "rebalance", "avoid", "insufficient_evidence"]},
        "evidence": {"type": "array", "items": evidence, "maxItems": 30},
        "counter_evidence": {"type": "array", "items": evidence, "maxItems": 20},
        "risks": {"type": "array", "items": {"type": "string"}, "maxItems": 20},
        "catalysts": {"type": "array", "items": {"type": "string"}, "maxItems": 20},
        "invalidation_conditions": {"type": "array", "items": {"type": "string"}, "maxItems": 20},
        "time_horizon": {"type": "string"}, "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "recommended_intent": intent,
    }, "required": ["thesis", "recommendation", "evidence", "counter_evidence", "risks", "catalysts", "invalidation_conditions", "time_horizon", "confidence", "recommended_intent"], "additionalProperties": False}


def parse_research_report(text: str, *, known_sources: set[str] | None = None) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ResearchReportError("Research final output is not JSON") from exc
    if not isinstance(value, dict):
        raise ResearchReportError("Research final output must be an object")
    required = set(research_report_schema()["required"])
    if set(value) != required:
        raise ResearchReportError("Research final output fields do not match the report contract")
    if not isinstance(value['thesis'], str) or not value['thesis'].strip():
        raise ResearchReportError('Research thesis must be nonempty')
    if value['recommendation'] not in {'watch', 'hold', 'rebalance', 'avoid', 'insufficient_evidence'}:
        raise ResearchReportError('Unknown research recommendation')
    for key in ('evidence', 'counter_evidence', 'risks', 'catalysts', 'invalidation_conditions'):
        if not isinstance(value[key], list):
            raise ResearchReportError(f'{key} must be an array')
    if not isinstance(value.get("confidence"), (int, float)) or not 0 <= value["confidence"] <= 1:
        raise ResearchReportError("Research confidence must be between 0 and 1")
    for entry in (*value["evidence"], *value["counter_evidence"]):
        if not isinstance(entry, dict) or not isinstance(entry.get('source_ref'), str) or not entry.get("source_ref"):
            raise ResearchReportError("Every research evidence item needs a source_ref")
        if entry.get('source_type') not in {'canonical', 'web', 'opinion', 'inference'}:
            raise ResearchReportError('Unknown evidence source type')
        context_ref = entry["source_ref"].startswith(("strategy_signals.", "opinion_context."))
        embedded_source = known_sources is not None and any(
            len(source) >= 12 and source in entry["source_ref"]
            for source in known_sources
            if source.startswith(("http", "snap-", "opinion-", "tool:"))
        )
        if known_sources is not None and entry["source_type"] != "inference" and entry["source_ref"] not in known_sources and not context_ref and not embedded_source:
            raise ResearchReportError(f"Research evidence cites an unobserved source: {entry['source_ref']}")
    if value["recommendation"] == "rebalance" and value["recommended_intent"] is None:
        raise ResearchReportError("rebalance research must include recommended_intent")
    return value
