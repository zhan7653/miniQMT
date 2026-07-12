from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date
from typing import Mapping

from fundlab.trading.models import (
    DecisionEnvelope, DecisionSourceType, ValidationResult, ValidationStatus,
)
from fundlab.trading.profiles import ResearchRiskProfile


@dataclass(frozen=True)
class DecisionValidationContext:
    universe: frozenset[str]
    missing_symbols: frozenset[str] = frozenset()
    cross_border_symbols: frozenset[str] = frozenset()
    trusted_premium_discount_symbols: frozenset[str] = frozenset()


def validate_target_weights(
    target_weights: Mapping[str, float],
    context: DecisionValidationContext,
    profile: ResearchRiskProfile,
) -> ValidationResult:
    found: set[str] = set()
    if not target_weights:
        found.add("empty_targets")
    total = 0.0
    for symbol, raw_weight in target_weights.items():
        try:
            weight = float(raw_weight)
        except (TypeError, ValueError):
            found.add("invalid_weight")
            weight = 0.0
        else:
            if not math.isfinite(weight) or weight < 0:
                found.add("invalid_weight")
            if math.isfinite(weight):
                total += weight
        if symbol != "cash" and symbol not in context.universe:
            found.add("symbol_not_in_universe")
        if symbol in context.missing_symbols:
            found.add("missing_required_data")
        if (profile.reject_untrusted_premium_discount and symbol in context.cross_border_symbols
                and symbol not in context.trusted_premium_discount_symbols):
            found.add("untrusted_premium_discount")
        if profile.max_position_weight is not None and symbol != "cash" and weight > profile.max_position_weight:
            found.add("position_limit")
    if total > 1.0 + 1e-12 and not profile.allow_leverage:
        found.add("leverage_not_allowed")
    if profile.minimum_cash_weight is not None and float(target_weights.get("cash", 0.0)) < profile.minimum_cash_weight:
        found.add("minimum_cash_not_met")
    code_order = (
        "empty_targets", "invalid_weight", "leverage_not_allowed", "symbol_not_in_universe",
        "missing_required_data", "untrusted_premium_discount", "position_limit", "minimum_cash_not_met",
    )
    unique = tuple(code for code in code_order if code in found)
    return ValidationResult(
        ValidationStatus.REJECTED if unique else ValidationStatus.VALID,
        unique,
        ";".join(unique) or None,
    )


def build_decision_envelope(
    *, decision_id: str, account_id: str, source_type: DecisionSourceType,
    source_id: str, config_version: str, decision_date: date,
    target_weights: Mapping[str, float], reason: str, data_version: str,
    observation_hash: str, context: DecisionValidationContext,
    profile: ResearchRiskProfile, source_metadata: Mapping[str, object] | None = None,
) -> DecisionEnvelope:
    original = {str(symbol): value for symbol, value in target_weights.items()}
    validation = validate_target_weights(original, context, profile)
    return DecisionEnvelope(
        decision_id=decision_id, account_id=account_id, source_type=source_type,
        source_id=source_id, config_version=config_version, decision_date=decision_date,
        target_weights=original, reason=reason, data_version=data_version,
        observation_hash=observation_hash, validation=validation,
        original_json=json.dumps(original, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        source_metadata=source_metadata or {},
    )
