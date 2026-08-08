"""Immutable, validated evidence emitted by official Agent evaluations.

Evaluation evidence is deliberately separate from executable decision files:
the trading kernel continues to consume only ``PortfolioIntent`` through the
decision-file contract, while the dashboard reads this prospective audit
history.  One fully validated JSON inode is published per immutable revision.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from fundlab.common.canonical import deep_freeze, stable_digest, to_primitive
from fundlab.common.dates import audit_now

EVALUATION_SCHEMA_VERSION = "agent-evaluation-v1"
_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_REVISION_ID = re.compile(r"^[0-9a-f]{64}$")
_DATE_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_KEYS = {
    "schema_version",
    "revision_id",
    "content_hash",
    "status",
    "account_id",
    "policy_kind",
    "agent_id",
    "evidence_date",
    "as_of",
    "decision_date",
    "snapshot_id",
    "config_hash",
    "state_hash",
    "generated_at",
    "hold",
    "action",
    "reason",
    "target_weights",
    "parameters",
    "audit",
    "error_type",
    "error",
}


class AgentEvaluationError(ValueError):
    """Evaluation evidence is invalid, corrupt, or immutably conflicting."""


@dataclass(frozen=True)
class AgentEvaluation:
    revision_id: str
    content_hash: str
    status: str
    account_id: str
    policy_kind: str
    agent_id: str
    evidence_date: date
    as_of: date | None
    decision_date: date | None
    snapshot_id: str | None
    config_hash: str
    state_hash: str | None
    generated_at: datetime
    hold: bool | None
    action: str | None
    reason: str
    target_weights: Mapping[str, Decimal] = field(default_factory=dict)
    parameters: Mapping[str, Any] = field(default_factory=dict)
    audit: Mapping[str, Any] = field(default_factory=dict)
    error_type: str | None = None
    error: str | None = None
    source_path: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_weights", MappingProxyType(dict(self.target_weights)))
        object.__setattr__(self, "parameters", deep_freeze(self.parameters))
        object.__setattr__(self, "audit", deep_freeze(self.audit))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": EVALUATION_SCHEMA_VERSION,
            "revision_id": self.revision_id,
            "content_hash": self.content_hash,
            "status": self.status,
            "account_id": self.account_id,
            "policy_kind": self.policy_kind,
            "agent_id": self.agent_id,
            "evidence_date": self.evidence_date.isoformat(),
            "as_of": None if self.as_of is None else self.as_of.isoformat(),
            "decision_date": (
                None if self.decision_date is None else self.decision_date.isoformat()
            ),
            "snapshot_id": self.snapshot_id,
            "config_hash": self.config_hash,
            "state_hash": self.state_hash,
            "generated_at": self.generated_at.isoformat(),
            "hold": self.hold,
            "action": self.action,
            "reason": self.reason,
            "target_weights": {
                instrument_id: str(weight)
                for instrument_id, weight in self.target_weights.items()
            },
            "parameters": to_primitive(self.parameters),
            "audit": to_primitive(self.audit),
            "error_type": self.error_type,
            "error": self.error,
            "source_path": self.source_path,
        }


def evaluation_root(decision_root: str | Path) -> Path:
    """Keep monitoring evidence beside, but never inside, executable decisions."""

    return Path(decision_root).parent / "evaluations"


def write_agent_evaluation(
    root: str | Path,
    *,
    status: str,
    account_id: str,
    policy_kind: str,
    agent_id: str,
    config_hash: str,
    parameters: Mapping[str, Any],
    as_of: date | None = None,
    decision_date: date | None = None,
    snapshot_id: str | None = None,
    state_hash: str | None = None,
    hold: bool | None = None,
    action: str | None = None,
    reason: str = "",
    target_weights: Mapping[str, Any] | None = None,
    audit: Mapping[str, Any] | None = None,
    error_type: str | None = None,
    error: str | None = None,
    generated_at: datetime | None = None,
) -> AgentEvaluation:
    """Validate, stage, and atomically publish one immutable revision.

    Repeating the same semantic evaluation identity is idempotent.  If the
    policy produces different evidence for an identical snapshot/config/state
    identity, publication fails instead of silently rewriting history.
    """

    generated = generated_at or audit_now()
    evidence_date = as_of or generated.date()
    semantic = _semantic_payload(
        status=status,
        account_id=account_id,
        policy_kind=policy_kind,
        agent_id=agent_id,
        evidence_date=evidence_date,
        as_of=as_of,
        decision_date=decision_date,
        snapshot_id=snapshot_id,
        config_hash=config_hash,
        state_hash=state_hash,
        hold=hold,
        action=action,
        reason=reason,
        target_weights=target_weights or {},
        parameters=parameters,
        audit=audit or {},
        error_type=error_type,
        error=error,
    )
    revision_id = stable_digest(_identity_payload(semantic))
    body = {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "revision_id": revision_id,
        **semantic,
        "generated_at": generated.isoformat(),
    }
    body["content_hash"] = stable_digest(body)
    base = Path(root)
    path = base / account_id / evidence_date.isoformat() / f"{revision_id}.json"

    if path.exists():
        existing = _load_evaluation_path(path, expected_root=base)
        _prove_idempotent(existing, semantic)
        return existing

    staging_parent = base / ".staging"
    staging_parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(tempfile.mkdtemp(prefix="evaluation-", dir=staging_parent))
    staged = (
        staging_root / account_id / evidence_date.isoformat() / f"{revision_id}.json"
    )
    staged.parent.mkdir(parents=True, exist_ok=True)
    try:
        staged.write_text(
            json.dumps(body, ensure_ascii=False, indent=2),
            encoding="utf-8",
            newline="\n",
        )
        _load_evaluation_path(staged, expected_root=staging_root)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(staged, path)
        except FileExistsError:
            winner = _load_evaluation_path(path, expected_root=base)
            _prove_idempotent(winner, semantic)
            return winner
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
        try:
            staging_parent.rmdir()
        except OSError:
            pass

    return _load_evaluation_path(path, expected_root=base)


def load_agent_evaluation(
    root: str | Path,
    account_id: str,
    evidence_date: date,
    revision_id: str,
) -> AgentEvaluation:
    _validate_account_id(account_id)
    if not _REVISION_ID.fullmatch(revision_id):
        raise AgentEvaluationError(f"Invalid evaluation revision id: {revision_id!r}")
    base = Path(root)
    path = base / account_id / evidence_date.isoformat() / f"{revision_id}.json"
    if not path.is_file():
        raise AgentEvaluationError(f"Evaluation evidence does not exist: {path}")
    return _load_evaluation_path(path, expected_root=base)


def list_agent_evaluations(
    root: str | Path,
    account_id: str,
    *,
    limit_dates: int | None = None,
) -> tuple[AgentEvaluation, ...]:
    """Return newest revisions first, optionally bounded by distinct dates."""

    _validate_account_id(account_id)
    base = Path(root)
    account_root = base / account_id
    if not account_root.is_dir():
        return ()
    date_dirs: list[tuple[date, Path]] = []
    for path in account_root.iterdir():
        if not path.is_dir() or not _DATE_DIR.fullmatch(path.name):
            continue
        try:
            parsed = date.fromisoformat(path.name)
        except ValueError:
            continue
        date_dirs.append((parsed, path))
    date_dirs.sort(reverse=True)
    if limit_dates is not None:
        if limit_dates <= 0:
            return ()
        date_dirs = date_dirs[:limit_dates]
    records: list[AgentEvaluation] = []
    for _, directory in date_dirs:
        for path in directory.glob("*.json"):
            records.append(_load_evaluation_path(path, expected_root=base))
    records.sort(
        key=lambda item: (item.evidence_date, item.generated_at, item.revision_id),
        reverse=True,
    )
    return tuple(records)


def _semantic_payload(
    *,
    status: str,
    account_id: str,
    policy_kind: str,
    agent_id: str,
    evidence_date: date,
    as_of: date | None,
    decision_date: date | None,
    snapshot_id: str | None,
    config_hash: str,
    state_hash: str | None,
    hold: bool | None,
    action: str | None,
    reason: str,
    target_weights: Mapping[str, Any],
    parameters: Mapping[str, Any],
    audit: Mapping[str, Any],
    error_type: str | None,
    error: str | None,
) -> dict[str, Any]:
    _validate_account_id(account_id)
    if status not in {"ready", "error"}:
        raise AgentEvaluationError(f"Unknown evaluation status: {status!r}")
    if not str(policy_kind).strip() or not str(agent_id).strip():
        raise AgentEvaluationError("Evaluation policy kind and agent id are required")
    if not str(config_hash).strip():
        raise AgentEvaluationError("Evaluation config hash is required")
    weights = _weights(target_weights)
    payload = {
        "status": status,
        "account_id": account_id,
        "policy_kind": str(policy_kind).strip(),
        "agent_id": str(agent_id).strip(),
        "evidence_date": evidence_date.isoformat(),
        "as_of": None if as_of is None else as_of.isoformat(),
        "decision_date": None if decision_date is None else decision_date.isoformat(),
        "snapshot_id": None if snapshot_id is None else str(snapshot_id).strip(),
        "config_hash": str(config_hash).strip(),
        "state_hash": None if state_hash is None else str(state_hash).strip(),
        "hold": hold,
        "action": None if action is None else str(action).strip(),
        "reason": str(reason).strip(),
        "target_weights": weights,
        "parameters": to_primitive(parameters),
        "audit": to_primitive(audit),
        "error_type": None if error_type is None else str(error_type).strip(),
        "error": None if error is None else str(error).strip()[:2000],
    }
    if status == "ready":
        required = (as_of, decision_date, payload["snapshot_id"], payload["state_hash"])
        if any(value in {None, ""} for value in required):
            raise AgentEvaluationError(
                "Ready evaluation needs as_of, decision_date, snapshot_id, and state_hash"
            )
        if not isinstance(hold, bool):
            raise AgentEvaluationError("Ready evaluation hold flag must be boolean")
        if not payload["reason"]:
            raise AgentEvaluationError("Ready evaluation reason is required")
        if payload["error_type"] is not None or payload["error"] is not None:
            raise AgentEvaluationError("Ready evaluation cannot carry an error")
    else:
        if not payload["error_type"] or not payload["error"]:
            raise AgentEvaluationError("Failed evaluation needs error_type and error")
        if hold is not None or weights:
            raise AgentEvaluationError("Failed evaluation cannot carry a portfolio decision")
    return payload


def _identity_payload(semantic: Mapping[str, Any]) -> dict[str, Any]:
    identity = {
        key: semantic.get(key)
        for key in (
            "status",
            "account_id",
            "policy_kind",
            "agent_id",
            "evidence_date",
            "as_of",
            "decision_date",
            "snapshot_id",
            "config_hash",
            "state_hash",
        )
    }
    if semantic.get("status") == "error":
        identity["error_type"] = semantic.get("error_type")
        identity["error"] = semantic.get("error")
    return identity


def _load_evaluation_path(path: Path, *, expected_root: Path) -> AgentEvaluation:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AgentEvaluationError(f"Unreadable evaluation evidence {path}: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != _KEYS:
        raise AgentEvaluationError(f"Evaluation evidence fields differ from contract: {path}")
    try:
        evidence_date = date.fromisoformat(str(payload["evidence_date"]))
        generated_at = datetime.fromisoformat(str(payload["generated_at"]))
        as_of = None if payload["as_of"] is None else date.fromisoformat(str(payload["as_of"]))
        decision_date = (
            None
            if payload["decision_date"] is None
            else date.fromisoformat(str(payload["decision_date"]))
        )
    except ValueError as exc:
        raise AgentEvaluationError(f"Evaluation evidence has invalid dates: {path}") from exc
    semantic = _semantic_payload(
        status=str(payload["status"]),
        account_id=str(payload["account_id"]),
        policy_kind=str(payload["policy_kind"]),
        agent_id=str(payload["agent_id"]),
        evidence_date=evidence_date,
        as_of=as_of,
        decision_date=decision_date,
        snapshot_id=payload["snapshot_id"],
        config_hash=str(payload["config_hash"]),
        state_hash=payload["state_hash"],
        hold=payload["hold"],
        action=payload["action"],
        reason=str(payload["reason"]),
        target_weights=payload["target_weights"],
        parameters=payload["parameters"],
        audit=payload["audit"],
        error_type=payload["error_type"],
        error=payload["error"],
    )
    revision_id = str(payload["revision_id"])
    if not _REVISION_ID.fullmatch(revision_id):
        raise AgentEvaluationError(f"Invalid evaluation revision id in {path}")
    if stable_digest(_identity_payload(semantic)) != revision_id:
        raise AgentEvaluationError(f"Evaluation revision hash mismatch: {path}")
    body = dict(payload)
    declared_hash = str(body.pop("content_hash"))
    if stable_digest(body) != declared_hash:
        raise AgentEvaluationError(f"Evaluation content hash mismatch: {path}")
    try:
        relative = path.resolve().relative_to(expected_root.resolve())
    except ValueError as exc:
        raise AgentEvaluationError(f"Evaluation path escapes its evidence root: {path}") from exc
    expected = Path(str(payload["account_id"])) / evidence_date.isoformat() / f"{revision_id}.json"
    if relative != expected:
        raise AgentEvaluationError(f"Evaluation identity does not match its path: {path}")
    weights = {
        instrument_id: Decimal(str(weight))
        for instrument_id, weight in semantic["target_weights"].items()
    }
    return AgentEvaluation(
        revision_id=revision_id,
        content_hash=declared_hash,
        status=semantic["status"],
        account_id=semantic["account_id"],
        policy_kind=semantic["policy_kind"],
        agent_id=semantic["agent_id"],
        evidence_date=evidence_date,
        as_of=as_of,
        decision_date=decision_date,
        snapshot_id=semantic["snapshot_id"],
        config_hash=semantic["config_hash"],
        state_hash=semantic["state_hash"],
        generated_at=generated_at,
        hold=semantic["hold"],
        action=semantic["action"],
        reason=semantic["reason"],
        target_weights=weights,
        parameters=semantic["parameters"],
        audit=semantic["audit"],
        error_type=semantic["error_type"],
        error=semantic["error"],
        source_path=str(path),
    )


def _weights(raw: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        raise AgentEvaluationError("Evaluation target_weights must be a mapping")
    found: dict[str, str] = {}
    total = Decimal("0")
    for instrument_id, value in raw.items():
        symbol = str(instrument_id).strip()
        if not symbol:
            raise AgentEvaluationError("Evaluation contains an empty instrument id")
        try:
            weight = Decimal(str(value))
        except InvalidOperation as exc:
            raise AgentEvaluationError(f"Invalid evaluation weight: {symbol}={value}") from exc
        if not weight.is_finite() or weight < 0:
            raise AgentEvaluationError(f"Invalid evaluation weight: {symbol}={value}")
        total += weight
        found[symbol] = str(weight)
    if total > Decimal("1"):
        raise AgentEvaluationError(f"Evaluation weights exceed one: {total}")
    return dict(sorted(found.items()))


def _validate_account_id(account_id: str) -> None:
    if not _ACCOUNT_ID.fullmatch(str(account_id)):
        raise AgentEvaluationError(f"Invalid evaluation account id: {account_id!r}")


def _prove_idempotent(existing: AgentEvaluation, semantic: Mapping[str, Any]) -> None:
    existing_semantic = existing.to_dict()
    existing_semantic.pop("schema_version")
    existing_semantic.pop("revision_id")
    existing_semantic.pop("content_hash")
    existing_semantic.pop("generated_at")
    existing_semantic.pop("source_path")
    if existing_semantic != to_primitive(semantic):
        raise AgentEvaluationError(
            "An immutable evaluation identity already exists with different evidence: "
            f"{existing.source_path}"
        )
