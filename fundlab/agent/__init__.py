"""Local decision agents: bounded research -> policy -> decision file.

The agent participates in the daily loop only through the JSON decision-file
contract (`data/agent/decisions/<account_id>/<date>.json`); it never talks to
the trading kernel directly. Policies implement the small `DecisionPolicy`
protocol while the service owns timing, idempotence, and validated delivery.
"""

from fundlab.agent.features import InstrumentSnapshot, build_instrument_snapshots
from fundlab.agent.charter import Charter, CharterError, load_charter
from fundlab.agent.dividend import DividendCandidate, build_dividend_candidates
from fundlab.agent.llm import ResponsesAPIError, ResponsesDividendValueAdviser
from fundlab.agent.policy import (
    AgentPolicyError,
    DecisionPolicy,
    DividendPolicyRuntime,
    DividendValuePolicy,
    Highlight,
    MomentumRotationPolicy,
    PolicyDecision,
    build_policy,
)
from fundlab.agent.service import AgentDecisionService, AgentServiceError

__all__ = [
    "AgentDecisionService",
    "AgentPolicyError",
    "AgentServiceError",
    "Charter",
    "CharterError",
    "DecisionPolicy",
    "DividendCandidate",
    "DividendPolicyRuntime",
    "DividendValuePolicy",
    "Highlight",
    "InstrumentSnapshot",
    "MomentumRotationPolicy",
    "PolicyDecision",
    "ResponsesAPIError",
    "ResponsesDividendValueAdviser",
    "build_dividend_candidates",
    "build_instrument_snapshots",
    "build_policy",
    "load_charter",
]
