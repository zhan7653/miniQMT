"""Local decision agent: point-in-time features -> policy -> decision file.

The agent participates in the daily loop only through the JSON decision-file
contract (`data/agent/decisions/<account_id>/<date>.json`); it never talks to
the trading kernel directly. Policies implement the small `DecisionPolicy`
protocol while the service owns timing, idempotence, and validated delivery.
"""

from fundlab.agent.features import InstrumentSnapshot, build_instrument_snapshots
from fundlab.agent.policy import (
    AgentPolicyError,
    DecisionPolicy,
    MomentumRotationPolicy,
    PolicyDecision,
    build_policy,
)
from fundlab.agent.service import AgentDecisionService, AgentServiceError

__all__ = [
    "AgentDecisionService",
    "AgentPolicyError",
    "AgentServiceError",
    "DecisionPolicy",
    "InstrumentSnapshot",
    "MomentumRotationPolicy",
    "PolicyDecision",
    "build_instrument_snapshots",
    "build_policy",
]
