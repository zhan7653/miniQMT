"""Intent sources: how a strategy or an external agent talks to the kernel.

Everything here emits immutable :class:`fundlab.trading.PortfolioIntent`
objects through the :class:`fundlab.trading.IntentSource` protocol; only the
trading kernel turns intents into orders.
"""

from fundlab.strategies.agent_file import (
    AgentDecision,
    AgentDecisionError,
    FileIntentSource,
    load_agent_decision,
    write_agent_decision,
)
from fundlab.strategies.static import StaticAllocationSource

__all__ = [
    "AgentDecision",
    "AgentDecisionError",
    "FileIntentSource",
    "StaticAllocationSource",
    "load_agent_decision",
    "write_agent_decision",
]
