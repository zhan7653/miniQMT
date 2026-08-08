"""Local decision agents: bounded research -> policy -> decision file.

The agent participates in the daily loop only through the JSON decision-file
contract (`data/agent/decisions/<account_id>/<date>.json`); it never talks to
the trading kernel directly. Policies implement the small `DecisionPolicy`
protocol while the service owns timing, idempotence, and validated delivery.
"""

from fundlab.agent.benchmark import (
    BenchmarkEvaluation,
    PortfolioPerformancePoint,
    evaluate_benchmark,
)
from fundlab.agent.charter import Charter, CharterError, load_charter
from fundlab.agent.dividend import DividendCandidate, build_dividend_candidates
from fundlab.agent.evaluations import (
    AgentEvaluation,
    AgentEvaluationError,
    evaluation_root,
    list_agent_evaluations,
    load_agent_evaluation,
    write_agent_evaluation,
)
from fundlab.agent.features import (
    CrisisInstrumentFeatures,
    InstrumentSnapshot,
    build_aligned_return_window,
    build_crisis_features,
    build_instrument_snapshots,
)
from fundlab.agent.llm import ResponsesAPIError, ResponsesDividendValueAdviser
from fundlab.agent.price_signals import PriceSignalCandidate, build_price_signal_candidates
from fundlab.agent.policy import (
    AgentPolicyError,
    CorrelationRiskParityPolicy,
    CrisisDrawdownPolicy,
    DecisionPolicy,
    DividendRulesPolicy,
    DividendPolicyRuntime,
    DividendValuePolicy,
    DualMomentumPolicy,
    Highlight,
    InverseVolatilityPolicy,
    LowBetaVolatilityPolicy,
    MomentumRotationPolicy,
    MovingAverageGridPolicy,
    PolicyDecision,
    PortfolioPolicyRuntime,
    SectorMomentumPolicy,
    StPriceMomentumPolicy,
    TrendVolatilityTargetPolicy,
    build_policy,
)
from fundlab.agent.service import AgentDecisionService, AgentServiceError

__all__ = [
    "AgentDecisionService",
    "AgentEvaluation",
    "AgentEvaluationError",
    "AgentPolicyError",
    "AgentServiceError",
    "BenchmarkEvaluation",
    "Charter",
    "CharterError",
    "CorrelationRiskParityPolicy",
    "CrisisDrawdownPolicy",
    "CrisisInstrumentFeatures",
    "DecisionPolicy",
    "DividendCandidate",
    "DividendPolicyRuntime",
    "DividendRulesPolicy",
    "DividendValuePolicy",
    "DualMomentumPolicy",
    "Highlight",
    "InstrumentSnapshot",
    "InverseVolatilityPolicy",
    "LowBetaVolatilityPolicy",
    "MomentumRotationPolicy",
    "MovingAverageGridPolicy",
    "PolicyDecision",
    "PriceSignalCandidate",
    "PortfolioPolicyRuntime",
    "PortfolioPerformancePoint",
    "ResponsesAPIError",
    "ResponsesDividendValueAdviser",
    "SectorMomentumPolicy",
    "StPriceMomentumPolicy",
    "TrendVolatilityTargetPolicy",
    "build_aligned_return_window",
    "build_crisis_features",
    "build_dividend_candidates",
    "build_instrument_snapshots",
    "build_price_signal_candidates",
    "build_policy",
    "evaluate_benchmark",
    "evaluation_root",
    "list_agent_evaluations",
    "load_agent_evaluation",
    "load_charter",
    "write_agent_evaluation",
]
