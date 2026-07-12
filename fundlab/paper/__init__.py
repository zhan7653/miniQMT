"""Public interfaces for the durable ETF paper-trading core."""

from fundlab.paper.lifecycle import CreateAccountRequest, PaperAccountLifecycle
from fundlab.paper.repository import (
    AccountRecord,
    ImmutableHistoryError,
    LedgerVersionRecord,
    PaperLedgerRepository,
)
from fundlab.paper.runner import (
    AccountDayResult,
    DailyPaperRunner,
    DailyRunResult,
    PreflightError,
    ReplayRunError,
    ReplayRunResult,
)
from fundlab.paper.reporting import PaperReportBundle, build_paper_report
from fundlab.paper.strategy_registry import StrategyRegistry

__all__ = [
    "AccountDayResult",
    "AccountRecord",
    "CreateAccountRequest",
    "DailyPaperRunner",
    "DailyRunResult",
    "ImmutableHistoryError",
    "LedgerVersionRecord",
    "PaperAccountLifecycle",
    "PaperLedgerRepository",
    "PaperReportBundle",
    "PreflightError",
    "ReplayRunError",
    "ReplayRunResult",
    "StrategyRegistry",
    "build_paper_report",
]
