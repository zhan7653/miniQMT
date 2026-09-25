"""Extensible domain definitions for autonomous research agents."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchDomain:
    domain_id: str
    instruction: str
    preferred_tools: tuple[str, ...] = ()


_DOMAINS: dict[str, ResearchDomain] = {
    "general": ResearchDomain("general", "Answer the research question with evidence and a clear no-action option."),
    "dividend": ResearchDomain("dividend", "Distinguish current cash yield from stale historical yield; verify units, freshness, and official evidence. Public discussion is optional context: use it only when a current event or disagreement could change the conclusion.", ("search_instruments", "dividend_history", "price_history", "read_opinion_snapshot", "search_opinion")),
    "crisis_drawdown": ResearchDomain("crisis_drawdown", "Assess systemic risk, index overlap, drawdown/rebound confirmation, volatility, liquidity, and historical analogues. Public discussion is optional context: check only recent, instrument-relevant changes and do not treat sentiment as confirmation.", ("crisis_features", "price_history", "portfolio_state", "read_opinion_snapshot", "compare_opinion_trend", "search_opinion")),
    "opinion": ResearchDomain("opinion", "Summarize public discussion with source quality, independent authors, disagreement, and explicit uncertainty. Start with compact summaries and expand individual details only when evidence needs inspection.", ("read_opinion_snapshot", "search_opinion", "compare_opinion_trend", "read_opinion_detail")),
}


def register_domain(domain: ResearchDomain) -> None:
    if not domain.domain_id or domain.domain_id == "general":
        raise ValueError("domain_id must be a non-empty id other than general")
    _DOMAINS[domain.domain_id] = domain


def get_domain(domain_id: str | None) -> ResearchDomain:
    key = (domain_id or "general").strip()
    try:
        return _DOMAINS[key]
    except KeyError as exc:
        raise ValueError(f"Unknown research domain: {key}") from exc


def domains() -> tuple[ResearchDomain, ...]:
    return tuple(_DOMAINS.values())
