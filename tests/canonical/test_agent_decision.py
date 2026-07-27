"""The local decision agent: point-in-time features -> policy -> decision file.

End-to-end discipline under test: the service decides only for the next
session after the account head, pins features to the published data head, and
its only side effect is a decision file the kernel's own loader accepts.  A
policy that cannot decide writes nothing — the contract turns that silence
into a hold.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date
from decimal import Decimal
from types import MappingProxyType

import pytest

from fundlab.agent import (
    AgentDecisionService,
    AgentPolicyError,
    AgentServiceError,
    build_policy,
)
from fundlab.agent.features import InstrumentSnapshot, build_instrument_snapshots
from fundlab.marketdata import CanonicalMarketData
from fundlab.pipeline import DailyPipeline
from fundlab.settings import AgentPolicySettings, AgentSettings, DailyAccountSettings
from fundlab.strategies import AgentDecisionError, load_agent_decision, write_agent_decision

from tests.canonical.fixtures import DAYS, FUTURE_DAYS, ready_market
from tests.canonical.test_daily_pipeline import (
    build_settings,
    evening_of,
    registry_with_calendars,
)


def policy_settings(**overrides) -> AgentPolicySettings:
    params = {
        "risk_instrument": "600000.SH",
        "defensive_instrument": "600000.SH",
        "momentum_days": 2,
        "threshold": "0",
        "risk_on": {"600000.SH": "0.6"},
        "risk_off": {"600000.SH": "0.1"},
    }
    params.update(overrides)
    return AgentPolicySettings("paper-agent", "momentum-rotation", params)


def snapshot_with_momentum(momentum: dict[int, float]) -> InstrumentSnapshot:
    return InstrumentSnapshot(
        instrument_id="600000.SH",
        as_of=DAYS[-1],
        sessions=10,
        last_close=13.0,
        momentum=MappingProxyType(momentum),
    )


def agent_settings(tmp_path, *accounts, policies=None):
    settings = build_settings(tmp_path, accounts or (
        DailyAccountSettings("paper-agent", "Paper Agent", Decimal("100000"), "agent-file"),
    ))
    if policies is None:
        policies = {"paper-agent": policy_settings()}
    return replace(settings, agent=AgentSettings(policies=policies))


def advance_once(settings) -> None:
    result = DailyPipeline(
        settings,
        registry=registry_with_calendars(),
        now_fn=lambda: evening_of(DAYS[-1]),
    ).run()
    assert result.status in {"ok", "up_to_date"}, result


# ---------------------------------------------------------------- features


def test_features_compute_adjusted_momentum_as_of_boundary(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    market = CanonicalMarketData.open(tmp_path / "market")

    full = build_instrument_snapshots(
        market, ["600000.SH"], as_of=DAYS[-1], windows=(2,),
    )["600000.SH"]
    assert full.sessions == len(DAYS)
    assert full.last_close == pytest.approx(13.0)
    assert full.momentum_for(2) == pytest.approx(13.0 / 11.0 - 1.0)

    # Two sessions of visible history cannot support a 2-session momentum:
    # the window must be absent, never extrapolated.
    early = build_instrument_snapshots(
        market, ["600000.SH"], as_of=DAYS[1], windows=(2,),
    )["600000.SH"]
    assert early.sessions == 2
    assert early.momentum_for(2) is None


# ------------------------------------------------------------------ policy


def test_momentum_policy_is_deterministic_and_config_bound():
    policy = build_policy("momentum-rotation", policy_settings().params)

    on = policy.decide_from_snapshots({
        "600000.SH": snapshot_with_momentum({2: 0.05}),
    })
    assert on.target_weights == {"600000.SH": Decimal("0.6")}
    assert "risk-on" in on.reason and "5.0000%" in on.reason

    off = policy.decide_from_snapshots({
        "600000.SH": snapshot_with_momentum({2: -0.05}),
    })
    assert off.target_weights == {"600000.SH": Decimal("0.1")}
    assert "risk-off" in off.reason

    reconfigured = build_policy(
        "momentum-rotation", policy_settings(momentum_days=3).params,
    )
    assert policy.config_hash != reconfigured.config_hash
    assert policy.config_hash == build_policy(
        "momentum-rotation", policy_settings().params,
    ).config_hash


def test_policy_fails_closed_without_enough_history():
    policy = build_policy("momentum-rotation", policy_settings().params)
    with pytest.raises(AgentPolicyError, match="refusing to guess"):
        policy.decide_from_snapshots({"600000.SH": snapshot_with_momentum({})})


def test_unknown_policy_kind_and_bad_weights_fail():
    with pytest.raises(AgentPolicyError, match="Unknown agent policy kind"):
        build_policy("mystery", {})
    with pytest.raises(AgentPolicyError, match="Unknown momentum-rotation params"):
        build_policy("momentum-rotation", policy_settings(momentun_days=3).params)
    with pytest.raises(AgentPolicyError, match="sum to <= 1"):
        build_policy("momentum-rotation", policy_settings(
            risk_on={"600000.SH": "0.9", "511010.SH": "0.3"},
        ).params)
    with pytest.raises(AgentPolicyError, match="empty instrument"):
        build_policy("momentum-rotation", policy_settings(risk_off={" ": "0.1"}).params)


# ----------------------------------------------------------------- service


def test_service_decides_next_session_and_drops_a_loadable_file(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)

    result = AgentDecisionService(settings).decide("paper-agent")

    assert result["written"] is True
    assert result["decision_date"] == FUTURE_DAYS[0].isoformat()
    assert result["as_of"] == DAYS[-1].isoformat()
    assert result["target_weights"] == {"600000.SH": "0.6"}

    decision = load_agent_decision(
        settings.daily.agent_decision_root, "paper-agent", FUTURE_DAYS[0],
    )
    assert decision is not None
    assert decision.target_weights == {"600000.SH": Decimal("0.6")}
    assert decision.agent_id == "momentum-rotation-v1"
    assert result["snapshot_id"] in decision.reason


def test_service_risk_off_on_falling_prices(tmp_path):
    ready_market(tmp_path / "market", close_values=(13.0, 12.0, 11.0, 10.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)

    result = AgentDecisionService(settings).decide("paper-agent")
    assert result["target_weights"] == {"600000.SH": "0.1"}


def test_service_overwrite_and_stale_date_guards(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)
    service = AgentDecisionService(settings)

    first = service.decide("paper-agent")
    repeated = service.decide("paper-agent")
    dry_repeated = service.decide("paper-agent", dry_run=True)
    persisted = load_agent_decision(
        settings.daily.agent_decision_root, "paper-agent", FUTURE_DAYS[0],
    )
    assert persisted is not None
    assert repeated["written"] is False
    assert repeated["skipped"] == "already_present"
    assert dry_repeated["skipped"] == "already_present"
    assert repeated["existing_content_hash"] == persisted.content_hash
    assert first["written"] is True
    assert service.decide("paper-agent", overwrite=True)["written"] is True

    with pytest.raises(AgentServiceError, match="already advanced"):
        service.decide("paper-agent", target_date=DAYS[-1])


def test_service_dry_run_writes_nothing(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)

    result = AgentDecisionService(settings).decide("paper-agent", dry_run=True)
    assert result["written"] is False
    assert load_agent_decision(
        settings.daily.agent_decision_root, "paper-agent", FUTURE_DAYS[0],
    ) is None


@pytest.mark.parametrize("options", (
    {},
    {"overwrite": True},
    {"dry_run": True},
    {"overwrite": True, "dry_run": True},
))
def test_service_rejects_an_existing_corrupt_decision_in_every_mode(tmp_path, options):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    advance_once(settings)
    path = (
        settings.daily.agent_decision_root
        / "paper-agent"
        / f"{FUTURE_DAYS[0].isoformat()}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(AgentServiceError, match="is invalid"):
        AgentDecisionService(settings).decide("paper-agent", **options)
    assert path.read_text(encoding="utf-8") == "{}"


def test_service_refuses_unconfigured_accounts(tmp_path):
    ready_market(tmp_path / "market")
    settings = agent_settings(tmp_path, policies={})
    with pytest.raises(AgentServiceError, match="no agent policy"):
        AgentDecisionService(settings).decide("paper-agent")

    static_settings = agent_settings(tmp_path, DailyAccountSettings(
        "paper-1", "Paper 1", Decimal("100000"), "static",
        {"600000.SH": Decimal("0.5")},
    ))
    with pytest.raises(AgentServiceError, match="Not a configured agent-file"):
        AgentDecisionService(static_settings).decide("paper-1")


def test_service_bootstraps_without_an_existing_ledger_head(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(tmp_path)
    result = AgentDecisionService(settings).decide("paper-agent", dry_run=True)
    assert result["decision_date"] == FUTURE_DAYS[0].isoformat()
    assert result["as_of"] == DAYS[-1].isoformat()
    assert result["target_weights"] == {"600000.SH": "0.6"}
    assert not settings.paths.trading_database.exists()


def test_decide_all_isolates_per_account_failures(tmp_path):
    ready_market(tmp_path / "market", close_values=(10.0, 11.0, 12.0, 13.0))
    settings = agent_settings(
        tmp_path,
        DailyAccountSettings("paper-agent", "Paper Agent", Decimal("100000"), "agent-file"),
        DailyAccountSettings("paper-agent-2", "Second", Decimal("100000"), "agent-file"),
    )
    advance_once(settings)

    outcomes = AgentDecisionService(settings).decide_all()
    by_account = {item["account_id"]: item for item in outcomes}
    assert by_account["paper-agent"]["written"] is True
    assert by_account["paper-agent-2"]["written"] is False
    assert "no agent policy" in by_account["paper-agent-2"]["error"]


# ------------------------------------------------------- shared write path


def test_write_agent_decision_never_leaves_a_bad_file(tmp_path):
    root = tmp_path / "decisions"
    with pytest.raises(AgentDecisionError):
        write_agent_decision(
            root,
            account_id="paper-agent",
            decision_date=date(2026, 7, 17),
            target_weights={"600000.SH": "-1"},
            reason="negative weight must be rejected",
            agent_id="test",
        )
    assert not (root / "paper-agent" / "2026-07-17.json").exists()

    written = write_agent_decision(
        root,
        account_id="paper-agent",
        decision_date=date(2026, 7, 17),
        target_weights={"600000.SH": Decimal("0.5")},
        reason="valid",
        agent_id="test",
    )
    assert written.target_weights == {"600000.SH": Decimal("0.5")}
    with pytest.raises(AgentDecisionError, match="already exists"):
        write_agent_decision(
            root,
            account_id="paper-agent",
            decision_date=date(2026, 7, 17),
            target_weights={"600000.SH": "0.5"},
            reason="no silent overwrite",
            agent_id="test",
        )

    path = root / "paper-agent" / "2026-07-17.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(AgentDecisionError, match="Agent decision"):
        write_agent_decision(
            root,
            account_id="paper-agent",
            decision_date=date(2026, 7, 17),
            target_weights={"600000.SH": "0.6"},
            reason="overwrite must not erase corrupt evidence",
            agent_id="test",
            overwrite=True,
        )
    assert path.read_text(encoding="utf-8") == "{}"


def test_concurrent_decision_writers_never_clobber_without_overwrite(tmp_path):
    root = tmp_path / "decisions"
    decision_date = date(2026, 7, 17)

    def attempt(index: int) -> str:
        try:
            write_agent_decision(
                root,
                account_id="paper-agent",
                decision_date=decision_date,
                target_weights={"600000.SH": Decimal(index) / Decimal("10")},
                reason=f"candidate {index}",
                agent_id=f"writer-{index}",
            )
        except AgentDecisionError:
            return "rejected"
        return "written"

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(attempt, range(1, 9)))

    assert outcomes.count("written") == 1
    assert outcomes.count("rejected") == 7
    persisted = load_agent_decision(root, "paper-agent", decision_date)
    assert persisted is not None
    assert persisted.reason.startswith("candidate ")
    assert not (root / ".staging").exists()
