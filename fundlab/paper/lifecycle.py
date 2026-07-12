from __future__ import annotations

from dataclasses import dataclass

from fundlab.paper.repository import AccountRecord, PaperLedgerRepository
from fundlab.trading import (
    AccountBindings,
    AccountStatus,
    ExecutionProfile,
    OrderStatus,
    RebalanceFrequency,
    ResearchRiskProfile,
)


@dataclass(frozen=True)
class CreateAccountRequest:
    account_id: str
    name: str
    bindings: AccountBindings
    execution_profile_id: str
    risk_profile_id: str
    initial_cash: float = 1_000_000.0
    schedule: RebalanceFrequency = RebalanceFrequency.DAILY


class PaperAccountLifecycle:
    """Guarded account management; closed accounts can never be reopened."""

    def __init__(self, repository: PaperLedgerRepository):
        self.repository = repository

    def register_profiles(
        self, execution_profile: ExecutionProfile, risk_profile: ResearchRiskProfile,
    ) -> None:
        with self.repository.transaction():
            self.repository.register_execution_profile(execution_profile)
            self.repository.register_risk_profile(risk_profile)

    def create(self, request: CreateAccountRequest) -> AccountRecord:
        return self.repository.create_account(
            account_id=request.account_id, name=request.name, initial_cash=request.initial_cash,
            bindings=request.bindings, execution_profile_id=request.execution_profile_id,
            risk_profile_id=request.risk_profile_id, schedule=request.schedule,
        )

    def pause(self, account_id: str) -> AccountRecord:
        with self.repository.transaction():
            account = self.repository.set_account_status(account_id, AccountStatus.PAUSED)
            for order in self.repository.pending_orders(account_id):
                self.repository.update_order_outcome(
                    order["order_id"], status=OrderStatus.CANCELLED,
                    requested_quantity=order["requested_quantity"], actual_quantity=0,
                    outcome_reason="account_paused",
                )
                self.repository.append_event(
                    account_id, account.selected_ledger_version, order["execution_date"],
                    "order_cancelled", "order", order["order_id"], {"reason": "account_paused"},
                )
        return self.repository.get_account(account_id)

    def resume(self, account_id: str) -> AccountRecord:
        with self.repository.transaction():
            return self.repository.set_account_status(account_id, AccountStatus.ACTIVE)

    def close(self, account_id: str) -> AccountRecord:
        with self.repository.transaction():
            account = self.repository.set_account_status(account_id, AccountStatus.CLOSED)
            for order in self.repository.pending_orders(account_id):
                self.repository.update_order_outcome(
                    order["order_id"], status=OrderStatus.CANCELLED,
                    requested_quantity=order["requested_quantity"], actual_quantity=0,
                    outcome_reason="account_closed",
                )
                self.repository.append_event(
                    account_id, account.selected_ledger_version, order["execution_date"],
                    "order_cancelled", "order", order["order_id"], {"reason": "account_closed"},
                )
        return self.repository.get_account(account_id)
