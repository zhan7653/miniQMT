from __future__ import annotations

from abc import ABC, abstractmethod

from fundlab.data.portal import DataPortal


class Strategy(ABC):
    strategy_id: str

    @abstractmethod
    def on_rebalance(self, date: str, data_portal: DataPortal, context: dict) -> dict[str, float]:
        raise NotImplementedError

