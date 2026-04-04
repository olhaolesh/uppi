"""Strategy boundary для розрахунку канону без зміни поточних формул."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from uppi.domain.canone_models import CanoneInput, CanoneResult
from uppi.domain.pescara2018 import calculate_canone


class CalculationStrategy(Protocol):
    """Явний контракт для calculation path, який використовує canone stage."""

    code: str

    def calculate(self, input_data: CanoneInput) -> CanoneResult:
        """Повертає результат розрахунку в current `CanoneResult` shape."""


@dataclass(frozen=True)
class Pescara2018Strategy:
    """Default strategy, що відтворює current Pescara 2018 calculation path."""

    code: str = "pescara2018_base"

    def calculate(self, input_data: CanoneInput) -> CanoneResult:
        """Делегує в чинний Pescara 2018 wrapper без зміни формул чи output."""
        return calculate_canone(input_data)


def get_default_calculation_strategy() -> CalculationStrategy:
    """Повертає canonical default strategy для поточного production flow."""
    return Pescara2018Strategy()
