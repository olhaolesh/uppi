"""Тести для explicit calculation strategy boundary без зміни поточних формул."""

from __future__ import annotations

from uppi.domain.calculation_strategy import (
    Pescara2018Strategy,
    get_default_calculation_strategy,
)
from uppi.domain.canone_models import CanoneInput, ContractKind
from uppi.domain.pescara2018_calc import compute_base_canone
from uppi.services.visura_processor import VisuraProcessor


def _representative_inputs() -> list[CanoneInput]:
    """Повертає representative inputs для equivalence-перевірок current formula path."""
    return [
        CanoneInput(
            superficie_catastale=80.0,
            micro_zona="1",
            foglio="12",
            categoria_catasto="A/2",
            classe_catasto="3",
            count_a=1,
            count_b=1,
            count_c=0,
            count_d=1,
            arredato=0.1,
            energy_class="B",
            contract_kind=ContractKind.TRANSITORIO,
            durata_anni=4,
            istat=5.0,
            ignore_surcharges=True,
        ),
        CanoneInput(
            superficie_catastale=120.0,
            micro_zona=None,
            foglio="24",
            categoria_catasto="A/7",
            classe_catasto="2",
            count_a=2,
            count_b=4,
            count_c=4,
            count_d=6,
            arredato=0.0,
            energy_class="A",
            contract_kind=ContractKind.CONCORDATO,
            durata_anni=3,
            istat=0.0,
            ignore_surcharges=False,
        ),
        CanoneInput(
            superficie_catastale=48.0,
            micro_zona=None,
            foglio="02",
            categoria_catasto="A/2",
            classe_catasto="1",
            count_a=2,
            count_b=3,
            count_c=3,
            count_d=0,
            arredato=0.0,
            energy_class=None,
            contract_kind=ContractKind.STUDENTI,
            durata_anni=6,
            istat=None,
            ignore_surcharges=False,
        ),
    ]


def test_default_pescara2018_strategy_matches_current_compute_base_canone_outputs():
    """Перевіряє сценарій, описаний у назві тесту."""
    strategy = Pescara2018Strategy()

    for canone_input in _representative_inputs():
        assert strategy.calculate(canone_input) == compute_base_canone(canone_input)


def test_get_default_calculation_strategy_returns_pescara2018_strategy():
    """Перевіряє сценарій, описаний у назві тесту."""
    strategy = get_default_calculation_strategy()

    assert isinstance(strategy, Pescara2018Strategy)
    assert strategy.code == "pescara2018_base"


def test_visura_processor_passes_injected_calculation_strategy_to_default_canone_stage():
    """Перевіряє сценарій, описаний у назві тесту."""

    class FakeStrategy:
        """Мінімальний strategy-double для перевірки DI seam."""

        code = "custom_strategy"

        def calculate(self, input_data):
            """Не має викликатися в цьому constructor-only сценарії."""
            raise AssertionError("calculate should not be called in this constructor-only test")

    strategy = FakeStrategy()
    processor = VisuraProcessor(calculation_strategy=strategy)

    assert processor.canone_stage_service.calculation_strategy is strategy
