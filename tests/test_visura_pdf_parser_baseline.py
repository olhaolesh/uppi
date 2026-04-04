"""Baseline-тести для поточного контракту VisuraParser."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

import uppi.parsers.visura_pdf_parser as parser_module
from uppi.parsers.visura_pdf_parser import VisuraParser


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "visura_parser"


def _load_fixture(name: str) -> dict:
    """Завантажує fixture або тестовий артефакт для цього набору тестів."""
    with (FIXTURES_DIR / name).open("r", encoding="utf-8") as f:
        return json.load(f)


class _FakePage:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, text: str = "", blocks=None):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self._text = text
        self._blocks = blocks or []

    def get_text(self, mode: str):
        """Допоміжний тестовий хелпер для цього модуля."""
        if mode == "blocks":
            return self._blocks
        if mode == "text":
            return self._text
        raise AssertionError(f"Unexpected get_text mode: {mode}")


class _FakeDoc:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, pages_data):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self._pages = [_FakePage(text=p.get("text", ""), blocks=p.get("blocks", [])) for p in pages_data]
        self.closed = False

    def __len__(self):
        """Повертає кількість елементів у тестовому контейнері."""
        return len(self._pages)

    def __getitem__(self, idx: int):
        """Повертає потрібний елемент із тестового контейнера."""
        return self._pages[idx]

    def close(self):
        """Імітує закриття тестового ресурсу."""
        self.closed = True


class _FakeTable:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, rows):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.df = pd.DataFrame(rows)


def _patch_parser_io(monkeypatch, fixture_data: dict, *, camelot_error_pages: set[str] | None = None, open_exception=None):
    """Підміняє зовнішні залежності контрольованими тестовими double-об’єктами."""
    camelot_error_pages = camelot_error_pages or set()

    def fake_open(_pdf_path):
        """Повертає контрольований PDF double або кидає задану open-помилку."""
        if open_exception is not None:
            raise open_exception
        return _FakeDoc(fixture_data.get("pages", []))

    def fake_read_pdf(_pdf_path, *, pages: str, flavor: str):
        """Повертає контрольовані таблиці або page-level Camelot failure."""
        assert flavor == "lattice"
        if pages in camelot_error_pages:
            raise RuntimeError(f"camelot error on page {pages}")
        return [_FakeTable(rows) for rows in fixture_data.get("tables", {}).get(pages, [])]

    monkeypatch.setattr(parser_module.fitz, "open", fake_open)
    monkeypatch.setattr(parser_module.camelot, "read_pdf", fake_read_pdf)


def test_parser_happy_path_extracts_current_core_fields(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    fixture = _load_fixture("happy_path.json")
    _patch_parser_io(monkeypatch, fixture)

    result = VisuraParser().parse("dummy.pdf")

    assert isinstance(result, list)
    assert len(result) == 1

    immobile = result[0]
    assert immobile["locatore_surname"] == "ROSSI"
    assert immobile["locatore_name"] == "MARIO"
    assert immobile["locatore_codice_fiscale"] == "RSSMRA80A01H501Z"
    assert immobile["immobile_comune"] == "PESCARA"
    assert immobile["immobile_comune_code"] == "G482"
    assert immobile["foglio"] == "12"
    assert immobile["numero"] == "345"
    assert immobile["sub"] == "7"
    assert immobile["zona_cens"] == "2"
    assert immobile["micro_zona"] == "5"
    assert immobile["categoria"] == "A/2"
    assert immobile["classe"] == "3"
    assert immobile["consistenza"] == "5 vani"
    assert immobile["rendita"] == "€ 123.45"
    assert immobile["superficie_totale"] == 98.7
    assert immobile["superficie_escluse"] == 90.1
    assert immobile["superficie_raw"] == "Totale: 98,7 Totale escluse aree scoperte**: 90,1"


def test_parser_known_current_behavior_address_fragment_preserves_p_dot_in_via_name_and_does_not_extract_piano(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    fixture = _load_fixture("happy_path.json")
    _patch_parser_io(monkeypatch, fixture)

    immobile = VisuraParser().parse("dummy.pdf")[0]

    assert immobile["via_type"] == "VIA"
    assert immobile["via_name"] == "ROMA 10 P."
    assert immobile["via_num"] == "3"
    assert immobile["scala"] == "A"
    assert immobile["interno"] == "2"
    assert immobile["piano"] is None
    assert immobile["indirizzo_raw"] == "VIA ROMA 10 SCALA A INTERNO 2 P. 3"


def test_parser_partial_input_returns_current_partial_result(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    fixture = _load_fixture("partial_missing_context.json")
    _patch_parser_io(monkeypatch, fixture)

    result = VisuraParser().parse("dummy.pdf")

    assert len(result) == 1
    immobile = result[0]
    assert immobile["locatore_surname"] is None
    assert immobile["locatore_name"] is None
    assert immobile["locatore_codice_fiscale"] is None
    assert immobile["immobile_comune"] is None
    assert immobile["immobile_comune_code"] is None
    assert immobile["foglio"] == "99"
    assert immobile["numero"] == "100"
    assert immobile["sub"] == ""
    assert immobile["categoria"] == "A/3"
    assert immobile["rendita"] == "€ 456.0"
    assert immobile["superficie_totale"] is None
    assert immobile["superficie_escluse"] is None
    assert immobile["superficie_raw"] == "nessun dato superficie"
    assert "zona_cens" not in immobile
    assert "micro_zona" not in immobile
    assert "classe" not in immobile


def test_parser_current_page_level_camelot_failure_is_skipped_and_later_pages_continue(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    fixture = _load_fixture("continue_after_page_error.json")
    _patch_parser_io(monkeypatch, fixture, camelot_error_pages={"1"})

    result = VisuraParser().parse("dummy.pdf")

    assert len(result) == 1
    immobile = result[0]
    assert immobile["locatore_surname"] == "VERDI"
    assert immobile["locatore_name"] == "LUCA"
    assert immobile["locatore_codice_fiscale"] == "VRDLCU80A01H501Z"
    assert immobile["immobile_comune"] == "CHIETI"
    assert immobile["immobile_comune_code"] == "C632"
    assert immobile["foglio"] == "8"
    assert immobile["numero"] == "77"
    assert immobile["rendita"] == "€ 222.0"


def test_parser_returns_empty_list_when_pdf_cannot_be_opened(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    _patch_parser_io(monkeypatch, {}, open_exception=RuntimeError("cannot open pdf"))

    result = VisuraParser().parse("broken.pdf")

    assert result == []


def test_parser_known_current_behavior_blank_rendita_cell_raises_type_error(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    fixture = _load_fixture("malformed_blank_rendita.json")
    _patch_parser_io(monkeypatch, fixture)

    with pytest.raises(TypeError):
        VisuraParser().parse("dummy.pdf")


def test_parse_superficie_returns_raw_text_and_none_values_when_markers_missing():
    """Перевіряє сценарій, описаний у назві тесту."""
    parser = VisuraParser()

    parsed = parser._parse_superficie("nessun dato superficie")

    assert parsed == {
        "superficie_totale": None,
        "superficie_escluse": None,
        "superficie_raw": "nessun dato superficie",
    }


def test_parse_rendita_blank_returns_none_in_current_behavior():
    """Перевіряє сценарій, описаний у назві тесту."""
    parser = VisuraParser()

    assert parser._parse_rendita("   ") is None
