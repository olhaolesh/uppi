"""Focused tests for the future bulk `clients.csv` loading surface."""

from __future__ import annotations

import csv

from uppi.domain.clients_csv import load_clients_csv, load_clients_csv_with_issues


def test_load_clients_csv_reads_and_normalizes_rows(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    csv_path = tmp_path / "clients.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=[" locatore_cf ", "note"])
        writer.writeheader()
        writer.writerow({" locatore_cf ": " RSSMRA80A01H501Z ", "note": " first "})
        writer.writerow({" locatore_cf ": "", "note": ""})

    rows = load_clients_csv(path=csv_path)

    assert len(rows) == 1
    assert rows[0].row_number == 2
    assert rows[0].locatore_cf == "RSSMRA80A01H501Z"
    assert rows[0].values == {"LOCATORE_CF": "RSSMRA80A01H501Z", "NOTE": "first"}
    assert rows[0].extra == {"NOTE": "first"}


def test_load_clients_csv_with_issues_keeps_invalid_rows_for_bulk_reporting(tmp_path):
    """Bulk CSV loading should preserve invalid-row diagnostics without a second parser."""
    csv_path = tmp_path / "clients.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=["codice_fiscale", "note"])
        writer.writeheader()
        writer.writerow({"codice_fiscale": " rssmra80a01h501z ", "note": "first"})
        writer.writerow({"codice_fiscale": "", "note": "missing cf"})
        writer.writerow({"codice_fiscale": "", "note": ""})

    result = load_clients_csv_with_issues(path=csv_path)

    assert result.total_rows == 3
    assert len(result.rows) == 1
    assert result.rows[0].locatore_cf == "rssmra80a01h501z"
    assert len(result.invalid_rows) == 2
    assert result.invalid_rows[0].code == "missing_locatore_cf"
    assert result.invalid_rows[1].code == "blank_row"
