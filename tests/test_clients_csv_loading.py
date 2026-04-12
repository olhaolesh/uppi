"""Focused tests for the future bulk `clients.csv` loading surface."""

from __future__ import annotations

import csv

from uppi.domain.clients_csv import load_clients_csv


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
