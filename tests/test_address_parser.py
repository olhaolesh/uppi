import pytest

from uppi.parsers.address_parser import parse_address


# ---------------------------------------------------------
# Основний параметризований тест
# ---------------------------------------------------------
# Кожен кейс описує реальний сценарій з життя,
# а не штучний regex-приклад.
# ---------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        # -------------------------------------------------
        # БАЗОВІ АДРЕСИ
        # -------------------------------------------------

        # Класичний повний варіант з усіма компонентами
        (
            "VIALE DELLA RIVIERA n. 285 Scala U Interno 1 Piano 1",
            {
                "via_type": "VIALE",
                "via_name": "DELLA RIVIERA",
                "via_num": "285",
                "scala": "U",
                "interno": "1",
                "piano": "1",
            },
        ),

        # Без додаткових компонентів
        (
            "VIA XX SETTEMBRE n. 15",
            {
                "via_type": "VIA",
                "via_name": "XX SETTEMBRE",
                "via_num": "15",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # Piazza не повинна ламатися через "P."
        (
            "PIAZZA GARIBALDI n. 2",
            {
                "via_type": "PIAZZA",
                "via_name": "GARIBALDI",
                "via_num": "2",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # -------------------------------------------------
        # НОМЕР БЕЗ "n."
        # -------------------------------------------------

        # Часто номер пишуть без явного маркера
        (
            "VIA ROMA 10",
            {
                "via_type": "VIA",
                "via_name": "ROMA",
                "via_num": "10",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # Номер з літерою
        (
            "VIA ROMA 10A",
            {
                "via_type": "VIA",
                "via_name": "ROMA",
                "via_num": "10A",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # -------------------------------------------------
        # SNC (senza numero civico)
        # -------------------------------------------------

        # Явно вказано, що номера немає
        (
            "VIA DEI MILLE SNC",
            {
                "via_type": "VIA",
                "via_name": "DEI MILLE",
                "via_num": None,
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # Варіант з "n. SNC"
        (
            "PIAZZA CAVOUR n. SNC",
            {
                "via_type": "PIAZZA",
                "via_name": "CAVOUR",
                "via_num": None,
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # -------------------------------------------------
        # VARIAZIONI COMPONENTI
        # -------------------------------------------------

        # Scala без Interno
        (
            "VIA MANZONI n. 5 Scala B",
            {
                "via_type": "VIA",
                "via_name": "MANZONI",
                "via_num": "5",
                "scala": "B",
                "interno": None,
                "piano": None,
            },
        ),

        # Interno без Scala
        (
            "VIA MANZONI n. 5 Interno 7",
            {
                "via_type": "VIA",
                "via_name": "MANZONI",
                "via_num": "5",
                "scala": None,
                "interno": "7",
                "piano": None,
            },
        ),

        # Piano testuale
        (
            "VIA VERDI n. 3 Piano Terra",
            {
                "via_type": "VIA",
                "via_name": "VERDI",
                "via_num": "3",
                "scala": None,
                "interno": None,
                "piano": "TERRA",
            },
        ),

        # Piano abbreviato
        (
            "VIA VERDI n. 3 P. 1",
            {
                "via_type": "VIA",
                "via_name": "VERDI",
                "via_num": "3",
                "scala": None,
                "interno": None,
                "piano": "1",
            },
        ),

        # -------------------------------------------------
        # FORMATI ALTERNATIVI
        # -------------------------------------------------

        # Località без номера
        (
            "LOCALITA COLLE ALTO",
            {
                "via_type": "LOCALITA",
                "via_name": "COLLE ALTO",
                "via_num": None,
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # Frazione з номером
        (
            "FRAZ. SAN PIETRO n. 12",
            {
                "via_type": "FRAZ",
                "via_name": "SAN PIETRO",
                "via_num": "12",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # -------------------------------------------------
        # EDGE CASES (але валідні)
        # -------------------------------------------------

        # Нема типу вулиці
        (
            "STRADA STATALE 16 250",
            {
                "via_type": "STRADA",
                "via_name": "STATALE 16",
                "via_num": "250",
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),

        # Лиш назва — мінімально допустимий варіант
        (
            "BORGO ANTICO",
            {
                "via_type": "BORGO",
                "via_name": "ANTICO",
                "via_num": None,
                "scala": None,
                "interno": None,
                "piano": None,
            },
        ),
    ],
)
def test_parse_address_components(raw, expected):
    """
    Перевіряє, що парсер:
    - коректно виділяє всі компоненти
    - не плутає типи
    - не створює фейкові значення
    """

    parsed = parse_address(raw)

    assert parsed.via_type == expected["via_type"]
    assert parsed.via_name == expected["via_name"]
    assert parsed.via_num == expected["via_num"]
    assert parsed.scala == expected["scala"]
    assert parsed.interno == expected["interno"]
    assert parsed.piano == expected["piano"]

    # Raw-рядок завжди має зберігатися без змін
    assert parsed.indirizzo_raw == raw