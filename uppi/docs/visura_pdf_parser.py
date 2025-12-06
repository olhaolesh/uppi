from pathlib import Path
import logging
import fitz
import camelot
import re
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

DOWNLOADS_DIR = Path(__file__).resolve().parents[2] / "downloads"
PDF_PATH = DOWNLOADS_DIR / "CCMMRT71S44H501X" / "DOC_1926488153.pdf"


class VisuraParser:
    """
    Парсер PDF-візури.

    На виході повертає плаский список dict'ів, один dict = один immobile.
    Кожен dict містить:
        - базові поля таблиці (foglio, numero, sub, categoria, ...),
        - адресу (via_type, via_name, via_num, piano, interno, indirizzo_raw),
        - superficie_*,
        - locatore_* з першої сторінки,
        - immobile_comune / immobile_comune_code з заголовка сторінки.
    """

    # -------------------------------
    #  REGEX PATTERNS
    # -------------------------------

    # Name and Codice Fiscale pattern
    NAME_CF = re.compile(
        r"^([A-ZÀÈÌÒÙ]{2,})\s+([A-Za-zÀÈÌÒÙ]+)\s+\(CF:\s*([A-Z0-9]{16})\)",
        re.UNICODE,
    )

    # Comune pattern
    COMUNE_TABLE = re.compile(
        r"Immobili\s+siti\s+nel\s+Comune\s+di\s+(.+?)\s+\(Codice\s+([A-Z0-9]+)\)",
        re.IGNORECASE,
    )

    # Superficie patterns
    SUPERFICIE_TOTALE = re.compile(r"Totale:\s*([0-9.,]+)")
    SUPERFICIE_ESCLUSE = re.compile(
        r"Totale escluse aree\s*scoperte\*\*:\s*([0-9.,]+)"
    )

    # -------------------------------
    #  TABLE KEYWORDS
    # -------------------------------
    GROUPED_HEADER_KEYWORDS = [
        "DATI IDENTIFICATIVI",
        "DATI DI CLASSAMENTO",
        "ALTRE INFORMAZIONI",
    ]

    INTABULAZIONE_KEYWORDS = [
        "DATI ANAGRAFICI",
        "DIRITTI E ONERI REALI",
    ]

    # ключові колонки для таблиць нерухомості
    REAL_ESTATE_COLUMNS = {"Foglio", "Numero", "Sub", "Categoria", "Classe"}

    # -------------------------------
    #  ADDRESS REGEX
    # -------------------------------
    ADDRESS_PATTERNS = {
        "via_num": re.compile(
            r"(?:n\.?\s*)?([\d]+[A-Z]?[\-/\dA-Z]*|SNC)",
            re.IGNORECASE,
        ),
        "scala": re.compile(
            r"(SCALA|SC\.?)\s*([A-Z0-9]+)",
            re.IGNORECASE,
        ),
        "interno": re.compile(
            r"(INTERNO|INT\.?)\s*([A-Z0-9]+)",
            re.IGNORECASE,
        ),
        "piano": re.compile(
            r"(PIANO|P\.?)\s*([-A-Z0-9°]+|T|TERRA|RIALZATO|AMMEZZATO|S\d)",
            re.IGNORECASE,
        ),
    }

    STREET_TYPE_REGEX = re.compile(
        r"^(VIA|VIALE|PIAZZA|P\.?ZZA|CORSO|STRADA|VICOLO|LARGO|BORGO|LOCALITÀ|LOC\.?|FRAZIONE|FRAZ\.?|CONTRADA)\s+(.+)",
        re.IGNORECASE | re.UNICODE,
    )

    # -------------------------------
    #  MAIN ENTRYPOINT
    # -------------------------------

    def parse(self, pdf_path: str | Path) -> List[Dict[str, Any]]:
        """
        Основний вхід: парсить PDF і повертає список словників Immobile.
        """
        pdf_path = str(pdf_path)
        logger.info("[VISURA_PARSER] Парсимо PDF: %s", pdf_path)

        # Відкриваємо документ
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            logger.exception("[VISURA_PARSER] Не вдалося відкрити PDF %s: %s", pdf_path, e)
            return []

        all_immobili: List[Dict[str, Any]] = []

        try:
            name_data = self._extract_name_cf(doc)
            logger.debug("[VISURA_PARSER] Ім'я/CF: %r", name_data)

            for page_idx in range(len(doc)):
                page = doc[page_idx]
                comune_name, comune_code = self._extract_comune_for_page(page)
                logger.debug(
                    "[VISURA_PARSER] Сторінка %d: comune=%r, code=%r",
                    page_idx + 1,
                    comune_name,
                    comune_code,
                )

                # Camelot читає конкретну сторінку
                try:
                    tables = camelot.read_pdf(
                        pdf_path,
                        pages=str(page_idx + 1),
                        flavor="lattice",
                    )
                except Exception as e:
                    logger.exception(
                        "[VISURA_PARSER] Помилка Camelot на сторінці %d (%s): %s",
                        page_idx + 1,
                        pdf_path,
                        e,
                    )
                    continue

                for table in tables:
                    parsed = self._process_table(table)
                    if parsed is None:
                        continue

                    immobili_list = parsed.get("immobili", [])
                    for immobile in immobili_list:
                        # Збагачуємо загальними даними
                        immobile.update(
                            {
                                "locatore_surname": name_data.get("locatore_surname"),
                                "locatore_name": name_data.get("locatore_name"),
                                "locatore_codice_fiscale": name_data.get("cf"),
                                "immobile_comune": comune_name,
                                "immobile_comune_code": comune_code,
                            }
                        )
                        all_immobili.append(immobile)

            logger.info(
                "[VISURA_PARSER] Готово: знайдено %d immobili у %s",
                len(all_immobili),
                pdf_path,
            )
            return all_immobili
        finally:
            doc.close()

    # -------------------------------
    #  HEADER NORMALIZATION
    # -------------------------------

    def _normalize_header(self, header: str) -> str:
        """
        Перетворює текст заголовка на snake_case:
        видаляє пунктуацію, переводить у нижній регістр та ставить підкреслення.
        'Zona Cens.' -> 'zona_cens', 'Dati Ulteriori' -> 'dati_ulteriori'.
        """
        snake = re.sub(r"[^A-Za-z0-9]+", "_", header).strip("_")
        return snake.lower()

    # -------------------------------
    #  NAME + CF ONLY FROM FIRST PAGE
    # -------------------------------

    def _extract_name_cf(self, doc) -> Dict[str, Any]:
        """Витягує surname, given name і codice fiscale з першої сторінки."""
        page = doc[0]
        blocks = page.get_text("blocks")

        locatore_surname = locatore_name = cf = None

        for _, _, _, _, text, *_ in blocks:
            for line in text.splitlines():
                m = self.NAME_CF.match(line.strip())
                if m:
                    locatore_surname, locatore_name, cf = m.groups()
                    return {
                        "locatore_surname": locatore_surname,
                        "locatore_name": locatore_name,
                        "cf": cf,
                    }

        logger.warning("[VISURA_PARSER] Не вдалося знайти ім'я/CF на першій сторінці")
        return {
            "locatore_surname": None,
            "locatore_name": None,
            "cf": None,
        }

    # --------------------------------------
    #  "Immobili siti nel Comune di ..."
    # --------------------------------------

    def _extract_comune_for_page(self, page) -> tuple[str | None, str | None]:
        """Витягує назву Comune та код зі сторінки."""
        txt = page.get_text("text")

        for line in txt.splitlines():
            line = line.strip()
            m = self.COMUNE_TABLE.search(line)
            if m:
                return m.group(1), m.group(2)

        return None, None

    # -------------------------------
    #  TABLE PROCESSING
    # -------------------------------

    def _process_table(self, table):
        """Обробляє одну таблицю Camelot і витягує дані нерухомості."""
        df = table.df

        if df.empty:
            return None

        # 1) detect grouped header (DATI IDENTIFICATIVI etc)
        first_row_text = " ".join(df.iloc[0]).upper()
        if any(k in first_row_text for k in self.GROUPED_HEADER_KEYWORDS):
            header_row = 1
            data_start_row = 2
        else:
            header_row = 0
            data_start_row = 1

        # 2) preliminary header
        header = list(df.iloc[header_row])

        # 3) normalize header (remove newlines)
        header = [h.replace("\n", " ").strip() for h in header]

        # 4) skip Intestazione tables (owners)
        header_join = " ".join(header).upper()
        if any(k in header_join for k in self.INTABULAZIONE_KEYWORDS):
            return None

        # 5) check if this table is real estate table
        if not any(col in header for col in self.REAL_ESTATE_COLUMNS):
            return None

        # 6) FIX merged headers: Classe + Consistenza
        normalized_header: List[str] = []
        for idx, col in enumerate(header):
            col_clean = col.replace("\n", " ").strip()

            # Fix empty first column → table_num_immobile
            if col_clean == "" and idx == 0:
                normalized_header.append("table_num_immobile")
                continue

            # Fix empty column at Classe position
            if col_clean == "" and idx == 8:
                normalized_header.append("classe")
                continue

            # Fix merged "Classe Consistenza"
            if col_clean == "Classe Consistenza":
                normalized_header.append("consistenza")
                continue

            normalized_header.append(col_clean)

        header = [self._normalize_header(col) for col in normalized_header]

        # 7) extract rows
        rows: List[Dict[str, Any]] = []
        for i in range(data_start_row, len(df)):
            row_dict: Dict[str, Any] = {}

            for col_index, col_name in enumerate(header):
                raw_value = df.iloc[i, col_index].strip()

                # Indirizzo
                if "indirizzo" in col_name:
                    parsed_addr = self._parse_address(raw_value)
                    row_dict.update(parsed_addr)
                    continue

                # Superficie
                if col_name == "superficie_catastale":
                    parsed = self._parse_superficie(raw_value)
                    row_dict.update(parsed)
                    continue

                row_dict[col_name] = raw_value

            rows.append(row_dict)

        if rows:
            logger.debug(
                "[VISURA_PARSER] Оброблена таблиця, рядків=%d, колонки=%s",
                len(rows),
                header,
            )

        return {
            "immobili": rows,
        }

    # -------------------------------
    #  SUPERFICIE PARSER
    # -------------------------------

    def _parse_superficie(self, text: str) -> Dict[str, Any]:
        """Витягує Totale та Totale escluse aree scoperte."""
        txt = text.replace("\n", " ")

        totale = None
        escluse = None

        m1 = self.SUPERFICIE_TOTALE.search(txt)
        if m1:
            totale = self._normalize_number(m1.group(1))

        m2 = self.SUPERFICIE_ESCLUSE.search(txt)
        if m2:
            escluse = self._normalize_number(m2.group(1))

        return {
            "superficie_totale": totale,
            "superficie_escluse": escluse,
            "superficie_raw": txt,
        }

    def _normalize_number(self, numstr: str) -> float:
        """Нормалізує рядок числа в float."""
        return float(numstr.replace(",", ".").replace(" ", ""))

    # -------------------------------
    #  ADDRESS PARSER
    # -------------------------------

    def _parse_address(self, text: str) -> Dict[str, Any]:
        raw = text.replace("\n", " ").strip()
        clean = raw

        parsed: Dict[str, Any] = {
            "via_type": None,  # тип вулиці (VIA/VIALE/...)
            "via_name": None,  # назва вулиці
            "via_num": None,
            "scala": None,
            "interno": None,
            "piano": None,
            "indirizzo_raw": raw,
        }

        # 1. Витягуємо via_num / scala / interno / piano
        for key, pattern in self.ADDRESS_PATTERNS.items():
            m = pattern.search(clean)
            if m:
                value = m.group(1 if key == "via_num" else 2).strip()
                parsed[key] = value
                clean = clean.replace(m.group(0), " ", 1)

        clean = clean.strip()

        # 2. Витягуємо тип та назву вулиці
        m = self.STREET_TYPE_REGEX.match(clean)
        if m:
            via_type = m.group(1).strip()
            rest = m.group(2).strip()

            # Обрізаємо зайвий текст після справжньої назви
            rest = re.split(
                r"\s{2,}|Variazione|Annotazione|Aggiornamento", rest, 1
            )[0].strip()

            parsed["via_type"] = via_type
            parsed["via_name"] = rest
        else:
            # fallback: якщо не впізнали тип, беремо весь рядок як назву
            parsed["via_name"] = clean

        return parsed


# --------------------------------------------------
#  Example usage (ручний тест)
# --------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    parser = VisuraParser()
    data = parser.parse(PDF_PATH)
    for t in data:
        print(t)