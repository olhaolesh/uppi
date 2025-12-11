from pathlib import Path
import pdfplumber


def pdf_to_markdown(pdf_path: str, md_path: str) -> None:
    pdf_path = Path(pdf_path)
    md_path = Path(md_path)

    with pdfplumber.open(pdf_path) as pdf, md_path.open("w", encoding="utf-8") as out:
        for page_idx, page in enumerate(pdf.pages, start=1):
            out.write(f"\n\n# Pagina {page_idx}\n\n")

            # 1) Основний текст сторінки
            text = page.extract_text() or ""
            text = text.replace("\u00A0", " ")
            out.write(text)
            out.write("\n")

            # 2) Таблиці → markdown-таблиці (дуже грубо, але працює)
            tables = page.extract_tables()
            for t_idx, table in enumerate(tables, start=1):
                if not table:
                    continue
                out.write(f"\n\n## Tabella {page_idx}.{t_idx}\n\n")

                header = [ (cell or "").strip() for cell in table[0] ]
                out.write("| " + " | ".join(header) + " |\n")
                out.write("|" + " --- |" * len(header) + "\n")

                for row in table[1:]:
                    row = [ (cell or "").strip() for cell in row ]
                    out.write("| " + " | ".join(row) + " |\n")


if __name__ == "__main__":
    pdf_to_markdown("uppi/docs/accordo_pescara_ocr.pdf", "uppi/docs/accordo_pescara.md")
