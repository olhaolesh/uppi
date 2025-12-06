"""
Helper для завантаження PDF-візури з SISTER.

Тут тільки очікування download-об'єкта і збереження файлу
в downloads/{CF}/VISURA_{CF}.pdf — за тим самим шляхом,
який повертає domain.storage.get_visura_path().
"""

from typing import Any
from pathlib import Path

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from uppi.ae.uppi_selectors import UppiSelectors
from uppi.domain.storage import get_visura_path


async def download_document(
    page: Page,
    codice_fiscale: str,
    logger: Any,
) -> str:
    """
    Тригерить завантаження документа і зберігає його у
    ./downloads/{codice_fiscale}/VISURA_{codice_fiscale}.pdf

    Шлях формується через get_visura_path(codice_fiscale), щоб бути
    синхронним з pipelines/storage/visura_pdf_parser.

    Повертає:
        повний шлях до файлу (str), якщо успішно
        None — якщо сталася помилка або download не відбувся
    """
    # формуємо canonical path через storage
    visura_path: Path = get_visura_path(codice_fiscale)
    downloads_dir = visura_path.parent

    logger.info(
        "[DOWNLOAD] Target path for CF=%s → %s (dir=%s)",
        codice_fiscale,
        visura_path,
        downloads_dir,
    )

    download_obj = None

    try:
        # Чекаємо об'єкт завантаження
        async with page.expect_download() as download_ctx:
            await page.wait_for_selector(UppiSelectors.APRI_BUTTON, timeout=60_000)
            await page.click(UppiSelectors.APRI_BUTTON)
            logger.info("[DOWNLOAD] 'Apri' clicked, waiting for download to start")

        download_obj = await download_ctx.value
        logger.debug("[DOWNLOAD] Download object captured: %s", download_obj)
    except PlaywrightTimeoutError as e:
        logger.warning("[DOWNLOAD] Waiting for download timed out: %s", e)
    except Exception as e:
        logger.exception("[DOWNLOAD] Unexpected error while initiating download: %s", e)

    if not download_obj:
        logger.error("[DOWNLOAD] No download object obtained. Aborting save.")
        return None

    try:
        await download_obj.save_as(str(visura_path))
        logger.info("[DOWNLOAD] File saved: %s", visura_path)
        return str(visura_path)
    except Exception as e:
        logger.exception(
            "[DOWNLOAD] Failed to save download to '%s': %s",
            visura_path,
            e,
        )
        return None