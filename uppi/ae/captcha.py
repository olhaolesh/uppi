"""
Обробка CAPTCHA для SISTER (2Captcha integration).

Тут логіка:
- перевірити, чи є CAPTCHA,
- якщо є — зняти скрін, відправити в 2Captcha, заповнити поле й натиснути 'Inoltra'.

Protected invariants:
- CAPTCHA flow є browser-critical.
- submit sequence, selector order і wait/click behavior не можна міняти як
  "optimization" без окремого high-risk етапу.
- allowed work тут обмежується documentation / characterization /
  safer logging / wrapper without semantic change.
"""

import os
import base64
from typing import Any

from twocaptcha import TwoCaptcha
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from uppi.ae.uppi_selectors import UppiSelectors


async def solve_captcha_if_present(
    page: Page,
    two_captcha_key: str,
    logger: Any,
    codice_fiscale: str = "",
) -> bool:
    """
    Перевірити, чи є CAPTCHA. Якщо немає — просто тиснемо 'Inoltra' і чекаємо.
    Якщо є — розв'язуємо через 2Captcha.

    Важливо:
    - обидві гілки (`no captcha` і `captcha present`) належать до protected flow;
    - змінювати submit ordering або retry semantics тут не можна без live regression.

    Повертає:
        True  - якщо або CAPTCHA не було, або її успішно відправили
        False - якщо виникла критична помилка в процесі
    """
    # Спершу визначаємо, чи є CAPTCHA на сторінці
    try:
        await page.wait_for_selector(UppiSelectors.IMG_CAPTCHA, timeout=5_000)
        captcha_present = True
        logger.info("[CAPTCHA] CAPTCHA detected on the page")
    except PlaywrightTimeoutError:
        captcha_present = False
        logger.info("[CAPTCHA] No CAPTCHA detected, trying plain 'Inoltra' submit")

    if not captcha_present:
        # Якщо CAPTCHA немає, відправляємо форму звичайним шляхом
        try:
            await page.click(UppiSelectors.INOLTRA_BUTTON)
            inoltra_button = page.locator(UppiSelectors.INOLTRA_BUTTON)
            try:
                await inoltra_button.wait_for(state="hidden", timeout=10_000)
                logger.info("[CAPTCHA] 'Inoltra' button disappeared, proceed")
            except PlaywrightTimeoutError:
                logger.warning("[CAPTCHA] 'Inoltra' button did not hide after submission")
            return True
        except PlaywrightTimeoutError as e:
            logger.warning("[CAPTCHA] Timeout clicking 'Inoltra' without captcha: %s", e)
            return False
        except Exception as e:
            logger.exception("[CAPTCHA] Unexpected error while clicking 'Inoltra' without captcha: %s", e)
            return False

    # Якщо ми тут, CAPTCHA є і її треба розв'язати
    try:
        await page.click(UppiSelectors.CAPTCHA_FIELD)
        logger.debug("[CAPTCHA] Focused CAPTCHA input field")

        solution = await _solve_captcha(
            playwright_page=page,
            solver_key=two_captcha_key,
            codice_fiscale=codice_fiscale,
            img_captcha_selector=UppiSelectors.IMG_CAPTCHA,
            logger=logger,
        )

        if not solution:
            logger.error("[CAPTCHA] Solver did not return a valid solution")
            return False

        await page.fill(UppiSelectors.CAPTCHA_FIELD, solution)
        logger.info("[CAPTCHA] CAPTCHA solution filled")

        await page.click(UppiSelectors.INOLTRA_BUTTON)
        inoltra_button = page.locator(UppiSelectors.INOLTRA_BUTTON)
        try:
            await inoltra_button.wait_for(state="hidden", timeout=10_000)
            logger.info("[CAPTCHA] CAPTCHA submitted, 'Inoltra' button disappeared")
        except PlaywrightTimeoutError:
            logger.warning("[CAPTCHA] 'Inoltra' button did not hide after captcha submission")

        return True

    except PlaywrightTimeoutError as e:
        logger.warning("[CAPTCHA] Timeout while solving/submit captcha: %s", e)
    except Exception as e:
        logger.exception("[CAPTCHA] Unexpected error in captcha handling: %s", e)

    return False


async def _solve_captcha(
    playwright_page: Page,
    solver_key: str,
    codice_fiscale: str,
    img_captcha_selector: str,
    logger: Any,
) -> str:
    """
    Витягує картинку CAPTCHA, відправляє в 2Captcha та повертає розпізнаний код.

    Повертає:
        str - код, якщо все ок
        None - якщо щось пішло не так
    """
    try:
        captcha_element = playwright_page.locator(img_captcha_selector)
        if not await captcha_element.is_visible():
            logger.warning("[CAPTCHA] CAPTCHA element not visible on page")
            return None
    except PlaywrightTimeoutError:
        logger.warning("[CAPTCHA] Timeout while locating CAPTCHA element")
        return None
    except Exception as e:
        logger.exception("[CAPTCHA] Unexpected error while locating CAPTCHA: %s", e)
        return None

    # Локальна директорія для скріншотів CAPTCHA
    folder_name = codice_fiscale or "unknown_cf"
    folder_path = os.path.join("captcha_images", folder_name)
    try:
        os.makedirs(folder_path, exist_ok=True)
    except Exception as e:
        logger.warning("[CAPTCHA] Cannot prepare local captcha image folder: %s", e)

    # Робимо скріншот CAPTCHA
    try:
        await playwright_page.wait_for_timeout(3_000)  # невелика пауза перед зняттям скріну
        image_path = os.path.join(folder_path, "captcha.png")
        captcha_bytes = await captcha_element.screenshot(path=image_path, type="png")
        if not captcha_bytes:
            logger.warning("[CAPTCHA] Failed to get screenshot bytes from CAPTCHA element")
            return None
        logger.info("[CAPTCHA] Screenshot captured for solver (bytes=%d)", len(captcha_bytes))
    except PlaywrightTimeoutError as e:
        logger.warning("[CAPTCHA] Timeout while taking CAPTCHA screenshot: %s", e)
        return None
    except Exception as e:
        logger.exception("[CAPTCHA] Unexpected error while taking CAPTCHA screenshot: %s", e)
        return None

    # Перетворюємо скріншот у base64 для 2Captcha
    try:
        captcha_base64 = base64.b64encode(captcha_bytes).decode("utf-8")
    except Exception as e:
        logger.exception("[CAPTCHA] Failed to encode screenshot to base64: %s", e)
        return None

    # Відправляємо CAPTCHA у 2Captcha
    try:
        solver = TwoCaptcha(solver_key)
        # У деяких версіях клієнта TwoCaptcha `normal()` приймає base64 без `file`
        result = solver.normal(captcha_base64)
        if isinstance(result, dict):
            logger.debug("[CAPTCHA] 2Captcha response received (keys=%s)", sorted(result.keys()))
        else:
            logger.debug("[CAPTCHA] 2Captcha response received (type=%s)", type(result).__name__)
    except Exception as e:
        logger.error("[CAPTCHA] Error while calling 2Captcha: %s", e)
        return None

    # Витягуємо розпізнаний код
    try:
        code = (result or {}).get("code", "").strip()
    except Exception:
        code = ""

    if not code:
        logger.warning("[CAPTCHA] 2Captcha returned empty or invalid code")
        return None

    logger.info("[CAPTCHA] CAPTCHA solved successfully")
    return code
