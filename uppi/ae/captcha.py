"""
CAPTCHA handling for SISTER (2Captcha integration).

Тут логіка:
- перевірити, чи є CAPTCHA,
- якщо є — зняти скрін, відправити в 2Captcha, заповнити поле й натиснути 'Inoltra'.
"""

import os
import base64
from typing import Any, Optional

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

    Повертає:
        True  - якщо або CAPTCHA не було, або її успішно відправили
        False - якщо виникла критична помилка в процесі
    """
    # Спочатку перевіряємо, чи є елемент CAPTCHA
    try:
        await page.wait_for_selector(UppiSelectors.IMG_CAPTCHA, timeout=5_000)
        captcha_present = True
        logger.info("[CAPTCHA] CAPTCHA detected on the page")
    except PlaywrightTimeoutError:
        captcha_present = False
        logger.info("[CAPTCHA] No CAPTCHA detected, trying plain 'Inoltra' submit")

    if not captcha_present:
        # Якщо CAPTCHA немає — просто тиснемо Inoltra і чекаємо зникнення кнопки / переходу
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

    # Якщо ми тут — CAPTCHA є, розв'язуємо
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

    # Директорія для скріншотів
    folder_name = codice_fiscale or "unknown_cf"
    folder_path = os.path.join("captcha_images", folder_name)
    try:
        os.makedirs(folder_path, exist_ok=True)
    except Exception as e:
        logger.warning("[CAPTCHA] Cannot create folder for captcha images '%s': %s", folder_path, e)

    # Робимо скріншот
    try:
        await playwright_page.wait_for_timeout(3_000)  # невелика пауза перед зняттям скріну
        image_path = os.path.join(folder_path, "captcha.png")
        captcha_bytes = await captcha_element.screenshot(path=image_path, type="png")
        if not captcha_bytes:
            logger.warning("[CAPTCHA] Failed to get screenshot bytes from CAPTCHA element")
            return None
        logger.info("[CAPTCHA] Screenshot saved: %s", image_path)
    except PlaywrightTimeoutError as e:
        logger.warning("[CAPTCHA] Timeout while taking CAPTCHA screenshot: %s", e)
        return None
    except Exception as e:
        logger.exception("[CAPTCHA] Unexpected error while taking CAPTCHA screenshot: %s", e)
        return None

    # Конвертуємо у base64
    try:
        captcha_base64 = base64.b64encode(captcha_bytes).decode("utf-8")
    except Exception as e:
        logger.exception("[CAPTCHA] Failed to encode screenshot to base64: %s", e)
        return None

    # Відправляємо в 2Captcha
    try:
        solver = TwoCaptcha(solver_key)
        # У деяких версіях TwoCaptcha normal() приймає base64 без параметра 'file'
        result = solver.normal(captcha_base64)
        logger.debug("[CAPTCHA] Raw 2Captcha result: %r", result)
    except Exception as e:
        logger.error("[CAPTCHA] Error while calling 2Captcha: %s", e)
        return None

    # Витягуємо код
    try:
        code = (result or {}).get("code", "").strip()
    except Exception:
        code = ""

    if not code:
        logger.warning("[CAPTCHA] 2Captcha returned empty or invalid code")
        return None

    logger.info("[CAPTCHA] CAPTCHA solved: %s", code)
    return code