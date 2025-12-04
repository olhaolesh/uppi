"""
AE (Agenzia Entrate) authentication helpers.

Відповідає тільки за логін у профіль AE через вкладку Fisconline.
"""

import os
from typing import Any

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from uppi.ae.uppi_selectors import UppiSelectors


async def authenticate_user(
    page: Page,
    ae_username: str,
    ae_password: str,
    ae_pin: str,
    logger: Any,
) -> bool:
    """
    Залогінитись у AE (Fisconline) на вже завантаженій сторінці логіну.

    Повертає:
        True  - якщо PROIFLE_INFO знайдено (логін вдалий),
        False - якщо сталася помилка / таймаут.

    При фейлі видаляє state.json, щоб не залишати битий стейт.
    """
    logger.info("[LOGIN] Starting AE authentication via Fisconline tab")

    try:
        # Переключаємось на вкладку Fisconline
        await page.wait_for_selector(UppiSelectors.FISCOLINE_TAB, timeout=10_000)
        await page.click(UppiSelectors.FISCOLINE_TAB)
        logger.debug("[LOGIN] Fisconline tab clicked")

        # Заповнюємо форму логіну
        await page.wait_for_selector(UppiSelectors.USERNAME_FIELD, timeout=10_000)
        await page.wait_for_timeout(1_000)  # невелика пауза, щоб форма стабілізувалась

        await page.fill(UppiSelectors.USERNAME_FIELD, ae_username)
        await page.fill(UppiSelectors.PASSWORD_FIELD, ae_password)
        await page.fill(UppiSelectors.PIN_FIELD, ae_pin)
        logger.debug("[LOGIN] Credentials and PIN filled")

        await page.click(UppiSelectors.ACCEDI_BUTON)
        logger.info("[LOGIN] 'Accedi' clicked, waiting for profile info marker")

        # Чекаємо появу профілю як ознаку успішного логіну
        try:
            await page.wait_for_selector(UppiSelectors.PROFILE_INFO, timeout=10_000)
            logger.info("[LOGIN] Login successful, PROFILE_INFO found")
            return True
        except PlaywrightTimeoutError as err:
            logger.error("[LOGIN] Profile info not found after login: %s", err)
            # Якщо стейт існує — видалимо, бо він некоректний
            if os.path.exists("state.json"):
                try:
                    os.remove("state.json")
                    logger.info("[LOGIN] Removed leftover state.json after failed login")
                except Exception as rm_err:
                    logger.warning("[LOGIN] Failed to remove leftover state.json: %s", rm_err)
            return False

    except PlaywrightTimeoutError as err:
        logger.error("[LOGIN] Timeout while performing login flow: %s", err)
    except Exception as exc:
        logger.exception("[LOGIN] Unexpected error during AE authentication: %s", exc)

    # У будь-якому випадку, якщо сюди дійшли — логін неуспішний
    return False