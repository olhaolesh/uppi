"""
Playwright-хелпери для навігації всередині SISTER після логіну в AE.

Тут:
- відкриття SISTER у новій вкладці з "I tuoi preferiti",
- перехід до форми "Visure catastali" та пошук по codice fiscale.
"""

from typing import Any, Optional

from decouple import config
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from uppi.ae.uppi_selectors import UppiSelectors

# URL сторінки "Visure catastali" (безпосередня форма пошуку)
SISTER_VISURE_CATASTALI_URL = config("SISTER_VISURE_CATASTALI_URL")


async def open_sister_service(
    ae_page: Page,
    servizi_url: str,
    logger: Any,
    safe_close_page,
) -> Optional[Page]:
    """
    Відкрити SISTER-сервіс у НОВІЙ вкладці з головної сторінки сервісів AE.

    Кроки:
    1. Перейти на AE_URL_SERVIZI (servizi_url).
    2. Натиснути "I tuoi preferiti".
    3. Серед улюблених натиснути "Vai al servizio" по SISTER з middle-click'ом
       і перехопити нову вкладку через context.expect_page().
    4. Закрити стару AE-сторінку (ae_page).
    5. Обробити вітальне вікно SISTER і зберегти storage_state в state.json.

    Повертає:
        sister_page (Page) або None у випадку фейлу.
    """
    logger.info("[OPEN_SISTER] Navigating to AE services home: %s", servizi_url)

    # Переходимо на сторінку сервісів
    try:
        await ae_page.goto(servizi_url, wait_until="networkidle", timeout=60_000)
        logger.debug("[OPEN_SISTER] AE services page loaded")
    except PlaywrightTimeoutError as e:
        logger.error("[OPEN_SISTER] Timeout while navigating to AE services page: %s", e)
        return None
    except Exception as e:
        logger.exception("[OPEN_SISTER] Unexpected error while navigating to AE services page: %s", e)
        return None

    sister_page: Optional[Page] = None

    try:
        # Чекаємо профіль та секцію "I tuoi preferiti"
        await ae_page.wait_for_selector(UppiSelectors.PROFILE_INFO, timeout=10_000)
        await ae_page.wait_for_selector(UppiSelectors.TUOI_PREFERITI_SECTION, timeout=10_000)

        await ae_page.locator(UppiSelectors.TUOI_PREFERITI_SECTION).click()
        logger.info("[OPEN_SISTER] 'I tuoi preferiti' section opened")

        # Відкриваємо SISTER у новій вкладці (middle-click)
        try:
            async with ae_page.context.expect_page() as ctx:
                await ae_page.click(UppiSelectors.VAI_AL_SERVIZIO_BUTTON, button="middle")
            sister_page = await ctx.value
            await sister_page.bring_to_front()
            logger.info("[OPEN_SISTER] SISTER page opened in a new tab")

            # Закриваємо стару AE-сторінку
            await safe_close_page(ae_page, "AE main page")
        except PlaywrightTimeoutError as e:
            logger.warning("[OPEN_SISTER] Timeout while opening SISTER page: %s", e)
            return None
        except Exception as e:
            logger.exception("[OPEN_SISTER] Unexpected error while opening SISTER: %s", e)
            return None

    except PlaywrightTimeoutError as e:
        logger.error("[OPEN_SISTER] Required selectors not found before opening SISTER: %s", e)
        return None
    except Exception as e:
        logger.exception("[OPEN_SISTER] Unexpected error while preparing to open SISTER: %s", e)
        return None

    # Обробляємо стартову сторінку SISTER: кнопка "Conferma" + збереження state.json
    try:
        await sister_page.wait_for_selector(UppiSelectors.CONFERMA_BUTTON, timeout=10_000)
        await sister_page.wait_for_timeout(1_000)
        await sister_page.click(UppiSelectors.CONFERMA_BUTTON)
        logger.info("[OPEN_SISTER] 'Conferma' button clicked on SISTER welcome page")

        await sister_page.wait_for_timeout(3_000)

        # Зберігаємо storage_state для повторного використання
        try:
            await sister_page.context.storage_state(path="state.json")
            logger.info("[OPEN_SISTER] storage_state saved to state.json")
        except Exception as e:
            logger.warning("[OPEN_SISTER] Failed to save storage_state to state.json: %s", e)
    except PlaywrightTimeoutError as e:
        logger.warning("[OPEN_SISTER] 'Conferma' button not found on SISTER welcome page: %s", e)
        # Можливо, вікна підтвердження немає, продовжуємо як є
    except Exception as e:
        logger.exception("[OPEN_SISTER] Error after opening SISTER: %s", e)

    return sister_page


async def navigate_to_visure_catastali(
    sister_page: Page,
    codice_fiscale: str,
    comune: str,
    tipo_catasto: str,
    ufficio_label: str,
    logger: Any,
) -> bool:
    """
    Перейти до форми 'Visure catastali' та запустити пошук за codice fiscale.

    Якщо все ок:
        - відкритий список омонімів
        - натиснута кнопка 'Visura per soggetto'

    Повертає:
        True  - якщо навігація пройшла успішно (до кліку по 'Visura per soggetto')
        False - якщо щось зламалось або CF не має нерухомості
    """
    logger.info(
        "[NAVIGATE] Start navigation to Visure catastali for CF=%s, comune=%s, catasto=%s, ufficio=%s",
        codice_fiscale,
        comune,
        tipo_catasto,
        ufficio_label,
    )

    try:
        # Переходимо напряму на URL форми Visure catastali
        await sister_page.goto(SISTER_VISURE_CATASTALI_URL, wait_until="networkidle", timeout=60_000)
        logger.debug("[NAVIGATE] Opened Visure catastali URL: %s", SISTER_VISURE_CATASTALI_URL)

        # Можливе вікно "Conferma Lettura"
        try:
            await sister_page.wait_for_selector(UppiSelectors.CONFERMA_LETTURA, timeout=2_000)
            await sister_page.click(UppiSelectors.CONFERMA_LETTURA)
            logger.info("[NAVIGATE] 'Conferma Lettura' accepted")
        except PlaywrightTimeoutError:
            logger.info("[NAVIGATE] 'Conferma Lettura' not found (maybe already accepted)")

        # Вибір ufficio
        try:
            select_ufficio = sister_page.locator(UppiSelectors.SELECT_UFFICIO)
            await select_ufficio.wait_for(timeout=5_000)
            await select_ufficio.select_option(label=ufficio_label)
            await sister_page.click(UppiSelectors.APLICA_BUTTON)
            logger.info("[NAVIGATE] Ufficio selected: %s", ufficio_label)
        except PlaywrightTimeoutError:
            logger.warning("[NAVIGATE] Ufficio selection failed or timed out")
            return False
        except Exception as e:
            logger.exception("[NAVIGATE] Unexpected error while selecting Ufficio: %s", e)
            return False

        # Вибір типу катасто
        try:
            select_catasto = sister_page.locator(UppiSelectors.SELECT_CATASTO)
            await select_catasto.wait_for(timeout=5_000)
            await sister_page.wait_for_timeout(1_000)
            await select_catasto.select_option(value=tipo_catasto)
            logger.info("[NAVIGATE] Catasto type selected: %s", tipo_catasto)
        except PlaywrightTimeoutError as e:
            logger.warning("[NAVIGATE] Timeout while selecting Catasto: %s", e)
            return False
        except Exception as e:
            logger.exception("[NAVIGATE] Unexpected error while selecting Catasto: %s", e)
            return False

        # Вибір comune
        try:
            select_comune = sister_page.locator(UppiSelectors.SELECT_COMUNE)
            await select_comune.wait_for(timeout=5_000)
            await sister_page.wait_for_timeout(1_000)
            await select_comune.select_option(label=comune)
            logger.info("[NAVIGATE] Comune selected: %s", comune)
        except PlaywrightTimeoutError as e:
            logger.warning("[NAVIGATE] Timeout while selecting Comune: %s", e)
            return False
        except Exception as e:
            logger.exception("[NAVIGATE] Unexpected error while selecting Comune: %s", e)
            return False

        # Вводимо codice fiscale і запускаємо пошук
        await sister_page.click(UppiSelectors.CODICE_FISCALE_RADIO)
        await sister_page.fill(UppiSelectors.CODICE_FISCALE_FIELD, codice_fiscale)
        await sister_page.click(UppiSelectors.RICERCA_BUTTON)
        logger.info("[NAVIGATE] Search triggered for CF=%s", codice_fiscale)

        # Обробляємо список омонімів
        try:
            await sister_page.wait_for_selector(UppiSelectors.SELECT_OMONIMI, timeout=3_000)
            await sister_page.click(UppiSelectors.SELECT_OMONIMI)
            logger.info("[NAVIGATE] Omonimi list handled (first option selected)")
        except PlaywrightTimeoutError:
            logger.info("[NAVIGATE] No omonimi list — probably no properties or invalid CF for %s", codice_fiscale)
            return False
        except Exception as e:
            logger.exception("[NAVIGATE] Error while selecting omonimi: %s", e)
            return False

        # Переходимо до "Visura per soggetto"
        try:
            await sister_page.click(UppiSelectors.VISURA_PER_SOGGECTO_BUTTON)
            logger.info("[NAVIGATE] 'Visura per soggetto' clicked for CF=%s", codice_fiscale)
        except PlaywrightTimeoutError as e:
            logger.warning("[NAVIGATE] Timeout clicking 'Visura per soggetto': %s", e)
            return False
        except Exception as e:
            logger.exception("[NAVIGATE] Unexpected error clicking 'Visura per soggetto': %s", e)
            return False

        logger.info("[NAVIGATE] Navigation completed successfully for CF=%s", codice_fiscale)
        return True

    except PlaywrightTimeoutError as e:
        logger.warning("[NAVIGATE] General timeout during navigation: %s", e)
        return False
    except Exception as e:
        logger.exception("[NAVIGATE] Unexpected error during navigation to Visure catastali: %s", e)
        return False