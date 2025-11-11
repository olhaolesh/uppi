import os
import scrapy
from typing import Optional
from decouple import config
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
from uppi.utils.stealth import STEALTH_SCRIPT
from uppi.utils.selectors import UppiSelectors
from uppi.utils.captcha_solver import solve_captcha
from uppi.utils.playwright_helpers import apply_stealth, log_requests, get_webgl_vendor

AE_LOGIN_URL = config("AE_LOGIN_URL")
AE_URL_SERVIZI = config("AE_URL_SERVIZI")
TWO_CAPTCHA_API_KEY = config("TWO_CAPTCHA_API_KEY")
AE_USERNAME = config("AE_USERNAME")
AE_PASSWORD = config("AE_PASSWORD")
AE_PIN = config("AE_PIN")
CODICE_FISCALE = config("CODICE_FISCALE")


class UppiSpider(scrapy.Spider):
    name = "uppi"
    allowed_domains = ["agenziaentrate.gov.it"]

    async def start(self):
        """Entry point: remove stale session file and start login request."""
        self.logger.info("[START] Cleaning old state.json if present")
        try:
            if os.path.exists("state.json"):
                os.remove("state.json")
                self.logger.info("[START] Old state.json removed")
        except Exception as e:
            self.logger.warning("[START] Failed to remove state.json: %s", e)

        yield scrapy.Request(
            url=AE_LOGIN_URL,
            callback=self.login,
            meta={
                "playwright": True,
                "playwright_context": "default",
                "playwright_include_page": True,
            },
            errback=self.errback_close_page,
        )

    async def login(self, response):
        """
        Login flow.
        - apply stealth
        - fill credentials
        - wait for PROFILE_INFO selector
        - always close original page at the end of this function
        """
        page: Optional[Page] = response.meta.get("playwright_page")
        if not page:
            self.logger.error("[LOGIN] No Playwright page in response.meta")
            return

        try:
            await apply_stealth(page, STEALTH_SCRIPT)
            await page.route("**", log_requests)

            # Wait and interact with UI
            await page.wait_for_selector(UppiSelectors.FISCOLINE_TAB)
            await page.click(UppiSelectors.FISCOLINE_TAB)

            await page.wait_for_selector(UppiSelectors.USERNAME_FIELD, timeout=10_000)
            await page.wait_for_timeout(1_000)
            await page.fill(UppiSelectors.USERNAME_FIELD, AE_USERNAME)
            await page.fill(UppiSelectors.PASSWORD_FIELD, AE_PASSWORD)
            await page.fill(UppiSelectors.PIN_FIELD, AE_PIN)
            await page.click(UppiSelectors.ACCEDI_BUTON)
            self.logger.info("[LOGIN] Accedi clicked, awaiting profile info")

            try:
                await page.wait_for_selector(UppiSelectors.PROFILE_INFO, timeout=10_000)
                self.logger.info("[LOGIN] Login successful. profile info found")
            except PlaywrightTimeoutError as err:
                self.logger.error("[LOGIN] Profile not found after login: %s", err)
                # if state.json exists remove it as login failed to produce valid state
                if os.path.exists("state.json"):
                    try:
                        os.remove("state.json")
                        self.logger.info("[LOGIN] Removed leftover state.json after failed login")
                    except Exception as rm_err:
                        self.logger.warning("[LOGIN] Failed remove leftover state.json: %s", rm_err)
            # continue regardless; next request will attempt to use existing session if any
        except PlaywrightTimeoutError as err:
            self.logger.error("[LOGIN] Playwright timeout during login: %s", err)
        except Exception as e:
            self.logger.exception("[LOGIN] Unexpected error during login: %s", e)
        finally:
            try:
                await page.close()
                self.logger.debug("[LOGIN] Playwright page closed")
            except Exception:
                # closing page might fail if page already closed; ignore
                pass

        # Continue to service parsing
        yield scrapy.Request(
            url=AE_URL_SERVIZI,
            callback=self.parse_ae_data,
            meta={
                "playwright": True,
                "playwright_context": "default",
                "playwright_include_page": True,
            },
            errback=self.errback_close_page,
        )

    async def parse_ae_data(self, response):
        """
        High-level coordinator after login.
        Splits work into smaller methods:
         - open SISTER service in new page
         - navigate/select options
         - handle captcha if present
         - trigger and save download
        """
        page: Optional[Page] = response.meta.get("playwright_page")
        if not page:
            self.logger.error("[PARSE] No Playwright page provided")
            return

        try:
            await apply_stealth(page, STEALTH_SCRIPT)
            await page.route("**", log_requests)
            vendor = await get_webgl_vendor(page)
            self.logger.debug("[PARSE] WebGL vendor: %s", vendor)
        except Exception as e:
            self.logger.warning("[PARSE] Pre-navigation setup failed: %s", e)

        # Open sister service and get sister_page
        sister_page = await self._open_sister_service(page)
        if not sister_page:
            self.logger.error("[PARSE] Could not open SISTER page. Aborting parse.")
            return

        # Perform navigation and selections inside sister
        navigated = await self._navigate_sister_options(sister_page)
        if not navigated:
            self.logger.error("[PARSE] Navigation inside SISTER failed. Attempting cleanup.")
            try:
                await sister_page.close()
            except Exception:
                pass
            # remove incomplete state
            if os.path.exists("state.json"):
                try:
                    os.remove("state.json")
                    self.logger.info("[PARSE] Removed state.json after failed navigation")
                except Exception as e:
                    self.logger.warning("[PARSE] Failed to remove state.json: %s", e)
            return

        # Optional CAPTCHA
        await self._solve_captcha_if_present(sister_page)

        # Download document
        await self._download_document(sister_page)

        # optionally pause for debugging (left as in original)
        try:
            await sister_page.pause()
        except Exception:
            # pause can throw if not supported; ignore
            pass

        self.logger.info("[PARSE] Finished parse_ae_data")

    async def _open_sister_service(self, page: Page) -> Optional[Page]:
        """Open SISTER in a new page/tab and save state.json. Returns sister_page or None."""
        new_page_ctx = None
        sister_page: Optional[Page] = None
        try:
            await page.wait_for_selector(UppiSelectors.PROFILE_INFO, timeout=10_000)
            await page.wait_for_selector(UppiSelectors.TUOI_PREFERITI_SECTION, timeout=10_000)
            await page.locator(UppiSelectors.TUOI_PREFERITI_SECTION).click()
            # open new page via middle click and capture it
            try:
                async with page.context.expect_page() as ctx:
                    await page.click('a[href*="ret2sister"]', button="middle")
                new_page_ctx = ctx
                sister_page = await new_page_ctx.value
                await sister_page.bring_to_front()
                await page.close()  # close main AE page
                self.logger.info("[OPEN_SISTER] SISTER page opened and AE main closed")
            except PlaywrightTimeoutError as e:
                self.logger.warning("[OPEN_SISTER] Opening SISTER timed out: %s", e)
                return None
        except PlaywrightTimeoutError as e:
            self.logger.error("[OPEN_SISTER] Required selector not found before opening SISTER: %s", e)
            return None
        except Exception as e:
            self.logger.exception("[OPEN_SISTER] Unexpected error while opening SISTER: %s", e)
            return None

        # Accept confirmation and save storage state
        try:
            await sister_page.wait_for_selector(UppiSelectors.CONFERMA_BUTTON, timeout=10_000)
            await sister_page.wait_for_timeout(1_000)
            await sister_page.click(UppiSelectors.CONFERMA_BUTTON)
            await sister_page.wait_for_timeout(3_000)
            # store auth state to file for reuse
            try:
                await sister_page.context.storage_state(path="state.json")
                self.logger.info("[OPEN_SISTER] state.json saved")
            except Exception as e:
                self.logger.warning("[OPEN_SISTER] Failed to save state.json: %s", e)
        except PlaywrightTimeoutError as e:
            self.logger.warning("[OPEN_SISTER] Conferma button not found: %s", e)
            # still return sister_page; maybe flow continues with different UI
        except Exception as e:
            self.logger.exception("[OPEN_SISTER] Error after opening SISTER: %s", e)

        return sister_page

    async def _navigate_sister_options(self, sister_page: Page) -> bool:
        """
        Make the series of clicks/selects inside SISTER to reach property list.
        Returns True on success, False on failure.
        """
        try:
            await sister_page.click(UppiSelectors.CONSULTAZIONI_CERTIFACAZIONI)
            await sister_page.wait_for_timeout(1_000)
            await sister_page.click(UppiSelectors.VISURE_CATASTALI)
            await sister_page.wait_for_timeout(1_000)
            await sister_page.click(UppiSelectors.CONFERMA_LETTURA)
            await sister_page.wait_for_timeout(1_000)

            select_ufficio = sister_page.locator(UppiSelectors.SELECT_UFFICIO)
            await select_ufficio.wait_for()
            await select_ufficio.select_option(value="PESCARA Territorio-PE")
            await sister_page.click(UppiSelectors.APLICA_BUTTON)
            await sister_page.wait_for_timeout(1_000)

            select_catasto = sister_page.locator(UppiSelectors.SELECT_CATASTO)
            await select_catasto.wait_for()
            await select_catasto.select_option(value="F")
            await sister_page.wait_for_timeout(1_000)

            select_comune = sister_page.locator(UppiSelectors.SELECT_COMUNE)
            await select_comune.wait_for()
            await select_comune.select_option(value="G482#PESCARA#0#0")
            await sister_page.wait_for_timeout(1_000)

            await sister_page.click(UppiSelectors.CODICE_FISCALE_RADIO)
            await sister_page.fill(UppiSelectors.CODICE_FISCALE_FIELD, CODICE_FISCALE)
            await sister_page.click(UppiSelectors.RICERCA_BUTTON)
            await sister_page.wait_for_timeout(1_000)

            # handle omonimi list and select first property
            await sister_page.wait_for_selector(UppiSelectors.SELECT_OMONIMI, timeout=10_000)
            await sister_page.click(UppiSelectors.SELECT_OMONIMI)
            await sister_page.click(UppiSelectors.IMOBILI_BUTTON)
            await sister_page.wait_for_timeout(1_000)
            await sister_page.wait_for_selector(UppiSelectors.ELENCO_IMOBILE, timeout=10_000)
            await sister_page.click(UppiSelectors.ELENCO_IMOBILE)
            await sister_page.click(UppiSelectors.VISURA_PER_IMOBILE_BUTTON)
            self.logger.info("[NAVIGATE] Completed navigation to property view")
            return True
        except PlaywrightTimeoutError as e:
            self.logger.warning("[NAVIGATE] Timeout during navigation: %s", e)
            return False
        except Exception as e:
            self.logger.exception("[NAVIGATE] Unexpected error during navigation: %s", e)
            return False

    async def _solve_captcha_if_present(self, sister_page: Page):
        """Detect and solve CAPTCHA when present. Logs detailed status."""
        try:
            await sister_page.wait_for_selector(UppiSelectors.IMG_CAPTCHA, timeout=5_000)
        except PlaywrightTimeoutError:
            self.logger.info("[CAPTCHA] No CAPTCHA detected")
            return

        # if we reach here, CAPTCHA element exists
        try:
            self.logger.info("[CAPTCHA] Captcha detected, invoking solver")
            await sister_page.click(UppiSelectors.CAPTCHA_FIELD)
            captcha_solution = await solve_captcha(sister_page, TWO_CAPTCHA_API_KEY, UppiSelectors.IMG_CAPTCHA)
            if not captcha_solution:
                self.logger.error("[CAPTCHA] Solver returned no solution")
                return

            await sister_page.fill(UppiSelectors.CAPTCHA_FIELD, captcha_solution)
            await sister_page.click(UppiSelectors.INOLTRA_BUTTON)
            inoltra_button = sister_page.locator(UppiSelectors.INOLTRA_BUTTON)
            try:
                await inoltra_button.wait_for(state="hidden", timeout=10_000)
            except PlaywrightTimeoutError:
                # If it does not hide, still proceed and log
                self.logger.warning("[CAPTCHA] Inoltra button did not hide after submission")
            self.logger.info("[CAPTCHA] CAPTCHA submitted")
        except PlaywrightTimeoutError as e:
            self.logger.warning("[CAPTCHA] Timeout while solving captcha: %s", e)
        except Exception as e:
            self.logger.exception("[CAPTCHA] Unexpected error in captcha handling: %s", e)

    async def _download_document(self, sister_page: Page):
        """Trigger document download and save file to downloads/ folder."""
        downloads_dir = os.path.join(os.getcwd(), "downloads")
        os.makedirs(downloads_dir, exist_ok=True)

        download_ctx = None
        download_obj = None

        try:
            # attempt to expect a download and click the 'Apri' button
            async with sister_page.expect_download() as download_ctx:
                await sister_page.wait_for_selector(UppiSelectors.APRI_BUTTON)
                await sister_page.click(UppiSelectors.APRI_BUTTON)
                self.logger.info("[DOWNLOAD] Clicked Apri, waiting for download to appear")
            # retrieve download object
            download_obj = await download_ctx.value
        except PlaywrightTimeoutError as e:
            self.logger.warning("[DOWNLOAD] Waiting for download timed out: %s", e)
        except Exception as e:
            self.logger.exception("[DOWNLOAD] Unexpected error when initiating download: %s", e)

        if not download_obj:
            self.logger.error("[DOWNLOAD] No download object obtained. Aborting save.")
            return

        try:
            suggested = download_obj.suggested_filename
            download_path = os.path.join(downloads_dir, suggested)
            await download_obj.save_as(download_path)
            self.logger.info("[DOWNLOAD] File saved: %s", download_path)
        except Exception as e:
            self.logger.exception("[DOWNLOAD] Failed to save download: %s", e)

    async def errback_close_page(self, failure):
        """Errback: ensure Playwright page is closed on request failure."""
        page: Optional[Page] = None
        try:
            page = failure.request.meta.get("playwright_page")
        except Exception:
            # if failure.request does not exist or meta missing
            pass

        if page:
            try:
                await page.close()
                self.logger.warning("[ERRBACK] Playwright page closed due to error")
            except Exception as e:
                self.logger.warning("[ERRBACK] Failed to close page: %s", e)
        else:
            self.logger.error("[ERRBACK] No playwright_page found in failure request meta")
