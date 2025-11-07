import os
import scrapy
from decouple import config
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError


AE_LOGIN_URL = config("AE_LOGIN_URL")
AE_URL_HOME = config("AE_URL_HOME")

AE_USERNAME = config("AE_USERNAME")
AE_PASSWORD = config("AE_PASSWORD")
AE_PIN = config("AE_PIN")

class UppiSelectors:
    """Selectors for the UppiSpider."""
    # Login form selectors
    FISCOLINE_TAB = 'ul > li > a[href="#tab-4"]'
    USERNAME_FIELD = '#username-fo-ent'
    PASSWORD_FIELD = '#password-fo-ent-1'
    PIN_FIELD = '#pin-fo-ent'
    ACCEDI_BUTON = 'button.btn-primary[type="submit"]'
    # Profile selector to confirm login
    PROFILE_INFO ='#user-info'

class UppiSpider(scrapy.Spider):
    name = "uppi"
    allowed_domains = ["agenziaentrate.gov.it"]

    async def start(self):
        """Start the spider by checking for existing session state."""
        if os.path.exists("state.json"):
            self.logger.info(f"✅ Found state.json! We are using a saved session. Go to URL: {AE_URL_HOME}")
            yield scrapy.Request(
                url=AE_URL_HOME,
                callback=self.parse_ae_data,
                meta={
                    "playwright": True,
                    "playwright_context": "default",
                    "playwright_include_page": True,
                },
                errback=self.errback_close_page,
            )
        else:
            self.logger.info("🔄 Performing authorization....")
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

    @staticmethod
    async def log_request(route, request):
        """Log all requests made by the page."""
        print(f"📡 Request: {request.url} | Method: {request.method}")
        await route.continue_()

    async def login(self, response):
        """Login to AE website and save state.json"""
        page = response.meta["playwright_page"]

        await page.route("**", self.log_request)

        await page.wait_for_selector(UppiSelectors.FISCOLINE_TAB)
        await page.click(UppiSelectors.FISCOLINE_TAB)

        await page.wait_for_selector(UppiSelectors.USERNAME_FIELD, timeout=10_000)
        await page.wait_for_timeout(1_000)
        await page.fill(UppiSelectors.USERNAME_FIELD, AE_USERNAME)
        await page.fill(UppiSelectors.PASSWORD_FIELD, AE_PASSWORD)
        await page.fill(UppiSelectors.PIN_FIELD, AE_PIN)
        await page.click(UppiSelectors.ACCEDI_BUTON)
        self.logger.info("⏳ Accedi button clicked, waiting for navigation...")

        try:
            await page.wait_for_selector(UppiSelectors.PROFILE_INFO, timeout=10_000)
            self.logger.info("✅ Login successful! Saving state.json")
            await page.wait_for_timeout(2_000)
            await page.context.storage_state(path="state.json")
        except PlaywrightTimeoutError as err:
            self.logger.error(f"❌ Login error! {err} Deleting state.json")
            if os.path.exists("state.json"):
                os.remove("state.json")
        finally:
            await page.close()
        yield scrapy.Request(
            url=AE_URL_HOME,
            callback=self.parse_ae_data,
            meta={
                "playwright": True,
                "playwright_context": "default",
                "playwright_include_page": True,
            },
            errback=self.errback_close_page,
        )

    async def parse_ae_data(self, response):
        """Parse AE data after ensuring authorization is active."""
        page = response.meta["playwright_page"]
        if not page:
            self.logger.error("❌ Playwright page not received. Loading without it")
            return
        try:
            await page.wait_for_selector(UppiSelectors.PROFILE_INFO, timeout=5000)
            self.logger.info("✅ Authorization is active")
        except PlaywrightTimeoutError:
            self.logger.warning("❌ Authorization not found. Deleting state.json")
            os.remove("state.json")

    async def errback_close_page(self, failure):
        """Errback to close Playwright page on error."""
        page: Page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
            self.logger.warning("❌ Playwright page closed due to error")
        else:
            self.logger.error("🚨 Error: Could not get `playwright_page` in `errback`")