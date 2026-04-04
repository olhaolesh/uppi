"""
Scrapy / Playwright settings для UPPI.

У цьому модулі є browser-critical конфігурація, яку не можна трактувати як
просте місце для "оптимізацій" під час структурного рефакторингу.

Protected areas:
- Playwright context для AE / SISTER flow.
- Підхоплення `state.json` у browser context.

Protected invariant для `state.json`, проти якого мають перевірятися будь-які
майбутні зміни:
`fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`.

Що дозволено тут без окремого high-risk етапу:
- documentation;
- characterization tests;
- safer logging around lifecycle;
- wrapper/API encapsulation без зміни порядку дій і semantics.

Що заборонено без окремого high-risk етапу:
- reuse старого `state.json` між новими сесіями;
- перенесення create/load/delete semantics;
- зміна browser flow через settings-driven "optimization".
"""

from uppi.config.workspace import bind_existing_state_json_storage_state

BOT_NAME = "uppi"

SPIDER_MODULES = ["uppi.spiders"]
NEWSPIDER_MODULE = "uppi.spiders"

# === Налаштування продуктивності Scrapy ===
CONCURRENT_REQUESTS = 1
DOWNLOAD_DELAY = 1

USER_AGENT = None

# === Налаштування Playwright ===
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    # "executable_path": "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "headless": False,
    "args": [
        "--disable-blink-features=AutomationControlled",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-extensions",
    ],
}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30_000
PLAYWRIGHT_MAX_CONTEXTS = 3
PLAYWRIGHT_CONTEXTS = {
    "default": {
        "viewport": {"width": 1920, "height": 1080},
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "java_script_enabled": True,
        "ignore_https_errors": True,
        "locale": 'it-IT',  # Італійська локаль
        "timezone_id": 'Europe/Rome',  # Італійский часовой пояс
        # "proxy": {
        #     "server": "http://myproxy.com:3128",
        #     "username": "user",
        #     "password": "pass",
        # },
        "extra_http_headers": {
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            "Connection": "keep-alive",
        },
        # "storage_state": "state.json",
    }
}
# Protected invariant: тут лише підхоплюється вже створений session state.
# Цю точку не можна перетворювати на механізм повторного використання старого
# стейту або змінювати create/load/delete semantics навколо `state.json`.
bind_existing_state_json_storage_state(PLAYWRIGHT_CONTEXTS)


DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# robots.txt у цьому проєкті не застосовується
ROBOTSTXT_OBEY = False

# Налаштування item pipelines
ITEM_PIPELINES = {
    "uppi.pipelines.UppiPipeline": 300,
}

# Фіксуємо сучасне значення reactor і кодування фідів
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
