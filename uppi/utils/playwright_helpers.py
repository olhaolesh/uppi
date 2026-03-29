"""Browser-adjacent helpers для Playwright без зміни критичного flow павука."""

import logging
from urllib.parse import urlsplit

from playwright.async_api import Page


logger = logging.getLogger(__name__)

async def apply_stealth(page: Page, script: str):
    """Підключає stealth script і базові browser permissions до сторінки."""
    await page.add_init_script(script)
    await page.context.grant_permissions(['geolocation'])
    await page.context.set_geolocation({'latitude': 41.9028, 'longitude': 12.4964})

async def log_requests(route, request):
    """Логує безпечний мета-рівень HTTP-запитів Playwright без raw URL-параметрів."""
    url = urlsplit(request.url)
    safe_target = f"{url.netloc}{url.path or '/'}"
    logger.debug("[PLAYWRIGHT] Request method=%s target=%s", request.method, safe_target)
    await route.continue_()

async def get_webgl_vendor(page: Page):
    """Повертає vendor/renderer WebGL для діагностики browser fingerprinting."""
    return await page.evaluate("""() => {
        try {
            const canvas = document.createElement('canvas');
            const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
            const dbg = gl.getExtension('WEBGL_debug_renderer_info');
            return dbg ? gl.getParameter(dbg.UNMASKED_VENDOR_WEBGL) + '|' + gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL) : null;
        } catch(e) { return null; }
    }""")
