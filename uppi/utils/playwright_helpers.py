from playwright.async_api import Page

async def apply_stealth(page: Page, script: str):
    await page.add_init_script(script)
    await page.context.grant_permissions(['geolocation'])
    await page.context.set_geolocation({'latitude': 41.9028, 'longitude': 12.4964})

async def log_requests(route, request):
    """Log all requests made by the page."""
    print(f"📡 Request: {request.url} | Method: {request.method}")
    await route.continue_()

async def get_webgl_vendor(page: Page):
    return await page.evaluate("""() => {
        try {
            const canvas = document.createElement('canvas');
            const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
            const dbg = gl.getExtension('WEBGL_debug_renderer_info');
            return dbg ? gl.getParameter(dbg.UNMASKED_VENDOR_WEBGL) + '|' + gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL) : null;
        } catch(e) { return null; }
    }""")

