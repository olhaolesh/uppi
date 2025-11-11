"""
JavaScript code to enhance stealth capabilities in a browser automation context.
"""
STEALTH_SCRIPT = r"""
// Basic navigator properties
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
Object.defineProperty(navigator, 'languages', { get: () => ['it-IT','it','en-US','en'] });
Object.defineProperty(navigator, 'language', { get: () => 'it-IT' });

window.chrome = window.chrome || { runtime: {} };

const originalQuery = navigator.permissions && navigator.permissions.query;
if (originalQuery) {
  navigator.permissions.query = (params) => {
    if (params && params.name === 'notifications') {
      return Promise.resolve({ state: Notification.permission });
    }
    return originalQuery(params);
  };
}

(function() {
  try {
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
      if (parameter === 37445) return "Intel Inc.";
      if (parameter === 37446) return "Intel(R) Iris(TM) Graphics";
      return getParameter.call(this, parameter);
    };
  } catch (e) {}
})();

(function() {
  const toDataURL = HTMLCanvasElement.prototype.toDataURL;
  HTMLCanvasElement.prototype.toDataURL = function() {
    try {
      const ctx = this.getContext('2d');
      if (ctx) {
        ctx.fillStyle = 'rgba(0,0,0,0)';
        ctx.fillRect(0,0,1,1);
      }
    } catch (e) {}
    return toDataURL.apply(this, arguments);
  };
})();

Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
"""
