/* eslint-disable no-var */
/**
 * Pre-React branding bootstrap. Must stay plain ES5 (it is inlined into index.html verbatim via
 * BOOTSTRAP_SOURCE). Applies a validated cached theme so the loading screen is already themed.
 */
function bootstrap(win: { localStorage: Storage; document: Document }): void {
  try {
    var raw = win.localStorage.getItem("dqx.branding.v1");
    if (!raw) return;
    var s = JSON.parse(raw);
    var TOKEN = /^--[a-z][a-z0-9-]*$/;
    var HEX = /^#[0-9A-Fa-f]{6}$/;
    var build = function (sel: string, map: Record<string, unknown>): string | null {
      if (!map || typeof map !== "object") return null;
      var body = "";
      for (var k in map) {
        if (!Object.prototype.hasOwnProperty.call(map, k)) continue;
        var v = map[k];
        if (!TOKEN.test(k) || typeof v !== "string" || !HEX.test(v)) return null;
        body += k + ":" + v + ";";
      }
      return body ? sel + "{" + body + "}" : "";
    };
    var light = build("html:root", s && s.overrides && s.overrides.light);
    var dark = build("html.dark", s && s.overrides && s.overrides.dark);
    if (light === null || dark === null) return;
    var css = light + dark;
    if (!css) return;
    var el = win.document.createElement("style");
    el.id = "dqx-branding";
    el.textContent = css;
    win.document.head.appendChild(el);
  } catch (e) {
    /* never block startup */
  }
}

export const BOOTSTRAP_SOURCE = `(${bootstrap.toString()})(window);`;
export const runBootstrap = bootstrap;
