import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { CACHE_KEY, DEFAULT_BRANDING_SNAPSHOT } from "./cache";
import { DARK_SELECTOR, LIGHT_SELECTOR, themeOverrides, toStyleSheet } from "./css";
import { THEMABLE_TOKENS } from "./derive";

const html = readFileSync(join(import.meta.dir, "../../index.html"), "utf8");

/** Body of the shipped `<script>` that follows the `// dqx-branding-bootstrap` marker. */
function shippedBootstrapBody(): string {
  const marker = html.indexOf("// dqx-branding-bootstrap");
  expect(marker).toBeGreaterThan(-1);
  const start = html.lastIndexOf("<script>", marker) + "<script>".length;
  return html.slice(start, html.indexOf("</script>", marker));
}

function runShipped(cached: string | null) {
  const store = new Map<string, string>(cached ? [[CACHE_KEY, cached]] : []);
  const appended: { id: string; textContent: string }[] = [];
  const document = {
    createElement: () => ({ id: "", textContent: "" }),
    head: { appendChild: (el: { id: string; textContent: string }) => appended.push(el) },
  } as unknown as Document;
  const localStorage = { getItem: (k: string) => store.get(k) ?? null } as unknown as Storage;
  new Function("window", shippedBootstrapBody())({ localStorage, document });
  return appended;
}

const logos = { light: null, dark: null };
const cache = (light: Record<string, string>, dark: Record<string, string> = {}) =>
  JSON.stringify({ overrides: { light, dark }, companyName: null, logoMode: "shared", logos });

describe("shipped index.html branding bootstrap", () => {
  test("applies a valid cache", () => {
    expect(runShipped(cache({ "--header": "#3F0E40" }))[0].textContent).toBe("html:root:not(.dark){--header:#3F0E40;}");
  });
  test("produces the same stylesheet as toStyleSheet", () => {
    const o = themeOverrides({ light: { header: "#3F0E40", page_background: "#FFF8E7" }, darkCustomised: true, dark: {} });
    expect(runShipped(cache(o.light, o.dark))[0].textContent).toBe(toStyleSheet(o));
    expect(shippedBootstrapBody()).toContain(`"${LIGHT_SELECTOR}"`);
    expect(shippedBootstrapBody()).toContain(`"${DARK_SELECTOR}"`);
  });
  test("cached DQX Default applies nothing", () => {
    expect(runShipped(JSON.stringify(DEFAULT_BRANDING_SNAPSHOT))).toEqual([]);
  });
  test("ignores a tampered cache", () => {
    expect(runShipped(cache({ "--header": "red;}body{display:none" }))).toEqual([]);
  });
  test("ignores a token outside the allowlist", () => {
    expect(runShipped(cache({ "--chart-1": "#000000" }))).toEqual([]);
  });
  test("no cache does nothing", () => {
    expect(runShipped(null)).toEqual([]);
  });
  test("embedded allowlist equals THEMABLE_TOKENS", () => {
    const body = shippedBootstrapBody();
    const list = body.slice(body.indexOf("var ALLOWED = ["), body.indexOf("];", body.indexOf("var ALLOWED = [")));
    const names = [...list.matchAll(/"(--[a-z0-9-]+)"/g)].map((m) => m[1]);
    expect([...names].sort()).toEqual([...THEMABLE_TOKENS].sort());
  });
});
