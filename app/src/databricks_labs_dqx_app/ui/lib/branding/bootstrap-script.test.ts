import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { runBootstrap } from "./bootstrap-script";
import { CACHE_KEY } from "./cache";

function fakeWin(cached: string | null) {
  const store = new Map<string, string>(cached ? [[CACHE_KEY, cached]] : []);
  const appended: { id: string; textContent: string }[] = [];
  const document = {
    getElementById: (id: string) => appended.find((e) => e.id === id) ?? null,
    createElement: () => ({ id: "", textContent: "" }),
    head: { appendChild: (el: { id: string; textContent: string }) => appended.push(el) },
  } as unknown as Document;
  const localStorage = { getItem: (k: string) => store.get(k) ?? null } as unknown as Storage;
  return { win: { localStorage, document }, appended };
}

const logos = { light: null, dark: null };

describe("pre-React bootstrap", () => {
  test("applies a valid cache", () => {
    const { win, appended } = fakeWin(
      JSON.stringify({ overrides: { light: { "--header": "#3F0E40" }, dark: {} }, companyName: null, logoMode: "shared", logos }),
    );
    runBootstrap(win);
    expect(appended[0].textContent).toBe("html:root{--header:#3F0E40;}");
  });
  test("ignores a tampered cache", () => {
    const { win, appended } = fakeWin(
      JSON.stringify({ overrides: { light: { "--header": "red;}body{display:none" }, dark: {} }, companyName: null, logoMode: "shared", logos }),
    );
    runBootstrap(win);
    expect(appended).toEqual([]);
  });
  test("no cache does nothing", () => {
    const { win, appended } = fakeWin(null);
    runBootstrap(win);
    expect(appended).toEqual([]);
  });
  test("index.html inlines the bootstrap", () => {
    const html = readFileSync(join(import.meta.dir, "../../index.html"), "utf8");
    expect(html).toContain('"dqx.branding.v1"');
    expect(html).toContain('el.id = "dqx-branding"');
    expect(html).toContain("/^#[0-9A-Fa-f]{6}$/");
  });
});
