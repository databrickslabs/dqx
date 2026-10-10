import { describe, expect, test } from "bun:test";
import {
  CACHE_KEY,
  clearBrandingCache,
  DEFAULT_BRANDING_SNAPSHOT,
  readBrandingCache,
  snapshotFromApi,
  writeBrandingCache,
  type BrandingSnapshot,
} from "./cache";

class MemoryStorage implements Storage {
  private m = new Map<string, string>();
  get length() { return this.m.size; }
  clear() { this.m.clear(); }
  getItem(k: string) { return this.m.get(k) ?? null; }
  key(i: number) { return [...this.m.keys()][i] ?? null; }
  removeItem(k: string) { this.m.delete(k); }
  setItem(k: string, v: string) { this.m.set(k, v); }
}

const good: BrandingSnapshot = {
  overrides: { light: { "--header": "#3F0E40" }, dark: {} },
  companyName: "Acme",
  logoMode: "shared",
  logos: { light: "abcdef0123456789", dark: null },
};

describe("branding cache", () => {
  test("round trip", () => {
    const s = new MemoryStorage();
    writeBrandingCache(good, s);
    expect(readBrandingCache(s)).toEqual(good);
  });
  test("tampered colour invalidates the whole cache", () => {
    const s = new MemoryStorage();
    s.setItem(CACHE_KEY, JSON.stringify({ ...good, overrides: { light: { "--header": "red;}" }, dark: {} } }));
    expect(readBrandingCache(s)).toBeNull();
  });
  test("bad logo hash invalidates", () => {
    const s = new MemoryStorage();
    s.setItem(CACHE_KEY, JSON.stringify({ ...good, logos: { light: "../../etc", dark: null } }));
    expect(readBrandingCache(s)).toBeNull();
  });
  test("garbage and missing are null", () => {
    const s = new MemoryStorage();
    expect(readBrandingCache(s)).toBeNull();
    s.setItem(CACHE_KEY, "{");
    expect(readBrandingCache(s)).toBeNull();
    s.setItem(CACHE_KEY, "null");
    expect(readBrandingCache(s)).toBeNull();
  });
  test("clear", () => {
    const s = new MemoryStorage();
    writeBrandingCache(good, s);
    clearBrandingCache(s);
    expect(readBrandingCache(s)).toBeNull();
  });
  test("DQX Default snapshot is a valid cache entry", () => {
    const s = new MemoryStorage();
    writeBrandingCache(DEFAULT_BRANDING_SNAPSHOT, s);
    expect(readBrandingCache(s)).toEqual(DEFAULT_BRANDING_SNAPSHOT);
  });
  test("snapshotFromApi of an uncustomised install equals the DQX Default snapshot", () => {
    const snap = snapshotFromApi({
      company_name: null,
      logo_mode: "shared",
      light: { colors: {} },
      dark: { customised: false, colors: {} },
      logos: { light: null, dark: null },
    });
    expect(snap).toEqual(DEFAULT_BRANDING_SNAPSHOT);
  });
  test("snapshotFromApi maps the API shape", () => {
    const snap = snapshotFromApi({
      company_name: "Acme",
      logo_mode: "separate",
      light: { colors: { header: "#3F0E40" } },
      dark: { customised: false, colors: {} },
      logos: { light: "abcdef0123456789", dark: null },
    });
    expect(snap.companyName).toBe("Acme");
    expect(snap.logoMode).toBe("separate");
    expect(snap.overrides.light["--header"]).toBe("#3F0E40");
    expect(snap.logos).toEqual({ light: "abcdef0123456789", dark: null });
  });
});
