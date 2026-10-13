import { describe, expect, test } from "bun:test";
import { PAGE_TAB_KEYS, resolvePageTab } from "./registry-rule-page-tab";

describe("resolvePageTab", () => {
  test("defaults to about when the tab is absent", () => {
    expect(resolvePageTab(undefined)).toBe("about");
  });

  test("defaults to about for an unknown tab", () => {
    expect(resolvePageTab("bogus")).toBe("about");
    expect(resolvePageTab("")).toBe("about");
  });

  test("keeps every known tab", () => {
    for (const key of PAGE_TAB_KEYS) {
      expect(resolvePageTab(key)).toBe(key);
    }
  });
});
