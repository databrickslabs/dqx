import { describe, expect, test } from "bun:test";
import { isClipped } from "./truncation";

describe("isClipped", () => {
  test("null element is not clipped", () => {
    expect(isClipped(null)).toBe(false);
  });
  test("equal widths are not clipped", () => {
    expect(isClipped({ scrollWidth: 100, clientWidth: 100 })).toBe(false);
  });
  test("wider scroll width is clipped", () => {
    expect(isClipped({ scrollWidth: 101, clientWidth: 100 })).toBe(true);
  });
});
