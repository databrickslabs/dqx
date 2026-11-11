import { describe, expect, test } from "bun:test";
import { isPolicyOrigin, policyIdOf } from "./policy-origin";

describe("isPolicyOrigin", () => {
  test("true for policy origin", () => {
    expect(isPolicyOrigin({ origin: "policy", policy_id: "p1" })).toBe(true);
  });
  test("false for other origins and missing metadata", () => {
    expect(isPolicyOrigin({ origin: "tag_auto" })).toBe(false);
    expect(isPolicyOrigin({ origin: "manual" })).toBe(false);
    expect(isPolicyOrigin({})).toBe(false);
    expect(isPolicyOrigin(null)).toBe(false);
    expect(isPolicyOrigin(undefined)).toBe(false);
  });
});

describe("policyIdOf", () => {
  test("returns the id only for policy-origin rows", () => {
    expect(policyIdOf({ origin: "policy", policy_id: "p1" })).toBe("p1");
    expect(policyIdOf({ origin: "tag_auto", policy_id: "p1" })).toBeNull();
    expect(policyIdOf({ origin: "policy" })).toBeNull();
    expect(policyIdOf(undefined)).toBeNull();
  });
});
