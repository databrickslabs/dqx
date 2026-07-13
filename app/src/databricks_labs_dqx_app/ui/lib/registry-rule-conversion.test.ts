import { describe, expect, test } from "bun:test";
import { deriveSlotsAndParameters, fnSupportsNegate } from "./registry-rule-conversion";
import { familyForSparkType } from "./slot-mapping";
import type { CheckFunctionDef } from "./api";

// Unit tests for the registry-rule <-> DQX-check conversion helpers touched by
// P19-D items 10 (typed slot families + ARRAY) and 11 (negate -> polarity).
// Run via `bun test` (see `make app-test-ui`).

const fn = (over: Partial<CheckFunctionDef> = {}): CheckFunctionDef =>
  ({ name: "x", rule_type: "row", category: "Other", doc: "", params: [], ...over }) as CheckFunctionDef;

const param = (name: string, kind: string, extra: Record<string, unknown> = {}) =>
  ({ name, kind, required: false, ...extra }) as CheckFunctionDef["params"][number];

describe("fnSupportsNegate", () => {
  test("true when the function declares a negate argument", () => {
    expect(fnSupportsNegate(fn({ params: [param("column", "column"), param("negate", "boolean")] }))).toBe(true);
  });
  test("false when there is no negate argument", () => {
    expect(fnSupportsNegate(fn({ params: [param("column", "column")] }))).toBe(false);
  });
  test("false for undefined", () => {
    expect(fnSupportsNegate(undefined)).toBe(false);
  });
});

describe("deriveSlotsAndParameters — item 10 family seeding + item 11 negate skip", () => {
  test("seeds the column slot family from the param's implied family", () => {
    const { slots } = deriveSlotsAndParameters(
      fn({ params: [param("column", "column", { family: "temporal" })] }),
    );
    expect(slots).toHaveLength(1);
    expect(slots[0]?.family).toBe("temporal");
  });

  test("falls back to any when the param carries no family", () => {
    const { slots } = deriveSlotsAndParameters(fn({ params: [param("column", "column")] }));
    expect(slots[0]?.family).toBe("any");
  });

  test("carries the array family through for array-column checks", () => {
    const { slots } = deriveSlotsAndParameters(
      fn({ params: [param("column", "column", { family: "array" })] }),
    );
    expect(slots[0]?.family).toBe("array");
  });

  test("drops negate from the derived parameter list (surfaced as polarity)", () => {
    const { parameters } = deriveSlotsAndParameters(
      fn({ params: [param("column", "column"), param("regex", "string"), param("negate", "boolean")] }),
    );
    expect(parameters.map((p) => p.name)).toEqual(["regex"]);
  });
});

describe("familyForSparkType — ARRAY family (item 10)", () => {
  test("classifies array column types as array", () => {
    expect(familyForSparkType("array<string>")).toBe("array");
    expect(familyForSparkType("ARRAY<INT>")).toBe("array");
  });
  test("still classifies the primitive families", () => {
    expect(familyForSparkType("bigint")).toBe("numeric");
    expect(familyForSparkType("string")).toBe("text");
    expect(familyForSparkType("timestamp")).toBe("temporal");
    expect(familyForSparkType("boolean")).toBe("boolean");
    expect(familyForSparkType("struct<a:int>")).toBe("any");
  });
});
