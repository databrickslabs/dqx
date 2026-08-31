import { describe, expect, test } from "bun:test";

import { findRefRanges, findRefs, findUnknownRefs, isReservedRef } from "./columnRefs";

describe("findRefs", () => {
  test("collects each distinct reference once, in first-seen order", () => {
    expect(findRefs("{{b}} > {{a}} AND {{b}} < 10")).toEqual(["b", "a"]);
  });

  test("tolerates whitespace inside the braces", () => {
    expect(findRefs("{{  amount  }} > 0")).toEqual(["amount"]);
  });

  test("reports reserved placeholders like any other reference", () => {
    // findRefs is the raw scan — lowcodeCompile relies on seeing every ref.
    expect(findRefs("SELECT {{id}} FROM {{input_view}}")).toEqual(["id", "input_view"]);
  });
});

describe("isReservedRef", () => {
  test("input_view is reserved", () => {
    expect(isReservedRef("input_view")).toBe(true);
  });

  test("an ordinary column name is not", () => {
    expect(isReservedRef("customer_id")).toBe(false);
  });
});

describe("findUnknownRefs", () => {
  test("flags a reference that is not a declared slot", () => {
    expect(findUnknownRefs("{{amount}} > 0", ["customer_id"])).toEqual(["amount"]);
  });

  test("accepts a declared slot", () => {
    expect(findUnknownRefs("{{amount}} > 0", ["amount"])).toEqual([]);
  });

  test("never flags {{input_view}}, which no author declares as a column", () => {
    const sql = "SELECT iv.{{customer_id}} FROM {{input_view}} iv";
    expect(findUnknownRefs(sql, ["customer_id"])).toEqual([]);
  });

  test("still flags real unknowns in a query that also uses {{input_view}}", () => {
    const sql = "SELECT {{typo_col}} FROM {{input_view}}";
    expect(findUnknownRefs(sql, ["customer_id"])).toEqual(["typo_col"]);
  });

  test("{{input_view}} is fine even when nothing is declared yet", () => {
    expect(findUnknownRefs("FROM {{input_view}}", [])).toEqual([]);
  });
});

describe("findRefRanges", () => {
  test("returns a range spanning the whole {{ref}} token", () => {
    const [range] = findRefRanges("x = {{amount}}");
    expect(range).toEqual({ name: "amount", from: 4, to: 14 });
  });

  test("ranges cover every occurrence, not just the first", () => {
    expect(findRefRanges("{{a}} {{a}}").map((r) => r.from)).toEqual([0, 6]);
  });
});
