import { describe, expect, it } from "bun:test";
import {
  EMPTY_FILTERS,
  toggleFacet,
  toNum,
  type MultiFilters,
} from "./BindingResultsTab";

describe("toggleFacet", () => {
  it("adds a value absent from the facet", () => {
    const next = toggleFacet(EMPTY_FILTERS, "dimension", "Completeness");
    expect(next.dimension).toEqual(["Completeness"]);
  });

  it("removes a value already present in the facet", () => {
    const filters: MultiFilters = { ...EMPTY_FILTERS, severity: ["Critical", "Warning"] };
    const next = toggleFacet(filters, "severity", "Critical");
    expect(next.severity).toEqual(["Warning"]);
  });

  it("leaves the other facets (and runId) untouched", () => {
    const filters: MultiFilters = {
      dimension: ["Validity"],
      severity: [],
      rule: ["r1"],
      column: [],
      runId: "run-9",
    };
    const next = toggleFacet(filters, "column", "amount");
    expect(next.dimension).toEqual(["Validity"]);
    expect(next.rule).toEqual(["r1"]);
    expect(next.runId).toBe("run-9");
    expect(next.column).toEqual(["amount"]);
  });

  it("does not mutate its input (EMPTY_FILTERS stays empty)", () => {
    toggleFacet(EMPTY_FILTERS, "rule", "r1");
    expect(EMPTY_FILTERS.rule).toEqual([]);
  });
});

describe("toNum", () => {
  it("passes finite numbers through", () => {
    expect(toNum(0.97)).toBe(0.97);
    expect(toNum(0)).toBe(0);
  });

  it("coerces numeric strings (the overall trend serialises pass_rate as a string)", () => {
    expect(toNum("0.85")).toBe(0.85);
  });

  it("maps null/undefined/non-numeric/non-finite to null", () => {
    expect(toNum(null)).toBeNull();
    expect(toNum(undefined)).toBeNull();
    expect(toNum("n/a")).toBeNull();
    expect(toNum(Number.NaN)).toBeNull();
    expect(toNum(Number.POSITIVE_INFINITY)).toBeNull();
  });
});
