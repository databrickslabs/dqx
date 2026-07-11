import { describe, expect, it } from "bun:test";
import {
  EMPTY_FILTERS,
  facetQueryParams,
  failedRowsRunParam,
  ruleChipDisplay,
  ruleFacetValue,
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

describe("ruleFacetValue (by-rule identity value, P5.2)", () => {
  it("prefers the registry rule_id when the group carries one", () => {
    expect(ruleFacetValue({ label: "new_name", rule_id: "rule-1" })).toBe("rule-1");
  });

  it("falls back to the label for legacy name-keyed groups", () => {
    expect(ruleFacetValue({ label: "legacy_check", rule_id: null })).toBe("legacy_check");
    expect(ruleFacetValue({ label: "legacy_check" })).toBe("legacy_check");
  });

  it("is null only when both are absent", () => {
    expect(ruleFacetValue({})).toBeNull();
  });
});

describe("ruleChipDisplay (rule chip shows the newest-run label, not the id)", () => {
  const byRule = [
    { label: "new_name", rule_id: "rule-1" },
    { label: "legacy_check", rule_id: null },
  ];

  it("resolves an active rule_id filter to its row's label", () => {
    expect(ruleChipDisplay("rule-1", byRule)).toBe("new_name");
  });

  it("passes label-only filter values through (legacy rows)", () => {
    expect(ruleChipDisplay("legacy_check", byRule)).toBe("legacy_check");
  });

  it("falls back to the raw value when no row matches", () => {
    expect(ruleChipDisplay("rule-gone", byRule)).toBe("rule-gone");
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

describe("failedRowsRunParam (run picker → failed-rows run_id, P5.5)", () => {
  it("omits the param for 'Latest' (no pinned run) so the backend resolves it", () => {
    expect(failedRowsRunParam(null, "r-latest")).toBeUndefined();
    expect(failedRowsRunParam(undefined, "r-latest")).toBeUndefined();
  });

  it("omits the param when the pinned run IS the latest (one cache entry for both spellings)", () => {
    expect(failedRowsRunParam("r-latest", "r-latest")).toBeUndefined();
  });

  it("passes an older pinned run through so exactly that run's records load", () => {
    expect(failedRowsRunParam("r-old", "r-latest")).toBe("r-old");
  });

  it("passes the pinned run through when the runs list has not resolved yet", () => {
    expect(failedRowsRunParam("r-old", undefined)).toBe("r-old");
  });
});

describe("facetQueryParams (facet chips → query params, per box)", () => {
  const filters: MultiFilters = {
    dimension: ["Completeness"],
    severity: ["Critical", "High"],
    rule: [],
    column: ["amount"],
    runId: "run-1",
  };

  it("maps each active facet to its list param and omits empty facets", () => {
    expect(facetQueryParams(filters)).toEqual({
      dimension: ["Completeness"],
      severity: ["Critical", "High"],
      rule: undefined,
      column: ["amount"],
    });
  });

  it("maps EMPTY_FILTERS to all-undefined (the BASE breakdown query shape)", () => {
    expect(facetQueryParams(EMPTY_FILTERS)).toEqual({
      dimension: undefined,
      severity: undefined,
      rule: undefined,
      column: undefined,
    });
  });

  it("never leaks runId into the facet params (run scoping is a separate param)", () => {
    expect(Object.keys(facetQueryParams(filters))).toEqual([
      "dimension",
      "severity",
      "rule",
      "column",
    ]);
  });

  it("feeds the FILTERED query only — toggling a facet changes the filtered shape, not the base", () => {
    // The filtered breakdown/failed-rows queries spread facetQueryParams(filters);
    // the base breakdown query passes NO facets (dqlake's filtered-vs-base wiring).
    const clicked = toggleFacet(EMPTY_FILTERS, "dimension", "Validity");
    expect(facetQueryParams(clicked).dimension).toEqual(["Validity"]);
    expect(facetQueryParams(EMPTY_FILTERS).dimension).toBeUndefined();
  });
});
