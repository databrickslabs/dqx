import { describe, expect, it } from "bun:test";
import { breakdownParams, nextTableSelection, sumTestCounts } from "./MultiTableResults";
import { EMPTY_FILTERS, toggleFacet } from "@/components/monitored-tables/BindingResultsTab";

// The composition-level helpers shared by the product tab, the global
// Results page, and (P2.6) the rule Results view. The product-specific
// aliases (productBreakdownParams etc.) are regression-tested through their
// re-exports in `components/data-products/productResultsTab.test.ts`.

describe("breakdownParams", () => {
  it("NEVER carries a run_id filter (a per-table run_id cannot scope a multi-table view)", () => {
    const params = breakdownParams({
      ...EMPTY_FILTERS,
      severity: ["Critical"],
      runId: "r7", // even a stray runId in the filters must not leak through
    });
    expect("run_id" in params).toBe(false);
    expect(params).toEqual({
      dimension: undefined,
      severity: ["Critical"],
      rule: undefined,
      column: undefined,
      table: undefined,
      axes: "breakdown",
    });
  });

  it("omits empty facets and pins axes to breakdown for the base (unfiltered) query", () => {
    expect(breakdownParams(EMPTY_FILTERS)).toEqual({
      dimension: undefined,
      severity: undefined,
      rule: undefined,
      column: undefined,
      table: undefined,
      axes: "breakdown",
    });
  });

  it("carries the column facet — a By column row click cross-filters the surface (P7.2 pin)", () => {
    // Verified end-to-end for the product surface: the By column box's
    // onSelect toggles filters.column, which flows through
    // facetQueryParams → this params object → the product/global/rule
    // endpoints' repeatable `column` query param (P3.6 axios repeat-key
    // serialization) → backend row_matches_facets. This pin holds the
    // frontend half of that chain.
    const clicked = toggleFacet(EMPTY_FILTERS, "column", "amount");
    expect(breakdownParams(clicked).column).toEqual(["amount"]);
  });

  it("carries the table facet so a By table click cross-filters the other boxes (P7.2)", () => {
    const clicked = toggleFacet(EMPTY_FILTERS, "table", "main.sales.orders");
    expect(breakdownParams(clicked).table).toEqual(["main.sales.orders"]);
  });

  it("carries as_of_batch only when a batch is pinned (B2-18 run-batch as-of)", () => {
    expect("as_of_batch" in breakdownParams(EMPTY_FILTERS)).toBe(false);
    expect("as_of_batch" in breakdownParams(EMPTY_FILTERS, false, null)).toBe(false);
    expect(breakdownParams(EMPTY_FILTERS, false, "r2").as_of_batch).toBe("r2");
  });
});

describe("nextTableSelection (By table click → selection + table facet, P7.2)", () => {
  it("selecting a row pins both the selection and the facet to it", () => {
    expect(nextTableSelection(null, "orders", "main.sales.orders")).toEqual({
      selected: "orders",
      table: ["main.sales.orders"],
    });
  });

  it("re-clicking the selected row clears both", () => {
    expect(nextTableSelection("orders", "orders", "main.sales.orders")).toEqual({
      selected: null,
      table: [],
    });
  });

  it("switching rows REPLACES the facet value (single-select, never accumulates)", () => {
    expect(nextTableSelection("orders", "items", "dev.sales.items")).toEqual({
      selected: "items",
      table: ["dev.sales.items"],
    });
  });

  it("a label with no resolvable FQN selects without filtering on a stale value", () => {
    expect(nextTableSelection(null, "ghost", undefined)).toEqual({
      selected: "ghost",
      table: [],
    });
  });
});

describe("sumTestCounts (ScoreBox subtitle sums)", () => {
  const byTable = [
    { label: "main.sales.orders", failed_tests: 10, total_tests: 100 },
    { label: "dev.sales.items", failed_tests: 30, total_tests: 200 },
  ];

  it("sums every row when no table facet is active", () => {
    expect(sumTestCounts(byTable)).toEqual({ failedTests: 40, totalTests: 300 });
  });

  it("re-applies the table facet client-side (the server self-excludes it from by_table)", () => {
    expect(sumTestCounts(byTable, ["main.sales.orders"])).toEqual({
      failedTests: 10,
      totalTests: 100,
    });
  });

  it("treats null/missing counts as zero", () => {
    expect(sumTestCounts([{ label: "t", failed_tests: null, total_tests: null }])).toEqual({
      failedTests: 0,
      totalTests: 0,
    });
  });
});
