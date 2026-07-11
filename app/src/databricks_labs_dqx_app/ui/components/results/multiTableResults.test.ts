import { describe, expect, it } from "bun:test";
import { breakdownParams } from "./MultiTableResults";
import { EMPTY_FILTERS } from "@/components/monitored-tables/BindingResultsTab";

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
      axes: "breakdown",
    });
  });

  it("omits empty facets and pins axes to breakdown for the base (unfiltered) query", () => {
    expect(breakdownParams(EMPTY_FILTERS)).toEqual({
      dimension: undefined,
      severity: undefined,
      rule: undefined,
      column: undefined,
      axes: "breakdown",
    });
  });
});
