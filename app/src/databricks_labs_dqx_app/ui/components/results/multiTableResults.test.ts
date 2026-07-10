import { describe, expect, it } from "bun:test";
import { breakdownParams, tableUniverse } from "./MultiTableResults";
import { EMPTY_FILTERS } from "@/components/monitored-tables/BindingResultsTab";
import type { GroupRowOut } from "@/lib/api";

// The composition-level helpers shared by the product tab, the global
// Results page, and (P2.6) the rule Results view. The product-specific
// aliases (productBreakdownParams etc.) are regression-tested through their
// re-exports in `components/data-products/productResultsTab.test.ts`.

function row(label: string | null, failed = 0): GroupRowOut {
  return { label, pass_rate: 1, failed_tests: failed };
}

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

describe("tableUniverse", () => {
  it("returns the distinct table FQNs of the by-table rows in first-seen order", () => {
    const groups = [row("c.s.orders"), row("c.s.customers"), row("c.s.orders")];
    expect(tableUniverse(groups)).toEqual(["c.s.orders", "c.s.customers"]);
  });

  it("skips null labels (defensive: table rows are always labelled)", () => {
    expect(tableUniverse([row(null), row("c.s.orders")])).toEqual(["c.s.orders"]);
  });

  it("is empty for undefined or empty input (no universe, so no Average line)", () => {
    expect(tableUniverse(undefined)).toEqual([]);
    expect(tableUniverse([])).toEqual([]);
  });
});
