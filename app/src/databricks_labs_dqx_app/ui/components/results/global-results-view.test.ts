import { describe, expect, it } from "bun:test";
import { applyFilterToggle, builtinViewState, reconcileView } from "@/components/data-table/saved-views";
import { EMPTY_FILTERS } from "@/components/monitored-tables/BindingResultsTab";
import {
  GLOBAL_RESULTS_FILTER_ORDER,
  buildGlobalResultsViewDefs,
  collectionTableFqns,
  filtersFromViewValues,
  topFilterParams,
  viewValueChanges,
  type GlobalResultsFilterOptionValues,
} from "./global-results-view";

const LOADING: GlobalResultsFilterOptionValues = {
  catalog: null,
  schema: null,
  owner: null,
  collection: null,
  table: null,
  rule: null,
  dimension: null,
  severity: null,
};

describe("buildGlobalResultsViewDefs", () => {
  const defs = buildGlobalResultsViewDefs(LOADING);

  it("ships the ten bubbles in spec order with the spec's default visibility", () => {
    expect(GLOBAL_RESULTS_FILTER_ORDER).toEqual([
      "catalog",
      "schema",
      "owner",
      "collection",
      "table",
      "rule",
      "dqScore",
      "outcome",
      "dimension",
      "severity",
    ]);
    const state = builtinViewState(defs);
    expect(state.columns).toBeNull();
    expect(state.filters.order.filter((k) => state.filters.visibility[k])).toEqual([
      "catalog",
      "schema",
      "table",
      "rule",
      "outcome",
    ]);
    expect(state.values.dqScore).toBe("all");
    expect(state.values.owner).toEqual([]);
  });

  it("keeps dimension/severity values when their bubble is hidden, clears others", () => {
    const state = { ...builtinViewState(defs), values: { ...builtinViewState(defs).values, table: ["a.b.c"] } };
    expect(applyFilterToggle(state, "table", defs).values.table).toEqual([]);
    const shown = applyFilterToggle(state, "dimension", defs);
    const withDim = { ...shown, values: { ...shown.values, dimension: ["Validity"] } };
    expect(applyFilterToggle(withDim, "dimension", defs).values.dimension).toEqual(["Validity"]);
    // dimension is hidden by default: a stored value still survives reconcile; owner's doesn't.
    const reconciled = reconcileView({ v: 1, values: { dimension: ["Validity"], owner: ["ann"] } }, defs);
    expect(reconciled.values.dimension).toEqual(["Validity"]);
    expect(reconciled.values.owner).toEqual([]);
  });

  it("drops values whose options are known and gone, keeps them while loading", () => {
    const loaded = buildGlobalResultsViewDefs({ ...LOADING, catalog: ["main"], collection: ["c1"] });
    const payload = { v: 1, values: { catalog: ["main", "gone"], collection: ["c9"], schema: ["x.y"], dqScore: "bogus" } };
    const state = reconcileView(payload, loaded);
    expect(state.values.catalog).toEqual(["main"]);
    expect(state.values.collection).toEqual([]);
    expect(state.values.schema).toEqual(["x.y"]);
    expect(state.values.dqScore).toBe("all");
  });
});

describe("filtersFromViewValues / viewValueChanges", () => {
  it("maps view values onto the facet filters, keeping the local column facet", () => {
    const filters = filtersFromViewValues({ catalog: ["main"], dqScore: "0-25", owner: ["ann"] }, ["amount"]);
    expect(filters).toEqual({
      ...EMPTY_FILTERS,
      catalog: ["main"],
      scoreBucket: "0-25",
      owner: ["ann"],
      column: ["amount"],
    });
  });

  it("reports only the changed view keys (never column)", () => {
    const next = { ...EMPTY_FILTERS, table: ["a.b.c"], scoreBucket: "none", column: ["x"] };
    expect(viewValueChanges(EMPTY_FILTERS, next)).toEqual([
      ["table", ["a.b.c"]],
      ["dqScore", "none"],
    ]);
    expect(viewValueChanges(next, next)).toEqual([]);
  });
});

describe("topFilterParams", () => {
  it("omits empty filters and the any-score bucket", () => {
    expect(topFilterParams(EMPTY_FILTERS)).toEqual({});
    expect("score_bucket" in topFilterParams(EMPTY_FILTERS)).toBe(false);
  });

  it("maps collection/owner arrays and dqScore → score_bucket", () => {
    expect(
      topFilterParams({ ...EMPTY_FILTERS, collection: ["c1"], owner: ["ann", "bob"], scoreBucket: "50-75" }),
    ).toEqual({ collection: ["c1"], owner: ["ann", "bob"], score_bucket: "50-75" });
  });

  it("ignores an unknown bucket", () => {
    expect(topFilterParams({ ...EMPTY_FILTERS, scoreBucket: "bogus" })).toEqual({});
  });
});

describe("collectionTableFqns", () => {
  const members = new Map([
    ["c1", ["main.a.t1", "main.a.t2"]],
    ["c2", ["main.b.t3", "main.a.t1"]],
  ]);

  it("is null (no narrowing) when no collection is selected", () => {
    expect(collectionTableFqns(members, [])).toBeNull();
  });

  it("unions the selected collections' members; unknown ids add nothing", () => {
    expect([...(collectionTableFqns(members, ["c1", "c2", "zz"]) ?? [])].sort()).toEqual([
      "main.a.t1",
      "main.a.t2",
      "main.b.t3",
    ]);
    expect(collectionTableFqns(members, ["zz"])?.size).toBe(0);
  });
});
