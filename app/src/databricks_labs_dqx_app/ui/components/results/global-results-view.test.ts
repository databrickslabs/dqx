import { describe, expect, it } from "bun:test";
import { applyFilterToggle, builtinViewState, isDirty, reconcileView, toPayload } from "@/components/data-table/saved-views";
import { EMPTY_FILTERS } from "@/components/monitored-tables/BindingResultsTab";
import {
  GLOBAL_RESULTS_FILTER_ORDER,
  RESULTS_ELEMENT_ORDER,
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
  tag: null,
};

describe("buildGlobalResultsViewDefs", () => {
  const defs = buildGlobalResultsViewDefs(LOADING);

  it("ships the eleven bubbles in spec order with the spec's default visibility", () => {
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
      "tag",
    ]);
    const state = builtinViewState(defs);
    // Tags are advanced, so the tag bubble starts hidden.
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

  it("keeps row-click filter values when their bubble is hidden, clears others", () => {
    const state = {
      ...builtinViewState(defs),
      values: { ...builtinViewState(defs).values, table: ["a.b.c"], rule: ["r1"], catalog: ["main"] },
    };
    expect(applyFilterToggle(state, "table", defs).values.table).toEqual(["a.b.c"]);
    expect(applyFilterToggle(state, "rule", defs).values.rule).toEqual(["r1"]);
    expect(applyFilterToggle(state, "catalog", defs).values.catalog).toEqual([]);
    const shown = applyFilterToggle(state, "dimension", defs);
    const withDim = { ...shown, values: { ...shown.values, dimension: ["Validity"] } };
    expect(applyFilterToggle(withDim, "dimension", defs).values.dimension).toEqual(["Validity"]);
    // dimension is hidden by default: a stored value still survives reconcile; owner's doesn't.
    const reconciled = reconcileView(
      {
        v: 1,
        values: { dimension: ["Validity"], table: ["a.b.c"], owner: ["ann"] },
        filters: { order: [], visibility: { table: false } },
      },
      defs,
    );
    expect(reconciled.values.dimension).toEqual(["Validity"]);
    expect(reconciled.values.table).toEqual(["a.b.c"]);
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

  it("lists the page elements in page order, with the tag charts hidden by default", () => {
    const state = builtinViewState(defs);
    expect(state.columns?.order).toEqual([...RESULTS_ELEMENT_ORDER]);
    expect(RESULTS_ELEMENT_ORDER.filter((k) => !state.columns?.visibility[k])).toEqual(["scoreByTag", "byTag"]);
  });

  it("saves which elements show", () => {
    const builtin = builtinViewState(defs);
    const shown = { ...builtin, columns: { ...builtin.columns!, visibility: { ...builtin.columns!.visibility, byTag: true } } };
    const restored = reconcileView(toPayload(shown, defs), defs);
    expect(restored.columns?.visibility.byTag).toBe(true);
    expect(isDirty(builtin, restored, defs)).toBe(true);
  });

  it("keeps saved tag filter tokens that are still offered", () => {
    const loaded = buildGlobalResultsViewDefs({ ...LOADING, tag: ["rule:domain", "rule:domain=finance", "!rule:domain"] });
    const payload = {
      v: 1,
      filters: { order: [], visibility: { tag: true } },
      values: { tag: ["rule:domain=finance", "!rule:domain", "rule:domain=gone"] },
    };
    expect(reconcileView(payload, loaded).values.tag).toEqual(["rule:domain=finance", "!rule:domain"]);
  });
});
