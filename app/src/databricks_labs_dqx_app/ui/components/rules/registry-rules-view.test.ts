import { describe, expect, it } from "bun:test";
import { builtinViewState, reconcileView } from "@/components/data-table/saved-views";
import {
  RULES_LEGACY_STORAGE_KEYS,
  buildRegistryRulesViewDefs,
  labelSelectionFromValue,
  labelSelectionToValue,
} from "./registry-rules-view";

const columns = {
  defaultOrder: ["name"] as const,
  columns: { name: { toggleable: false, defaultVisible: true, defaultWidth: 200 } },
};
const loaded = { dimensions: ["accuracy", "validity"], severities: ["high", "low"] };

describe("label selection values", () => {
  it("round-trips chosen values and any-value keys", () => {
    const selection = new Map([
      ["team", new Set(["finance", "ops"])],
      ["pii", new Set<string>()],
    ]);
    const value = labelSelectionToValue(selection);
    expect(labelSelectionFromValue(value)).toEqual(selection);
  });

  it("stores the same selection the same way whatever the order it was built in", () => {
    const a = new Map([
      ["b", new Set(["2", "1"])],
      ["a", new Set<string>()],
    ]);
    const b = new Map([
      ["a", new Set<string>()],
      ["b", new Set(["1", "2"])],
    ]);
    expect(labelSelectionToValue(a)).toEqual(labelSelectionToValue(b));
  });

  it("keeps an empty tag value apart from any value", () => {
    const selection = new Map([["flag", new Set([""])]]);
    expect(labelSelectionFromValue(labelSelectionToValue(selection))).toEqual(selection);
  });

  it("an empty value is no selection", () => {
    expect(labelSelectionToValue(new Map())).toEqual([]);
    expect(labelSelectionFromValue([]).size).toBe(0);
  });
});

describe("buildRegistryRulesViewDefs", () => {
  it("starts with every filter cleared and Group by off", () => {
    const state = builtinViewState(buildRegistryRulesViewDefs(columns, loaded));
    expect(state.values).toEqual({
      groupBy: "none",
      search: "",
      status: "all",
      dimension: "all",
      severity: "all",
      owner: "all",
      labels: [],
    });
    expect(state.filters.order[0]).toBe("groupBy");
  });

  it("drops a dimension or severity that is no longer defined, once definitions load", () => {
    const payload = { v: 1, values: { dimension: "gone", severity: "high", owner: "ana@x.com", labels: ["pii"] } };
    expect(reconcileView(payload, buildRegistryRulesViewDefs(columns, { dimensions: null, severities: null })).values.dimension).toBe("gone");
    const state = reconcileView(payload, buildRegistryRulesViewDefs(columns, loaded));
    expect(state.values).toMatchObject({ dimension: "all", severity: "high", owner: "ana@x.com", labels: ["pii"] });
  });

  it("lists Status between search and dimensions, hidden by default", () => {
    const state = builtinViewState(buildRegistryRulesViewDefs(columns, loaded));
    expect(state.filters.order.slice(1, 4)).toEqual(["search", "status", "dimension"]);
    expect(state.filters.visibility.status).toBe(false);
  });

  it("keeps a known status and drops an unknown one", () => {
    const defs = buildRegistryRulesViewDefs(columns, loaded);
    const visible = { visibility: { status: true } };
    expect(reconcileView({ v: 1, filters: visible, values: { status: "modified" } }, defs).values.status).toBe("modified");
    expect(reconcileView({ v: 1, filters: visible, values: { status: "archived" } }, defs).values.status).toBe("all");
  });

  it("adds Status, hidden, to a view saved before it existed", () => {
    const order = ["groupBy", "search", "dimension", "severity", "owner", "labels"];
    const state = reconcileView({ v: 1, filters: { order } }, buildRegistryRulesViewDefs(columns, loaded));
    expect(state.filters.order).toContain("status");
    expect(state.filters.visibility.status).toBe(false);
  });

  it("offers Status as a Group by", () => {
    const state = reconcileView(
      { v: 1, filters: { visibility: { groupBy: true } }, values: { groupBy: "status" } },
      buildRegistryRulesViewDefs(columns, loaded),
    );
    expect(state.values.groupBy).toBe("status");
  });

  it("falls back to no grouping for an unknown Group by", () => {
    const state = reconcileView(
      { v: 1, filters: { visibility: { groupBy: true } }, values: { groupBy: "catalog" } },
      buildRegistryRulesViewDefs(columns, loaded),
    );
    expect(state.values.groupBy).toBe("none");
  });

  it("cleans up the browser-only layouts", () => {
    expect(buildRegistryRulesViewDefs(columns, loaded).legacyStorageKeys).toEqual(RULES_LEGACY_STORAGE_KEYS);
  });
});
