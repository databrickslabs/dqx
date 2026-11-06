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
