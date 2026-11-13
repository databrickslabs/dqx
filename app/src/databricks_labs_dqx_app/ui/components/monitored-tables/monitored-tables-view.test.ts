import { describe, expect, it } from "bun:test";
import { builtinViewState, reconcileView } from "@/components/data-table/saved-views";
import { buildMonitoredTablesViewDefs, TABLES_LEGACY_STORAGE_KEYS } from "./monitored-tables-view";

const columns = {
  defaultOrder: ["catalog", "table"] as const,
  columns: {
    catalog: { toggleable: true, defaultVisible: true, defaultWidth: 100 },
    table: { toggleable: false, defaultVisible: true, defaultWidth: 200 },
  },
};
const loading = { catalogs: null, schemas: null, owners: null };
const loaded = { catalogs: ["main", "sales"], schemas: ["bronze", "gold"], owners: ["ana@x.com"] };
const stored = {
  v: 1,
  filters: { order: [], visibility: { groupBy: true } },
  values: { catalog: "sales", schema: "gold", owner: "ana@x.com", dqScore: "50-75", groupBy: "catalog", search: "orders" },
};

describe("buildMonitoredTablesViewDefs", () => {
  it("starts with every filter cleared and Group by off", () => {
    const state = builtinViewState(buildMonitoredTablesViewDefs(columns, loaded));
    expect(state.values).toEqual({
      groupBy: "none",
      search: "",
      catalog: "all",
      schema: "all",
      owner: "all",
      dqScore: "all",
    });
    expect(state.filters.order[0]).toBe("groupBy");
    expect(state.filters.visibility.groupBy).toBe(false);
    expect(state.columns?.order).toEqual(["catalog", "table"]);
  });

  it("keeps saved values while the tables are still loading", () => {
    const state = reconcileView({ ...stored, values: { ...stored.values, catalog: "gone" } }, buildMonitoredTablesViewDefs(columns, loading));
    expect(state.values.catalog).toBe("gone");
  });

  it("restores saved values that still exist, search and Group by included", () => {
    const state = reconcileView(stored, buildMonitoredTablesViewDefs(columns, loaded));
    expect(state.values).toEqual(stored.values);
  });

  it("drops values whose option is gone and unknown Group by / DQ score values", () => {
    const state = reconcileView(
      { ...stored, values: { catalog: "gone", schema: "gone", owner: "bob", dqScore: "1-2", groupBy: "owner" } },
      buildMonitoredTablesViewDefs(columns, loaded),
    );
    expect(state.values).toMatchObject({ catalog: "all", schema: "all", owner: "all", dqScore: "all", groupBy: "none" });
  });

  it("cleans up the browser-only layouts", () => {
    expect(buildMonitoredTablesViewDefs(columns, loaded).legacyStorageKeys).toEqual(TABLES_LEGACY_STORAGE_KEYS);
    expect(TABLES_LEGACY_STORAGE_KEYS).toContain("dqx.monitoredTables.filters");
  });
});
