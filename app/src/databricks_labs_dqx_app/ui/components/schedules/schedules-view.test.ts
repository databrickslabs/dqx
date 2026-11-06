import { describe, expect, it } from "bun:test";
import { builtinViewState, reconcileView } from "@/components/data-table/saved-views";
import { EMPTY_SCHEDULE_FILTERS } from "./schedule-list";
import { SCHEDULES_LEGACY_STORAGE_KEYS, buildSchedulesViewDefs, scheduleFiltersFromValues } from "./schedules-view";

const columns = {
  defaultOrder: ["name"] as const,
  columns: { name: { toggleable: false, defaultVisible: true, defaultWidth: 200 } },
};

describe("buildSchedulesViewDefs", () => {
  it("starts with every filter cleared", () => {
    const state = builtinViewState(buildSchedulesViewDefs(columns, { owners: [] }));
    expect(scheduleFiltersFromValues(state.values)).toEqual(EMPTY_SCHEDULE_FILTERS);
    expect(state.filters.order).toEqual(["search", "type", "status", "owner"]);
  });

  it("restores saved values that still exist", () => {
    const values = { search: "nightly", type: "collection", status: "failing", owner: "ana@x.com" };
    const state = reconcileView({ v: 1, values }, buildSchedulesViewDefs(columns, { owners: ["ana@x.com"] }));
    expect(scheduleFiltersFromValues(state.values)).toEqual(values);
  });

  it("keeps a saved owner while loading and drops it once it owns no schedule", () => {
    const payload = { v: 1, values: { owner: "bob@x.com", type: "job", status: "broken" } };
    expect(reconcileView(payload, buildSchedulesViewDefs(columns, { owners: null })).values.owner).toBe("bob@x.com");
    const state = reconcileView(payload, buildSchedulesViewDefs(columns, { owners: ["ana@x.com"] }));
    expect(state.values).toMatchObject({ owner: "all", type: "all", status: "all" });
  });

  it("cleans up the browser-only layouts", () => {
    expect(buildSchedulesViewDefs(columns, { owners: null }).legacyStorageKeys).toEqual(SCHEDULES_LEGACY_STORAGE_KEYS);
  });
});

describe("scheduleFiltersFromValues", () => {
  it("falls back to no filter for unknown or missing values", () => {
    expect(scheduleFiltersFromValues({ type: "job", status: ["paused"] })).toEqual(EMPTY_SCHEDULE_FILTERS);
  });
});
