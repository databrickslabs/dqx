import { describe, expect, it } from "bun:test";
import { builtinViewState, reconcileView } from "@/components/data-table/saved-views";
import { COLLECTIONS_LEGACY_STORAGE_KEYS, buildCollectionsViewDefs } from "./collections-view";

const columns = {
  defaultOrder: ["name"] as const,
  columns: { name: { toggleable: false, defaultVisible: true, defaultWidth: 200 } },
};

describe("buildCollectionsViewDefs", () => {
  it("starts with every filter cleared", () => {
    const state = builtinViewState(buildCollectionsViewDefs(columns, { owners: [], tags: [] }));
    expect(state.values).toEqual({ search: "", owner: "all", dqScore: "all", tags: [] });
    expect(state.filters.order).toEqual(["search", "owner", "dqScore", "tags"]);
  });

  it("restores saved values that still exist", () => {
    const values = { search: "finance", owner: "ana@x.com", dqScore: "0-25", tags: ["table:tier=gold"] };
    expect(reconcileView({ v: 1, values }, buildCollectionsViewDefs(columns, { owners: ["ana@x.com"], tags: ["table:tier=gold"] })).values).toEqual(
      values,
    );
  });

  it("keeps a saved owner while loading and drops it once it has no collections", () => {
    const payload = { v: 1, values: { owner: "bob@x.com", dqScore: "bogus" } };
    expect(reconcileView(payload, buildCollectionsViewDefs(columns, { owners: null, tags: null })).values.owner).toBe("bob@x.com");
    const state = reconcileView(payload, buildCollectionsViewDefs(columns, { owners: ["ana@x.com"], tags: ["table:tier=gold"] }));
    expect(state.values).toMatchObject({ owner: "all", dqScore: "all" });
  });

  it("cleans up the browser-only layouts", () => {
    expect(buildCollectionsViewDefs(columns, { owners: null, tags: null }).legacyStorageKeys).toEqual(COLLECTIONS_LEGACY_STORAGE_KEYS);
  });
});
