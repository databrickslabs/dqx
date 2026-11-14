import { describe, expect, it } from "bun:test";
import { reconcileLayout } from "./column-layout";
import { defaultFilterVisibility, type FilterLayoutDef } from "./filter-layout";

type Key = "expander" | "name" | "owner" | "status";
const DEFAULT_ORDER: readonly Key[] = ["expander", "name", "owner", "status"];
const DEFAULT_VISIBILITY: Record<Key, boolean> = { expander: false, name: true, owner: true, status: true };

describe("reconcileLayout", () => {
  it("falls back to the shipped order and visibility when nothing is stored", () => {
    expect(reconcileLayout<Key>({}, DEFAULT_ORDER, DEFAULT_VISIBILITY)).toEqual({
      order: ["expander", "name", "owner", "status"],
      visibility: DEFAULT_VISIBILITY,
    });
  });

  it("keeps the user's order and visibility choices", () => {
    const stored = { order: ["status", "owner", "name", "expander"] as Key[], visibility: { owner: false } };
    const { order, visibility } = reconcileLayout<Key>(stored, DEFAULT_ORDER, DEFAULT_VISIBILITY);
    expect(order).toEqual(["status", "owner", "name", "expander"]);
    expect(visibility.owner).toBe(false);
    expect(visibility.name).toBe(true);
  });

  it("inserts a new leading key first rather than at the end", () => {
    const stored = { order: ["name", "owner", "status"] as Key[], visibility: {} };
    expect(reconcileLayout<Key>(stored, DEFAULT_ORDER, DEFAULT_VISIBILITY).order).toEqual([
      "expander",
      "name",
      "owner",
      "status",
    ]);
  });

  it("inserts a new key after its nearest shipped neighbour", () => {
    const stored = { order: ["status", "name", "expander"] as Key[], visibility: {} };
    expect(reconcileLayout<Key>(stored, DEFAULT_ORDER, DEFAULT_VISIBILITY).order).toEqual([
      "status",
      "name",
      "owner",
      "expander",
    ]);
  });

  it("drops unknown and duplicate stored keys and ignores non-boolean visibility", () => {
    const stored = {
      order: ["name", "retired", "name", "owner", "status", "expander"] as Key[],
      visibility: { name: "yes" } as unknown as Partial<Record<Key, boolean>>,
    };
    const { order, visibility } = reconcileLayout<Key>(stored, DEFAULT_ORDER, DEFAULT_VISIBILITY);
    expect(order).toEqual(["name", "owner", "status", "expander"]);
    expect(visibility.name).toBe(true);
  });
});

describe("defaultFilterVisibility", () => {
  it("reads each filter's shipped default", () => {
    const filters: Record<"search" | "groupBy", FilterLayoutDef> = {
      search: { labelKey: "a", defaultVisible: true },
      groupBy: { labelKey: "b", defaultVisible: false },
    };
    expect(defaultFilterVisibility(["search", "groupBy"], filters)).toEqual({ search: true, groupBy: false });
  });
});
