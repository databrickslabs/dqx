import { describe, expect, it } from "bun:test";
import { reconcileLayout } from "./column-layout";
import {
  defaultFilterVisibility,
  isPinnedFilter,
  moveFilter,
  normalizeFilterOrder,
  splitPinnedFilters,
  type FilterLayoutDef,
} from "./filter-layout";

type Key = "groupBy" | "search" | "owner" | "labels";
const DEFAULT_ORDER: readonly Key[] = ["groupBy", "search", "owner", "labels"];
const FILTERS: Record<Key, FilterLayoutDef> = {
  groupBy: { labelKey: "common.groupBy", defaultVisible: false, pinned: true },
  search: { labelKey: "search", defaultVisible: true },
  owner: { labelKey: "owner", defaultVisible: true },
  labels: { labelKey: "labels", defaultVisible: true },
};

describe("isPinnedFilter", () => {
  it("is true only for filters marked pinned", () => {
    expect(isPinnedFilter<Key>("groupBy", FILTERS)).toBe(true);
    expect(isPinnedFilter<Key>("search", FILTERS)).toBe(false);
  });
});

describe("normalizeFilterOrder", () => {
  it("moves a pinned filter saved elsewhere back to the front", () => {
    expect(normalizeFilterOrder<Key>(["search", "owner", "labels", "groupBy"], DEFAULT_ORDER, FILTERS)).toEqual([
      "groupBy",
      "search",
      "owner",
      "labels",
    ]);
  });

  it("keeps the user's order of the unpinned filters", () => {
    expect(normalizeFilterOrder<Key>(["labels", "groupBy", "owner", "search"], DEFAULT_ORDER, FILTERS)).toEqual([
      "groupBy",
      "labels",
      "owner",
      "search",
    ]);
  });

  it("normalises a reconciled legacy layout that listed Group by last", () => {
    const stored = { order: ["search", "owner", "labels", "groupBy"] as Key[], visibility: { groupBy: true } };
    const loaded = reconcileLayout<Key>(stored, DEFAULT_ORDER, defaultFilterVisibility(DEFAULT_ORDER, FILTERS));
    expect(normalizeFilterOrder(loaded.order, DEFAULT_ORDER, FILTERS)[0]).toBe("groupBy");
    expect(loaded.visibility.groupBy).toBe(true);
  });
});

describe("defaultFilterVisibility", () => {
  it("leaves the pinned Group by off by default", () => {
    expect(defaultFilterVisibility(DEFAULT_ORDER, FILTERS)).toEqual({
      groupBy: false,
      search: true,
      owner: true,
      labels: true,
    });
  });
});

describe("moveFilter", () => {
  const order: Key[] = ["groupBy", "search", "owner", "labels"];

  it("reorders unpinned filters", () => {
    expect(moveFilter(order, "labels", "search", FILTERS)).toEqual(["groupBy", "labels", "search", "owner"]);
  });

  it("never moves a pinned filter", () => {
    expect(moveFilter(order, "groupBy", "labels", FILTERS)).toEqual(order);
  });

  it("never lets a filter be dropped above a pinned one", () => {
    expect(moveFilter(order, "owner", "groupBy", FILTERS)).toEqual(order);
  });

  it("ignores unknown keys", () => {
    expect(moveFilter(order, "search", "missing" as Key, FILTERS)).toEqual(order);
  });
});

describe("splitPinnedFilters", () => {
  it("separates the pinned lead from the rest, keeping order", () => {
    const isPinned = (k: Key) => isPinnedFilter(k, FILTERS);
    expect(splitPinnedFilters<Key>(["groupBy", "labels", "search"], isPinned)).toEqual({
      pinned: ["groupBy"],
      rest: ["labels", "search"],
    });
    expect(splitPinnedFilters<Key>(["labels", "search"], isPinned)).toEqual({ pinned: [], rest: ["labels", "search"] });
  });
});
