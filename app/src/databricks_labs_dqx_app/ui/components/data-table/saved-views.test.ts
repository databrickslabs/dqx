import { describe, expect, it } from "bun:test";
import type { SavedViewListOut, SavedViewOut } from "@/lib/api";
import {
  applyFilterToggle,
  builtinViewState,
  cleanViewName,
  isDirty,
  rowActions,
  saveDisabledReason,
  viewNameError,
  reconcileView,
  resolveActiveView,
  sortViews,
  toPayload,
  type ViewDefs,
} from "./saved-views";

type C = "name" | "owner" | "newCol";
type F = "groupBy" | "status" | "tags";

const col = (defaultVisible: boolean) => ({ toggleable: true, defaultVisible, defaultWidth: 100 });
const defs: ViewDefs<C, F> = {
  columns: {
    defaultOrder: ["name", "owner", "newCol"],
    columns: { name: col(true), owner: col(true), newCol: col(false) },
  },
  filters: {
    defaultOrder: ["groupBy", "status", "tags"],
    filters: {
      groupBy: { labelKey: "g", defaultVisible: true, pinned: true },
      status: { labelKey: "s", defaultVisible: true },
      tags: { labelKey: "t", defaultVisible: false },
    },
  },
  emptyValues: { groupBy: "none", status: "all", tags: [] },
};

describe("builtin / reconcile", () => {
  it("null payload yields the builtin", () => {
    const s = reconcileView(null, defs);
    expect(s).toEqual(builtinViewState(defs));
    expect(s.columns?.order).toEqual(["name", "owner", "newCol"]);
    expect(s.values).toEqual({ groupBy: "none", status: "all", tags: [] });
  });

  it("drops unknown columns and appends new ones with default visibility", () => {
    const s = reconcileView({ v: 1, columns: { order: ["gone", "owner", "name"], visibility: { gone: true, owner: false } } }, defs);
    expect(s.columns?.order).toEqual(["owner", "newCol", "name"]);
    expect(s.columns?.visibility.owner).toBe(false);
    expect(s.columns?.visibility.newCol).toBe(false);
  });

  it("resets the value of a hidden filter, keeps visible and type-checks", () => {
    const s = reconcileView(
      { v: 1, filters: { order: [], visibility: { status: false, tags: true } }, values: { status: "x", tags: "bad", groupBy: "table" } },
      defs,
    );
    expect(s.values.status).toBe("all");
    expect(s.values.tags).toEqual([]);
    expect(s.values.groupBy).toBe("table");
  });

  it("honours keepValueOnHide", () => {
    const s = reconcileView(
      { v: 1, filters: { visibility: { status: false } }, values: { status: "x" } },
      { ...defs, keepValueOnHide: ["status"] },
    );
    expect(s.values.status).toBe("x");
  });

  it("forces pinned filters first", () => {
    const s = reconcileView({ v: 1, filters: { order: ["tags", "status", "groupBy"] } }, defs);
    expect(s.filters.order).toEqual(["groupBy", "tags", "status"]);
  });

  it("applies sanitizeValue", () => {
    const s = reconcileView({ v: 1, values: { status: "stale" } }, { ...defs, sanitizeValue: (_k, v) => (v === "stale" ? "all" : v) });
    expect(s.values.status).toBe("all");
  });
});

describe("isDirty / toPayload", () => {
  const payload = { v: 1 as const, filters: { visibility: { tags: true } }, values: { tags: ["a", "b"] } };

  it("is clean for identical state", () => {
    expect(isDirty(reconcileView(payload, defs), reconcileView(payload, defs), defs)).toBe(false);
  });

  it("toggle and toggle back is clean", () => {
    const saved = reconcileView(payload, defs);
    const working = reconcileView(payload, defs);
    working.columns!.visibility.owner = false;
    expect(isDirty(saved, working, defs)).toBe(true);
    working.columns!.visibility.owner = true;
    expect(isDirty(saved, working, defs)).toBe(false);
  });

  it("array values are order-insensitive; changes are dirty", () => {
    const saved = reconcileView({ ...payload, values: { tags: ["b", "a"] } }, defs);
    const working = reconcileView(payload, defs);
    expect(isDirty(saved, working, defs)).toBe(false);
    working.values.tags = ["a"];
    expect(isDirty(saved, working, defs)).toBe(true);
  });

  it("omits empty values from the payload when defs are given", () => {
    const p = toPayload(builtinViewState(defs), defs);
    expect(p.v).toBe(1);
    expect(p.values).toEqual({});
  });
});

const view = (over: Partial<SavedViewOut>): SavedViewOut => ({
  id: "x", page: "registry_rules", name: "n", scope: "org", payload: null, is_builtin: false,
  is_org_default: false, is_my_default: false, starred: false, can_edit: true, can_set_org_default: true, ...over,
});

describe("sortViews / resolveActiveView", () => {
  it("sorts starred first then by name, per section", () => {
    const { mine, org } = sortViews([
      view({ id: "1", name: "alpha" }),
      view({ id: "2", name: "Zeta", starred: true }),
      view({ id: "builtin:p", name: "Default", is_builtin: true, starred: true }),
      view({ id: "3", name: "Exec", starred: true }),
      view({ id: "4", name: "mine", scope: "user" }),
    ]);
    expect(org.map((v) => v.name)).toEqual(["Default", "Exec", "Zeta", "alpha"]);
    expect(mine.map((v) => v.name)).toEqual(["mine"]);
  });

  it("resolves requested, then effective default, then builtin", () => {
    const list: SavedViewListOut = {
      views: [view({ id: "a" }), view({ id: "builtin:p", is_builtin: true })],
      effective_default_id: "a",
    };
    expect(resolveActiveView(list, "builtin:p").id).toBe("builtin:p");
    expect(resolveActiveView(list, "nope").id).toBe("a");
    expect(resolveActiveView({ ...list, effective_default_id: "dangling" }, null).id).toBe("builtin:p");
  });
});

describe("applyFilterToggle", () => {
  const withKeep: ViewDefs<C, F> = { ...defs, keepValueOnHide: ["tags"] };

  it("hiding a filter resets its value to empty", () => {
    const s = { ...builtinViewState(defs), values: { groupBy: "none", status: "open", tags: [] } };
    const next = applyFilterToggle(s, "status", defs);
    expect(next.filters.visibility.status).toBe(false);
    expect(next.values.status).toBe("all");
    expect(s.values.status).toBe("open");
  });

  it("keeps the value of a keepValueOnHide filter", () => {
    const base = builtinViewState(withKeep);
    const shown = applyFilterToggle(base, "tags", withKeep);
    const s = { ...shown, values: { ...shown.values, tags: ["pii"] } };
    const hidden = applyFilterToggle(s, "tags", withKeep);
    expect(hidden.filters.visibility.tags).toBe(false);
    expect(hidden.values.tags).toEqual(["pii"]);
  });

  it("showing a filter keeps its value and leaves isDirty consistent with reconcile", () => {
    const base = builtinViewState(defs);
    const shown = applyFilterToggle(base, "tags", defs);
    expect(shown.filters.visibility.tags).toBe(true);
    const back = applyFilterToggle(shown, "tags", defs);
    expect(isDirty(base, back, defs)).toBe(false);
  });
});

describe("view names", () => {
  const views = [
    view({ id: "o1", name: "Exec overview", scope: "org" }),
    view({ id: "u1", name: "Finance tables", scope: "user" }),
  ];

  it("cleans whitespace like the server", () => {
    expect(cleanViewName("  a \t  b  ")).toBe("a b");
  });

  it("requires 1-80 characters", () => {
    expect(viewNameError("   ", "user", views)).toBe("required");
    expect(viewNameError("x".repeat(81), "user", views)).toBe("tooLong");
    expect(viewNameError("x".repeat(80), "user", views)).toBeNull();
  });

  it("flags duplicates case-insensitively within the same scope only", () => {
    expect(viewNameError(" finance  TABLES ", "user", views)).toBe("duplicate");
    expect(viewNameError("Finance tables", "org", views)).toBeNull();
    expect(viewNameError("exec overview", "org", views)).toBe("duplicate");
  });

  it("ignores the view being renamed", () => {
    expect(viewNameError("Exec Overview", "org", views, "o1")).toBeNull();
  });
});

describe("rowActions", () => {
  it("omits rename and delete on the builtin view", () => {
    const a = rowActions(view({ is_builtin: true, can_edit: true }), true);
    expect(a.rename).toBeNull();
    expect(a.delete).toBeNull();
  });

  it("disables org-view edits for non-admins with a reason", () => {
    const a = rowActions(view({ scope: "org", can_edit: false, can_set_org_default: false }), false);
    expect(a.rename).toEqual({ enabled: false, reasonKey: "savedViews.reasons.orgViewAdminsOnly" });
    expect(a.delete).toEqual({ enabled: false, reasonKey: "savedViews.reasons.orgViewAdminsOnly" });
    expect(a.setOrgDefault).toEqual({ enabled: false, reasonKey: "savedViews.reasons.orgDefaultAdminsOnly" });
    expect(a.setMyDefault).toEqual({ enabled: true, reasonKey: null });
  });

  it("only org views can be the org default", () => {
    const a = rowActions(view({ scope: "user", can_edit: true, can_set_org_default: false }), true);
    expect(a.rename?.enabled).toBe(true);
    expect(a.setOrgDefault).toEqual({ enabled: false, reasonKey: "savedViews.reasons.orgDefaultOrgViewsOnly" });
  });

  it("disables defaults that are already set", () => {
    const a = rowActions(view({ is_my_default: true, is_org_default: true }), true);
    expect(a.setMyDefault).toEqual({ enabled: false, reasonKey: "savedViews.reasons.alreadyMyDefault" });
    expect(a.setOrgDefault).toEqual({ enabled: false, reasonKey: "savedViews.reasons.alreadyOrgDefault" });
  });

  it("enables org default for admins on org views", () => {
    expect(rowActions(view({ scope: "org" }), true).setOrgDefault).toEqual({ enabled: true, reasonKey: null });
  });
});

describe("saveDisabledReason", () => {
  it("is disabled without a reason when nothing changed", () => {
    expect(saveDisabledReason(view({}), false)).toEqual({ disabled: true, reasonKey: null });
    expect(saveDisabledReason(null, true)).toEqual({ disabled: true, reasonKey: null });
  });

  it("explains a read-only view", () => {
    expect(saveDisabledReason(view({ can_edit: false }), true)).toEqual({
      disabled: true,
      reasonKey: "savedViews.saveReadOnly",
    });
    expect(saveDisabledReason(view({ can_edit: false }), false).reasonKey).toBe("savedViews.saveReadOnly");
  });

  it("is enabled for a dirty, editable view", () => {
    expect(saveDisabledReason(view({}), true)).toEqual({ disabled: false, reasonKey: null });
  });
});
