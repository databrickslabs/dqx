import { describe, expect, it } from "bun:test";
import type { SavedViewListOut, SavedViewOut } from "@/lib/api";
import {
  applyFilterToggle,
  applyStarOptimistic,
  builtinViewState,
  cleanViewName,
  isDirty,
  MAX_VIEW_STRING_LENGTH,
  myDefaultTarget,
  oneOf,
  optionsSanitizer,
  rowActions,
  sanitizeAgainstOptions,
  sanitizeViewValues,
  stringArrayValue,
  stringValue,
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

  it("clamps string values to the server's 512-character limit", () => {
    const state = builtinViewState(defs);
    const p = toPayload({ ...state, values: { ...state.values, status: "x".repeat(600), tags: ["y".repeat(600)] } }, defs);
    expect(MAX_VIEW_STRING_LENGTH).toBe(512);
    expect(p.values?.status).toBe("x".repeat(512));
    expect(p.values?.tags).toEqual(["y".repeat(600)]);
  });

  it("clamps by code point so a surrogate pair is never split", () => {
    const state = builtinViewState(defs);
    const p = toPayload({ ...state, values: { ...state.values, status: "\u{1F600}".repeat(600) } }, defs);
    expect(Array.from(p.values?.status as string)).toHaveLength(512);
    expect(p.values?.status).toBe("\u{1F600}".repeat(512));
  });

  it("leaves values within the limit untouched", () => {
    const state = builtinViewState(defs);
    const p = toPayload({ ...state, values: { ...state.values, status: "x".repeat(512) } }, defs);
    expect(p.values?.status).toBe("x".repeat(512));
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

  it("replaces control characters like the server", () => {
    expect(cleanViewName("a\u0000b\u001fc\u007fd\u0085e")).toBe("a b c d e");
    expect(cleanViewName("\u0001Sales\u0002")).toBe("Sales");
  });

  it("treats a name differing only by control characters as a duplicate", () => {
    expect(viewNameError("Exec\u0000overview", "org", views)).toBe("duplicate");
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
  const disabled = (reasonKey: string) => ({ enabled: false, reasonKey });
  const enabled = { enabled: true, reasonKey: null };
  const personal = (over: Partial<SavedViewOut> = {}) =>
    view({ id: "p1", scope: "user", can_edit: true, can_set_org_default: false, ...over });

  it("personal view, admin: share shown, org default absent", () => {
    const a = rowActions(personal(), true, "builtin:p");
    expect(a.rename).toEqual(enabled);
    expect(a.delete).toEqual(enabled);
    expect(a.share).toEqual(enabled);
    expect(a.setOrgDefault).toBeNull();
    expect(a.setMyDefault).toEqual(enabled);
  });

  it("personal view, non-admin: neither share nor org default is shown", () => {
    const a = rowActions(personal(), false, "builtin:p");
    expect(a.share).toBeNull();
    expect(a.setOrgDefault).toBeNull();
    expect(a.rename).toEqual(enabled);
  });

  it("org view, admin: org default enabled, no share", () => {
    const a = rowActions(view({ id: "o1", scope: "org" }), true, "builtin:p");
    expect(a.setOrgDefault).toEqual(enabled);
    expect(a.share).toBeNull();
  });

  it("org view, non-admin: edits and org default disabled with reasons, no share", () => {
    const a = rowActions(view({ id: "o1", scope: "org", can_edit: false, can_set_org_default: false }), false, "builtin:p");
    expect(a.rename).toEqual(disabled("savedViews.reasons.orgViewAdminsOnly"));
    expect(a.delete).toEqual(disabled("savedViews.reasons.orgViewAdminsOnly"));
    expect(a.setOrgDefault).toEqual(disabled("savedViews.reasons.orgDefaultAdminsOnly"));
    expect(a.setMyDefault).toEqual(enabled);
    expect(a.share).toBeNull();
  });

  it("org view that is already the org default: org default disabled", () => {
    const a = rowActions(view({ id: "o1", scope: "org", is_org_default: true }), true, "p1");
    expect(a.setOrgDefault).toEqual(disabled("savedViews.reasons.alreadyOrgDefault"));
  });

  it("builtin view: no rename, delete or share; org default kept", () => {
    const a = rowActions(view({ id: "builtin:p", is_builtin: true, can_edit: false }), true, "o1");
    expect(a.rename).toBeNull();
    expect(a.delete).toBeNull();
    expect(a.share).toBeNull();
    expect(a.setOrgDefault).toEqual(enabled);
  });

  it("disables Set as my default on the effective default even without a personal pick", () => {
    const builtin = view({ id: "builtin:p", is_builtin: true, can_edit: false, is_org_default: true });
    expect(rowActions(builtin, false, "builtin:p").setMyDefault).toEqual(disabled("savedViews.reasons.alreadyMyDefault"));
  });

  it("enables Set as my default on a view that is not the effective default", () => {
    const stale = personal({ is_my_default: true });
    expect(rowActions(stale, false, "builtin:p").setMyDefault).toEqual(enabled);
    expect(rowActions(personal(), false, "p1").setMyDefault).toEqual(disabled("savedViews.reasons.alreadyMyDefault"));
  });
});

describe("myDefaultTarget", () => {
  it("clears the personal pointer when the view is the org default", () => {
    expect(myDefaultTarget(view({ id: "o1", is_org_default: true }))).toBeNull();
  });

  it("points at the view otherwise", () => {
    expect(myDefaultTarget(view({ id: "o1" }))).toBe("o1");
    expect(myDefaultTarget(view({ id: "p1", scope: "user" }))).toBe("p1");
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

  it("explains the built-in Default view, ahead of the read-only reason", () => {
    for (const can_edit of [true, false]) {
      expect(saveDisabledReason(view({ is_builtin: true, can_edit }), true)).toEqual({
        disabled: true,
        reasonKey: "savedViews.saveBuiltin",
      });
    }
  });

  it("is enabled for a dirty, editable view", () => {
    expect(saveDisabledReason(view({}), true)).toEqual({ disabled: false, reasonKey: null });
  });
});

describe("sanitizeAgainstOptions", () => {
  it("keeps any value while the options are still loading", () => {
    expect(sanitizeAgainstOptions("sales", null, "all")).toBe("sales");
    expect(sanitizeAgainstOptions(["a", "gone"], null, [])).toEqual(["a", "gone"]);
  });

  it("drops a single value that is no longer an option", () => {
    expect(sanitizeAgainstOptions("gone", ["sales", "ops"], "all")).toBe("all");
    expect(sanitizeAgainstOptions("sales", ["sales", "ops"], "all")).toBe("sales");
  });

  it("always keeps the empty value", () => {
    expect(sanitizeAgainstOptions("all", [], "all")).toBe("all");
  });

  it("filters arrays to known options", () => {
    expect(sanitizeAgainstOptions(["a", "gone", "b"], ["a", "b"], [])).toEqual(["a", "b"]);
  });
});

describe("optionsSanitizer / sanitizeViewValues", () => {
  const withOptions = (status: readonly string[] | null): ViewDefs<C, F> => ({
    ...defs,
    sanitizeValue: optionsSanitizer<F>({ status }, defs.emptyValues),
  });

  it("leaves filters without an option list alone", () => {
    const sanitize = optionsSanitizer<F>({ status: ["open"] }, defs.emptyValues);
    expect(sanitize("groupBy", "anything")).toBe("anything");
  });

  it("keeps a saved value on first render, then drops it once options load without it", () => {
    const loaded = reconcileView({ v: 1, values: { status: "archived" } }, withOptions(null));
    expect(loaded.values.status).toBe("archived");

    expect(sanitizeViewValues(loaded, withOptions(["archived", "open"]))).toBe(loaded);
    const cleaned = sanitizeViewValues(loaded, withOptions(["open"]));
    expect(cleaned.values.status).toBe("all");
    expect(cleaned.filters).toBe(loaded.filters);
  });

  it("is not dirty when the saved and working values were sanitised against the same options", () => {
    const payload = { v: 1, values: { status: "archived" } };
    const stale = reconcileView(payload, withOptions(null));
    const current = withOptions(["open"]);
    expect(isDirty(reconcileView(payload, current), sanitizeViewValues(stale, current), current)).toBe(false);
  });
});

describe("value readers", () => {
  it("stringValue falls back for non-strings", () => {
    expect(stringValue("x", "all")).toBe("x");
    expect(stringValue(null, "all")).toBe("all");
    expect(stringValue(["x"], "all")).toBe("all");
    expect(stringValue(undefined, "all")).toBe("all");
  });

  it("stringArrayValue wraps strings and empties null", () => {
    expect(stringArrayValue(["a"])).toEqual(["a"]);
    expect(stringArrayValue("a")).toEqual(["a"]);
    expect(stringArrayValue(null)).toEqual([]);
  });

  it("oneOf narrows to the allowed values", () => {
    const allowed = ["none", "catalog"] as const;
    expect(oneOf("catalog", allowed, "none")).toBe("catalog");
    expect(oneOf("bogus", allowed, "none")).toBe("none");
    expect(oneOf(null, allowed, "none")).toBe("none");
  });
});

describe("applyStarOptimistic", () => {
  const list: SavedViewListOut = {
    views: [view({ id: "a", name: "A", starred: false }), view({ id: "b", name: "B", starred: false })],
    effective_default_id: "a",
  };
  it("flips only the targeted view and keeps the rest", () => {
    const next = applyStarOptimistic(list, "b", true);
    expect(next.views.map((v) => v.starred)).toEqual([false, true]);
    expect(next.effective_default_id).toBe("a");
    expect(list.views[1].starred).toBe(false);
  });
  it("re-sorts starred views first via sortViews", () => {
    const next = applyStarOptimistic(list, "b", true);
    expect(sortViews(next.views).org.map((v) => v.id)).toEqual(["b", "a"]);
  });
  it("returns the same list for an unknown view", () => {
    expect(applyStarOptimistic(list, "zzz", true)).toBe(list);
  });
});
