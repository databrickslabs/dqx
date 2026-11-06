import type { SavedViewListOut, SavedViewOut, SavedViewPayload } from "@/lib/api";
import { reconcileLayout, type ColumnLayoutDef } from "./column-layout";
import {
  defaultFilterVisibility,
  normalizeFilterOrder,
  type FilterLayoutDef,
} from "./filter-layout";

export type ViewValue = string | string[] | null;
export type ViewValues = Record<string, ViewValue>;

export interface LayoutState<K extends string> {
  order: K[];
  visibility: Record<K, boolean>;
}

export interface ViewState<C extends string, F extends string> {
  columns: LayoutState<C> | null;
  filters: LayoutState<F>;
  values: ViewValues;
}

export interface ViewDefs<C extends string, F extends string> {
  columns?: { defaultOrder: readonly C[]; columns: Record<C, ColumnLayoutDef> };
  filters: { defaultOrder: readonly F[]; filters: Record<F, FilterLayoutDef> };
  /** Value used when a filter is unset/hidden, per filter key (e.g. "all", [], ""). */
  emptyValues: Record<F, ViewValue>;
  /** Optional: drop values whose options no longer exist. Return the cleaned value. */
  sanitizeValue?: (key: F, value: ViewValue) => ViewValue;
  /** Filters whose value is kept when the filter is hidden (set by other UI, e.g. breakdown clicks). */
  keepValueOnHide?: readonly F[];
  /** Legacy localStorage layout keys; `useSavedViews` removes them on mount (they are not migrated). */
  legacyStorageKeys?: string[];
}

export interface ViewSections {
  mine: SavedViewOut[];
  org: SavedViewOut[];
}

function cloneEmpty(value: ViewValue): ViewValue {
  return Array.isArray(value) ? [...value] : value;
}

function emptyValuesOf<C extends string, F extends string>(defs: ViewDefs<C, F>): ViewValues {
  const out: ViewValues = {};
  for (const key of defs.filters.defaultOrder) out[key] = cloneEmpty(defs.emptyValues[key]);
  return out;
}

/** The view with every column/filter at its shipped default. */
export function builtinViewState<C extends string, F extends string>(defs: ViewDefs<C, F>): ViewState<C, F> {
  const columns = defs.columns
    ? {
        order: [...defs.columns.defaultOrder],
        visibility: Object.fromEntries(
          defs.columns.defaultOrder.map((k) => [k, defs.columns!.columns[k].defaultVisible]),
        ) as Record<C, boolean>,
      }
    : null;
  return {
    columns,
    filters: {
      order: normalizeFilterOrder(defs.filters.defaultOrder, defs.filters.defaultOrder, defs.filters.filters),
      visibility: defaultFilterVisibility(defs.filters.defaultOrder, defs.filters.filters),
    },
    values: emptyValuesOf(defs),
  };
}

function valueMatchesKind(value: unknown, empty: ViewValue): value is ViewValue {
  if (Array.isArray(empty)) return Array.isArray(value) && value.every((v) => typeof v === "string");
  if (typeof empty === "string") return typeof value === "string";
  return value === null || typeof value === "string";
}

/**
 * Reconciles a stored payload against the page's current definitions: unknown keys are
 * dropped, new keys appended with default visibility, pinned filters forced first, and
 * values kept only for known, visible filters of the right type.
 */
export function reconcileView<C extends string, F extends string>(
  payload: SavedViewPayload | null | undefined,
  defs: ViewDefs<C, F>,
): ViewState<C, F> {
  if (!payload) return builtinViewState(defs);

  let columns: LayoutState<C> | null = null;
  if (defs.columns) {
    const { defaultOrder, columns: colDefs } = defs.columns;
    const defaultVisibility = Object.fromEntries(defaultOrder.map((k) => [k, colDefs[k].defaultVisible])) as Record<
      C,
      boolean
    >;
    const stored = payload.columns ?? {};
    columns = reconcileLayout<C>(
      { order: stored.order as C[] | undefined, visibility: stored.visibility as Partial<Record<C, boolean>> },
      defaultOrder,
      defaultVisibility,
    );
  }

  const { defaultOrder: filterOrder, filters: filterDefs } = defs.filters;
  const loaded = reconcileLayout<F>(
    {
      order: payload.filters?.order as F[] | undefined,
      visibility: payload.filters?.visibility as Partial<Record<F, boolean>>,
    },
    filterOrder,
    defaultFilterVisibility(filterOrder, filterDefs),
  );
  const filters: LayoutState<F> = {
    visibility: loaded.visibility,
    order: normalizeFilterOrder(loaded.order, filterOrder, filterDefs),
  };

  const stored = payload.values ?? {};
  const values: ViewValues = {};
  for (const key of filterOrder) {
    const empty = defs.emptyValues[key];
    const keep = filters.visibility[key] || (defs.keepValueOnHide ?? []).includes(key);
    const raw = Object.prototype.hasOwnProperty.call(stored, key) ? stored[key] : undefined;
    if (keep && raw !== undefined && valueMatchesKind(raw, empty)) {
      const cleaned = defs.sanitizeValue ? defs.sanitizeValue(key, raw) : raw;
      values[key] = Array.isArray(cleaned) ? [...cleaned] : cleaned;
    } else {
      values[key] = cloneEmpty(empty);
    }
  }
  return { columns, filters, values };
}

function sameValue(a: ViewValue, b: ViewValue): boolean {
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && [...a].sort().every((v, i) => v === [...b].sort()[i]);
  }
  return a === b;
}

/** Serialises state for storage. With *defs*, values equal to their empty value are omitted. */
export function toPayload<C extends string, F extends string>(
  state: ViewState<C, F>,
  defs?: ViewDefs<C, F>,
): SavedViewPayload {
  const values: Record<string, ViewValue> = {};
  for (const [key, value] of Object.entries(state.values)) {
    const empty = defs?.emptyValues[key as F];
    if (empty !== undefined && sameValue(value, empty)) continue;
    values[key] = Array.isArray(value) ? [...value] : value;
  }
  const payload: SavedViewPayload = {
    v: 1,
    filters: { order: [...state.filters.order], visibility: { ...state.filters.visibility } },
    values,
  };
  if (state.columns) {
    payload.columns = { order: [...state.columns.order], visibility: { ...state.columns.visibility } };
  }
  return payload;
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function canonicalPayload<C extends string, F extends string>(
  state: ViewState<C, F>,
  defs: ViewDefs<C, F>,
): string {
  const payload = toPayload(state, defs);
  const values = Object.fromEntries(
    Object.entries(payload.values ?? {}).map(([k, v]) => [k, Array.isArray(v) ? [...v].sort() : v]),
  );
  return stableStringify({ ...payload, values });
}

/** True when *working* differs from *saved* (array filter values compare order-insensitively). */
export function isDirty<C extends string, F extends string>(
  saved: ViewState<C, F>,
  working: ViewState<C, F>,
  defs: ViewDefs<C, F>,
): boolean {
  return canonicalPayload(saved, defs) !== canonicalPayload(working, defs);
}

function byStarredThenName(a: SavedViewOut, b: SavedViewOut): number {
  if (a.starred !== b.starred) return a.starred ? -1 : 1;
  return a.name.localeCompare(b.name, undefined, { sensitivity: "base" });
}

/** Splits views into "mine" (user scope) and "org" sections, starred first then by name. */
export function sortViews(views: SavedViewOut[]): ViewSections {
  return {
    mine: views.filter((v) => v.scope === "user").sort(byStarredThenName),
    org: views.filter((v) => v.scope === "org").sort(byStarredThenName),
  };
}

/** requestedId if present, else the effective default, else the builtin, else the first view. */
export function resolveActiveView(list: SavedViewListOut, requestedId?: string | null): SavedViewOut {
  const byId = (id: string | null | undefined): SavedViewOut | undefined =>
    id ? list.views.find((v) => v.id === id) : undefined;
  return byId(requestedId) ?? byId(list.effective_default_id) ?? list.views.find((v) => v.is_builtin) ?? list.views[0];
}

/**
 * Shows or hides filter *key*. Hiding resets its value to its empty value unless the
 * filter is in *keepValueOnHide* — the same rule {@link reconcileView} applies, so a
 * hidden filter never narrows the rows and {@link isDirty} stays consistent.
 */
export function applyFilterToggle<C extends string, F extends string>(
  state: ViewState<C, F>,
  key: F,
  defs: ViewDefs<C, F>,
): ViewState<C, F> {
  const wasVisible = state.filters.visibility[key] ?? false;
  const filters = { ...state.filters, visibility: { ...state.filters.visibility, [key]: !wasVisible } };
  const keep = (defs.keepValueOnHide ?? []).includes(key);
  const values = wasVisible && !keep ? { ...state.values, [key]: cloneEmpty(defs.emptyValues[key]) } : state.values;
  return { ...state, filters, values };
}

export const MAX_VIEW_NAME_LENGTH = 80;

export type ViewNameError = "required" | "tooLong" | "duplicate";

/** Collapses whitespace and trims, matching the server's name cleaning. */
export function cleanViewName(raw: string): string {
  return raw.replace(/\s+/g, " ").trim();
}

/**
 * Why *raw* can't be used as a view name in *scope*, or null when it can. Names are
 * unique per page within a scope (case-insensitive); *excludeId* skips the view being renamed.
 */
export function viewNameError(
  raw: string,
  scope: SavedViewOut["scope"],
  views: readonly SavedViewOut[],
  excludeId?: string,
): ViewNameError | null {
  const name = cleanViewName(raw);
  if (name.length === 0) return "required";
  if (name.length > MAX_VIEW_NAME_LENGTH) return "tooLong";
  const lower = name.toLowerCase();
  const taken = views.some(
    (v) => v.scope === scope && v.id !== excludeId && cleanViewName(v.name).toLowerCase() === lower,
  );
  return taken ? "duplicate" : null;
}

/** Whether a menu action can be used and, if not, the i18n key of the reason shown in its tooltip. */
export interface ActionState {
  enabled: boolean;
  reasonKey: string | null;
}

/** The actions in a saved view's ⋯ menu. Rename and Delete are null (not shown) on the built-in view. */
export interface RowActions {
  rename: ActionState | null;
  delete: ActionState | null;
  setMyDefault: ActionState;
  setOrgDefault: ActionState;
}

const ENABLED: ActionState = { enabled: true, reasonKey: null };
const disabledBecause = (reasonKey: string): ActionState => ({ enabled: false, reasonKey });

/** Which row actions *view* allows for the current user. */
export function rowActions(view: SavedViewOut, isAdmin: boolean): RowActions {
  const edit = view.can_edit ? ENABLED : disabledBecause("savedViews.reasons.orgViewAdminsOnly");
  let setOrgDefault: ActionState = ENABLED;
  if (view.is_org_default) setOrgDefault = disabledBecause("savedViews.reasons.alreadyOrgDefault");
  else if (!isAdmin) setOrgDefault = disabledBecause("savedViews.reasons.orgDefaultAdminsOnly");
  else if (view.scope !== "org" || !view.can_set_org_default)
    setOrgDefault = disabledBecause("savedViews.reasons.orgDefaultOrgViewsOnly");
  return {
    rename: view.is_builtin ? null : edit,
    delete: view.is_builtin ? null : edit,
    setMyDefault: view.is_my_default ? disabledBecause("savedViews.reasons.alreadyMyDefault") : ENABLED,
    setOrgDefault,
  };
}

/** Whether Save is disabled and, when the active view is read-only, the i18n key of the reason. */
export function saveDisabledReason(
  activeView: SavedViewOut | null,
  dirty: boolean,
): { disabled: boolean; reasonKey: string | null } {
  if (activeView && !activeView.can_edit) return { disabled: true, reasonKey: "savedViews.saveReadOnly" };
  return { disabled: !activeView || !dirty, reasonKey: null };
}
