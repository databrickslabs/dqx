import { useCallback, useEffect, useMemo, useState } from "react";
import {
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
  type SensorDescriptor,
  type SensorOptions,
} from "@dnd-kit/core";
import { arrayMove } from "@dnd-kit/sortable";
import { useQueryClient } from "@tanstack/react-query";
import { isAxiosError } from "axios";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import {
  getListSavedViewsQueryKey,
  useCreateSavedView,
  useCurrentUserRole,
  useDeleteSavedView,
  useListSavedViews,
  useRenameSavedView,
  useSetMySavedViewDefault,
  useSetOrgSavedViewDefault,
  useStarSavedView,
  useUnstarSavedView,
  useUpdateSavedView,
  type SavedViewListOut,
  type SavedViewOut,
  type SavedViewPage,
  type UserRoleOut,
} from "@/lib/api";
import selector from "@/lib/selector";
import { useColumnResize, type ColumnLayoutDef } from "./column-layout";
import { isPinnedFilter, moveFilter, type FilterLayout } from "./filter-layout";
import {
  applyFilterToggle,
  builtinViewState,
  cleanViewName,
  isDirty,
  reconcileView,
  resolveActiveView,
  sanitizeViewValues,
  sortViews,
  toPayload,
  type ViewDefs,
  type ViewSections,
  type ViewState,
  type ViewValue,
  type ViewValues,
} from "./saved-views";

/** Input of "Save as new view". */
export interface SaveAsNewInput {
  name: string;
  scope: "user" | "org";
  makeMyDefault: boolean;
  makeOrgDefault: boolean;
}

/** Column layout surface — the same shape `useColumnLayout` returns, so pages wire it unchanged. */
export interface SavedViewsColumnLayout<C extends string> {
  colOrder: C[];
  colVisibility: Record<C, boolean>;
  visibleKeys: C[];
  toggleColumn: (key: C) => void;
  handleDragEnd: (event: DragEndEvent) => void;
  sensors: SensorDescriptor<SensorOptions>[];
  colWidths: Record<C, number>;
  onResizeStart: (key: C, e: React.MouseEvent) => void;
}

/**
 * Everything the Saved Views page of the Edit View menu needs. Pass it as the
 * *savedViews* prop of `EditColumnsDropdown`.
 *
 * Every action rejects on failure. Failures other than a duplicate name (see
 * {@link isDuplicateViewNameError}) are already reported with a toast.
 */
export interface SavedViewsMenuConfig {
  loading: boolean;
  activeView: SavedViewOut | null;
  dirty: boolean;
  sections: ViewSections;
  effectiveDefaultId: string | null;
  isAdmin: boolean;
  switchTo: (id: string) => void;
  reset: () => void;
  save: () => Promise<void>;
  saveAsNew: (input: SaveAsNewInput) => Promise<void>;
  rename: (id: string, name: string) => Promise<void>;
  remove: (id: string) => Promise<void>;
  setStar: (id: string, starred: boolean) => Promise<void>;
  setMyDefault: (id: string | null) => Promise<void>;
  setOrgDefault: (id: string) => Promise<void>;
}

export interface UseSavedViewsResult<C extends string, F extends string> extends SavedViewsMenuConfig {
  /** Working state: the active view plus any unsaved edits. */
  working: ViewState<C, F>;
  /** Null when the page has no column layout (Global Results). */
  columnLayout: SavedViewsColumnLayout<C> | null;
  filterLayout: FilterLayout<F>;
  values: ViewValues;
  setValue: (key: F, value: ViewValue) => void;
  /** Ready-made *savedViews* prop for `EditColumnsDropdown`. */
  menuConfig: SavedViewsMenuConfig;
}

/** True when *error* is the server's 409 for a name already used in the view's scope. */
export function isDuplicateViewNameError(error: unknown): boolean {
  return isAxiosError(error) && error.response?.status === 409;
}

const EMPTY_SECTIONS: ViewSections = { mine: [], org: [] };
const NO_COLUMNS: { defaultOrder: readonly never[]; columns: Record<never, ColumnLayoutDef> } = {
  defaultOrder: [],
  columns: {},
};

/**
 * Saved views for an overview page: loads the page's views, resolves the one to
 * show first (your default, then the org default, then Default) and owns the
 * working column layout, filter layout and filter values. Unsaved edits live in
 * memory only. Column widths are session-only and never saved.
 *
 * *defs* must keep a stable identity: a module-level constant, or memoised on
 * the option lists its *sanitizeValue* checks against. Whenever *defs* changes
 * (e.g. a filter's options finish loading) the working values are re-sanitised.
 * While the list loads, the working state is the built-in layout so the page
 * renders straight away.
 */
export function useSavedViews<C extends string, F extends string>(
  pageKey: SavedViewPage,
  defs: ViewDefs<C, F>,
): UseSavedViewsResult<C, F> {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const queryKey = useMemo(() => getListSavedViewsQueryKey({ page: pageKey }), [pageKey]);

  const listQuery = useListSavedViews({ page: pageKey }, selector<SavedViewListOut>());
  const list = listQuery.data;
  const roleQuery = useCurrentUserRole({ query: { ...selector<UserRoleOut>().query, staleTime: Infinity } });
  const isAdmin = roleQuery.data?.role === "admin";

  const [activeId, setActiveId] = useState<string | null>(null);
  const [working, setWorking] = useState<ViewState<C, F>>(() => builtinViewState(defs));
  const [sanitizedFor, setSanitizedFor] = useState(defs);

  // New defs usually mean new filter options: drop values whose option is gone.
  // Adjusted during render (not in an effect) so a stale value never paints.
  if (sanitizedFor !== defs) {
    setSanitizedFor(defs);
    setWorking((prev) => sanitizeViewValues(prev, defs));
  }

  // On first load — or when the active view disappears (deleted here or elsewhere) —
  // adopt the resolved view during render, so the page never paints a stale layout.
  if (list && list.views.length > 0 && (activeId === null || !list.views.some((v) => v.id === activeId))) {
    const first = resolveActiveView(list);
    setActiveId(first.id);
    setWorking(reconcileView(first.payload, defs));
  }

  useEffect(() => {
    for (const key of defs.legacyStorageKeys ?? []) {
      try {
        localStorage.removeItem(key);
      } catch {
        // localStorage unavailable — nothing to clean up
      }
    }
  }, [defs.legacyStorageKeys]);

  const activeView = useMemo(
    () => (list && activeId ? (list.views.find((v) => v.id === activeId) ?? null) : null),
    [list, activeId],
  );
  const saved = useMemo(
    () => (activeView ? reconcileView(activeView.payload, defs) : builtinViewState(defs)),
    [activeView, defs],
  );
  const dirty = activeView !== null && isDirty(saved, working, defs);
  const sections = useMemo(() => (list ? sortViews(list.views) : EMPTY_SECTIONS), [list]);

  // -- mutations ----------------------------------------------------------
  const { mutateAsync: createView } = useCreateSavedView();
  const { mutateAsync: updateView } = useUpdateSavedView();
  const { mutateAsync: renameView } = useRenameSavedView();
  const { mutateAsync: deleteView } = useDeleteSavedView();
  const { mutateAsync: starView } = useStarSavedView();
  const { mutateAsync: unstarView } = useUnstarSavedView();
  const { mutateAsync: setMyDefaultView } = useSetMySavedViewDefault();
  const { mutateAsync: setOrgDefaultView } = useSetOrgSavedViewDefault();

  const refetchList = useCallback(
    () => queryClient.invalidateQueries({ queryKey }),
    [queryClient, queryKey],
  );

  /**
   * Runs *action*, refreshes the list and reports failures (except duplicate names) with a toast.
   * The list is refreshed on failure too: a 404 means the view was deleted elsewhere, and the
   * fresh list lets adoption fall back to the effective default instead of a ghost view.
   */
  const run = useCallback(
    async <T,>(action: () => Promise<T>, failureKey: string): Promise<T> => {
      let result: T;
      try {
        result = await action();
      } catch (error) {
        if (!isDuplicateViewNameError(error)) {
          toast.error(t(failureKey));
          await refetchList();
        }
        throw error;
      }
      await refetchList();
      return result;
    },
    [refetchList, t],
  );

  const switchTo = useCallback(
    (id: string) => {
      const target = list?.views.find((v) => v.id === id);
      if (!target) return;
      setActiveId(target.id);
      setWorking(reconcileView(target.payload, defs));
    },
    [list, defs],
  );

  const reset = useCallback(() => setWorking(saved), [saved]);

  const save = useCallback(async () => {
    if (!activeView) return;
    await run(
      () => updateView({ viewId: activeView.id, data: { payload: toPayload(working, defs) } }),
      "savedViews.errors.save",
    );
  }, [activeView, working, defs, run, updateView]);

  const saveAsNew = useCallback(
    async (input: SaveAsNewInput) => {
      const created = await run(
        () =>
          createView({
            data: {
              page: pageKey,
              name: cleanViewName(input.name),
              scope: input.scope,
              payload: toPayload(working, defs),
              make_my_default: input.makeMyDefault,
              make_org_default: input.scope === "org" && input.makeOrgDefault,
            },
          }),
        "savedViews.errors.save",
      );
      // Keep the working state: it is now exactly what the new view holds.
      setActiveId(created.data.id);
    },
    [pageKey, working, defs, run, createView],
  );

  const rename = useCallback(
    async (id: string, name: string) => {
      await run(
        () => renameView({ viewId: id, data: { name: cleanViewName(name) } }),
        "savedViews.errors.rename",
      );
    },
    [run, renameView],
  );

  // Deleting the active view falls back to the resolved default on the next render.
  const remove = useCallback(
    async (id: string) => {
      await run(() => deleteView({ viewId: id }), "savedViews.errors.delete");
    },
    [run, deleteView],
  );

  const setStar = useCallback(
    async (id: string, starred: boolean) => {
      await run(
        () => (starred ? starView : unstarView)({ viewId: id }),
        "savedViews.errors.star",
      );
    },
    [run, starView, unstarView],
  );

  const setMyDefault = useCallback(
    async (id: string | null) => {
      await run(
        () => setMyDefaultView({ data: { page: pageKey, view_id: id } }),
        "savedViews.errors.default",
      );
    },
    [run, setMyDefaultView, pageKey],
  );

  const setOrgDefault = useCallback(
    async (id: string) => {
      await run(
        () => setOrgDefaultView({ data: { page: pageKey, view_id: id } }),
        "savedViews.errors.default",
      );
    },
    [run, setOrgDefaultView, pageKey],
  );

  // -- layout surface -----------------------------------------------------
  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 5 } }));
  const columnDefs = defs.columns ?? NO_COLUMNS;
  const { colWidths, onResizeStart } = useColumnResize<C>(
    columnDefs.defaultOrder as readonly C[],
    columnDefs.columns as Record<C, ColumnLayoutDef>,
  );

  const toggleColumn = useCallback((key: C) => {
    setWorking((prev) =>
      prev.columns
        ? {
            ...prev,
            columns: {
              ...prev.columns,
              visibility: { ...prev.columns.visibility, [key]: !prev.columns.visibility[key] },
            },
          }
        : prev,
    );
  }, []);

  const handleColumnDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    setWorking((prev) => {
      if (!prev.columns) return prev;
      const order = prev.columns.order;
      const from = order.indexOf(active.id as C);
      const to = order.indexOf(over.id as C);
      if (from === -1 || to === -1) return prev;
      return { ...prev, columns: { ...prev.columns, order: arrayMove(order, from, to) } };
    });
  }, []);

  const columnLayout = useMemo((): SavedViewsColumnLayout<C> | null => {
    const columns = working.columns;
    if (!columns || !defs.columns) return null;
    const colDefs = defs.columns.columns;
    return {
      colOrder: columns.order,
      colVisibility: columns.visibility,
      visibleKeys: columns.order.filter((k) => !colDefs[k].toggleable || columns.visibility[k]),
      toggleColumn,
      handleDragEnd: handleColumnDragEnd,
      sensors,
      colWidths,
      onResizeStart,
    };
  }, [working.columns, defs.columns, toggleColumn, handleColumnDragEnd, sensors, colWidths, onResizeStart]);

  const toggleFilter = useCallback((key: F) => setWorking((prev) => applyFilterToggle(prev, key, defs)), [defs]);

  const handleFilterDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event;
      if (!over || active.id === over.id) return;
      setWorking((prev) => ({
        ...prev,
        filters: {
          ...prev.filters,
          order: moveFilter(prev.filters.order, active.id as F, over.id as F, defs.filters.filters),
        },
      }));
    },
    [defs],
  );

  const filterLayout = useMemo((): FilterLayout<F> => {
    const { order, visibility } = working.filters;
    return {
      order,
      visibleKeys: order.filter((k) => visibility[k]),
      isPinned: (key) => isPinnedFilter(key, defs.filters.filters),
      isVisible: (key) => visibility[key] ?? false,
      toggle: toggleFilter,
      handleDragEnd: handleFilterDragEnd,
      sensors,
    };
  }, [working.filters, defs, toggleFilter, handleFilterDragEnd, sensors]);

  const setValue = useCallback((key: F, value: ViewValue) => {
    setWorking((prev) => ({ ...prev, values: { ...prev.values, [key]: value } }));
  }, []);

  const loading = listQuery.isPending;
  const effectiveDefaultId = list?.effective_default_id ?? null;

  const menuConfig = useMemo(
    (): SavedViewsMenuConfig => ({
      loading,
      activeView,
      dirty,
      sections,
      effectiveDefaultId,
      isAdmin,
      switchTo,
      reset,
      save,
      saveAsNew,
      rename,
      remove,
      setStar,
      setMyDefault,
      setOrgDefault,
    }),
    [
      loading,
      activeView,
      dirty,
      sections,
      effectiveDefaultId,
      isAdmin,
      switchTo,
      reset,
      save,
      saveAsNew,
      rename,
      remove,
      setStar,
      setMyDefault,
      setOrgDefault,
    ],
  );

  return {
    ...menuConfig,
    working,
    columnLayout,
    filterLayout,
    values: working.values,
    setValue,
    menuConfig,
  };
}
