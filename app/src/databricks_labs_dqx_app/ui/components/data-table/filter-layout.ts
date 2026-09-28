import { useCallback, useEffect, useRef, useState } from "react";
import { PointerSensor, useSensor, useSensors, type DragEndEvent } from "@dnd-kit/core";
import { arrayMove } from "@dnd-kit/sortable";
import { persistLayout, readStoredLayout, reconcileLayout } from "./column-layout";
import type { SortableToggleListConfig } from "./EditColumnsDropdown";

/** Per-filter layout metadata for an overview filter bar. */
export interface FilterLayoutDef {
  /** i18n key of the name listed in Edit View > Filters. */
  labelKey: string;
  defaultVisible: boolean;
  /**
   * Pinned filters (e.g. Group by) always lead the filter bar, in
   * *defaultOrder*, set apart by a divider. They can still be toggled but
   * never dragged, and nothing can be dragged above them.
   */
  pinned?: boolean;
}

export interface UseFilterLayoutOptions<K extends string> {
  /** localStorage key the visibility/order payload is persisted under. */
  storageKey: string;
  /** Filter order as shipped in code (reconciled against the stored order). */
  defaultOrder: readonly K[];
  filters: Record<K, FilterLayoutDef>;
  /** Called when a filter is switched off, so the page can reset its value —
   *  a hidden filter must never keep narrowing the rows. */
  onHide?: (key: K) => void;
}

export interface FilterLayout<K extends string> {
  /** Full drag-reorderable order, visible or not. */
  order: K[];
  /** `order` narrowed to the filters that should render, in render order
   *  (pinned filters first). */
  visibleKeys: K[];
  /** Pinned filters are listed first and can't be reordered. */
  isPinned: (key: K) => boolean;
  isVisible: (key: K) => boolean;
  toggle: (key: K) => void;
  handleDragEnd: (event: DragEndEvent) => void;
  sensors: ReturnType<typeof useSensors>;
}

/** Default visibility for every filter in *defaultOrder*. */
export function defaultFilterVisibility<K extends string>(
  defaultOrder: readonly K[],
  filters: Record<K, FilterLayoutDef>,
): Record<K, boolean> {
  return Object.fromEntries(defaultOrder.map((k) => [k, filters[k].defaultVisible])) as Record<K, boolean>;
}

/** True when *key* is a pinned filter. */
export function isPinnedFilter<K extends string>(key: K, filters: Record<K, FilterLayoutDef>): boolean {
  return filters[key]?.pinned === true;
}

/**
 * Puts pinned filters first, in *defaultOrder*, keeping the relative order
 * of everything else. Applied to stored layouts so one saved before a filter
 * was pinned (or edited by hand) still renders the pinned filter first.
 */
export function normalizeFilterOrder<K extends string>(
  order: readonly K[],
  defaultOrder: readonly K[],
  filters: Record<K, FilterLayoutDef>,
): K[] {
  const pinned = defaultOrder.filter((k) => isPinnedFilter(k, filters) && order.includes(k));
  const rest = order.filter((k) => !isPinnedFilter(k, filters));
  return [...pinned, ...rest];
}

/**
 * The order after dragging *activeKey* onto *overKey*. Pinned filters neither
 * move nor accept a drop, so no filter can be dragged above them; a drop that
 * would do either returns *order* unchanged.
 */
export function moveFilter<K extends string>(
  order: readonly K[],
  activeKey: K,
  overKey: K,
  filters: Record<K, FilterLayoutDef>,
): K[] {
  const from = order.indexOf(activeKey);
  const to = order.indexOf(overKey);
  if (
    from === -1 ||
    to === -1 ||
    from === to ||
    isPinnedFilter(activeKey, filters) ||
    isPinnedFilter(overKey, filters)
  ) {
    return [...order];
  }
  return arrayMove([...order], from, to);
}

/** Splits render-ordered *keys* into the pinned lead and the rest. */
export function splitPinnedFilters<K extends string>(
  keys: readonly K[],
  isPinned: (key: K) => boolean,
): { pinned: K[]; rest: K[] } {
  return { pinned: keys.filter(isPinned), rest: keys.filter((k) => !isPinned(k)) };
}

/**
 * Which filter pills an overview shows, and in what order — the Filters view
 * of {@link EditColumnsDropdown}. Persisted to localStorage and reconciled
 * against the shipped filter set exactly like column layouts
 * (see `useColumnLayout`), so adding a filter never resets a user's choices.
 */
export function useFilterLayout<K extends string>({
  storageKey,
  defaultOrder,
  filters,
  onHide,
}: UseFilterLayoutOptions<K>): FilterLayout<K> {
  const [initial] = useState(() => {
    const loaded = reconcileLayout(
      readStoredLayout<K>(storageKey),
      defaultOrder,
      defaultFilterVisibility(defaultOrder, filters),
    );
    return { ...loaded, order: normalizeFilterOrder(loaded.order, defaultOrder, filters) };
  });
  const [visibility, setVisibility] = useState<Record<K, boolean>>(initial.visibility);
  const [order, setOrder] = useState<K[]>(initial.order);
  const onHideRef = useRef(onHide);
  useEffect(() => {
    onHideRef.current = onHide;
  }, [onHide]);

  useEffect(() => {
    persistLayout(storageKey, visibility, order);
  }, [storageKey, visibility, order]);

  const isVisible = useCallback((key: K) => visibility[key] ?? false, [visibility]);
  const isPinned = useCallback((key: K) => isPinnedFilter(key, filters), [filters]);

  const toggle = useCallback(
    (key: K) => {
      const wasVisible = visibility[key] ?? false;
      setVisibility((prev) => ({ ...prev, [key]: !wasVisible }));
      if (wasVisible) onHideRef.current?.(key);
    },
    [visibility],
  );

  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 5 } }));

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event;
      if (!over || active.id === over.id) return;
      setOrder((prev) => moveFilter(prev, active.id as K, over.id as K, filters));
    },
    [filters],
  );

  return {
    order,
    visibleKeys: order.filter((k) => visibility[k]),
    isPinned,
    isVisible,
    toggle,
    handleDragEnd,
    sensors,
  };
}

/** Adapts a {@link FilterLayout} to the `filters` prop of `EditColumnsDropdown`. */
export function filterLayoutMenuConfig<K extends string>(
  layout: FilterLayout<K>,
  labelOf: (key: K) => string,
): SortableToggleListConfig<K> {
  return {
    order: layout.order,
    labelOf,
    toggleableOf: () => true,
    pinnedOf: layout.isPinned,
    isChecked: layout.isVisible,
    onToggle: layout.toggle,
    onDragEnd: layout.handleDragEnd,
    sensors: layout.sensors,
  };
}
