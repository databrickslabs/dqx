import { useCallback, useEffect, useRef, useState } from "react";
import { PointerSensor, useSensor, useSensors, type DragEndEvent } from "@dnd-kit/core";
import { arrayMove } from "@dnd-kit/sortable";
import { persistLayout, readStoredLayout, reconcileLayout } from "./column-layout";
import type { SortableToggleListConfig } from "./EditColumnsDropdown";

/** Per-filter layout metadata for an overview filter bar. */
export interface FilterLayoutDef {
  /** i18n key of the name listed in Edit Columns > Filters. */
  labelKey: string;
  defaultVisible: boolean;
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
  /** `order` narrowed to the filters that should render, in render order. */
  visibleKeys: K[];
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
  const [initial] = useState(() =>
    reconcileLayout(readStoredLayout<K>(storageKey), defaultOrder, defaultFilterVisibility(defaultOrder, filters)),
  );
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

  const toggle = useCallback(
    (key: K) => {
      const wasVisible = visibility[key] ?? false;
      setVisibility((prev) => ({ ...prev, [key]: !wasVisible }));
      if (wasVisible) onHideRef.current?.(key);
    },
    [visibility],
  );

  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 5 } }));

  const handleDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    setOrder((prev) => arrayMove(prev, prev.indexOf(active.id as K), prev.indexOf(over.id as K)));
  }, []);

  return {
    order,
    visibleKeys: order.filter((k) => visibility[k]),
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
    isChecked: layout.isVisible,
    onToggle: layout.toggle,
    onDragEnd: layout.handleDragEnd,
    sensors: layout.sensors,
  };
}
