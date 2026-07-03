import { useCallback, useEffect, useRef, useState } from "react";
import {
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import { arrayMove } from "@dnd-kit/sortable";

/** Per-column layout metadata needed to compute defaults + drive rendering.
 *  Column-specific rendering (labels, cell content) stays with the caller —
 *  this module only owns visibility/order/width bookkeeping. */
export interface ColumnLayoutDef {
  toggleable: boolean;
  defaultVisible: boolean;
  defaultWidth: number;
  /** Icon-only columns stay locked to their natural width — no resize handle. */
  resizable?: boolean;
}

export interface UseColumnLayoutOptions<K extends string> {
  /** localStorage key the visibility/order payload is persisted under. */
  storageKey: string;
  /** Full column order as shipped in code — the source of truth for
   *  reconciling against whatever a user has persisted from an older
   *  version of the column set. */
  defaultOrder: readonly K[];
  columns: Record<K, ColumnLayoutDef>;
}

export interface UseColumnLayoutResult<K extends string> {
  colOrder: K[];
  colVisibility: Record<K, boolean>;
  colWidths: Record<K, number>;
  /** `colOrder` filtered down to the columns that should actually render,
   *  respecting each column's toggle state (non-toggleable columns are
   *  always visible). */
  visibleKeys: K[];
  toggleColumn: (id: K) => void;
  handleDragEnd: (event: DragEndEvent) => void;
  sensors: ReturnType<typeof useSensors>;
  onResizeStart: (key: K, e: React.MouseEvent) => void;
}

type StoredLayout<K extends string> = {
  visibility: Partial<Record<K, boolean>>;
  order: K[];
};

type LoadedLayout<K extends string> = {
  visibility: Record<K, boolean>;
  order: K[];
};

function defaultVisibilityOf<K extends string>(
  defaultOrder: readonly K[],
  columns: Record<K, ColumnLayoutDef>,
): Record<K, boolean> {
  return Object.fromEntries(defaultOrder.map((k) => [k, columns[k].defaultVisible])) as Record<K, boolean>;
}

function defaultWidthsOf<K extends string>(
  defaultOrder: readonly K[],
  columns: Record<K, ColumnLayoutDef>,
): Record<K, number> {
  return Object.fromEntries(defaultOrder.map((k) => [k, columns[k].defaultWidth])) as Record<K, number>;
}

/**
 * Loads a persisted column layout, reconciled against the current set of
 * column keys shipped in code:
 *   - Columns in storage no longer known in code -> dropped.
 *   - Columns known in code but missing from storage (newly added) ->
 *     appended in `defaultOrder` position, with the current defaultVisible.
 *   - Everything the user explicitly set keeps its value.
 * Ported from dqlake's `dqlake.bindings.layout` / `dqlake.rules.layout`
 * reconciliation strategy so new columns don't silently reset user prefs.
 */
function loadLayout<K extends string>(
  storageKey: string,
  defaultOrder: readonly K[],
  columns: Record<K, ColumnLayoutDef>,
): LoadedLayout<K> {
  let stored: Partial<StoredLayout<K>> = {};
  try {
    const raw = localStorage.getItem(storageKey);
    if (raw) stored = JSON.parse(raw) as Partial<StoredLayout<K>>;
  } catch {
    // localStorage unavailable or malformed payload — fall back to defaults
  }

  const storedOrder = Array.isArray(stored.order) ? stored.order : [];
  const known = storedOrder.filter((k): k is K => k in columns);
  const missing = defaultOrder.filter((k) => !known.includes(k));
  const order = [...known, ...missing];

  const storedVis = (stored.visibility ?? {}) as Partial<Record<K, boolean>>;
  const visibility = { ...defaultVisibilityOf(defaultOrder, columns) } as Record<K, boolean>;
  for (const key of order) {
    if (Object.prototype.hasOwnProperty.call(storedVis, key)) {
      const v = storedVis[key];
      if (typeof v === "boolean") visibility[key] = v;
    }
  }

  return { visibility, order };
}

function persistLayout<K extends string>(storageKey: string, visibility: Record<K, boolean>, order: K[]): void {
  try {
    localStorage.setItem(storageKey, JSON.stringify({ visibility, order }));
  } catch {
    // ignore
  }
}

/**
 * Shared column visibility/order/width/resize state for tables with a
 * drag-reorderable, toggleable "Edit Columns" dropdown. Ported from
 * dqlake's `BindingsTable` and used by both the Rules Registry list
 * (`RulesTable`) and the Monitored Tables list (`MonitoredTablesTable`) so
 * the two don't drift in behavior.
 */
export function useColumnLayout<K extends string>({
  storageKey,
  defaultOrder,
  columns,
}: UseColumnLayoutOptions<K>): UseColumnLayoutResult<K> {
  const initialLayout = useState(() => loadLayout(storageKey, defaultOrder, columns))[0];
  const [colVisibility, setColVisibility] = useState<Record<K, boolean>>(() => initialLayout.visibility);
  const [colOrder, setColOrder] = useState<K[]>(() => initialLayout.order);
  const [colWidths, setColWidths] = useState<Record<K, number>>(() => defaultWidthsOf(defaultOrder, columns));

  useEffect(() => {
    persistLayout(storageKey, colVisibility, colOrder);
  }, [storageKey, colVisibility, colOrder]);

  const toggleColumn = useCallback((id: K) => {
    setColVisibility((prev) => ({ ...prev, [id]: !prev[id] }));
  }, []);

  const show = useCallback((id: K) => colVisibility[id] ?? false, [colVisibility]);

  const visibleKeys = colOrder.filter((k) => (columns[k].toggleable ? show(k) : true));

  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 5 } }));

  const handleDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event;
    if (over && active.id !== over.id) {
      setColOrder((prev) => {
        const oldIdx = prev.indexOf(active.id as K);
        const newIdx = prev.indexOf(over.id as K);
        return arrayMove(prev, oldIdx, newIdx);
      });
    }
  }, []);

  // -- Column resizing (session-only; not persisted, matching dqlake) -----
  const resizingRef = useRef<{ key: K; startX: number; startW: number } | null>(null);

  const onResizeMove = useCallback((e: MouseEvent) => {
    const rCtx = resizingRef.current;
    if (!rCtx) return;
    const next = Math.max(50, rCtx.startW + (e.clientX - rCtx.startX));
    setColWidths((prev) => ({ ...prev, [rCtx.key]: next }));
  }, []);

  const onResizeEnd = useCallback(() => {
    resizingRef.current = null;
    document.removeEventListener("mousemove", onResizeMove);
    document.removeEventListener("mouseup", onResizeEnd);
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  }, [onResizeMove]);

  const onResizeStart = useCallback(
    (key: K, e: React.MouseEvent) => {
      e.preventDefault();
      e.stopPropagation();
      resizingRef.current = { key, startX: e.clientX, startW: colWidths[key] ?? columns[key].defaultWidth };
      document.addEventListener("mousemove", onResizeMove);
      document.addEventListener("mouseup", onResizeEnd);
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
    },
    [colWidths, columns, onResizeMove, onResizeEnd],
  );

  useEffect(() => {
    return () => {
      document.removeEventListener("mousemove", onResizeMove);
      document.removeEventListener("mouseup", onResizeEnd);
    };
  }, [onResizeMove, onResizeEnd]);

  return {
    colOrder,
    colVisibility,
    colWidths,
    visibleKeys,
    toggleColumn,
    handleDragEnd,
    sensors,
    onResizeStart,
  };
}
