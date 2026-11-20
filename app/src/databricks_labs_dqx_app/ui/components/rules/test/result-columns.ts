import { arrayMove } from "@dnd-kit/sortable";

// Column bookkeeping for the Rule Test result grid: which columns show, and in
// what order. Columns are identified by their position in the result, so menu
// entries and React keys stay unique and each cell is read from its own index.

export interface ResultColumn {
  name: string;
  mapped: boolean; // referenced/mapped by the rule -> bold header
}

/** A result column with its position in each row and a stable id for menus and React keys. */
export interface IndexedColumn extends ResultColumn {
  id: string;
  index: number;
}

export function indexColumns(columns: readonly ResultColumn[]): IndexedColumn[] {
  return columns.map((column, index) => ({ ...column, id: String(index), index }));
}

/** The columns to render: `order` minus the hidden ids. Unknown ids are skipped. */
export function visibleColumns(
  columns: readonly IndexedColumn[],
  order: readonly string[],
  hidden: ReadonlySet<string>,
): IndexedColumn[] {
  const byId = new Map(columns.map((column) => [column.id, column]));
  return order.flatMap((id) => {
    const column = byId.get(id);
    return column && !hidden.has(id) ? [column] : [];
  });
}

/** How many of `order` are not hidden. */
export function visibleCount(order: readonly string[], hidden: ReadonlySet<string>): number {
  return order.reduce((count, id) => (hidden.has(id) ? count : count + 1), 0);
}

/** Whether `id` may be toggled: the last visible column can't be hidden, or the grid would be empty. */
export function canToggleColumn(id: string, hidden: ReadonlySet<string>, shownCount: number): boolean {
  return hidden.has(id) || shownCount > 1;
}

/** Hides or shows `id`; hiding the last visible column is a no-op. */
export function toggleColumn(id: string, order: readonly string[], hidden: ReadonlySet<string>): Set<string> {
  const next = new Set(hidden);
  if (!canToggleColumn(id, hidden, visibleCount(order, hidden))) return next;
  if (next.has(id)) next.delete(id);
  else next.add(id);
  return next;
}

/** Moves `id` to where `overId` sits (a drag-and-drop reorder). Unknown ids leave the order unchanged. */
export function moveColumn(order: readonly string[], id: string, overId: string): string[] {
  const from = order.indexOf(id);
  const to = order.indexOf(overId);
  if (from < 0 || to < 0 || from === to) return [...order];
  return arrayMove([...order], from, to);
}

/** Identifies a result's column list, so per-result choices (hidden columns, order) reset when it changes. */
export function columnsSignature(columns: readonly ResultColumn[]): string {
  return JSON.stringify(columns.map((column) => column.name));
}
