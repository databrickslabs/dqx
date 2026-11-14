import { Fragment, type ReactNode } from "react";
import { Separator } from "@/components/ui/separator";
import { splitPinnedFilters } from "./filter-layout";

/**
 * The row above an overview table: filter pills on the left, wrapping onto
 * further rows as needed, and the Edit View control pinned to the right of
 * the first row, so a long filter bar never pushes it out of line.
 */
export function FilterToolbar({ filters, editColumns }: { filters?: ReactNode; editColumns: ReactNode }) {
  return (
    <div className="flex items-start gap-2">
      <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">{filters}</div>
      {editColumns}
    </div>
  );
}

export interface FilterPillsProps<K extends string> {
  /** Filters to render, in order — typically `useFilterLayout().visibleKeys`. */
  keys: readonly K[];
  /** Typically `useFilterLayout().isPinned`. */
  isPinned: (key: K) => boolean;
  renderPill: (key: K) => ReactNode;
}

/**
 * Renders filter pills in layout order, with pinned filters (Group by) first
 * and a vertical divider between them and the rest.
 */
export function FilterPills<K extends string>({ keys, isPinned, renderPill }: FilterPillsProps<K>) {
  const { pinned, rest } = splitPinnedFilters(keys, isPinned);
  return (
    <>
      {pinned.map((key) => (
        <Fragment key={key}>{renderPill(key)}</Fragment>
      ))}
      {/* Separator's `data-[orientation=vertical]:h-full` outranks a bare `h-6`
          (attribute variant specificity) and resolves to 0 in an auto-height
          row, hence `!h-6` — as in the collection tables picker. */}
      {pinned.length > 0 && rest.length > 0 && <Separator orientation="vertical" className="!h-6" />}
      {rest.map((key) => (
        <Fragment key={key}>{renderPill(key)}</Fragment>
      ))}
    </>
  );
}
