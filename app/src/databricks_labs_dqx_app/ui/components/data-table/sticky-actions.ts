import type { ColumnLayoutDef } from "./column-layout";

/**
 * Shared "frozen" (sticky-right) treatment for the pinned Actions column —
 * used by both RulesTable and MonitoredTablesTable. Kept in one place so the
 * visual contract can't drift between the two tables (P21 items 7/22): the
 * column stays pinned under horizontal scroll, is fully opaque so scrolled
 * columns can't show through it, and its hover fade is synced with the
 * row's own hover fade instead of snapping instantly.
 *
 * Consumers must also put `group` on the `<TableRow>` so `group-hover:`
 * below tracks the row's hover state.
 */

/** Header cell: pinned, with a solid (non-translucent) background — a
 *  `/50` alpha modifier here would let horizontally-scrolled columns show
 *  through the "frozen" header (item 22b). */
export const STICKY_ACTIONS_HEAD_CLASS = "sticky right-0 z-10 bg-muted border-l";

/** Body cell: solid at rest (`bg-background`), and — critically —
 *  `transition-colors` on the cell itself, not just the parent `<tr>`, so
 *  the hover fade to `bg-muted` animates in lockstep with the row's own
 *  `hover:bg-muted/50` fade instead of lagging behind it (item 22a). The
 *  hover target is a fully opaque token rather than `bg-muted/50` (item
 *  22b), for the same show-through reason as the header cell above. */
export const STICKY_ACTIONS_CELL_CLASS =
  "sticky right-0 z-10 bg-background border-l transition-colors group-hover:bg-muted";

/**
 * Shared width (px) for the pinned Actions column, kept in one place so all
 * three overview tables (RulesTable, MonitoredTablesTable, DataProductsTable)
 * stay consistent (item B2-43). The widest row state shows 5 ghost icon
 * buttons (Run + Approve + Reject + ViewChanges + Revert) on Monitored Tables
 * and Table Spaces pending-approval rows with runnables — Export moved to the
 * selection action bar and Delete is hidden while pending:
 * 5 × 28px (`h-7 w-7`) + 4 × 4px (`gap-1`) + 2 × 8px cell padding (`p-2`)
 * = 172px of content. 208 leaves comfortable slack.
 */
export const ACTIONS_COL_WIDTH = 208;

/**
 * Data cells keep the row height an Actions cell gives a row (28px icon buttons
 * plus 8px vertical padding = 44px), so hiding the Actions column doesn't make
 * rows shrink. Put this on the content box inside a collapsible cell
 * (CollapsibleCellContent, which holds the cell's vertical padding): the
 * collapse animation clips that box, so collapsed rows still reach zero height.
 * The flex column keeps content vertically centred inside the taller box. Wrap
 * the cell content in a plain div so only that wrapper is a flex item: inline
 * badges then keep their natural width instead of stretching across the cell.
 */
export const ACTIONS_ROW_MIN_HEIGHT_CLASS = "flex min-h-11 flex-col justify-center";

/** The same row height for a plain cell (no collapsible content box). A table
 *  cell's height acts as a minimum, and includes its padding. */
export const ACTIONS_ROW_CELL_HEIGHT_CLASS = "h-11";

/** Layout key of the Actions column in a table's saved-view column layout. */
export const ACTIONS_COLUMN_KEY = "actions";
export type ActionsColumnKey = typeof ACTIONS_COLUMN_KEY;

/** Layout metadata for the Actions column: hideable, shown by default, fixed
 *  width. It always renders last, wherever it sits in the persisted order. */
export const ACTIONS_COLUMN_LAYOUT: ColumnLayoutDef = {
  toggleable: true,
  defaultVisible: true,
  defaultWidth: ACTIONS_COL_WIDTH,
  resizable: false,
};

/**
 * Splits the Actions column out of a saved-view column layout, for tables that
 * render it separately from their data columns: *order* and *visibleKeys* come
 * back without it, and *actionsVisible* says whether it is toggled on.
 */
export function splitActionsColumn<K extends string>(
  order: readonly K[],
  visibleKeys: readonly K[],
): { order: Exclude<K, ActionsColumnKey>[]; visibleKeys: Exclude<K, ActionsColumnKey>[]; actionsVisible: boolean } {
  const isDataKey = (k: K): k is Exclude<K, ActionsColumnKey> => k !== ACTIONS_COLUMN_KEY;
  return {
    order: order.filter(isDataKey),
    visibleKeys: visibleKeys.filter(isDataKey),
    actionsVisible: visibleKeys.includes(ACTIONS_COLUMN_KEY as K),
  };
}
