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
