/**
 * Shared, null-aware client-sort primitives for the overview tables (Rules
 * Registry, Monitored Tables, Table Spaces). Ported alongside dqlake's list
 * tables and extended for B2-92 "steward-minded sort defaults".
 *
 * Two ideas the plain `a < b` comparator couldn't express:
 *
 *  1. **Per-column first-click direction.** A data-quality steward clicks a
 *     header to surface the rows that most need attention — lowest DQ score,
 *     stalest run, thinnest coverage. That target sits at ASC for some
 *     columns and DESC for others, so each column declares its own
 *     `defaultSortDir` (see each table's `COLUMNS` config). Repeat clicks
 *     still toggle to the opposite direction, then clear.
 *
 *  2. **Fixed null placement.** "Never scored" / "never run" / "no steward"
 *     rows are pinned to one end of the list and stay there when the steward
 *     reverses direction — they are not data points on the scale, so they
 *     shouldn't flip from bottom to top on the second click. Present values
 *     compare normally and flip with direction; missing values do not.
 */

export type SortDirection = "asc" | "desc";

/** A column's comparable value for a single row. `null` marks a missing /
 *  "never" value (unscored, never run, no steward, unversioned, …) — the
 *  comparator pins these per the column's `nullsFirst` flag. */
export type SortValue = string | number | null;

/** Per-column sort behaviour resolved from a table's column config. */
export interface SortColumnConfig {
  /** Direction applied on the FIRST click of this column's header. */
  dir: SortDirection;
  /** When true, missing (`null`) values sort to the TOP regardless of
   *  direction; otherwise they sort to the BOTTOM. Fixed — never flips with
   *  the sort direction. */
  nullsFirst: boolean;
}

/**
 * Compares two column sort values with fixed null placement.
 *
 * Missing (`null`) values are pinned to one end of the list — the end chosen
 * by `nullsFirst` — and that placement does NOT change when `dir` toggles.
 * Two present values compare naturally (numeric subtraction or string order)
 * and the result flips with `dir`.
 */
export function compareSortValues(
  av: SortValue,
  bv: SortValue,
  dir: SortDirection,
  nullsFirst: boolean,
): number {
  const aMissing = av === null;
  const bMissing = bv === null;
  if (aMissing || bMissing) {
    if (aMissing && bMissing) return 0;
    // A missing value goes last (nullsFirst false) or first (nullsFirst true),
    // independent of `dir`.
    const missingComesFirst = nullsFirst ? -1 : 1;
    return aMissing ? missingComesFirst : -missingComesFirst;
  }

  let cmp: number;
  if (typeof av === "number" && typeof bv === "number") {
    cmp = av - bv;
  } else {
    const as = String(av);
    const bs = String(bv);
    cmp = as < bs ? -1 : as > bs ? 1 : 0;
  }
  return dir === "asc" ? cmp : -cmp;
}
