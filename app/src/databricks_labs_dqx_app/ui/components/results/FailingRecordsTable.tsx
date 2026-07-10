import { useLayoutEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { pickTopSeverity } from "./severityRank";
import { columnsFromRows } from "./failedRecordsExport";

// Re-exported so existing importers (and tests) can keep resolving it from this
// module; the implementation now lives in failedRecordsExport.
export { buildFailedRecordsCsv } from "./failedRecordsExport";

/** Estimated tooltip box size, used to clamp it inside the viewport. The height
 *  is only a fallback — the component measures the rendered box and passes the
 *  real height in, so the flipped-above position hugs the cursor instead of
 *  overshooting by a full estimate. */
const TT_W = 320; // matches max-w-sm
const TT_H = 200;

/** Gap between the cursor and the tooltip's BOTTOM edge when it flips above. */
const TT_FLIP_GAP = 8;

/** Position the cursor-following tooltip so it never falls off-screen: offset
 *  down-right by default, flip above the cursor when near the bottom, and clamp
 *  horizontally near the right edge. When flipping above, the tooltip's BOTTOM
 *  edge sits `TT_FLIP_GAP` above the cursor (top = y - h - TT_FLIP_GAP) using the
 *  MEASURED height `h`, so it hugs the cursor rather than jumping a full
 *  estimated-height (200px) up. */
export function clampTooltip(
  x: number,
  y: number,
  vw: number,
  vh: number,
  w = TT_W,
  h = TT_H,
): { left: number; top: number } {
  let left = x + 12;
  let top = y + 12;
  if (left + w > vw) left = Math.max(0, vw - w);
  if (top + h > vh) top = Math.max(0, y - h - TT_FLIP_GAP); // flip above
  return { left, top };
}

export type FailingRecord = {
  record_key: string;
  row_values: Record<string, string | null>;
  failed_columns: string[];
  failures: Array<{
    rule_name?: string;
    severity?: string;
    quality_dimension?: string;
    message?: string;
    columns?: string[];
  }>;
};

/** Client-side page size for the failed-records table. */
const PAGE_SIZE = 20;

/**
 * Drill-down table over failing source records. One column per source field
 * (union of all rows' row_values keys, first-seen order). Failed cells are
 * highlighted red; hovering a row reveals the failures that hit it in a
 * panel that follows the cursor. No data fetching — callers pass the rows in.
 */
export function FailingRecordsTable({
  rows,
  total,
  loading,
  severityColors,
  severityRanks,
  dimensionColors,
}: {
  rows: FailingRecord[];
  /** True total matching failed records (may exceed `rows.length` when the
   *  fetch was capped). Drives the "first N of X total" caption. Defaults to
   *  rows.length. */
  total?: number;
  /** When true, show a centred spinner instead of the table (a filter
   *  refetch is in flight). */
  loading?: boolean;
  /** severity name → hex colour. Used to tint the severity chip + cells. */
  severityColors?: Record<string, string>;
  /** severity name → rank (higher = more severe). Picks the colour when a
   *  cell has several failed rules of differing severities. */
  severityRanks?: Record<string, number>;
  /** dimension name → hex colour. Used to tint the dimension text. */
  dimensionColors?: Record<string, string>;
}) {
  const { t } = useTranslation();
  // Hovered row index + cursor position. The detail panel is a `fixed`
  // element positioned at the cursor (offset a few px) rather than a Radix
  // tooltip pinned to the row, so it tracks the pointer.
  const [hover, setHover] = useState<{
    index: number;
    x: number;
    y: number;
    /** The column under the cursor, if any. Drives which tooltip rule rows
     *  render prominent (the rest stay muted). */
    col: string | null;
  } | null>(null);
  // The rendered tooltip's measured height, so a near-the-bottom flip positions
  // its bottom edge close to the cursor instead of overshooting by the estimate.
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [tooltipHeight, setTooltipHeight] = useState<number | undefined>(
    undefined,
  );
  // Client-side pagination over the already-fetched rows.
  const [page, setPage] = useState(0);

  // Measure the rendered tooltip after each hover update; clampTooltip then
  // flips it close above the cursor using the real height. Runs before paint
  // so the corrected position lands without a visible jump.
  useLayoutEffect(() => {
    if (!hover) {
      setTooltipHeight(undefined);
      return;
    }
    const h = tooltipRef.current?.getBoundingClientRect().height;
    if (h && h !== tooltipHeight) setTooltipHeight(h);
  }, [hover, tooltipHeight]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-16">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (rows.length === 0) {
    return <p className="text-sm text-muted-foreground">{t("resultsUi.noFailedRecords")}</p>;
  }

  // Header = union of all row_values keys, in first-seen order.
  const columns = columnsFromRows(rows);

  // True total failed records (may exceed the loaded rows when capped).
  const totalCount = total ?? rows.length;
  const capped = totalCount > rows.length;
  // Clamp the page so a filter change that shrinks the rows can't strand us on
  // an out-of-range page; slice the current page out of the full rows.
  const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages - 1);
  const startIdx = safePage * PAGE_SIZE;
  const pageRows = rows.slice(startIdx, startIdx + PAGE_SIZE);
  const goToPage = (p: number) => {
    setHover(null); // the hovered row may not be on the new page
    setPage(p);
  };
  const range = `${startIdx + 1}–${startIdx + pageRows.length}`;
  const caption = capped
    ? t("resultsUi.failedRecordsCaptionCapped", {
        range,
        loaded: rows.length,
        total: totalCount.toLocaleString(),
      })
    : t("resultsUi.failedRecordsCaption", {
        range,
        total: totalCount.toLocaleString(),
      });

  const hovered = hover != null ? rows[hover.index] : null;
  // A rule applied to several columns produces one failure entry per column, so
  // the same (rule, severity) can repeat in a record's failures. Show each rule
  // once per severity — dedupe, merging the columns so the cell-hover highlight
  // still matches any column the rule touched.
  const dedupedFailures: FailingRecord["failures"] = (() => {
    if (!hovered) return [];
    const byKey = new Map<string, FailingRecord["failures"][number]>();
    const out: FailingRecord["failures"] = [];
    for (const f of hovered.failures) {
      // NUL separator (dqlake uses a literal NUL byte here): rule names and
      // severities can contain spaces, so a printable separator could collide.
      const key = `${f.rule_name ?? ""}\u0000${f.severity ?? ""}`;
      const existing = byKey.get(key);
      if (existing) {
        for (const c of f.columns ?? []) {
          if (!(existing.columns ?? []).includes(c)) {
            existing.columns = [...(existing.columns ?? []), c];
          }
        }
      } else {
        const copy = { ...f, columns: [...(f.columns ?? [])] };
        byKey.set(key, copy);
        out.push(copy);
      }
    }
    return out;
  })();

  return (
    <div className="space-y-2">
      <div className="rounded-md border overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="bg-muted/30 border-b">
            <tr>
              {columns.map((col) => (
                <th
                  key={col}
                  className="text-left px-3 py-2 uppercase tracking-wide text-[10px] text-muted-foreground font-mono whitespace-nowrap"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageRows.map((row, idx) => {
              const i = startIdx + idx; // absolute row index (hover keys off it)
              return (
              <tr
                key={i}
                className="border-b last:border-b-0 cursor-default hover:bg-muted/40 transition-colors"
                onMouseLeave={() =>
                  setHover((h) => (h?.index === i ? null : h))
                }
              >
                {columns.map((col) => {
                  const failed = row.failed_columns.includes(col);
                  // Tint by the highest-priority severity among the failures
                  // hitting this column (fall back to all failures when none
                  // name the column). No severity colour → flat-red fallback.
                  const sevForCol = failed
                    ? row.failures
                        .filter(
                          (f) => !f.columns?.length || f.columns.includes(col),
                        )
                        .map((f) => f.severity)
                    : [];
                  const top = failed
                    ? pickTopSeverity(sevForCol, severityRanks ?? {})
                    : undefined;
                  const tint = top ? severityColors?.[top] : undefined;
                  return (
                    <td
                      key={col}
                      onMouseMove={(e) =>
                        setHover({
                          index: i,
                          x: e.clientX,
                          y: e.clientY,
                          col,
                        })
                      }
                      className={`px-3 py-2 align-top font-mono whitespace-nowrap ${
                        failed && !tint
                          ? "text-red-600 bg-red-50 dark:bg-red-950/40 font-medium"
                          : failed
                            ? "font-medium"
                            : ""
                      }`}
                      style={
                        tint
                          ? { color: tint, backgroundColor: `${tint}1a` }
                          : undefined
                      }
                    >
                      {row.row_values[col] ?? "—"}
                    </td>
                  );
                })}
              </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {hovered && hover && (
        <div
          ref={tooltipRef}
          role="tooltip"
          // Light mode: the tooltip is grey and a relevant (highlighted) row is
          // white — the inverse of the cards. Dark mode keeps the popover look.
          className="pointer-events-none fixed z-50 max-w-sm rounded-md border bg-muted text-popover-foreground dark:bg-popover p-3 text-xs shadow-md"
          style={clampTooltip(
            hover.x,
            hover.y,
            typeof window !== "undefined" ? window.innerWidth : 1024,
            typeof window !== "undefined" ? window.innerHeight : 768,
            TT_W,
            tooltipHeight,
          )}
        >
          {/* Rows are flush (no inter-row gap): adjacent highlighted rules then
              form ONE continuous grey block instead of separate pills with a
              dark stripe between them. */}
          <ul>
            {dedupedFailures.map((f, j) => {
              const sevColor = f.severity
                ? severityColors?.[f.severity]
                : undefined;
              const dimColor = f.quality_dimension
                ? dimensionColors?.[f.quality_dimension]
                : undefined;
              // Every rule renders in the full prominent style. When the cursor
              // is over a failed cell, the rule(s) reporting against that column
              // (a column-less rule applies to the whole row) get a filled grey
              // background to single them out.
              const isHi = (g: FailingRecord["failures"][number]) =>
                hover.col != null &&
                (!g.columns?.length || g.columns.includes(hover.col));
              const highlighted = isHi(f);
              const prevHi = j > 0 && isHi(dedupedFailures[j - 1]);
              const nextHi =
                j < dedupedFailures.length - 1 && isHi(dedupedFailures[j + 1]);
              // EVERY row carries the same full-width inset + vertical padding
              // (-mx-3 px-3 py-1.5), so the tooltip's size is constant no matter
              // which row is highlighted; the grey just fills the always-present
              // space behind the highlighted row (no resize / compression).
              // First/last rows bleed into the popover's own top/bottom padding
              // (-mt-3 pt-3 / -mb-3 pb-3) — by POSITION, not highlight — so the
              // grey reaches the popover edge there. Corners round only at the
              // ENDS of a contiguous highlighted run, so adjacent highlighted
              // rows read as one block.
              const isFirst = j === 0;
              const isLast = j === dedupedFailures.length - 1;
              const cls = [
                "space-y-0.5 -mx-3 px-3 py-1.5",
                isFirst && "-mt-3 pt-3",
                isLast && "-mb-3 pb-3",
                highlighted && "bg-background dark:bg-muted",
                highlighted && !prevHi && "rounded-t-md",
                highlighted && !nextHi && "rounded-b-md",
              ]
                .filter(Boolean)
                .join(" ");
              return (
                <li key={j} className={cls}>
                  {/* Line 1: rule name + severity chip (severity colour). */}
                  <div className="flex flex-wrap items-center gap-2">
                    {f.rule_name && (
                      <span className="font-semibold">{f.rule_name}</span>
                    )}
                    {f.severity && (
                      <span
                        className="rounded border px-1.5 py-0.5 text-[10px] uppercase tracking-wide font-medium"
                        style={sevColor ? { color: sevColor } : undefined}
                      >
                        {f.severity}
                      </span>
                    )}
                  </div>
                  {/* Line 2: dimension (dimension colour). */}
                  {f.quality_dimension && (
                    <div style={dimColor ? { color: dimColor } : undefined}>
                      {f.quality_dimension}
                    </div>
                  )}
                  {/* Line 3: description (the rule description, sent as message). */}
                  {f.message && (
                    <div className="text-muted-foreground break-words">
                      {f.message}
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        </div>
      )}

      <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
        <div className="flex items-center gap-3">
          <span>{caption}</span>
        </div>
        {totalPages > 1 && (
          <div className="flex items-center gap-2 shrink-0">
            <Button
              variant="outline"
              size="sm"
              className="h-7"
              disabled={safePage <= 0}
              onClick={() => goToPage(safePage - 1)}
            >
              {t("resultsUi.prevPage")}
            </Button>
            <span>
              {t("resultsUi.pageOf", { page: safePage + 1, pages: totalPages })}
            </span>
            <Button
              variant="outline"
              size="sm"
              className="h-7"
              disabled={safePage >= totalPages - 1}
              onClick={() => goToPage(safePage + 1)}
            >
              {t("resultsUi.nextPage")}
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
