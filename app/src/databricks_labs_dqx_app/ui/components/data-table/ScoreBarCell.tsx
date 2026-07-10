import { scoreColor } from "@/components/results/ScoreBox";

/**
 * List-table DQ score cell: a small red→green bar plus a whole-number
 * percent label — copied from dqlake's `BindingsTable` / `DataProductsTable`
 * dqScore cells (same `scoreColor` continuous ramp the results page uses,
 * not discrete buckets, so 83% reads green-ish rather than "mean" orange).
 * Shared by the Monitored Tables and Table Spaces list tables (P3.4).
 *
 * `score` is the cached [0, 1] pass rate; null/undefined (never computed,
 * or no published run yet) renders the muted em dash like every other
 * empty list cell.
 */
export function ScoreBarCell({ score }: { score: number | null | undefined }) {
  if (score == null) return <span className="text-muted-foreground">—</span>;
  const pct = score * 100;
  // Always a whole-number percent — no "0.0%" decimals in the list.
  const label = pct >= 99.95 ? "100%" : `${Math.round(pct)}%`;
  return (
    <div className="flex items-center gap-2 w-full pr-4">
      <div className="h-1 w-20 bg-muted rounded overflow-hidden">
        <div
          className="h-full transition-[width] duration-200"
          // minWidth keeps a sliver visible even at 0% (a touch of red).
          style={{
            width: `${Math.max(0, Math.min(100, pct))}%`,
            minWidth: "0.25rem",
            backgroundColor: scoreColor(score),
          }}
        />
      </div>
      <span className="text-[10px] text-muted-foreground font-mono whitespace-nowrap">{label}</span>
    </div>
  );
}

/** Sort value for a score column: unscored rows group below 0%. */
export function scoreSortValue(score: number | null | undefined): number {
  return score ?? -1;
}
