import type { CountRow } from "./ScoreTrendChart";

/**
 * Flatten per-run count rows (e.g. trend_counts / trend_failures dicts) into
 * long-form CountRow series for a count-mode ScoreTrendChart. Each `map` entry
 * pulls one numeric field out of every row under a display label.
 */
export function toCountSeries<T extends object>(
  rows: ReadonlyArray<T> | undefined,
  map: Array<{ key: string; label: string }>,
): CountRow[] {
  return (rows ?? []).flatMap((row) => {
    const r = row as Record<string, unknown>;
    return map.map((m) => ({
      run_date: String(r.run_date ?? ""),
      series: m.label,
      value: typeof r[m.key] === "number" ? (r[m.key] as number) : null,
    }));
  });
}
