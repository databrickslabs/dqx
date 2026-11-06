import { toNum } from "@/components/monitored-tables/BindingResultsTab";
import type { TrendPointOut } from "@/lib/api";

export type BreachMarker = {
  run_date: string;
  criticality: "error" | "warn";
  score: number | null;
  series?: string;
};

/**
 * One marker per breaching point of each per-table series, anchored at that
 * table's own score. The overall Average's `breached` flag is carried forward
 * from earlier runs, so markers must come from the per-table points instead.
 * `series` is the table name exactly as the chart's lines are keyed.
 */
export function perTableBreachMarkers(trendByTable: TrendPointOut[] | undefined): BreachMarker[] {
  return (trendByTable ?? []).flatMap((p) => {
    if (!p.breached) return [];
    const crit = p.breach_criticality;
    if (crit !== "error" && crit !== "warn") return [];
    const rate = toNum(p.pass_rate);
    return [
      {
        run_date: String(p.run_date ?? ""),
        criticality: crit,
        score: rate == null ? null : rate * 100,
        series: p.series ?? undefined,
      },
    ];
  });
}
