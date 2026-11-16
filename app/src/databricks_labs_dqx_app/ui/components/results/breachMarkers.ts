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

/** Epoch-ms of a marker's run_date, or null when it doesn't parse. */
function runTime(runDate: string | number): number | null {
  const t = typeof runDate === "number" ? runDate : Date.parse(runDate);
  return Number.isFinite(t) ? t : null;
}

/** Series (table names) that breached at *runDate* (ISO string or epoch ms). Series-less markers are ignored. */
export function breachedSeriesAt(markers: BreachMarker[] | undefined, runDate: string | number | null | undefined): Set<string> {
  const out = new Set<string>();
  const at = runDate == null ? null : runTime(runDate);
  if (at == null) return out;
  for (const m of markers ?? []) {
    if (m.series && runTime(m.run_date) === at) out.add(m.series);
  }
  return out;
}

/** series -> epoch-ms run times at which that series breached (series-less markers ignored). */
export function breachTimesBySeries(markers: BreachMarker[] | undefined): Map<string, Set<number>> {
  const out = new Map<string, Set<number>>();
  for (const m of markers ?? []) {
    const t = runTime(m.run_date);
    if (!m.series || t == null) continue;
    const times = out.get(m.series) ?? new Set<number>();
    times.add(t);
    out.set(m.series, times);
  }
  return out;
}

/** Whether *ts* (epoch ms or numeric string) is one of a series' breach times. */
export function isBreachTime(times: Set<number> | undefined, ts: number | string | null | undefined): boolean {
  return times != null && ts != null && times.has(Number(ts));
}
