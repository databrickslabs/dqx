import { useEffect, useMemo, useRef, useState } from "react";
import type * as React from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, CircleHelp } from "lucide-react";
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  useXAxisScale,
  useYAxisScale,
} from "recharts";
// recharts' own vendored d3 (no new dep). Centripetal Catmull-Rom (alpha 0.5)
// is smooth like "natural" but provably free of loops/cusps, so the curve never
// goes back on itself even when runs cluster close in time.
import { curveCatmullRom } from "victory-vendor/d3-shape";
import {
  Tooltip as UITooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { scoreColor } from "./ScoreBox";
import { CollapseRegion } from "./CollapseRegion";

/** Count-mode series are drawn in distinct but muted/neutral colours (a small
 *  slate/stone/zinc palette) AND told apart by marker SHAPE. Colours cycle in
 *  this order, in lockstep with the shapes below, so each series gets a stable
 *  (colour, shape) pair. Kept understated — these aren't the vivid categorical
 *  hues used by the By-Dimension/Severity charts. */
export const COUNT_COLORS = ["#93c5fd", "#60a5fa", "#3b82f6", "#2563eb", "#1e40af"] as const;
const MARKERS = ["circle", "square", "triangle", "diamond", "cross"] as const;
type Marker = (typeof MARKERS)[number];

/** Map count-series names to their per-series neutral colour, cycling
 *  COUNT_COLORS by index (in lockstep with the marker shapes). Each series gets
 *  a DISTINCT muted colour rather than one shared grey. */
export function countSeriesColors(series: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  series.forEach((name, idx) => {
    out[name] = COUNT_COLORS[idx % COUNT_COLORS.length];
  });
  return out;
}

/** A small inline SVG of a marker `shape` in `fill`, sized to sit beside text.
 *  Used in the count-mode tooltip so its per-series icon matches the line's
 *  marker (circle/square/triangle/diamond), not a generic round dot. */
function MarkerIcon({ shape, fill }: { shape: Marker; fill: string }) {
  const s = 10;
  const c = s / 2;
  const r = 3.5;
  return (
    <svg
      width={s}
      height={s}
      viewBox={`0 0 ${s} ${s}`}
      className="shrink-0"
      aria-hidden
    >
      {shape === "square" ? (
        <rect x={c - r} y={c - r} width={r * 2} height={r * 2} fill={fill} />
      ) : shape === "triangle" ? (
        <polygon points={`${c},${c - r} ${c - r},${c + r} ${c + r},${c + r}`} fill={fill} />
      ) : shape === "diamond" ? (
        <polygon points={`${c},${c - r} ${c + r},${c} ${c},${c + r} ${c - r},${c}`} fill={fill} />
      ) : shape === "cross" ? (
        <path d={`M${c - r},${c} h${2 * r} M${c},${c - r} v${2 * r}`} stroke={fill} strokeWidth={1.5} />
      ) : (
        <circle cx={c} cy={c} r={r} fill={fill} />
      )}
    </svg>
  );
}

/** A Recharts dot renderer that draws `shape` at (cx, cy) in `fill`. Used for
 *  count series so each line is distinguishable by marker, not colour. */
function markerDot(shape: Marker, r: number, fill: string) {
  return (props: { cx?: number; cy?: number }) => {
    const { cx, cy } = props;
    if (cx == null || cy == null) return <g key={`${cx}-${cy}`} />;
    const key = `${cx}-${cy}`;
    if (shape === "square")
      return <rect key={key} x={cx - r} y={cy - r} width={r * 2} height={r * 2} fill={fill} />;
    if (shape === "triangle")
      return <polygon key={key} points={`${cx},${cy - r} ${cx - r},${cy + r} ${cx + r},${cy + r}`} fill={fill} />;
    if (shape === "diamond")
      return <polygon key={key} points={`${cx},${cy - r} ${cx + r},${cy} ${cx},${cy + r} ${cx - r},${cy}`} fill={fill} />;
    if (shape === "cross")
      return (
        <path key={key} d={`M${cx - r},${cy} h${2 * r} M${cx},${cy - r} v${2 * r}`} stroke={fill} strokeWidth={1.5} />
      );
    return <circle key={key} cx={cx} cy={cy} r={r} fill={fill} />;
  };
}

/** Toggle a series name in the count-chart "hidden" set (legend click-to-hide):
 *  remove if present, add if absent. Returns a NEW set. */
export function toggleHidden(cur: Set<string>, name: string): Set<string> {
  const next = new Set(cur);
  if (next.has(name)) next.delete(name);
  else next.add(name);
  return next;
}

export type TrendRow = {
  run_date: string;
  series?: string;
  pass_rate: number | null;
};

/** A single point in a COUNT-mode series (raw counts, not percentages). */
export type CountRow = { run_date: string; series: string; value: number | null };

const DEFAULT_SERIES = "Pass rate";
const CHART_FALLBACK = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
];

export type PivotResult = {
  /** Wide rows keyed by run_date with one numeric key per series. */
  points: Array<Record<string, string | number | null>>;
  /** Distinct series names, in first-seen order. */
  series: string[];
};

/**
 * Pivot long trend rows into wide form: one row per run_date, one numeric
 * key per distinct series (value = pass_rate * 100). When no row carries a
 * `series`, a single default series is used (i18n: the component passes the
 * translated "Pass rate" label in; the default keeps the helper pure).
 */
export function pivot(
  data: Array<TrendRow>,
  defaultSeries: string = DEFAULT_SERIES,
): PivotResult {
  const series: string[] = [];
  const byDate = new Map<string, Record<string, string | number | null>>();

  for (const d of data) {
    if (d.run_date == null) continue;
    const name = d.series ?? defaultSeries;
    if (!series.includes(name)) series.push(name);

    let row = byDate.get(d.run_date);
    if (!row) {
      // `ts` (epoch ms) is the numeric x for the time-proportional axis; spacing
      // then reflects real elapsed time, not equal slots per run.
      row = { run_date: d.run_date, ts: Date.parse(d.run_date) };
      byDate.set(d.run_date, row);
    }
    row[name] = d.pass_rate == null ? null : d.pass_rate * 100;
  }

  const points = Array.from(byDate.values()).sort(
    (a, b) => (a.ts as number) - (b.ts as number),
  );
  return { points, series };
}

/**
 * Pivot long COUNT rows into wide form: one row per run_date, one numeric key
 * per distinct series. Values are NOT scaled (raw counts).
 */
export function pivotCounts(data: Array<CountRow>): PivotResult {
  const series: string[] = [];
  const byDate = new Map<string, Record<string, string | number | null>>();

  for (const d of data) {
    if (d.run_date == null) continue;
    if (!series.includes(d.series)) series.push(d.series);

    let row = byDate.get(d.run_date);
    if (!row) {
      row = { run_date: d.run_date, ts: Date.parse(d.run_date) };
      byDate.set(d.run_date, row);
    }
    row[d.series] = d.value;
  }

  const points = Array.from(byDate.values()).sort(
    (a, b) => (a.ts as number) - (b.ts as number),
  );
  return { points, series };
}

/** Format a run timestamp compactly, e.g. "Jun 11 14:30". Falls back to the
 *  raw string when it isn't a parseable date. */
/** Evenly-spaced epoch-ms tick values across [minTs, maxTs] at a "nice" time
 *  step (so the axis shows regular ticks like 11:26 / 11:30 / 11:34 instead of
 *  one label per data point). Points keep their true time positions; only the
 *  ticks are regularised. The domain is widened to the enclosing step boundaries
 *  so the first/last ticks sit at the axis ends. Returns [] / a single value for
 *  degenerate ranges (callers fall back to the auto axis then). */
const _TIME_STEPS_MS = [
  1_000, 5_000, 10_000, 15_000, 30_000,
  60_000, 2 * 60_000, 5 * 60_000, 10 * 60_000, 15 * 60_000, 30 * 60_000,
  3_600_000, 2 * 3_600_000, 3 * 3_600_000, 6 * 3_600_000, 12 * 3_600_000,
  86_400_000, 2 * 86_400_000, 7 * 86_400_000,
];

export function niceTimeTicks(
  minTs: number,
  maxTs: number,
  target = 6,
): number[] {
  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs)) return [];
  if (maxTs <= minTs) return [minTs];
  const raw = (maxTs - minTs) / Math.max(1, target);
  const step =
    _TIME_STEPS_MS.find((s) => s >= raw) ??
    _TIME_STEPS_MS[_TIME_STEPS_MS.length - 1];
  const start = Math.floor(minTs / step) * step;
  const end = Math.ceil(maxTs / step) * step;
  const ticks: number[] = [];
  for (let t = start; t <= end + step / 2; t += step) ticks.push(t);
  return ticks;
}

export function formatTs(value: string | number): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

/** Fraction (0–1, clamped) of the plot height a cursor pixel-y sits ABOVE the
 *  plot bottom: `plotBottom` → 0, `plotTop` → 1. The axis-agnostic basis for
 *  both the percent-mode box-zoom and the wheel-zoom y-anchor (B2-56). A
 *  degenerate (non-positive) span returns 0. */
export function fracFromChartY(
  chartY: number,
  plotTop: number,
  plotBottom: number,
): number {
  const span = plotBottom - plotTop;
  if (span <= 0) return 0;
  return Math.max(0, Math.min(1, (plotBottom - chartY) / span));
}

/** Invert a cursor pixel-y within the plot area to a 0–100 percent value,
 *  clamped to [0, 100]. `plotTop` maps to 100% (top of the plot) and
 *  `plotBottom` to 0%. Used by the percent-mode 2D box-zoom (B2-17) to turn a
 *  vertical drag into a y-domain window. */
export function percentFromChartY(
  chartY: number,
  plotTop: number,
  plotBottom: number,
): number {
  return fracFromChartY(chartY, plotTop, plotBottom) * 100;
}

/** Scale a numeric [lo, hi] window around `anchor` by `factor` — factor < 1
 *  zooms IN (narrower window), > 1 zooms OUT — keeping the anchor value fixed
 *  in value-space (so wheel-zoom stays centred on the cursor). */
export function zoomWindow(
  lo: number,
  hi: number,
  anchor: number,
  factor: number,
): [number, number] {
  return [anchor - (anchor - lo) * factor, anchor + (hi - anchor) * factor];
}

/** Clamp a [lo, hi] window inside [min, max], preserving low≤high ordering. */
export function clampWindow(
  lo: number,
  hi: number,
  min: number,
  max: number,
): [number, number] {
  return [
    Math.max(min, Math.min(lo, hi)),
    Math.min(max, Math.max(lo, hi)),
  ];
}

/** Shift a [lo, hi] window by `delta` and clamp inside [min, max] WITHOUT
 *  changing its width — so a pan stops at the data edge instead of scrolling
 *  into empty space. A window at least as wide as the extent pins to [min,max]
 *  (nothing to pan). Powers drag-to-pan (B2-56). */
export function panWindow(
  lo: number,
  hi: number,
  delta: number,
  min: number,
  max: number,
): [number, number] {
  const width = hi - lo;
  if (width >= max - min) return [min, max];
  let nLo = lo + delta;
  let nHi = hi + delta;
  if (nLo < min) {
    nLo = min;
    nHi = min + width;
  } else if (nHi > max) {
    nHi = max;
    nLo = max - width;
  }
  return [nLo, nHi];
}

/** True when a window covers (or exceeds) the full [min, max] extent — used to
 *  collapse a fully zoomed-out axis back to its default (null) domain. */
export function isFullExtent(
  lo: number,
  hi: number,
  min: number,
  max: number,
): boolean {
  return lo <= min && hi >= max;
}

type TooltipPayloadEntry = {
  name?: string | number;
  value?: string | number | null;
  color?: string;
};

/** Controlled tooltip: replaces Recharts' default opaque box. For percent mode
 *  it shows "<series>: <pct>%"; for count mode it shows the raw value. */
/** A small inline "×" glyph in `color`, matching the member-table line markers
 *  in the Average chart (legend + tooltip). */
function XGlyph({ color }: { color?: string }) {
  return (
    <svg width={10} height={10} viewBox="0 0 10 10" className="shrink-0" aria-hidden>
      <path d="M1.5,1.5 L8.5,8.5 M1.5,8.5 L8.5,1.5"
            stroke={color ?? "currentColor"} strokeWidth={1.5} fill="none" />
    </svg>
  );
}

export function TrendTooltip({
  active,
  payload,
  label,
  mode,
  seriesShapes,
  seriesColors,
  xMarkers,
  averageByDate,
  latestAverage,
  overallLabel,
}: {
  active?: boolean;
  payload?: TooltipPayloadEntry[];
  label?: string | number;
  mode: "percent" | "count";
  /** Count mode: series name → marker shape, so the tooltip icon matches the
   *  line's marker instead of a generic round dot. */
  seriesShapes?: Record<string, Marker>;
  /** Count mode: series name → neutral colour, so the tooltip marker matches
   *  the line/legend colour for that series. */
  seriesColors?: Record<string, string>;
  /** Average mode (product chart): draw member-table markers as × (matching the
   *  plot) instead of round dots. */
  xMarkers?: boolean;
  /** Average mode: run_date → the Average score (0–100) at that instant. */
  averageByDate?: Record<string, number>;
  /** Average mode: the latest Average, shown for runs before any complete one. */
  latestAverage?: number | null;
  /** Average mode: the prominent series' label (e.g. "Average"). */
  overallLabel?: string;
}) {
  const { t } = useTranslation();
  if (!active || !payload || payload.length === 0) return null;
  const resolvedOverallLabel = overallLabel ?? t("resultsUi.averageSeries");
  // `label` is the numeric epoch-ms x (time axis); keep it numeric for the
  // Average lookup and date formatting.
  const runKey = label;
  // Recharts orders tooltip items by value; force the count series into the
  // canonical order Rules → Checks → Tests → Rows instead.
  const COUNT_ORDER = ["Rules", "Checks", "Tests", "Rows"];
  const orderIdx = (n?: string | number) => {
    const i = COUNT_ORDER.indexOf(String(n ?? ""));
    return i === -1 ? COUNT_ORDER.length : i;
  };
  const items =
    mode === "count"
      ? [...payload].sort((a, b) => orderIdx(a.name) - orderIdx(b.name))
      : payload;
  return (
    <div
      className="rounded-md border bg-popover text-popover-foreground p-2 text-xs shadow-md"
      style={{ borderColor: "var(--border)" }}
    >
      <div className="mb-1 font-medium">{formatTs(runKey ?? "")}</div>
      <ul className="space-y-0.5">
        {items.map((p, i) => {
          const name = String(p.name ?? "");
          const value = typeof p.value === "number" ? p.value : null;
          const text =
            mode === "percent"
              ? value == null
                ? "—"
                : `${value.toFixed(1)}%`
              : value == null
                ? "—"
                : `${value}`;
          return (
            <li key={i} className="flex items-center gap-2">
              {mode === "count" ? (
                <MarkerIcon
                  shape={seriesShapes?.[name] ?? "circle"}
                  fill={seriesColors?.[name] ?? COUNT_COLORS[0]}
                />
              ) : xMarkers ? (
                <XGlyph color={p.color} />
              ) : (
                <span
                  className="inline-block h-2 w-2 rounded-full shrink-0"
                  style={{ background: p.color }}
                />
              )}
              <span>
                {name}: {text}
              </span>
            </li>
          );
        })}
        {/* Average mode: show the Average this run contributes to — the Average
            at this instant if it's a complete one, else the latest Average. */}
        {mode === "percent" && xMarkers && (() => {
          const avg =
            (runKey != null ? averageByDate?.[runKey] : undefined) ??
            latestAverage ??
            null;
          if (avg == null) return null;
          return (
            <li className="mt-0.5 flex items-center gap-2 border-t pt-0.5"
                style={{ borderColor: "var(--border)" }}>
              <span className="inline-block h-2 w-2 rounded-full shrink-0"
                    style={{ background: "var(--foreground)" }} />
              <span className="font-medium">
                {resolvedOverallLabel}: {avg.toFixed(1)}%
              </span>
            </li>
          );
        })()}
      </ul>
    </div>
  );
}

/** One point on the product "Average" series — the mean-of-tables score at a run
 *  instant where EVERY current member table has run as-of then. `value` is
 *  already scaled to 0–100. */
export type OverallStep = { run_date: string; value: number | null };

/** A recharts dot renderer drawing an "×" at (cx, cy). Used for the member-table
 *  points so they read as recessive marks, distinct from the Average's bold
 *  circular markers. */
function xMarkerDot(r: number, color: string, opacity = 0.7) {
  return (props: { cx?: number; cy?: number }) => {
    const { cx, cy } = props;
    if (cx == null || cy == null) return <g key={`${cx}-${cy}`} />;
    return (
      <path
        key={`${cx}-${cy}`}
        d={`M${cx - r},${cy - r} L${cx + r},${cy + r} M${cx - r},${cy + r} L${cx + r},${cy - r}`}
        stroke={color}
        strokeWidth={1.25}
        strokeOpacity={opacity}
        fill="none"
      />
    );
  };
}

/** Smooth (Catmull-Rom → cubic Bézier) SVG path through the given points. A
 *  gentle trendline rather than a jagged poly-line; ≤1 point yields "".
 *  Each control point's x is clamped to its segment [p1.x, p2.x] so the curve
 *  stays monotonic in x and can't bow past a point or loop back on itself when
 *  points cluster close together in time. */
function smoothPath(pts: Array<{ x: number; y: number }>): string {
  if (pts.length < 2) return "";
  const d: string[] = [`M${pts[0].x},${pts[0].y}`];
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[i - 1] ?? pts[i];
    const p1 = pts[i];
    const p2 = pts[i + 1];
    const p3 = pts[i + 2] ?? p2;
    const lo = Math.min(p1.x, p2.x);
    const hi = Math.max(p1.x, p2.x);
    const clampX = (x: number) => Math.max(lo, Math.min(hi, x));
    const c1x = clampX(p1.x + (p2.x - p0.x) / 6);
    const c1y = p1.y + (p2.y - p0.y) / 6;
    const c2x = clampX(p2.x - (p3.x - p1.x) / 6);
    const c2y = p2.y - (p3.y - p1.y) / 6;
    d.push(`C${c1x},${c1y} ${c2x},${c2y} ${p2.x},${p2.y}`);
  }
  return d.join(" ");
}

/** Bespoke "Average" layer for the product score chart, drawn via recharts'
 *  axis-scale hooks so it shares the plot geometry:
 *   - a SMOOTH foreground trendline through the Average points (the mean of the
 *     member tables, plotted only at instants where every member has run);
 *   - large `--foreground` circular markers (white in dark mode / black in
 *     light) — the most prominent thing on the chart.
 *  When no instant yet has every member's run, there are no Average points. */
function OverallLayer({
  points,
}: {
  points: Array<OverallStep>;
}) {
  const xScale = useXAxisScale();
  const yScale = useYAxisScale();
  if (!xScale || !yScale) return null;
  // A category x-axis may use a band scale (left-edge) or a point scale
  // (centred); offset by half a band so points sit on the tick either way.
  const maybeBand = xScale as unknown as { bandwidth?: () => number };
  const half =
    typeof maybeBand.bandwidth === "function" ? maybeBand.bandwidth() / 2 : 0;
  // The axis is a numeric time scale, so map the run_date string → epoch ms.
  const xOf = (d: string) => {
    const v = xScale(Date.parse(d));
    return v == null ? null : Number(v) + half;
  };
  const pts = points
    .filter((p) => p.value != null)
    .map((p) => {
      const x = xOf(p.run_date);
      const y = yScale(p.value as number);
      return x == null || y == null ? null : { x, y };
    })
    .filter((p): p is { x: number; y: number } => p != null);
  if (pts.length === 0) return null;
  const fg = "var(--foreground)";
  return (
    <g style={{ pointerEvents: "none" }}>
      {/* Smooth Average trendline (only when there are ≥2 points). */}
      {pts.length >= 2 && (
        <path d={smoothPath(pts)} fill="none" stroke={fg} strokeWidth={2.5} />
      )}
      {/* Average markers: large foreground circles, the most prominent element. */}
      {pts.map((p, i) => (
        <circle key={`dot-${i}`} cx={p.x} cy={p.y} r={6}
                fill={fg} stroke="var(--background)" strokeWidth={1.5} />
      ))}
    </g>
  );
}

/** Shorten a fully-qualified `catalog.schema.table` to just the table for a
 *  compact legend label; non-qualified names pass through unchanged. */
function shortLabel(name: string): string {
  const parts = name.split(".");
  return parts[parts.length - 1] || name;
}

/**
 * Trend-over-time chart. In percent mode (default) plots pass-rate lines; in
 * count mode plots raw-count lines. No data fetching — callers pass the rows in.
 */
export function ScoreTrendChart({
  data,
  countData,
  mode = "percent",
  colorMap,
  dashedSeries,
  step,
  overall,
  overallLabel: overallLabelProp,
  onSeriesClick,
  title,
  info,
  collapsed,
  onToggleCollapse,
  animate = false,
  versionMarkers,
}: {
  data?: Array<TrendRow>;
  countData?: Array<CountRow>;
  mode?: "percent" | "count";
  colorMap?: Record<string, string>;
  dashedSeries?: string[];
  /** When true, the percent-mode line(s)/area use a stepAfter interpolation
   *  (each value held until the next run) instead of a smooth curve — correct
   *  for the product over-time chart whose member tables run on independent
   *  schedules. */
  step?: boolean;
  /** Product "Average" mode: when provided, the per-series `data` lines are
   *  drawn recessive (thin, dull, smooth, × markers — the member tables) and a
   *  prominent foreground Average trendline (these points) is drawn on top, with
   *  a toggleable legend below. Points are the mean of the member tables, only at
   *  instants where every member has run as-of then. */
  overall?: Array<OverallStep>;
  /** Legend + ScoreBox wording for the prominent series (default "Overall"; the
   *  product chart passes "Average"). */
  overallLabel?: string;
  /** Percent multi-series charts only: when provided, clicking a series line
   *  calls this with the series name (used to toggle a drilldown facet). */
  onSeriesClick?: (series: string) => void;
  title?: string;
  /** When set, a muted help "?" icon follows the title; hovering it shows this
   *  content in a tooltip. */
  info?: React.ReactNode;
  /** When `onToggleCollapse` is provided the title becomes a clickable row with
   *  a trailing chevron and the chart body animates open/closed by `collapsed`. */
  collapsed?: boolean;
  onToggleCollapse?: () => void;
  /** When true, animate the overall-score line drawing in left→right on mount
   *  (recharts' native reveal). Used by the homepage. */
  animate?: boolean;
  /** Optional subtle vertical markers at the runs where the monitored-table
   *  binding version incremented (#65). Each `run_date` is matched to its `ts`
   *  on the time axis; `label` is the pre-formatted tag (e.g. "v2"). Only the
   *  single-table overall-score trend passes these. */
  versionMarkers?: Array<{ run_date: string; label: string }>;
}) {
  const { t } = useTranslation();
  const overallLabel = overallLabelProp ?? t("resultsUi.overallSeries");
  const { points, series } =
    mode === "count"
      ? pivotCounts(countData ?? [])
      : pivot(data ?? [], t("resultsUi.passRateSeries"));

  // Evenly-spaced time ticks across the run range (so the axis reads 11:26 /
  // 11:30 / 11:34 … instead of one label per run). Points stay at their true
  // times; only the ticks are regularised, and the domain widens to the
  // enclosing step boundaries. Falls back to the auto axis for ≤1 point.
  const timeTicks = useMemo(() => {
    const ts = points
      .map((p) => p.ts as number)
      .filter((t) => Number.isFinite(t));
    if (ts.length < 2) return [] as number[];
    return niceTimeTicks(Math.min(...ts), Math.max(...ts));
  }, [points]);
  const hasTimeTicks = timeTicks.length > 1;

  // Plot geometry, shared by the y-zoom inversion below and the Overall-DQ-score
  // gradient further down. The chart is a fixed 200px tall with an 8px top
  // margin and a ~30px x-axis, so the plot area runs from y=PLOT_TOP (the 100%
  // line) to y=PLOT_BOTTOM (the 0% line) in SVG pixels.
  const CHART_HEIGHT = 200;
  const TOP_MARGIN = 8;
  const XAXIS_HEIGHT = 30;
  const PLOT_TOP = TOP_MARGIN;
  const PLOT_BOTTOM = CHART_HEIGHT - XAXIS_HEIGHT;

  // ── Zoom + pan (B2-56). Three gestures, all confined to the plot and mapped
  // through the REAL plotting rectangle measured from the DOM (the Recharts
  // cartesian-grid <g>), so every conversion uses the actual pixel geometry
  // rather than hardcoded axis/margin guesses:
  //   • WHEEL — zooms BOTH axes toward the cursor (percent AND count mode). The
  //     data point under the cursor stays fixed under the cursor (zoom-to-cursor)
  //     because the cursor's fractional position in the measured grid rect is
  //     mapped into the live domain and held as the zoom anchor.
  //   • PLAIN DRAG — draws a visible selection rectangle and zooms to it on
  //     release (both axes). A plain click (no movement) commits nothing, so
  //     per-series onClick still fires.
  //   • SHIFT+DRAG — pans the zoomed view; the grab point tracks the cursor and
  //     the window clamps at the data edges (no scrolling into empty space).
  // A "Reset" control clears both axes. Wheel/pan act on committed domains via
  // functional updates, collapsing an axis back to its default (null) once fully
  // zoomed out.
  const [dragStart, setDragStart] = useState<number | null>(null);
  const [dragEnd, setDragEnd] = useState<number | null>(null);
  const [dragStartY, setDragStartY] = useState<number | null>(null);
  const [dragEndY, setDragEndY] = useState<number | null>(null);
  const [zoomDomain, setZoomDomain] = useState<[number, number] | null>(null);
  const [yZoomDomain, setYZoomDomain] = useState<[number, number] | null>(null);
  const [panning, setPanning] = useState(false);
  const zoomed = zoomDomain != null;
  const yZoomed = yZoomDomain != null;
  // Selection-rectangle edges (only present mid box-drag). Each axis commits
  // independently, so a purely horizontal (or vertical) drag zooms just that
  // axis instead of collapsing the other to a zero-width window.
  const boxX1 = dragStart != null && dragEnd != null ? Math.min(dragStart, dragEnd) : undefined;
  const boxX2 = dragStart != null && dragEnd != null ? Math.max(dragStart, dragEnd) : undefined;
  const boxY1 = dragStartY != null && dragEndY != null ? Math.min(dragStartY, dragEndY) : undefined;
  const boxY2 = dragStartY != null && dragEndY != null ? Math.max(dragStartY, dragEndY) : undefined;
  const boxXActive = dragStart != null && dragEnd != null && dragStart !== dragEnd;
  const boxYActive = dragStartY != null && dragEndY != null && dragStartY !== dragEndY;
  const dragging = boxXActive || boxYActive;

  // Full-extent bounds for clamping zoom/pan, matching the default axis domains:
  // x uses the widened nice-tick bounds when present (else data min/max); y is
  // [0,100] in percent mode and [0, dataMax] in count mode. `enoughToZoom` gates
  // every gesture off for a degenerate (≤1 point / flat) range.
  const xExtent = useMemo<[number, number] | null>(() => {
    const ts = points.map((p) => p.ts as number).filter((v) => Number.isFinite(v));
    if (ts.length < 2) return null;
    return [Math.min(...ts), Math.max(...ts)];
  }, [points]);
  const xLo = hasTimeTicks ? timeTicks[0] : (xExtent?.[0] ?? 0);
  const xHi = hasTimeTicks ? timeTicks[timeTicks.length - 1] : (xExtent?.[1] ?? 0);
  const enoughToZoom = xHi > xLo;
  const countYMax = useMemo(() => {
    if (mode !== "count") return 0;
    let m = 0;
    for (const p of points) {
      for (const name of series) {
        const v = p[name];
        if (typeof v === "number" && v > m) m = v;
      }
    }
    return m;
  }, [mode, points, series]);
  const yBase: [number, number] = mode === "count" ? [0, countYMax > 0 ? countYMax : 1] : [0, 100];

  const wrapRef = useRef<HTMLDivElement>(null);
  const gestureRef = useRef<"none" | "box" | "pan">("none");
  // The plotting rectangle measured at gesture start (stable while a drag runs);
  // pixel→value conversions read it instead of guessing insets.
  const gestureRectRef = useRef<DOMRect | null>(null);
  // yBase can change with the data; a ref keeps the stable wheel listener current.
  const yBaseRef = useRef<[number, number]>(yBase);
  yBaseRef.current = yBase;

  // X-axis padding (matches the <XAxis padding> below): the data range is inset
  // this many pixels inside the grid rect on each side.
  const PAD_X = 16;
  const clamp01 = (v: number): number => Math.min(1, Math.max(0, v));
  // The real plot rectangle in client coords — the cartesian grid spans exactly
  // the plot area (its horizontal lines run the full width; the top/bottom lines
  // sit at the axis extremes).
  const plotRect = (): DOMRect | null =>
    wrapRef.current
      ?.querySelector<SVGGElement>(".recharts-cartesian-grid")
      ?.getBoundingClientRect() ?? null;
  // Cursor client-x → ts in the CURRENT x-domain, via the measured grid rect.
  const tsFromClientX = (clientX: number, rect: DOMRect): number => {
    const rMin = rect.left + PAD_X;
    const rMax = rect.right - PAD_X;
    const frac = rMax > rMin ? clamp01((clientX - rMin) / (rMax - rMin)) : 0.5;
    const [lo, hi] = zoomDomain ?? [xLo, xHi];
    return lo + frac * (hi - lo);
  };
  // Cursor client-y → value in the CURRENT y-domain (bottom→lo, top→hi).
  const yFromClientY = (clientY: number, rect: DOMRect): number => {
    const frac = rect.height > 0 ? clamp01((rect.bottom - clientY) / rect.height) : 0;
    const [lo, hi] = yZoomDomain ?? yBase;
    return lo + frac * (hi - lo);
  };

  // Recharts passes the chart state as the first mouse-handler arg (`activeLabel`
  // = ts under the cursor) and the native event as the second (read for the Shift
  // modifier, client coords and pixel movement).
  type ChartMouseState = { activeLabel?: string | number } | null;
  type ChartMouseEvent = React.MouseEvent<SVGElement> | undefined;
  const tsFromState = (s: ChartMouseState): number | null => {
    const v = s?.activeLabel;
    return typeof v === "number" ? v : typeof v === "string" ? Number(v) : null;
  };
  const handleMouseDown = (s: ChartMouseState, e: ChartMouseEvent) => {
    if (!enoughToZoom) return;
    gestureRectRef.current = plotRect();
    if (e?.shiftKey) {
      // Shift+drag → pan the zoomed view.
      gestureRef.current = "pan";
      setPanning(true);
      return;
    }
    // Plain drag → box-select zoom. Anchor one corner at the cursor.
    gestureRef.current = "box";
    const rect = gestureRectRef.current;
    if (e && rect) {
      setDragStart(tsFromClientX(e.clientX, rect));
      setDragStartY(yFromClientY(e.clientY, rect));
    } else {
      setDragStart(tsFromState(s));
      setDragStartY(null);
    }
    setDragEnd(null);
    setDragEndY(null);
  };
  const handleMouseMove = (s: ChartMouseState, e: ChartMouseEvent) => {
    if (gestureRef.current === "box") {
      const rect = gestureRectRef.current;
      if (e && rect) {
        setDragEnd(tsFromClientX(e.clientX, rect));
        setDragEndY(yFromClientY(e.clientY, rect));
      } else {
        const ts = tsFromState(s);
        if (ts != null) setDragEnd(ts);
      }
      return;
    }
    if (gestureRef.current === "pan" && e) {
      const dx = e.movementX ?? 0;
      const dy = e.movementY ?? 0;
      if (dx === 0 && dy === 0) return;
      const rect = gestureRectRef.current;
      const rangeX = rect ? Math.max(1, rect.width - 2 * PAD_X) : 1;
      const rangeY = rect ? Math.max(1, rect.height) : 1;
      // X: content follows the cursor, so the domain shifts opposite the drag.
      setZoomDomain((prev) => {
        const [lo, hi] = prev ?? [xLo, xHi];
        const next = panWindow(lo, hi, -(dx / rangeX) * (hi - lo), xLo, xHi);
        return isFullExtent(next[0], next[1], xLo, xHi) ? null : next;
      });
      // Y: dragging down reveals higher values (domain shifts up).
      const [yb0, yb1] = yBase;
      setYZoomDomain((prev) => {
        const [lo, hi] = prev ?? [yb0, yb1];
        const next = panWindow(lo, hi, (dy / rangeY) * (hi - lo), yb0, yb1);
        return isFullExtent(next[0], next[1], yb0, yb1) ? null : next;
      });
    }
  };
  const handleMouseUp = () => {
    if (gestureRef.current === "box") {
      // Commit each axis only if it actually spanned a range (so a plain click
      // or a 1-D drag doesn't collapse an axis to a zero-width window).
      if (dragStart != null && dragEnd != null && dragStart !== dragEnd) {
        setZoomDomain([Math.min(dragStart, dragEnd), Math.max(dragStart, dragEnd)]);
      }
      if (dragStartY != null && dragEndY != null && dragStartY !== dragEndY) {
        setYZoomDomain([Math.min(dragStartY, dragEndY), Math.max(dragStartY, dragEndY)]);
      }
    }
    gestureRef.current = "none";
    gestureRectRef.current = null;
    setPanning(false);
    setDragStart(null);
    setDragEnd(null);
    setDragStartY(null);
    setDragEndY(null);
  };
  const resetZoom = () => {
    setZoomDomain(null);
    setYZoomDomain(null);
  };

  // Wheel-to-zoom both axes toward the cursor. Registered as a NON-passive native
  // listener (React's onWheel is passive, so it can't preventDefault the page
  // scroll). The cursor's fractional position in the measured grid rect is mapped
  // into each live domain as the zoom anchor, so the point under the cursor stays
  // put. Deps re-bind when the x-extent changes.
  useEffect(() => {
    const el = wrapRef.current;
    if (!el || !enoughToZoom) return;
    const onWheel = (e: WheelEvent) => {
      const rect = plotRect();
      if (!rect) return;
      e.preventDefault();
      const factor = e.deltaY > 0 ? 1.2 : 1 / 1.2;
      const rMinX = rect.left + PAD_X;
      const rMaxX = rect.right - PAD_X;
      const fracX = rMaxX > rMinX ? clamp01((e.clientX - rMinX) / (rMaxX - rMinX)) : 0.5;
      const fracY = rect.height > 0 ? clamp01((rect.bottom - e.clientY) / rect.height) : 0.5;
      setZoomDomain((prev) => {
        const [lo, hi] = prev ?? [xLo, xHi];
        const anchor = lo + fracX * (hi - lo);
        const z = zoomWindow(lo, hi, anchor, factor);
        const c = clampWindow(z[0], z[1], xLo, xHi);
        if (c[1] - c[0] <= 0) return prev;
        return isFullExtent(c[0], c[1], xLo, xHi) ? null : c;
      });
      const [yb0, yb1] = yBaseRef.current;
      setYZoomDomain((prev) => {
        const [lo, hi] = prev ?? [yb0, yb1];
        const anchor = lo + fracY * (hi - lo);
        const z = zoomWindow(lo, hi, anchor, factor);
        const c = clampWindow(z[0], z[1], yb0, yb1);
        if (c[1] - c[0] <= 0) return prev;
        return isFullExtent(c[0], c[1], yb0, yb1) ? null : c;
      });
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [enoughToZoom, xLo, xHi]);

  // Effective X domain + ticks: when zoomed, recompute nice time ticks within
  // the zoom window (clipped to it); otherwise use the full-range niceTimeTicks
  // (or the auto domain for ≤1 point). The bespoke OverallLayer reads the axis
  // scale, so it re-clamps to whichever domain is active for free.
  const { xDomain, xTicks } = useMemo(() => {
    if (zoomDomain) {
      const [lo, hi] = zoomDomain;
      const ticks = niceTimeTicks(lo, hi).filter((tk) => tk >= lo && tk <= hi);
      return {
        xDomain: [lo, hi] as [number, number],
        xTicks: ticks.length > 1 ? ticks : undefined,
      };
    }
    return {
      xDomain: (hasTimeTicks
        ? [timeTicks[0], timeTicks[timeTicks.length - 1]]
        : ["dataMin", "dataMax"]) as [number, number] | [string, string],
      xTicks: hasTimeTicks ? timeTicks : undefined,
    };
  }, [zoomDomain, hasTimeTicks, timeTicks]);

  // Recharts orders the legend/lines by key, not our config order — force the
  // count series into the canonical Rules → Checks → Tests → Rows order so the
  // legend, lines, markers and tooltip all agree.
  if (mode === "count") {
    const order = ["Rules", "Checks", "Tests", "Rows"];
    const rank = (n: string) => {
      const i = order.indexOf(n);
      return i === -1 ? order.length : i;
    };
    series.sort((a, b) => rank(a) - rank(b));
  }

  // Count mode: clicking a legend item hides/shows that line. Track the hidden
  // series names; each <Line hide> reflects membership.
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const toggleSeries = (name: string) =>
    setHidden((cur) => toggleHidden(cur, name));
  // series name → marker shape, in series order (markers cycle by index). Used
  // both by the legend/lines and the tooltip so the icon always matches.
  // series name → (marker shape, neutral colour), both cycling by index so each
  // series gets a stable (shape, colour) pair shared by the line, legend icon
  // and tooltip marker.
  const seriesShapes: Record<string, Marker> = {};
  series.forEach((name, idx) => {
    seriesShapes[name] = MARKERS[idx % MARKERS.length];
  });
  const seriesColors = countSeriesColors(series);

  // With a single run there's no line segment to draw, so Recharts' default
  // dot can render nothing. Force a larger, always-painted dot in that case so
  // the lone point is visible.
  const singlePoint = points.length <= 1;
  const lineDot = singlePoint ? { r: 4 } : { r: 2 };
  // Percent-mode interpolation: stepAfter for the product over-time step series,
  // else centripetal Catmull-Rom (alpha 0.5) — natural-looking smoothness that,
  // unlike recharts' built-in "natural"/"monotone", passes through the points
  // without overshooting or looping back when runs cluster close in time.
  const lineType = step ? "stepAfter" : curveCatmullRom.alpha(0.5);

  // Product "Average" mode: the `data` series are the contributing member tables
  // (drawn dull + thin) and `overall` is the prominent foreground Average
  // trendline drawn on top via OverallLayer, with a toggleable legend below.
  const overallMode = overall != null;
  // Average value per run instant (0–100) + the latest, so the tooltip can show
  // the Average a hovered run contributes to.
  // Keyed by epoch-ms (matching the time axis' numeric label) so the tooltip can
  // look up the Average for a hovered run.
  const averageByDate: Record<string, number> = {};
  for (const p of overall ?? []) {
    if (p.value != null) averageByDate[Date.parse(p.run_date)] = p.value;
  }
  const avgVals = (overall ?? []).filter((p) => p.value != null);
  const latestAverage = avgVals.length
    ? (avgVals[avgVals.length - 1].value as number)
    : null;

  // The single-series "Overall DQ Score" chart (score/percent mode, one series,
  // no caller colour map) is coloured on a red(0%)→green(100%) ramp by value,
  // matching ScoreBox's scoreColor. Multi-series (colorMap) and count-mode
  // charts are left on their flat/categorical colours. Overall mode draws its
  // own foreground step layer, so the gradient single-series path is off.
  const isOverallScore =
    mode === "percent" && !colorMap && series.length === 1 && !overallMode;
  // The gradients map to the ABSOLUTE 0–100 axis range (not the data's own
  // min/max) so a given score always renders the same hue: a 50% point is
  // yellow regardless of its neighbours. We use gradientUnits="userSpaceOnUse"
  // with y coordinates spanning the plot area in SVG pixels — the chart is a
  // fixed 200px tall with an 8px top margin and a ~30px x-axis, so the plot
  // area runs from y=PLOT_TOP (the 100% line, green) to y=PLOT_BOTTOM (the 0%
  // line, red) — see the plot-geometry constants defined above.
  const scoreGradientId = "score-trend-gradient";
  // Vertical gradient over the plot's y-range: top (100%) green, bottom (0%)
  // red, with a yellow midpoint — the ramp's 1/0.5/0 hues.
  const scoreGradientStops = [
    { offset: "0%", color: scoreColor(1) },
    { offset: "50%", color: scoreColor(0.5) },
    { offset: "100%", color: scoreColor(0) },
  ];
  // Separate fill gradient: same vertical red→green ramp but semi-transparent
  // so the filled area reads as a tint under the connecting line. Green at the
  // top (high score) fading to red at the bottom (low score).
  const scoreFillGradientId = "score-trend-fill-gradient";
  const scoreFillGradientStops = [
    { offset: "0%", color: scoreColor(1), opacity: 0.35 },
    { offset: "50%", color: scoreColor(0.5), opacity: 0.18 },
    { offset: "100%", color: scoreColor(0), opacity: 0.05 },
  ];

  // Overall-mode legend, shown just beneath the chart: "Overall" (foreground
  // swatch) plus one chip per contributing table (its dull colour). Clicking a
  // chip toggles that element's visibility via the shared `hidden` set.
  const overallLegend = overallMode ? (
    <ul className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 px-1 text-[11px]">
      {[overallLabel, ...series].map((name) => {
        const isOverall = name === overallLabel;
        const swatch = isOverall
          ? "var(--foreground)"
          : (colorMap?.[name] ?? CHART_FALLBACK[0]);
        const isHidden = hidden.has(name);
        return (
          <li
            key={name}
            className={`flex cursor-pointer items-center gap-1.5 ${
              isHidden ? "opacity-40 line-through" : ""
            }`}
            onClick={() => toggleSeries(name)}
          >
            {isOverall ? (
              // Average: a filled foreground dot, matching its circular markers.
              <span
                className="inline-block h-2.5 w-2.5 shrink-0 rounded-full"
                style={{ background: swatch }}
              />
            ) : (
              // Member tables: an × glyph, matching their plot markers.
              <XGlyph color={swatch} />
            )}
            <span
              className={isOverall ? "font-medium text-foreground" : ""}
              style={isOverall ? undefined : { color: swatch }}
            >
              {isOverall ? overallLabel : shortLabel(name)}
            </span>
          </li>
        );
      })}
    </ul>
  ) : null;

  const collapsible = onToggleCollapse != null;
  const body =
    points.length === 0 ? (
      <p className="text-sm text-muted-foreground">{t("resultsUi.noRunsYet")}</p>
    ) : (
      <>
      <div ref={wrapRef} className="relative min-w-0 overflow-hidden rounded-md border p-2 [&_.recharts-surface]:outline-none [&_.recharts-wrapper]:outline-none [&_svg]:outline-none [&_*:focus]:outline-none [&_*:focus-visible]:outline-none">
          {(zoomed || yZoomed) && (
            <button
              type="button"
              onClick={resetZoom}
              className="absolute right-2 top-2 z-10 rounded border bg-background/80 px-2 py-0.5 text-[11px] text-muted-foreground shadow-sm backdrop-blur hover:text-foreground"
            >
              {t("resultsUi.resetZoom")}
            </button>
          )}
          <ResponsiveContainer width="100%" height={200} minWidth={0}>
            <ComposedChart
              data={points}
              margin={{ top: 8, right: 12, left: 0, bottom: 0 }}
              onMouseDown={handleMouseDown}
              onMouseMove={handleMouseMove}
              onMouseUp={handleMouseUp}
              onMouseLeave={handleMouseUp}
              style={
                dragging
                  ? { cursor: "crosshair", userSelect: "none" }
                  : panning
                    ? { cursor: "grabbing", userSelect: "none" }
                    : enoughToZoom
                      ? { cursor: "crosshair" }
                      : undefined
              }
            >
              {isOverallScore && !yZoomed && (
                <defs>
                  {/* userSpaceOnUse + plot-area y coords → the ramp is keyed to
                      the absolute 0–100 axis, not the line's own extent. Under a
                      y-zoom (B2-17) the axis no longer spans 0–100, so these
                      pixel-keyed stops would mis-map; the gradient is dropped
                      then and the line/fill fall back to a flat colour (the dots
                      stay value-coloured — they key on each point's own score,
                      not pixels, so they remain correct). */}
                  <linearGradient
                    id={scoreGradientId}
                    gradientUnits="userSpaceOnUse"
                    x1="0"
                    y1={PLOT_TOP}
                    x2="0"
                    y2={PLOT_BOTTOM}
                  >
                    {scoreGradientStops.map((s) => (
                      <stop
                        key={s.offset}
                        offset={s.offset}
                        stopColor={s.color}
                      />
                    ))}
                  </linearGradient>
                  <linearGradient
                    id={scoreFillGradientId}
                    gradientUnits="userSpaceOnUse"
                    x1="0"
                    y1={PLOT_TOP}
                    x2="0"
                    y2={PLOT_BOTTOM}
                  >
                    {scoreFillGradientStops.map((s) => (
                      <stop
                        key={s.offset}
                        offset={s.offset}
                        stopColor={s.color}
                        stopOpacity={s.opacity}
                      />
                    ))}
                  </linearGradient>
                </defs>
              )}
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="var(--border)"
                vertical={false}
              />
              <XAxis
                // Time-proportional axis: x is the epoch-ms `ts`, so spacing
                // reflects real elapsed time between runs (not equal slots).
                // We pass explicit evenly-spaced `ticks` (niceTimeTicks) so the
                // axis shows regular intervals rather than one label per run; the
                // domain widens to the enclosing tick boundaries so points sit
                // within. ≤1 point falls back to the auto time axis.
                dataKey="ts"
                type="number"
                scale="time"
                allowDataOverflow={zoomed}
                domain={xDomain}
                ticks={xTicks}
                tick={{ fontSize: 11, fill: "var(--muted-foreground)" }}
                tickMargin={8}
                minTickGap={40}
                tickFormatter={(v) => formatTs(v as number)}
                stroke="var(--border)"
                // Inset the first/last points from the plot edges so values
                // (and their labels) don't sit flush against the borders.
                padding={{ left: 16, right: 16 }}
              />
              <YAxis
                // Controlled y-domain: the committed y-zoom window when set
                // (percent mode only), else the mode default. allowDataOverflow
                // clips points outside the zoom window (B2-17).
                domain={yZoomDomain ?? (mode === "count" ? [0, "auto"] : [0, 100])}
                allowDataOverflow={yZoomed}
                tick={{ fontSize: 11, fill: "var(--muted-foreground)" }}
                tickFormatter={
                  mode === "count" ? (v) => `${v}` : (v) => `${v}%`
                }
                width={44}
                allowDecimals={false}
                stroke="var(--border)"
              />
              {/* cursor={false} removes Recharts' default opaque cursor box on
                  click; the controlled TrendTooltip replaces the white box. */}
              {/* Pin the tooltip near the top of the plot (stable y) so it
                  follows the cursor horizontally but doesn't snap above/below
                  as the pointer moves up and down. */}
              <Tooltip
                cursor={false}
                // Suppress the tooltip mid drag-select / pan so it doesn't fight
                // the zoom-region highlight; restore default behaviour otherwise.
                active={dragging || panning ? false : undefined}
                isAnimationActive={false}
                offset={12}
                position={{ y: 8 }}
                content={(props) => (
                  <TrendTooltip
                    active={props.active}
                    payload={
                      props.payload as unknown as
                        | TooltipPayloadEntry[]
                        | undefined
                    }
                    label={props.label}
                    mode={mode}
                    seriesShapes={seriesShapes}
                    seriesColors={seriesColors}
                    xMarkers={overallMode}
                    averageByDate={overallMode ? averageByDate : undefined}
                    latestAverage={overallMode ? latestAverage : undefined}
                    overallLabel={overallLabel}
                  />
                )}
              />
              {mode === "count" && (
                <Legend
                  // Recharts' default legend orders by internal registration and
                  // this version omits the `payload` prop, so render the legend
                  // ourselves: sort the items into the canonical order, draw the
                  // matching marker shape, grey hidden series, and toggle on click.
                  content={(props) => {
                    const order = ["Rules", "Checks", "Tests", "Rows"];
                    const items = [
                      ...((props.payload ?? []) as Array<{ value?: unknown }>),
                    ].sort((a, b) => {
                      const rank = (v: unknown) => {
                        const i = order.indexOf(String(v ?? ""));
                        return i === -1 ? order.length : i;
                      };
                      return rank(a.value) - rank(b.value);
                    });
                    return (
                      <ul className="flex flex-wrap items-center justify-center gap-x-4 gap-y-1 text-[11px]">
                        {items.map((it) => {
                          const name = String(it.value ?? "");
                          const isHidden = hidden.has(name);
                          return (
                            <li
                              key={name}
                              className={`flex cursor-pointer items-center gap-1.5 ${
                                isHidden ? "opacity-40 line-through" : ""
                              }`}
                              onClick={() => toggleSeries(name)}
                            >
                              <MarkerIcon
                                shape={seriesShapes[name] ?? "circle"}
                                fill={seriesColors[name] ?? COUNT_COLORS[0]}
                              />
                              <span style={{ color: seriesColors[name] }}>
                                {name}
                              </span>
                            </li>
                          );
                        })}
                      </ul>
                    );
                  }}
                />
              )}
              {series.map((name, idx) => {
                // Overall DQ Score: a filled red→green gradient area under a
                // gradient-stroked line, with each dot painted in its own score
                // colour ALWAYS (not just the hover/active dot) so the value
                // reads at a glance without hovering.
                if (isOverallScore) {
                  const scoreDot = (props: {
                    cx?: number;
                    cy?: number;
                    value?: number;
                    payload?: Record<string, unknown>;
                  }) => {
                    const { cx, cy, value, payload } = props;
                    if (cx == null || cy == null)
                      return <g key={`${cx}-${cy}`} />;
                    // Recharts doesn't always pass `value` to a custom dot; fall
                    // back to the row's own value so the dot is ALWAYS coloured
                    // by its score (not just the active/hover dot).
                    const v =
                      typeof value === "number"
                        ? value
                        : typeof payload?.[name] === "number"
                          ? (payload[name] as number)
                          : null;
                    // pivot scales pass_rate to 0–100; scoreColor wants 0–1.
                    const fill = v == null ? scoreColor(null) : scoreColor(v / 100);
                    return (
                      <circle
                        key={`${cx}-${cy}`}
                        cx={cx}
                        cy={cy}
                        r={4}
                        fill={fill}
                        stroke={fill}
                        data-testid="score-trend-dot"
                      />
                    );
                  };
                  // activeDot (hover) reuses the same value-coloured renderer at
                  // a slightly larger radius, so hover matches the static dots.
                  const scoreActiveDot = (props: {
                    cx?: number;
                    cy?: number;
                    value?: number;
                    payload?: Record<string, unknown>;
                  }) => {
                    const { cx, cy, value, payload } = props;
                    if (cx == null || cy == null)
                      return <g key={`a-${cx}-${cy}`} />;
                    const v =
                      typeof value === "number"
                        ? value
                        : typeof payload?.[name] === "number"
                          ? (payload[name] as number)
                          : null;
                    const fill = v == null ? scoreColor(null) : scoreColor(v / 100);
                    return (
                      <circle key={`a-${cx}-${cy}`} cx={cx} cy={cy} r={5} fill={fill} stroke={fill} />
                    );
                  };
                  return (
                    <Area
                      key={name}
                      type={lineType}
                      dataKey={name}
                      // Flat fallback while y-zoomed (the pixel-keyed gradient
                      // would mis-map — see the guarded <defs> above).
                      stroke={yZoomed ? "var(--foreground)" : `url(#${scoreGradientId})`}
                      strokeWidth={2}
                      fill={yZoomed ? "var(--foreground)" : `url(#${scoreFillGradientId})`}
                      fillOpacity={yZoomed ? 0.06 : undefined}
                      connectNulls
                      dot={scoreDot}
                      activeDot={scoreActiveDot}
                      isAnimationActive={animate}
                      animationDuration={animate ? 900 : 0}
                      animationEasing="ease-out"
                    />
                  );
                }
                // Count series: a distinct but muted/neutral colour per series,
                // ALSO told apart by marker SHAPE. The colour is applied to the
                // line stroke, the markers, the legend icon and (via
                // seriesColors) the tooltip marker so they all match.
                if (mode === "count") {
                  const shape = seriesShapes[name];
                  const color = seriesColors[name];
                  return (
                    <Line
                      key={name}
                      type={lineType}
                      dataKey={name}
                      stroke={color}
                      strokeWidth={0.75}
                      connectNulls
                      hide={hidden.has(name)}
                      dot={markerDot(shape, 3.5, color)}
                      activeDot={markerDot(shape, 5, color)}
                      legendType={shape}
                      isAnimationActive={false}
                    />
                  );
                }
                // Percent multi-series (By dimension / By severity): categorical
                // colour per series from the caller's colorMap.
                const stroke =
                  colorMap?.[name] ??
                  CHART_FALLBACK[idx % CHART_FALLBACK.length];
                // In Average mode the per-table lines are intentionally
                // recessive: thin, dull (lower opacity), smooth, with × markers
                // (always shown — a single-run table has no segment, so without a
                // marker it would be invisible) so the bold Average line leads.
                if (overallMode) {
                  return (
                    <Line
                      key={name}
                      type="monotone"
                      dataKey={name}
                      stroke={stroke}
                      strokeWidth={1}
                      strokeOpacity={0.55}
                      connectNulls
                      hide={hidden.has(name)}
                      dot={xMarkerDot(3.5, stroke, 0.7)}
                      activeDot={xMarkerDot(4.5, stroke, 1)}
                      isAnimationActive={false}
                    />
                  );
                }
                return (
                  <Line
                    key={name}
                    type={lineType}
                    dataKey={name}
                    stroke={stroke}
                    strokeWidth={2}
                    strokeDasharray={
                      dashedSeries?.includes(name) ? "5 4" : undefined
                    }
                    connectNulls
                    dot={lineDot}
                    activeDot={{ r: 4 }}
                    isAnimationActive={false}
                    // Clicking a series line toggles that facet in the drilldown
                    // (only when the caller wires onSeriesClick).
                    onClick={
                      onSeriesClick ? () => onSeriesClick(name) : undefined
                    }
                    style={onSeriesClick ? { cursor: "pointer" } : undefined}
                  />
                );
              })}
              {overallMode && !hidden.has(overallLabel) && (
                <OverallLayer points={overall ?? []} />
              )}
              {/* Subtle version-increment markers (#65): a dashed vertical rule
                  with a small "v{n}" tag at each run where the binding version
                  bumped. Clipped when the run falls outside the zoom window. */}
              {(versionMarkers ?? []).map((m) => {
                const x = Date.parse(m.run_date);
                if (!Number.isFinite(x)) return null;
                return (
                  <ReferenceLine
                    key={`vm-${m.run_date}`}
                    x={x}
                    stroke="var(--muted-foreground)"
                    strokeDasharray="2 3"
                    strokeOpacity={0.5}
                    ifOverflow="hidden"
                    label={{
                      value: m.label,
                      position: "insideTopLeft",
                      fontSize: 10,
                      fill: "var(--muted-foreground)",
                    }}
                  />
                );
              })}
              {/* Visible selection rectangle drawn while a plain drag is in
                  progress; the region is committed on mouse-up and cleared on
                  release. Edges left undefined span the full axis, so a 1-D drag
                  reads as a full-width/height band. */}
              {dragging && (
                <ReferenceArea
                  x1={boxXActive ? boxX1 : undefined}
                  x2={boxXActive ? boxX2 : undefined}
                  y1={boxYActive ? boxY1 : undefined}
                  y2={boxYActive ? boxY2 : undefined}
                  stroke="var(--foreground)"
                  strokeOpacity={0.3}
                  fill="var(--foreground)"
                  fillOpacity={0.08}
                  ifOverflow="hidden"
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
        {overallLegend}
      </>
      );

  const titleText = title && (
    <span className="text-xs uppercase tracking-wide text-muted-foreground">
      {title}
    </span>
  );

  const helpIcon = info ? (
    <TooltipProvider delayDuration={200}>
      <UITooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            aria-label={t("resultsUi.whatDoTheseMeanAria")}
            onClick={(e) => e.stopPropagation()}
            className="text-muted-foreground hover:text-foreground"
          >
            <CircleHelp className="h-3.5 w-3.5" />
          </button>
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs">
          {info}
        </TooltipContent>
      </UITooltip>
    </TooltipProvider>
  ) : null;

  const titleRow =
    titleText || helpIcon || collapsible ? (
      collapsible ? (
        <button
          type="button"
          onClick={onToggleCollapse}
          aria-expanded={!collapsed}
          className="group flex w-full items-center gap-1.5 text-left"
        >
          {titleText}
          {helpIcon}
          <ChevronDown
            className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${
              collapsed ? "-rotate-90" : ""
            }`}
          />
        </button>
      ) : (
        <div className="flex items-center gap-1.5">
          {titleText}
          {helpIcon}
        </div>
      )
    ) : null;

  return (
    <div className="space-y-2">
      {titleRow}
      {collapsible ? (
        <CollapseRegion open={!collapsed}>{body}</CollapseRegion>
      ) : (
        body
      )}
    </div>
  );
}
