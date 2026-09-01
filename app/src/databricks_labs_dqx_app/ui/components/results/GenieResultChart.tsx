import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

// Ported from dqlake's `components/results/GenieResultChart.tsx` (the spec —
// keep the auto-plan heuristics aligned with it). Sanctioned deviation: the
// caption via t(). The pure helpers (`planChart`, `toNumber`) are exported
// for the bun tests.

const MAX_POINTS = 12;

/** Strip a trailing % and parse; returns null when the cell isn't a number. */
export function toNumber(cell: string | null): number | null {
  if (cell == null) return null;
  const s = cell.trim().replace(/%$/, "");
  if (s === "") return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

/** A column whose cells are JSON objects (e.g. the to_json'd `failing_record`)
 *  must never be a chart axis — its values aren't a category, they're a record. */
function isJsonObjectColumn(rows: (string | null)[][], ci: number): boolean {
  for (const r of rows) {
    const cell = r[ci];
    if (cell == null || cell === "") continue;
    return cell.trim().startsWith("{");
  }
  return false;
}

/** True for most non-null cells of a column being numeric. */
function isNumericColumn(rows: (string | null)[][], ci: number): boolean {
  let seen = 0;
  let numeric = 0;
  for (const r of rows) {
    const cell = r[ci];
    if (cell == null || cell === "") continue;
    seen++;
    if (toNumber(cell) != null) numeric++;
  }
  return seen > 0 && numeric / seen >= 0.8;
}

const TIME_RE = /(run\s*ts|run\s*date|timestamp|\bdate\b|\btime\b|month|day|week|year)/i;
// Prefer these as the value when several numeric columns are present.
const VALUE_PREF_RE = /(pass.?rate|rate|score|pct|percent|delta|count|failed|tests|total)/i;
// Identifier-ish columns are never a measure even when they parse as numbers
// (e.g. record_key, rule_id) — charting them produces nonsense.
const ID_RE = /(^id$|_id$|record_key|^key$|_key$|version)/i;

// At/above this many points a categorical x-axis is treated as an ordered
// sequence (a trend) rather than discrete comparison bars: a dozen cramped,
// angled bar labels read worse than a line, and long axes are almost always
// ordered in practice (time buckets, top-N rankings). Below it, a few
// categories stay as bars.
const LINE_MIN_POINTS = 8;

/** True when the sequence is sorted non-decreasing OR non-increasing — i.e. an
 *  already-ordered numeric axis (a trend), not a scrambled set of categories. */
function isMonotonic(nums: number[]): boolean {
  if (nums.length < 2) return false;
  let nonDecreasing = true;
  let nonIncreasing = true;
  for (let i = 1; i < nums.length; i++) {
    if (nums[i] < nums[i - 1]) nonDecreasing = false;
    if (nums[i] > nums[i - 1]) nonIncreasing = false;
  }
  return nonDecreasing || nonIncreasing;
}

/**
 * Line vs bar. A line reads as a trend over an ORDERED x-axis; a bar reads as a
 * comparison across discrete, unordered categories. dqlake only ever picked a
 * line when the label column NAME matched TIME_RE; this broadens it to any
 * ordered x-axis. Choose a line when:
 *  - the column NAME is time-like (run_ts / date / month / year / ...), OR
 *  - the labels are all numeric AND already monotonic (an ordered numeric axis
 *    such as a bucket index or day-of-month), OR
 *  - there are many points (>= LINE_MIN_POINTS): a long category axis is
 *    unreadable as bars and is almost always ordered.
 * Otherwise (a small, unordered categorical) keep a bar.
 *
 * NOTE: the numeric-monotonic branch is a defensive guard — planChart requires
 * the label axis to be a NON-numeric column, so all-numeric labels normally
 * never reach here; it still fires for a numeric-looking column that fell below
 * isNumericColumn's threshold. Exported (with the helper) for the unit tests.
 */
export function chooseChartKind(labelColumn: string, labels: string[]): "bar" | "line" {
  if (TIME_RE.test(labelColumn)) return "line";
  const nums = labels.map((l) => toNumber(l));
  const allNumeric = nums.every((n) => n != null);
  if (allNumeric && isMonotonic(nums as number[])) return "line";
  if (labels.length >= LINE_MIN_POINTS) return "line";
  return "bar";
}

/** Themed tooltip — recharts' default is a solid white box, which hides the
 *  (often light) series text in dark mode. Match the app's popover tokens. */
function ChartTooltip({
  active,
  payload,
  label,
  valueName,
}: {
  active?: boolean;
  payload?: { value?: number | string }[];
  label?: string | number;
  valueName: string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-md border bg-popover p-2 text-xs text-popover-foreground shadow-md">
      <p className="font-medium">{label}</p>
      <p className="text-muted-foreground">
        {valueName}: <span className="text-popover-foreground">{payload[0]?.value}</span>
      </p>
    </div>
  );
}

export type Plan = {
  kind: "bar" | "line";
  labelIdx: number;
  valueIdx: number;
  valueName: string;
  data: { label: string; value: number }[];
};

/** Decide whether a Genie result is worth charting and how. Returns null when a
 *  plain table is the better representation (e.g. an exploded record shape,
 *  which has no single numeric value column). */
export function planChart(columns: string[], rows: (string | null)[][]): Plan | null {
  if (rows.length < 2 || columns.length < 2) return null;

  const numericIdx: number[] = [];
  for (let ci = 0; ci < columns.length; ci++) {
    if (isNumericColumn(rows, ci)) numericIdx.push(ci);
  }
  // Candidate measures = numeric columns that aren't identifiers.
  const measureIdx = numericIdx.filter((ci) => !ID_RE.test(columns[ci] ?? ""));
  if (measureIdx.length === 0) return null;

  // Value column: a preferred-named measure, else the last measure.
  const valueIdx =
    measureIdx.find((ci) => VALUE_PREF_RE.test(columns[ci] ?? "")) ??
    measureIdx[measureIdx.length - 1];

  // Label column: the first non-numeric, non-JSON column; bail if there's no
  // natural category axis (everything numeric, or only a JSON-record column).
  const labelIdx = columns.findIndex(
    (_, ci) =>
      ci !== valueIdx &&
      !numericIdx.includes(ci) &&
      !isJsonObjectColumn(rows, ci),
  );
  if (labelIdx === -1) return null;

  const data: { label: string; value: number }[] = [];
  for (const r of rows.slice(0, MAX_POINTS)) {
    const value = toNumber(r[valueIdx]);
    const label = r[labelIdx];
    if (value == null || label == null) continue;
    data.push({ label, value });
  }
  if (data.length < 2) return null;

  // Line for an ordered x-axis (time-like name / monotonic-numeric / many
  // points), bar for a small unordered categorical — see chooseChartKind.
  const kind = chooseChartKind(columns[labelIdx] ?? "", data.map((d) => d.label));
  return { kind, labelIdx, valueIdx, valueName: columns[valueIdx] ?? "value", data };
}

/** Compact bar/line chart of a Genie result, shown alongside the table when the
 *  data has a clear category/time axis and a numeric measure. Renders nothing
 *  when the result isn't a good fit for a chart. */
export function GenieResultChart({
  columns,
  rows,
  plan: planProp,
}: {
  columns: string[];
  rows: (string | null)[][];
  /** Precomputed plan from the parent — the parent already runs planChart to
   *  decide whether to render this chart at all, so passing the result avoids
   *  recomputing it here (B2-19). Omit to have the chart compute its own. */
  plan?: Plan | null;
}) {
  const { t } = useTranslation();
  // `??` short-circuits: planChart only runs when the parent didn't supply a
  // plan, so a provided plan is reused without recomputation.
  const plan = useMemo(() => planProp ?? planChart(columns, rows), [planProp, columns, rows]);
  if (!plan) return null;

  const tickColor = "var(--muted-foreground)";
  const axisProps = {
    tick: { fontSize: 10, fill: tickColor },
    stroke: "var(--border)",
  } as const;

  return (
    <div className="mt-2 rounded-md border p-2">
      <p className="mb-1 px-1 text-[11px] font-medium text-muted-foreground">
        {t("genie.chartCaption", {
          value: plan.valueName,
          label: columns[plan.labelIdx],
        })}
      </p>
      <ResponsiveContainer width="100%" height={160}>
        {plan.kind === "bar" ? (
          <BarChart data={plan.data} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
            <XAxis dataKey="label" {...axisProps} interval={0} angle={-15} textAnchor="end" height={44} />
            <YAxis {...axisProps} width={36} />
            <Tooltip
              cursor={{ fill: "var(--muted)", opacity: 0.4 }}
              content={<ChartTooltip valueName={plan.valueName} />}
            />
            <Bar dataKey="value" name={plan.valueName} fill="var(--primary)" radius={[3, 3, 0, 0]} />
          </BarChart>
        ) : (
          <LineChart data={plan.data} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
            <XAxis dataKey="label" {...axisProps} interval="preserveStartEnd" height={28} />
            <YAxis {...axisProps} width={36} />
            <Tooltip content={<ChartTooltip valueName={plan.valueName} />} />
            <Line
              type="monotone"
              dataKey="value"
              name={plan.valueName}
              stroke="var(--primary)"
              strokeWidth={2}
              dot={{ r: 2 }}
            />
          </LineChart>
        )}
      </ResponsiveContainer>
    </div>
  );
}
