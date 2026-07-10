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

  // A time-like label reads as a trend (line); anything else is a comparison
  // across categories (bar).
  const kind = TIME_RE.test(columns[labelIdx] ?? "") ? "line" : "bar";
  return { kind, labelIdx, valueIdx, valueName: columns[valueIdx] ?? "value", data };
}

/** Compact bar/line chart of a Genie result, shown alongside the table when the
 *  data has a clear category/time axis and a numeric measure. Renders nothing
 *  when the result isn't a good fit for a chart. */
export function GenieResultChart({
  columns,
  rows,
}: {
  columns: string[];
  rows: (string | null)[][];
}) {
  const { t } = useTranslation();
  const plan = useMemo(() => planChart(columns, rows), [columns, rows]);
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
