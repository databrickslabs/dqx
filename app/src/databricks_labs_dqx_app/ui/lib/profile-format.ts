/**
 * Pure formatting helpers for the monitored-table Profile tab's dqlake-style
 * column cards. No React, no i18n — display strings live in the components
 * that consume these (via `t()`), this module only shapes/classifies values.
 */

export type StatTypeGroup = "numeric" | "string" | "timestamp" | "boolean" | "other";

/** Map a Spark/UC type name (e.g. "bigint", "string", "decimal(10,2)") to a stat group. */
export function typeGroup(sparkType: string | undefined): StatTypeGroup {
  if (!sparkType) return "other";
  const t = sparkType.toLowerCase();
  if (t === "boolean") return "boolean";
  if (t.startsWith("timestamp") || t === "date") return "timestamp";
  if (
    t === "tinyint" ||
    t === "smallint" ||
    t === "int" ||
    t === "integer" ||
    t === "bigint" ||
    t === "float" ||
    t === "double" ||
    t.startsWith("decimal")
  ) {
    return "numeric";
  }
  if (t === "string" || t.startsWith("varchar") || t.startsWith("char")) return "string";
  return "other";
}

/** Pill colour classes by stat group — dark-mode aware. */
export function typePillClasses(group: StatTypeGroup): string {
  switch (group) {
    case "numeric":
      return "bg-blue-50 text-blue-800 border-blue-200 dark:bg-blue-950/40 dark:text-blue-300 dark:border-blue-800/60";
    case "string":
      return "bg-green-50 text-green-800 border-green-200 dark:bg-green-950/40 dark:text-green-300 dark:border-green-800/60";
    case "timestamp":
      return "bg-amber-50 text-amber-800 border-amber-200 dark:bg-amber-950/40 dark:text-amber-300 dark:border-amber-800/60";
    case "boolean":
      return "bg-purple-50 text-purple-800 border-purple-200 dark:bg-purple-950/40 dark:text-purple-300 dark:border-purple-800/60";
    case "other":
      return "bg-muted text-muted-foreground border-border";
  }
}

/** Completeness bar tone: traffic-light by percentage. */
export function completeBarClass(pct: number): string {
  if (pct >= 95) return "bg-green-600";
  if (pct >= 50) return "bg-amber-600";
  return "bg-red-600";
}

/** Distinct/unique bars are neutral — never red/green. */
export const NEUTRAL_BAR_CLASS = "bg-slate-500";

/** Compact number: 1,234,567 → "1.2M"; 12,345 → "12.3k"; 234 → "234". */
export function compactNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1).replace(/\.0$/, "")}k`;
  return n.toString();
}

/** Format an arbitrary stat value for display. */
export function fmtStat(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "number") {
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toFixed(4);
  }
  return String(v);
}

/** Percentage formatter: >=10% -> 0 decimals, otherwise 1 decimal, >=99.95 -> "100%". */
export function fmtPct(num: number, total: number | null | undefined): string {
  if (!total || total <= 0) return "—";
  const p = (num / total) * 100;
  if (p >= 99.95) return "100%";
  if (p >= 10) return `${p.toFixed(0)}%`;
  return `${p.toFixed(1)}%`;
}
