/**
 * Low-code operator/aggregate vocabulary + value-cell shapes. Ported from
 * dqlake's `ui/lib/lowcodeOperators.ts` verbatim (framework-agnostic).
 *
 * Families here are UPPERCASE (NUMERIC/TEXTUAL/TEMPORAL/BOOLEAN/ANY) — the
 * low-code builder's own vocabulary. The Rules-Registry `RuleSlot.family`
 * axis is lowercase (numeric/text/temporal/boolean/any); the form maps
 * between them via `slotFamilyToLowcode` in `lowcodeCompile.ts`.
 */
export type Family = "NUMERIC" | "TEXTUAL" | "TEMPORAL" | "BOOLEAN" | "ANY";

const VALIDITY_OPS = ["is a valid", "is not a valid"];

// The types a steward can validate a text column against. Picked by the
// value cell when one of the VALIDITY_OPS is in play. Each entry's `value`
// is what gets persisted in the AST; `sqlType` is the TRY_CAST target.
export const VALIDITY_TYPES = [
  { value: "tinyint", label: "tinyint", sqlType: "TINYINT" },
  { value: "smallint", label: "smallint", sqlType: "SMALLINT" },
  { value: "int", label: "int", sqlType: "INT" },
  { value: "bigint", label: "bigint", sqlType: "BIGINT" },
  { value: "float", label: "float", sqlType: "FLOAT" },
  { value: "double", label: "double", sqlType: "DOUBLE" },
  { value: "decimal", label: "decimal", sqlType: "DECIMAL" },
  { value: "date", label: "date", sqlType: "DATE" },
  { value: "timestamp", label: "timestamp", sqlType: "TIMESTAMP" },
  { value: "timestamp_ntz", label: "timestamp_ntz", sqlType: "TIMESTAMP_NTZ" },
  { value: "boolean", label: "boolean", sqlType: "BOOLEAN" },
  { value: "binary", label: "binary", sqlType: "BINARY" },
] as const;

export const VALIDITY_TYPE_VALUES = VALIDITY_TYPES.map((t) => t.value);
export const VALIDITY_SQL_TYPE: Record<string, string> = Object.fromEntries(
  VALIDITY_TYPES.map((t) => [t.value, t.sqlType]),
);

// Order within each family mirrors what a Data Quality steward reaches for
// first. Universal operators (is null / is not null, generic equality) live
// in ANY only and are not duplicated into every family.
// Operators that invoke a Databricks AI (Foundation Model) SQL function —
// real per-row cost + latency, and only available on AI-Functions-enabled
// workspaces. Grouped + flagged separately in the operator dropdown.
export const AI_OPS = new Set(["has positive sentiment", "has negative sentiment"]);

export const OPERATORS_BY_FAMILY: Record<Family, string[]> = {
  NUMERIC: [
    "between",
    "=",
    "!=",
    ">=",
    ">",
    "<=",
    "<",
    "in",
    "not in",
    "is positive",
    "is negative",
    "is non-negative",
    "is a whole number",
    "is a multiple of",
    "passes luhn check",
  ],
  TEXTUAL: [
    "equals",
    "not equals",
    "contains",
    "does not contain",
    "starts with",
    "ends with",
    "in",
    "not in",
    "matches regex",
    "does not match regex",
    "has length",
    "is longer than",
    "is shorter than",
    "length between",
    "is not empty",
    "is empty",
    "contains only digits",
    "is uppercase",
    "is lowercase",
    "is a valid email",
    "is a valid uuid",
    "is a valid ipv4",
    "passes luhn check",
    "has leading or trailing whitespace",
    "has no leading or trailing whitespace",
    ...VALIDITY_OPS,
    "has positive sentiment",
    "has negative sentiment",
  ],
  TEMPORAL: [
    "on or after",
    "on or before",
    "after",
    "before",
    "between",
    "is in last",
    "is in the future",
    "is in the past",
    "is today",
    "=",
    "!=",
  ],
  BOOLEAN: ["is true", "is false"],
  ANY: ["is null", "is not null", "=", "!=", "in", "not in", "is not empty", "is empty"],
};

// Ordered by how often a DQ steward reaches for them.
export const AGGREGATES = [
  "count",
  "count_distinct",
  "null_rate",
  "approx_count_distinct",
  "sum",
  "avg",
  "min",
  "max",
  "stddev",
  "stddev_samp",
  "variance",
  "var_samp",
  "median",
  "percentile",
  "percentile_approx",
  "bool_and",
  "bool_or",
  "any_value",
  "mode",
] as const;

// Which column families each aggregate accepts. "ANY" means every family.
export const AGGREGATE_INPUT_FAMILIES: Record<string, Family[] | "ANY"> = {
  count: "ANY",
  count_distinct: "ANY",
  null_rate: "ANY",
  approx_count_distinct: "ANY",
  any_value: "ANY",
  mode: "ANY",
  sum: ["NUMERIC"],
  avg: ["NUMERIC"],
  stddev: ["NUMERIC"],
  stddev_samp: ["NUMERIC"],
  variance: ["NUMERIC"],
  var_samp: ["NUMERIC"],
  median: ["NUMERIC"],
  percentile: ["NUMERIC"],
  percentile_approx: ["NUMERIC"],
  min: ["NUMERIC", "TEMPORAL", "TEXTUAL"],
  max: ["NUMERIC", "TEMPORAL", "TEXTUAL"],
  bool_and: ["BOOLEAN"],
  bool_or: ["BOOLEAN"],
};

export const AGGREGATES_TAKING_PARAM = new Set(["percentile", "percentile_approx"]);

export function aggregateAcceptsFamily(agg: string, family: Family): boolean {
  const spec = AGGREGATE_INPUT_FAMILIES[agg];
  if (!spec) return false;
  if (spec === "ANY") return true;
  return spec.includes(family);
}

export function aggregateOutputFamily(agg: string, colFamily: Family): Family {
  if (
    [
      "count",
      "count_distinct",
      "null_rate",
      "approx_count_distinct",
      "sum",
      "avg",
      "stddev",
      "stddev_samp",
      "variance",
      "var_samp",
      "median",
      "percentile",
      "percentile_approx",
    ].includes(agg)
  )
    return "NUMERIC";
  if (agg === "min" || agg === "max" || agg === "any_value" || agg === "mode") return colFamily;
  if (agg === "bool_and" || agg === "bool_or") return "BOOLEAN";
  return "ANY";
}

export type ValueCellShape =
  | { kind: "single"; type: "text" | "number" | "date" }
  | { kind: "double" }
  | { kind: "chip" }
  | { kind: "none" }
  | { kind: "interval" }
  | { kind: "type-picker" };

const NONE_OPS = new Set([
  "is null",
  "is not null",
  "is true",
  "is false",
  "has leading or trailing whitespace",
  "has no leading or trailing whitespace",
  // Valueless validators / predicates added to the catalog — each is a
  // self-contained SQL test against the column alone (no operand).
  "is not empty",
  "is empty",
  "contains only digits",
  "is uppercase",
  "is lowercase",
  "is a valid email",
  "is a valid uuid",
  "is a valid ipv4",
  "passes luhn check",
  "is positive",
  "is negative",
  "is non-negative",
  "is a whole number",
  "is in the future",
  "is in the past",
  "is today",
  "has positive sentiment",
  "has negative sentiment",
]);
const TYPE_PICKER_OPS = new Set(VALIDITY_OPS);

// Operators whose operand is a NUMBER even though the column is TEXTUAL
// (length comparisons, multiple-of). Without this they'd fall through to the
// TEXTUAL text-input default and offer a text box for a numeric threshold.
const NUMBER_VALUE_OPS = new Set(["has length", "is longer than", "is shorter than", "is a multiple of"]);

export function valueCellShape(operator: string, family: Family): ValueCellShape {
  if (NONE_OPS.has(operator)) return { kind: "none" };
  if (TYPE_PICKER_OPS.has(operator)) return { kind: "type-picker" };
  if (operator === "between" || operator === "length between") return { kind: "double" };
  if (operator === "in" || operator === "not in") return { kind: "chip" };
  if (operator === "is in last") return { kind: "interval" };
  if (NUMBER_VALUE_OPS.has(operator)) return { kind: "single", type: "number" };
  if (family === "NUMERIC") return { kind: "single", type: "number" };
  if (family === "TEMPORAL") return { kind: "single", type: "date" };
  return { kind: "single", type: "text" };
}

export const FAMILY_LABEL: Record<Family, string> = {
  NUMERIC: "Numeric",
  TEXTUAL: "Text",
  TEMPORAL: "Date or time",
  BOOLEAN: "Boolean",
  ANY: "Any",
};
