import type { RegistryRuleOut } from "@/lib/api";

export type RuleColumnTypeGroup =
  "any" | "boolean" | "mixed" | "numeric" | "table" | "temporal" | "text";

const COLUMN_TYPE_GROUP_ORDER: RuleColumnTypeGroup[] = [
  "any",
  "numeric",
  "text",
  "temporal",
  "boolean",
  "mixed",
  "table",
];

/** One stable marketplace category derived from the rule's typed column slots. */
export function ruleColumnTypeGroup(
  rule: RegistryRuleOut,
): RuleColumnTypeGroup {
  const slots = rule.definition?.slots ?? [];
  if (slots.length === 0) return "table";
  const families = new Set(
    slots
      .map((slot) => slot.family || "any")
      .filter((family) => family !== "any"),
  );
  if (families.size === 0) return "any";
  if (families.size > 1) return "mixed";
  const family = [...families][0];
  if (
    family === "numeric" ||
    family === "text" ||
    family === "temporal" ||
    family === "boolean"
  ) {
    return family;
  }
  return "any";
}

export function compareRuleColumnTypeGroups(
  a: RegistryRuleOut,
  b: RegistryRuleOut,
): number {
  return (
    COLUMN_TYPE_GROUP_ORDER.indexOf(ruleColumnTypeGroup(a)) -
    COLUMN_TYPE_GROUP_ORDER.indexOf(ruleColumnTypeGroup(b))
  );
}

/** Scoring granularity of a rule: one verdict per row, or one for the whole table. */
export type RuleLevel = "row" | "table";

/** Display order of rule levels (e.g. group headers): Row before Table. */
export const RULE_LEVEL_ORDER: readonly RuleLevel[] = ["row", "table"];

/** Names of the check functions registered as dataset rules (CHECK_FUNC_REGISTRY type "dataset"). */
export function datasetCheckFunctionNames(
  functions: readonly { name: string; rule_type: string }[],
): Set<string> {
  return new Set(functions.filter((fn) => fn.rule_type === "dataset").map((fn) => fn.name));
}

function hasMergeColumns(value: unknown): boolean {
  return Array.isArray(value) && value.length > 0;
}

/**
 * Classify *rule* as row- or table-level, mirroring the backend score view's
 * check_granularity (score_view_service): a sql_query check is table-level
 * unless it carries merge_columns (low-code group-by and merged SQL rules flag
 * their source rows); a native check is table-level when its function is in
 * *datasetFunctions*; everything else (SQL predicates and any function the
 * list doesn't know) is row-level, matching the backend's conservative
 * default for unknown or custom functions.
 */
export function ruleLevel(rule: RegistryRuleOut, datasetFunctions: ReadonlySet<string>): RuleLevel {
  const body = (rule.definition?.body ?? {}) as Record<string, unknown>;
  if (rule.mode === "dqx_native") {
    const fn = typeof body.function === "string" ? body.function : "";
    if (fn === "sql_query") {
      const args = (body.arguments ?? {}) as Record<string, unknown>;
      return hasMergeColumns(args.merge_columns) ? "row" : "table";
    }
    return datasetFunctions.has(fn) ? "table" : "row";
  }
  if (typeof body.sql_query === "string") {
    return hasMergeColumns(body.merge_columns) ? "row" : "table";
  }
  return "row";
}
