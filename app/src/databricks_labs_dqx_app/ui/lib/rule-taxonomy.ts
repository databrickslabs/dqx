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
