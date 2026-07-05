// Shared helpers for the Apply Rules tab components (registry-rule tag
// lookups, label coloring, API error extraction). Kept in one place so
// AddRulesDialog / RuleConfigCard / RulesByColumn agree on the same
// conventions instead of re-deriving them.

import { Badge } from "@/components/ui/badge";
import type { AppliedRuleOut, RegistryRuleOut } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";

export const RESERVED_NAME_KEY = "name";
export const RESERVED_DIMENSION_KEY = "dimension";
export const RESERVED_SEVERITY_KEY = "severity";

export function getTag(rule: RegistryRuleOut, key: string): string {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const v = md[key];
  return typeof v === "string" ? v : "";
}

export function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

export function colorFor(defs: LabelDefinition[], key: string, value: string): string | undefined {
  const def = defs.find((d) => d.key === key);
  return def?.value_colors?.[value] ?? undefined;
}

// ---------------------------------------------------------------------------
// Rule grouping — DQX materializes one `dq_applied_rules` ROW per mapping
// GROUP (each call to `useApplyRuleToTable` with a single-entry
// `column_mapping` array creates its own row/applied-check). dqlake's
// bindings model keeps every mapping group inside a single binding entity;
// to render the equivalent "N mapping groups under one rule card" UI here,
// the by-rule lens groups the flat `AppliedRuleOut[]` list by `rule_id` and
// flattens every row's `column_mapping` into one combined list.
// ---------------------------------------------------------------------------

export interface RuleRowGroup {
  ruleId: string;
  /** Every applied-rule ROW for this rule_id, in list order. Each row is
   *  expected to carry exactly one mapping group (the convention every
   *  apply path in this app follows), so `rows[i]` owns combined-mapping
   *  index `i` — used to resolve "remove this mapping group" back to the
   *  concrete row id to delete. */
  rows: AppliedRuleOut[];
}

/** Group a flat applied-rules list by `rule_id`, preserving first-seen order. */
export function groupAppliedRulesByRuleId(appliedRules: AppliedRuleOut[]): RuleRowGroup[] {
  const order: string[] = [];
  const map = new Map<string, AppliedRuleOut[]>();
  for (const rule of appliedRules) {
    if (!map.has(rule.rule_id)) {
      map.set(rule.rule_id, []);
      order.push(rule.rule_id);
    }
    map.get(rule.rule_id)!.push(rule);
  }
  return order.map((ruleId) => ({ ruleId, rows: map.get(ruleId)! }));
}

/** Merge a rule-id's rows into one display-only `AppliedRuleOut` whose
 *  `column_mapping` is the concatenation of every row's mapping groups
 *  (display metadata — name/dimension/severity/pin — comes from the first
 *  row). Used everywhere the by-rule lens needs one card per rule_id. */
export function mergeRuleRowGroup(group: RuleRowGroup): AppliedRuleOut {
  const [first] = group.rows;
  return {
    ...first,
    column_mapping: group.rows.flatMap((row) => row.column_mapping ?? []),
  };
}

/** Every column name a rule is already mapped to, across every mapping
 *  group and slot (multi-value slots store their columns as a
 *  comma-joined string — see `AddRulesDialog#handleApply`). Used by the
 *  "+ Apply to another column" flow to exclude columns the rule already
 *  covers from the column picker, mirroring dqlake's `usedSetForNew`
 *  exclusion in `bindings/MappingChips.tsx`. */
export function getUsedColumnsForRule(rule: AppliedRuleOut): string[] {
  const used = new Set<string>();
  for (const group of rule.column_mapping ?? []) {
    for (const value of Object.values(group)) {
      if (!value) continue;
      for (const col of value.split(",")) {
        if (col) used.add(col);
      }
    }
  }
  return [...used];
}

export function TagBadge({ label, color }: { label: string; color?: string }) {
  if (!label) return null;
  return (
    <Badge variant="outline" className="gap-1 text-[10px] font-normal">
      {color && (
        <span className="inline-block h-1.5 w-1.5 rounded-full" style={{ backgroundColor: color }} aria-hidden />
      )}
      {label}
    </Badge>
  );
}
