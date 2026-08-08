// Schema drift detection for Apply Rules / Results — compares live Unity
// Catalog columns against each applied rule's column_mapping. Pure and
// cache-friendly: callers already fetch columns via useGetTableColumns
// (DiscoveryService, 10-min TTL); this is an in-memory walk of mappings.

import type { AppliedRuleOut, ColumnOut, RuleSlot } from "@/lib/api";
import { familyForType, type ColumnFamily } from "./ColumnPicker";

export type SchemaDriftKind = "missing" | "type_mismatch";

export interface SchemaDriftIssue {
  ruleId: string;
  slotName: string;
  groupIdx: number;
  /** Mapped column name that drifted (one entry per comma-joined multi-value part). */
  columnName: string;
  kind: SchemaDriftKind;
  /** Slot family when kind is type_mismatch. */
  slotFamily?: ColumnFamily;
  /** Live column family when kind is type_mismatch. */
  liveFamily?: ColumnFamily;
  /** Live Spark/UC type_name when available. */
  liveType?: string;
}

export interface SchemaDriftSummary {
  issues: SchemaDriftIssue[];
  missingCount: number;
  typeMismatchCount: number;
  /** Distinct rule_ids with at least one issue. */
  affectedRuleIds: string[];
}

export interface RuleSchemaDrift {
  issues: SchemaDriftIssue[];
  missingCount: number;
  typeMismatchCount: number;
}

const EMPTY_RULE_DRIFT: RuleSchemaDrift = {
  issues: [],
  missingCount: 0,
  typeMismatchCount: 0,
};

const EMPTY_SUMMARY: SchemaDriftSummary = {
  issues: [],
  missingCount: 0,
  typeMismatchCount: 0,
  affectedRuleIds: [],
};

/** Split a mapping value into column names. Multi-value slots store a
 *  comma-joined string (see getUsedColumnsForRule / AddRulesDialog). */
export function splitMappedColumns(value: string | undefined | null): string[] {
  if (!value) return [];
  return value
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function slotByName(slots: RuleSlot[]): Map<string, RuleSlot> {
  const m = new Map<string, RuleSlot>();
  for (const s of slots) m.set(s.name, s);
  return m;
}

function columnByName(columns: ColumnOut[]): Map<string, ColumnOut> {
  const m = new Map<string, ColumnOut>();
  for (const c of columns) m.set(c.name, c);
  return m;
}

/**
 * Drift for one applied rule (merged group list). When `columns` is empty
 * (still loading / unavailable) returns no issues — never invent missing
 * columns from an empty live schema.
 */
export function computeRuleSchemaDrift(
  rule: Pick<AppliedRuleOut, "rule_id" | "column_mapping">,
  slots: RuleSlot[],
  columns: ColumnOut[],
): RuleSchemaDrift {
  if (columns.length === 0 || slots.length === 0) return EMPTY_RULE_DRIFT;

  const slotsMap = slotByName(slots);
  const live = columnByName(columns);
  const issues: SchemaDriftIssue[] = [];

  const groups = rule.column_mapping ?? [];
  groups.forEach((group, groupIdx) => {
    for (const [slotName, raw] of Object.entries(group)) {
      const slot = slotsMap.get(slotName);
      for (const columnName of splitMappedColumns(raw)) {
        const col = live.get(columnName);
        if (!col) {
          issues.push({
            ruleId: rule.rule_id,
            slotName,
            groupIdx,
            columnName,
            kind: "missing",
          });
          continue;
        }
        // Type-family mismatch only when both sides declare a concrete family.
        if (!slot || slot.family === "any") continue;
        const liveFamily = familyForType(col.type_name);
        if (liveFamily === "any") continue;
        if (liveFamily !== slot.family) {
          issues.push({
            ruleId: rule.rule_id,
            slotName,
            groupIdx,
            columnName,
            kind: "type_mismatch",
            slotFamily: slot.family as ColumnFamily,
            liveFamily,
            liveType: col.type_name,
          });
        }
      }
    }
  });

  let missingCount = 0;
  let typeMismatchCount = 0;
  for (const i of issues) {
    if (i.kind === "missing") missingCount += 1;
    else typeMismatchCount += 1;
  }
  return { issues, missingCount, typeMismatchCount };
}

/** Aggregate drift across every applied rule. `slotsByRuleId` supplies each
 *  rule's declared slots (family + name); rules without an entry are skipped. */
export function computeSchemaDriftSummary(
  rules: Array<Pick<AppliedRuleOut, "rule_id" | "column_mapping">>,
  columns: ColumnOut[],
  slotsByRuleId: Map<string, RuleSlot[]> | ReadonlyMap<string, RuleSlot[]>,
): SchemaDriftSummary {
  if (columns.length === 0 || rules.length === 0) return EMPTY_SUMMARY;

  const issues: SchemaDriftIssue[] = [];
  const affected = new Set<string>();
  for (const rule of rules) {
    const slots = slotsByRuleId.get(rule.rule_id) ?? [];
    const per = computeRuleSchemaDrift(rule, slots, columns);
    if (per.issues.length === 0) continue;
    issues.push(...per.issues);
    affected.add(rule.rule_id);
  }

  let missingCount = 0;
  let typeMismatchCount = 0;
  for (const i of issues) {
    if (i.kind === "missing") missingCount += 1;
    else typeMismatchCount += 1;
  }
  return {
    issues,
    missingCount,
    typeMismatchCount,
    affectedRuleIds: [...affected],
  };
}

export function ruleHasSchemaDrift(drift: RuleSchemaDrift): boolean {
  return drift.issues.length > 0;
}

/** Whether a mapped column name (or comma-joined multi-value) is missing or
 *  type-mismatched against the live schema for the given slot. Used by chips. */
export function columnMappingDriftKind(
  columnLabel: string,
  slot: Pick<RuleSlot, "family"> | undefined,
  columns: ColumnOut[],
): SchemaDriftKind | null {
  if (columns.length === 0) return null;
  const live = columnByName(columns);
  const names = splitMappedColumns(columnLabel);
  let sawMismatch = false;
  for (const name of names) {
    const col = live.get(name);
    if (!col) return "missing";
    if (!slot || slot.family === "any") continue;
    const liveFamily = familyForType(col.type_name);
    if (liveFamily === "any") continue;
    if (liveFamily !== slot.family) sawMismatch = true;
  }
  return sawMismatch ? "type_mismatch" : null;
}
