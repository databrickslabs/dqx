import type { ColumnOut, RuleSlot, RuleSlotFamily } from "@/lib/api";

/**
 * Best-effort Spark SQL type name -> rule slot family classifier, used to
 * respect a slot's declared family when auto-matching a same-named column
 * (Rules Registry Phase 7D-d "Apply to tables" flow). Deliberately
 * conservative: an unrecognized type (arrays, structs, binary, etc.) falls
 * back to no family match, so it only ever satisfies an "any" slot.
 */
export function familyForSparkType(typeName: string): RuleSlotFamily {
  const normalized = typeName.trim().toLowerCase();
  if (/^(tinyint|smallint|int|integer|bigint|float|double|decimal)/.test(normalized)) return "numeric";
  if (/^(string|varchar|char)/.test(normalized)) return "text";
  if (/^(date|timestamp)/.test(normalized)) return "temporal";
  if (normalized === "boolean") return "boolean";
  if (normalized.startsWith("array")) return "array";
  return "any";
}

/**
 * Best-effort slot -> column-name mapping for one table, matching each slot
 * to a same-named column that also satisfies the slot's declared family.
 *
 * Returns ``null`` (mapping incomplete) when any slot can't be resolved:
 * no same-named column, a family mismatch, or a "many" cardinality slot
 * (composite/multi-column slots need a human to pick the column list, so
 * they're never auto-mapped here). Callers should skip applying the rule
 * to that table and report it as needing manual mapping.
 */
export function buildSlotMapping(slots: RuleSlot[], columns: ColumnOut[]): Record<string, string> | null {
  const byLowerName = new Map(columns.map((c) => [c.name.toLowerCase(), c]));
  const mapping: Record<string, string> = {};
  for (const slot of slots) {
    if (slot.cardinality === "many") return null;
    const column = byLowerName.get(slot.name.toLowerCase());
    if (!column) return null;
    if (slot.family !== "any" && familyForSparkType(column.type_name) !== slot.family) return null;
    mapping[slot.name] = column.name;
  }
  return mapping;
}
