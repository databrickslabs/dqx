// Infer which columns a rule expects on a reference table, so the manual test's
// grid for a `{{table}}` slot starts with the right headers instead of an empty
// canvas the author has to guess at.
//
// A reference grid's columns can't come from the rule's slots the way the input
// grid's do: the rule refers to the reference table through its own join alias
// (`LEFT JOIN {{ref_table}} c ON c.id = {{customer_id}}` → the grid needs `id`),
// so the names live in the SQL text. This reads them back out.
//
// Deliberately conservative — it only claims a column when it can tie an alias
// to the slot's own JOIN clause. Missing one is fine (the author adds it, or
// "Generate test data" fills the grid); inventing one is not.

import { stripSqlLineComments } from "./sqlComments";

/** Identifier characters we accept for an alias or column name. */
const IDENT = "[A-Za-z_][A-Za-z0-9_]*";

/** Reserved: `{{input_view}}` always resolves to the table being checked, so a
 *  slot by that name never names a reference table to bind or fabricate. */
export const INPUT_VIEW_SLOT = "input_view";

/** Whether *slot* names a reference table the author must supply. */
export function isReferenceTableSlot(slot: { name: string; family?: string | null }): boolean {
  return slot.family === "table" && slot.name !== INPUT_VIEW_SLOT;
}

/**
 * The alias bound to `{{slot}}` in *sql*, e.g. `c` in
 * `LEFT JOIN {{ref_table}} c ON …` (also matching the explicit `AS c` form).
 * Returns null when the join uses no alias, in which case columns are referenced
 * unqualified and can't be attributed to this table.
 */
export function refTableAlias(sql: string, slot: string): string | null {
  const scan = stripSqlLineComments(sql);
  const placeholder = `\\{\\{\\s*${slot}\\s*\\}\\}`;
  const m = new RegExp(`${placeholder}\\s+(?:as\\s+)?(${IDENT})`, "i").exec(scan);
  if (!m) return null;
  // `JOIN {{ref}} ON …` has no alias — `ON` matched as an identifier.
  return /^(on|using|where|left|right|inner|full|cross|join|group|order|limit)$/i.test(m[1]) ? null : m[1];
}

/**
 * Column names *sql* reads from the reference table bound to *slot*, in
 * first-appearance order. Empty when the join declares no alias.
 */
export function inferRefTableColumns(sql: string, slot: string): string[] {
  const alias = refTableAlias(sql, slot);
  if (!alias) return [];
  const scan = stripSqlLineComments(sql);
  const re = new RegExp(`\\b${alias}\\.(${IDENT})\\b`, "gi");
  const seen = new Set<string>();
  const out: string[] = [];
  for (const m of scan.matchAll(re)) {
    const col = m[1];
    if (seen.has(col)) continue;
    seen.add(col);
    out.push(col);
  }
  return out;
}
