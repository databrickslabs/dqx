// Which tables a cross-table rule joins, and which columns it reads off each —
// so the manual test can stand a fabricated grid in for every table the rule
// touches, pre-headed with the right columns instead of an empty canvas.
//
// A rule belongs to ONE table, so the table it joins is named by its literal
// fully-qualified name in the rule's own SQL; that text is the only place the
// reference lives, and this reads it back out. Mirrors
// `backend/rule_test_sql.find_table_refs` / `normalize_table_ref`: the same scan
// runs server-side to decide which grids a query needs, so both must agree on
// what counts as a table reference and on how a name is spelled.
//
// Column inference is deliberately conservative — it only claims a column when
// an alias ties it to that table's own JOIN clause. Missing one is fine (the
// author adds it, or "Generate test data" fills the grid); inventing one is not.

import { stripSqlLineComments } from "./sqlComments";

/** Identifier characters we accept for an alias or column name. */
const IDENT = "[A-Za-z_][A-Za-z0-9_]*";
/** One part of a table reference: bare, or backtick-quoted for an exotic name
 *  (which doubles any backtick the name itself contains). */
const REF_PART = "(?:`(?:[^`]|``)+`|[A-Za-z_][A-Za-z0-9_$]*)";
/** A relation introduced by FROM / JOIN and written as a DOTTED name — i.e. a
 *  real table, as opposed to `{{input_view}}` or a subquery. */
const tableRefRe = (): RegExp =>
  new RegExp(`\\b(?:FROM|JOIN)\\s+(${REF_PART}(?:\\s*\\.\\s*${REF_PART}){1,2})`, "gi");

/** Reserved: `{{input_view}}` always resolves to the table being checked, so it
 *  never names a table the author has to supply. */
export const INPUT_VIEW_SLOT = "input_view";

/**
 * Canonical spelling of a table reference: backticks and the whitespace around
 * the dots are the author's typing, not part of the name, so `` `main` . `ref`.`fx` ``
 * and `main.ref.fx` are one table and key one grid.
 */
export function normalizeTableRef(ref: string): string {
  return (ref.match(new RegExp(REF_PART, "g")) ?? [])
    .map((part) => (part.startsWith("`") ? part.slice(1, -1).replaceAll("``", "`") : part))
    .join(".");
}

/**
 * Tables *sql* reads through FROM / JOIN, in first-appearance order, normalized
 * and de-duplicated case-insensitively (Unity Catalog identifiers are). Comments
 * are stripped first, so a `-- LEFT JOIN main.ref.old` line left behind doesn't
 * ask the author for a grid.
 */
export function findReferenceTables(sql: string): string[] {
  const scan = stripSqlLineComments(sql);
  const seen = new Set<string>();
  const out: string[] = [];
  for (const m of scan.matchAll(tableRefRe())) {
    const ref = normalizeTableRef(m[1]);
    if (!ref || seen.has(ref.toLowerCase())) continue;
    seen.add(ref.toLowerCase());
    out.push(ref);
  }
  return out;
}

/**
 * The alias bound to *table* in *sql*, e.g. `c` in
 * `LEFT JOIN main.ref.customers c ON …` (also matching the explicit `AS c` form).
 * Returns null when the join uses no alias, in which case columns are referenced
 * unqualified and can't be attributed to this table.
 */
export function refTableAlias(sql: string, table: string): string | null {
  const scan = stripSqlLineComments(sql);
  const wanted = normalizeTableRef(table).toLowerCase();
  for (const m of scan.matchAll(tableRefRe())) {
    if (normalizeTableRef(m[1]).toLowerCase() !== wanted) continue;
    const after = scan.slice((m.index ?? 0) + m[0].length);
    const alias = new RegExp(`^\\s+(?:as\\s+)?(${IDENT})`, "i").exec(after);
    if (!alias) return null;
    // `JOIN main.ref.t ON …` has no alias — `ON` matched as an identifier.
    return /^(on|using|where|left|right|inner|full|cross|join|group|order|limit)$/i.test(alias[1])
      ? null
      : alias[1];
  }
  return null;
}

/**
 * Column names *sql* reads from *table*, in first-appearance order. Empty when
 * the join declares no alias.
 */
export function inferRefTableColumns(sql: string, table: string): string[] {
  const alias = refTableAlias(sql, table);
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
