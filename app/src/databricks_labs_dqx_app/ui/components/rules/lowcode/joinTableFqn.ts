/**
 * Pure helpers for the low-code Joins card's single-table pick modal.
 *
 * A join's `target_table` is stored as a fully-qualified "catalog.schema.table"
 * string (or "" when unset). These functions convert between that string and
 * its three parts without touching React/UI state, so they can be unit-tested
 * in isolation.
 */

export interface TableParts {
  catalog: string;
  schema: string;
  table: string;
}

/**
 * Splits a fully-qualified table name into its catalog/schema/table parts.
 * Returns empty strings for every part unless the input has exactly three
 * non-empty, dot-separated segments (a real, complete FQN).
 */
export function parseTableFqn(fqn: string | null | undefined): TableParts {
  const parts = (fqn ?? "").split(".");
  if (parts.length === 3 && parts[0] && parts[1] && parts[2]) {
    return { catalog: parts[0], schema: parts[1], table: parts[2] };
  }
  return { catalog: "", schema: "", table: "" };
}

/**
 * Joins catalog/schema/table into a fully-qualified name, or "" when any part
 * is missing (an incomplete selection has no valid FQN).
 */
export function buildTableFqn(catalog: string, schema: string, table: string): string {
  if (catalog && schema && table) {
    return `${catalog}.${schema}.${table}`;
  }
  return "";
}
