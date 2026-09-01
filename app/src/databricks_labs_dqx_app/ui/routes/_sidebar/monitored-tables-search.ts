/** Normalize a Tables-page search string so pasted FQNs work:
 *  backticks (`cat`.`sch`.`tbl`), surrounding quotes, and whitespace
 *  around dots are stripped before matching. */
export function normalizeTableSearchQuery(raw: string): string {
  return raw
    .trim()
    .toLowerCase()
    .replace(/[`"']/g, "")
    .replace(/\s*\.\s*/g, ".")
    .replace(/\s+/g, " ");
}

/**
 * Match a monitored-table FQN against the search box.
 *
 * Supports bare table names AND `catalog.schema.table` (also 1–2 part
 * prefixes like `catalog.schema` or `schema.table`). When the query
 * contains dots, each segment is matched as a prefix of the corresponding
 * FQN part from the right (so `bakehouse.sales` matches
 * `samples.bakehouse.sales_transactions`).
 */
export function matchesTableFqnSearch(tableFqn: string, rawQuery: string): boolean {
  const needle = normalizeTableSearchQuery(rawQuery);
  if (!needle) return true;

  const fqn = tableFqn.toLowerCase();
  if (fqn.includes(needle)) return true;

  if (!needle.includes(".")) return false;

  const needleParts = needle.split(".").filter(Boolean);
  const fqnParts = fqn.split(".").filter(Boolean);
  if (needleParts.length === 0 || needleParts.length > fqnParts.length) return false;

  // Align needle parts to the RIGHT of the FQN so `schema.table` and
  // `catalog.schema.table` both work; also try left-aligned for
  // `catalog.schema` prefixes.
  const rightAligned = fqnParts.slice(-needleParts.length);
  if (needleParts.every((part, i) => rightAligned[i]?.startsWith(part))) return true;

  const leftAligned = fqnParts.slice(0, needleParts.length);
  return needleParts.every((part, i) => leftAligned[i]?.startsWith(part));
}

/** True when the query looks like a multi-part FQN / path — catalog/schema
 *  dropdown filters should not also constrain the result (the search itself
 *  already scopes by catalog/schema/table). */
export function isFqnLikeTableSearch(rawQuery: string): boolean {
  const needle = normalizeTableSearchQuery(rawQuery);
  return needle.includes(".");
}
