/**
 * Parse free-text table FQNs pasted into the Monitor / Apply-to-tables pickers.
 *
 * Accepts one or many `catalog.schema.table` values separated by newlines,
 * commas, or semicolons. Surrounding backticks (common when copying from
 * notebooks / SQL) are stripped. Incomplete or malformed tokens are returned
 * in ``invalid`` rather than silently dropped.
 */
export function parsePastedTableFqns(raw: string): { valid: string[]; invalid: string[] } {
  const tokens = raw
    .split(/[\n,;]+/)
    .map((token) => token.trim().replace(/^`+|`+$/g, "").trim())
    .filter(Boolean);

  const valid: string[] = [];
  const invalid: string[] = [];
  const seen = new Set<string>();

  for (const token of tokens) {
    const parts = token.split(".").map((part) => part.trim());
    if (parts.length !== 3 || parts.some((part) => !part)) {
      invalid.push(token);
      continue;
    }
    const fqn = `${parts[0]}.${parts[1]}.${parts[2]}`;
    if (seen.has(fqn)) continue;
    seen.add(fqn);
    valid.push(fqn);
  }

  return { valid, invalid };
}
