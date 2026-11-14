/**
 * Orders two rule severities for the Rules overview's "Group by: Severity":
 * by their position in *displayOrder* — the severity label definition's
 * values as the admin settings list them (most severe first, see
 * `orderSeverityValuesForDisplay`). Severities not defined in settings follow,
 * alphabetically; rules with no severity (empty value) come last. Matching is
 * case-insensitive so a hand-edited "high" still lands with "High".
 */
export function compareSeverityGroups(a: string, b: string, displayOrder: readonly string[]): number {
  const rankA = severityGroupRank(a, displayOrder);
  const rankB = severityGroupRank(b, displayOrder);
  if (rankA !== rankB) return rankA - rankB;
  return rankA === displayOrder.length ? a.localeCompare(b) : 0;
}

/** Position in *displayOrder*; undefined severities rank after it, and no severity last. */
function severityGroupRank(value: string, displayOrder: readonly string[]): number {
  const trimmed = value.trim();
  if (!trimmed) return displayOrder.length + 1;
  const lower = trimmed.toLowerCase();
  const index = displayOrder.findIndex((v) => v.trim().toLowerCase() === lower);
  return index === -1 ? displayOrder.length : index;
}
