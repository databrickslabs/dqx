/** Pick the highest-priority severity from a list, by rank (higher rank = more
 *  severe, matching the admin severity order: Critical 4 > High 3 > …).
 *  Unknown severities get rank 0. Returns undefined for an empty list. */
export function pickTopSeverity(
  severities: Array<string | undefined>,
  rankByName: Record<string, number>,
): string | undefined {
  let best: string | undefined;
  let bestRank = -Infinity;
  for (const s of severities) {
    if (!s) continue;
    const rank = rankByName[s] ?? 0;
    if (rank > bestRank) {
      bestRank = rank;
      best = s;
    }
  }
  return best;
}
