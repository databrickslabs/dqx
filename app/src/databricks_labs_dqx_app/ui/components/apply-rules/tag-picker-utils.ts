/** Filter the available class.* tags: drop ones already selected, then
 *  substring-match the query (case-insensitive). Preserves input order. */
export function filterTags(all: string[], selected: string[], query: string): string[] {
  const selectedSet = new Set(selected);
  const q = query.trim().toLowerCase();
  return all.filter((t) => !selectedSet.has(t) && (q === "" || t.toLowerCase().includes(q)));
}
