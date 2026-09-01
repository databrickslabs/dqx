/**
 * Decide whether to autofill the "Merge results back on" field. Returns the
 * comma-joined slot names when the field is empty (or whitespace-only) and there
 * is at least one slot; otherwise null (leave the author's / loaded value alone).
 */
export function computeMergeColumnsAutofill(current: string, slotNames: string[]): string | null {
  if (current.trim() !== "") return null;
  if (slotNames.length === 0) return null;
  return slotNames.join(", ");
}
