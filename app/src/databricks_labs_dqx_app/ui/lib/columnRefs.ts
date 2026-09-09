/**
 * `{{column_name}}` reference parsing shared by the SQL predicate editor.
 * Ported from dqlake's `ui/lib/columnRefs.ts`.
 */
import { INPUT_VIEW_SLOT } from "./refTableColumns";

const REF_RE = /\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}/g;

/**
 * Placeholders DQX resolves on its own, so they are never declared as column
 * slots and must not be reported as unknown. `{{input_view}}` becomes the table
 * being checked at run time — a query with joins is REQUIRED to read from it
 * (see `sqlMissingInputView` and the backend's `require_input_view`), so
 * squiggling it told the author to remove the one reference they must keep.
 */
const RESERVED_REFS: ReadonlySet<string> = new Set([INPUT_VIEW_SLOT]);

/** Whether *name* is a placeholder DQX substitutes rather than a column slot. */
export function isReservedRef(name: string): boolean {
  return RESERVED_REFS.has(name);
}

export function findRefs(text: string): string[] {
  const seen: Record<string, true> = {};
  const out: string[] = [];
  for (const m of text.matchAll(REF_RE)) {
    if (!seen[m[1]]) {
      seen[m[1]] = true;
      out.push(m[1]);
    }
  }
  return out;
}

export function findRefRanges(text: string): { name: string; from: number; to: number }[] {
  const out: { name: string; from: number; to: number }[] = [];
  for (const m of text.matchAll(REF_RE)) {
    const from = m.index ?? 0;
    out.push({ name: m[1], from, to: from + m[0].length });
  }
  return out;
}

export function findUnknownRefs(text: string, declared: string[]): string[] {
  const set = new Set(declared);
  return findRefs(text).filter((n) => !set.has(n) && !isReservedRef(n));
}
