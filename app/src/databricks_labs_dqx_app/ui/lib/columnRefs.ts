/**
 * `{{column_name}}` reference parsing shared by the SQL predicate editor.
 * Ported from dqlake's `ui/lib/columnRefs.ts`.
 */
const REF_RE = /\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}/g;

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
  return findRefs(text).filter((n) => !set.has(n));
}
