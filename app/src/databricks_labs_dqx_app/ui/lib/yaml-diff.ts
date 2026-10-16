import yaml from "js-yaml";

/**
 * YAML rendering and line diffs for the change dialogs (rule drafts, table
 * versions, table changes). No diff library is bundled with the app.
 */

export function toYaml(value: unknown): string {
  if (value === null || value === undefined) return "";
  try {
    return yaml.dump(value, { sortKeys: false, noRefs: true, lineWidth: 100 });
  } catch {
    return String(value);
  }
}

// ---------------------------------------------------------------------------
// Line-level diff (in-house LCS): the longest-common-subsequence of the two
// line arrays, back-tracked into removed/added/equal operations. Removed and
// added lines within the same change hunk are then paired row-for-row so a
// modified line sits opposite its replacement — the alignment that makes a
// split diff readable (null cells pad the shorter side).
// ---------------------------------------------------------------------------

export type LineTag = "equal" | "removed" | "added";
export interface DiffCell {
  text: string;
  tag: LineTag;
}
export interface DiffRow {
  left: DiffCell | null;
  right: DiffCell | null;
}

export function diffLines(previous: string, proposed: string): DiffRow[] {
  const a = previous.length ? previous.split("\n") : [];
  const b = proposed.length ? proposed.split("\n") : [];
  const n = a.length;
  const m = b.length;

  // lcs[i][j] = length of LCS of a[i:] and b[j:]
  const lcs: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
    }
  }

  const ops: DiffCell[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      ops.push({ text: a[i], tag: "equal" });
      i++;
      j++;
    } else if (lcs[i + 1][j] >= lcs[i][j + 1]) {
      ops.push({ text: a[i], tag: "removed" });
      i++;
    } else {
      ops.push({ text: b[j], tag: "added" });
      j++;
    }
  }
  while (i < n) ops.push({ text: a[i++], tag: "removed" });
  while (j < m) ops.push({ text: b[j++], tag: "added" });

  const rows: DiffRow[] = [];
  let k = 0;
  while (k < ops.length) {
    if (ops[k].tag === "equal") {
      const cell = ops[k];
      rows.push({ left: cell, right: cell });
      k++;
      continue;
    }
    // Gather the contiguous run of non-equal ops and pair removed with added.
    const removed: DiffCell[] = [];
    const added: DiffCell[] = [];
    while (k < ops.length && ops[k].tag !== "equal") {
      if (ops[k].tag === "removed") removed.push(ops[k]);
      else added.push(ops[k]);
      k++;
    }
    const max = Math.max(removed.length, added.length);
    for (let r = 0; r < max; r++) {
      rows.push({ left: removed[r] ?? null, right: added[r] ?? null });
    }
  }
  return rows;
}

/** One row of a unified diff: an unchanged line once, else the removed line then the added one. */
export function unifiedRows(rows: DiffRow[]): DiffCell[] {
  const out: DiffCell[] = [];
  for (const row of rows) {
    if (row.left && row.right && row.left.tag === "equal") {
      out.push(row.left);
      continue;
    }
    if (row.left) out.push(row.left);
    if (row.right) out.push(row.right);
  }
  return out;
}

const WORD_CHAR = /[A-Za-z0-9_]/;

/**
 * The changed middle of a line against its counterpart, widened to whole
 * words (`line_no` → `line_number` marks the word, not `o` / `umber`). The
 * common prefix and suffix outside it are left plain.
 */
export function inlineChange(line: string, other: string): { before: string; changed: string; after: string } {
  let start = 0;
  while (start < line.length && start < other.length && line[start] === other[start]) start++;
  let end = 0;
  while (
    end < line.length - start &&
    end < other.length - start &&
    line[line.length - 1 - end] === other[other.length - 1 - end]
  ) {
    end++;
  }
  if (start === line.length && start === other.length) return { before: line, changed: "", after: "" };
  while (start > 0 && WORD_CHAR.test(line[start - 1])) start--;
  while (end > 0 && WORD_CHAR.test(line[line.length - end])) end--;
  return { before: line.slice(0, start), changed: line.slice(start, line.length - end), after: line.slice(line.length - end) };
}

/** One side of a change (zero, one or several checks) as YAML. */
export function sideYaml(side: unknown[]): string {
  return side.length ? toYaml(side.length === 1 ? side[0] : side).trim() : "";
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value as Record<string, unknown>)
        .sort()
        .map((k) => [k, canonical((value as Record<string, unknown>)[k])]),
    );
  }
  return value;
}

/**
 * The status of an item from its two sides, as the server-side comparisons
 * report it. Key order doesn't count as a change.
 */
export function itemStatus(before: unknown[], after: unknown[]): "added" | "removed" | "modified" | "unchanged" {
  if (!before.length) return "added";
  if (!after.length) return "removed";
  return JSON.stringify(canonical(before)) === JSON.stringify(canonical(after)) ? "unchanged" : "modified";
}
