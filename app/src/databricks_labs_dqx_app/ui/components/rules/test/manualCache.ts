// Module-level cache so typed manual test rows survive Test-tab switches
// (the panel unmounts/remounts). Ported from dqlake's `test/manualCache.ts`.
// The hash captures the rule's logic; if it changes the cached grid is dropped
// and a fresh one is built. Ephemeral only — nothing is persisted (dqlake does
// not persist test cases either).

/** One inline grid standing in for a reference table a cross-table rule joins.
 *  Its columns are the real column names the rule reads through its join alias
 *  (not the rule's slots), so they're author-defined and carry their own types. */
export interface RefGridState {
  columns: string[];
  rows: (string | null)[][];
  /** Column name -> slot family, used to type each cell. */
  families: Record<string, string>;
}

export interface ManualState {
  columns: string[];
  rows: (string | null)[][];
  /** Table FQN, as the rule's query joins it -> the grid standing in for it. */
  refs: Record<string, RefGridState>;
}

interface Entry {
  hash: string;
  state: ManualState;
}

const cache = new Map<string, Entry>();

export function getManual(key: string, hash: string): ManualState | null {
  const e = cache.get(key);
  if (!e || e.hash !== hash) return null;
  return e.state.refs ? e.state : { ...e.state, refs: {} };
}

export function setManual(key: string, hash: string, state: ManualState): void {
  cache.set(key, { hash, state });
}
