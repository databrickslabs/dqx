// Module-level cache so typed manual test rows survive Test-tab switches
// (the panel unmounts/remounts). Ported from dqlake's `test/manualCache.ts`.
// The hash captures the rule's logic; if it changes the cached grid is dropped
// and a fresh one is built. Ephemeral only — nothing is persisted (dqlake does
// not persist test cases either).

export interface ManualState {
  columns: string[];
  rows: (string | null)[][];
}

interface Entry {
  hash: string;
  state: ManualState;
}

const cache = new Map<string, Entry>();

export function getManual(key: string, hash: string): ManualState | null {
  const e = cache.get(key);
  return e && e.hash === hash ? e.state : null;
}

export function setManual(key: string, hash: string, state: ManualState): void {
  cache.set(key, { hash, state });
}
