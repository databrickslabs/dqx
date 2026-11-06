/** Lets only one save-like action run at a time.
 *
 *  A collection save diffs the edit buffer against the last server snapshot.
 *  If a second save (for example Submit for review) starts before the first
 *  one's refetch lands, it diffs against the stale snapshot and replays the
 *  first save's removals, which the server rejects as "member not found".
 *  Running saves through one gate, and keeping the gate closed until the
 *  refetch has landed, removes that window. */
export interface SaveGate {
  /** True while an action is running. */
  readonly busy: boolean;
  /** Run *work* unless another action is already running, in which case
   *  nothing runs and `null` is returned. The gate reopens when *work*
   *  settles, whether it resolves or throws. */
  run<T>(work: () => Promise<T>): Promise<T | null>;
}

export function createSaveGate(): SaveGate {
  let busy = false;
  return {
    get busy() {
      return busy;
    },
    async run<T>(work: () => Promise<T>): Promise<T | null> {
      if (busy) return null;
      busy = true;
      try {
        return await work();
      } finally {
        busy = false;
      }
    },
  };
}
