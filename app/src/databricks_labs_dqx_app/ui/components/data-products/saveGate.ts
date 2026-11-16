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

/** Run *work* through *gate*, reporting `persisting` as true for the whole run
 *  (including the refetch inside *work*) and false once it settles. Returns
 *  false, without running *work*, when another save already holds the gate. */
export async function runWhilePersisting(
  gate: SaveGate,
  setPersisting: (persisting: boolean) => void,
  work: () => Promise<boolean>,
): Promise<boolean> {
  const result = await gate.run(async () => {
    setPersisting(true);
    try {
      return await work();
    } finally {
      setPersisting(false);
    }
  });
  return result ?? false;
}

/** The steps of one save: write the edits, then report and refetch. */
export interface SaveSteps {
  /** Write the buffered edits; throws on failure. */
  save: () => Promise<void>;
  /** Refetch the product so the next save diffs against what the server holds. */
  refetch: () => Promise<void>;
  onSaved: () => void;
  onFailed: (error: unknown) => void;
}

/** Save, then refetch, and only then resolve. A failed save still refetches
 *  (so a retry diffs against what the server holds after any partial writes).
 *  Resolving only after the refetch is what keeps the gate, and `persisting`,
 *  held until the next save would see fresh data. */
export async function saveThenRefetch({ save, refetch, onSaved, onFailed }: SaveSteps): Promise<boolean> {
  try {
    await save();
  } catch (error) {
    onFailed(error);
    await refetch();
    return false;
  }
  onSaved();
  await refetch();
  return true;
}
