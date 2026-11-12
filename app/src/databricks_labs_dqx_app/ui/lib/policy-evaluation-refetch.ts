/** Re-read a monitored table while a rule policy evaluation started by opening it is running.
 *
 *  Opening a table evaluates the rule policies reaching it in the background; the
 *  detail response says so with `policy_evaluation_pending`. The page then re-reads
 *  the detail a few times, shortly after each response, until nothing is pending
 *  (the server debounces repeated opens, so the loop ends on its own), and merges
 *  rows the evaluation attached into the staged editor. */

export const POLICY_REFETCH_DELAY_MS = 2500;
export const POLICY_REFETCH_MAX_ATTEMPTS = 3;

/** Delay before the next re-read, or `null` when no re-read is due. */
export function nextPolicyRefetchDelay(pending: boolean | undefined, attemptsSoFar: number): number | null {
  return pending === true && attemptsSoFar < POLICY_REFETCH_MAX_ATTEMPTS ? POLICY_REFETCH_DELAY_MS : null;
}

/** Rows in *current* whose key was not in *previous* (what appeared server-side between two reads). */
export function newServerRows<T>(previous: T[], current: T[], key: (row: T) => string): T[] {
  const seen = new Set(previous.map(key));
  return current.filter((row) => !seen.has(key(row)));
}

/** *rows* plus every row of *additions* whose key is not already present; the same array when none is new. */
export function appendMissingRows<T>(rows: T[], additions: T[], key: (row: T) => string): T[] {
  const seen = new Set(rows.map(key));
  const fresh: T[] = [];
  for (const row of additions) {
    const k = key(row);
    if (!seen.has(k)) {
      seen.add(k);
      fresh.push(row);
    }
  }
  return fresh.length ? [...rows, ...fresh] : rows;
}
