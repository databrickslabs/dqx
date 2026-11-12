/** Re-read a monitored table while a rule policy evaluation started by opening it is running.
 *
 *  Opening a table evaluates the rule policies reaching it in the background; the
 *  detail response says so with `policy_evaluation_pending`. The page then re-reads
 *  the detail with a growing delay while it stays pending, for about a minute in
 *  total (the server retries a claim that has run longer than that), and merges
 *  rows the evaluation attached into the staged editor. The server debounces
 *  repeated opens, so `pending` turns false once the evaluation is done. */

export const POLICY_REFETCH_FIRST_DELAY_MS = 2500;
export const POLICY_REFETCH_MAX_DELAY_MS = 15_000;
export const POLICY_REFETCH_BUDGET_MS = 60_000;
const BACKOFF = 1.6;

function delayFor(attempt: number): number {
  return Math.min(Math.round(POLICY_REFETCH_FIRST_DELAY_MS * BACKOFF ** attempt), POLICY_REFETCH_MAX_DELAY_MS);
}

/** Delay before re-read number *attemptsSoFar* + 1, or `null` when no re-read is due. */
export function nextPolicyRefetchDelay(pending: boolean | undefined, attemptsSoFar: number): number | null {
  if (pending !== true) return null;
  let elapsed = 0;
  for (let attempt = 0; attempt < attemptsSoFar; attempt++) elapsed += delayFor(attempt);
  const delay = delayFor(attemptsSoFar);
  return elapsed + delay <= POLICY_REFETCH_BUDGET_MS ? delay : null;
}

/** Re-read count to continue from: a new evaluation (pending going false -> true) restarts the budget. */
export function refetchAttemptsAfter(
  previousPending: boolean | undefined,
  pending: boolean | undefined,
  attempts: number,
): number {
  return previousPending === false && pending === true ? 0 : attempts;
}

/** Persisted ids of the rows the editor loaded — sent with a save so it only removes what the user saw. */
export function knownAppliedIds(rows: { id?: string | null }[]): string[] {
  return [...new Set(rows.map((row) => row.id).filter((id): id is string => typeof id === "string" && id !== ""))];
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
