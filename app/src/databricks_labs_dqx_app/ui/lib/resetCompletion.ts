/** Whether a database reset the user is waiting on has just finished.
 *
 *  The reset runs in the background and its outcome is polled. It counts as
 *  finished when the polled status turns terminal after having been seen
 *  `running`, or (when the user launched a reset and is waiting on it) when a
 *  terminal status belongs to a newer run than the one before launch. The
 *  second case covers a reset that completes before the poll ever sees it
 *  running; without it the dialog would stay locked. */
export function resetJustFinished({
  previousState,
  state,
  startedAt,
  awaitingReset,
  launchStartedAt,
}: {
  previousState: string | undefined;
  state: string | undefined;
  startedAt: string | undefined;
  awaitingReset: boolean;
  launchStartedAt: string | undefined;
}): boolean {
  const terminal = state === "succeeded" || state === "failed";
  if (!terminal) return false;
  if (previousState === "running") return true;
  return awaitingReset && startedAt !== launchStartedAt;
}

/** Milliseconds between reset-status polls. */
export const RESET_POLL_MS = 2000;

/** How often to poll the reset status: while a reset is running, and the whole
 *  time the user is waiting on one they launched. Polling on `awaitingReset`
 *  alone (not just on a polled `running`) keeps the poll going when the first
 *  status check after launch fails or hasn't caught up yet, which would
 *  otherwise leave the dialog locked with nothing left to unlock it. */
export function resetPollInterval({
  state,
  awaitingReset,
}: {
  state: string | undefined;
  awaitingReset: boolean;
}): number | false {
  return state === "running" || awaitingReset ? RESET_POLL_MS : false;
}
