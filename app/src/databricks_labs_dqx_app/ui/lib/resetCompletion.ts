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
