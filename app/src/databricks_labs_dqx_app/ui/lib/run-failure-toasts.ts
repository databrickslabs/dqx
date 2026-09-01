// ---------------------------------------------------------------------------
// App-wide run-failure toast decision logic (item 58).
//
// Pure, side-effect-free helpers backing ``useRunFailureToasts``. Extracted
// here (a) so the storm-avoidance / dedup logic can be unit-tested without
// React, and (b) so the hook stays a thin polling+toast shell.
// ---------------------------------------------------------------------------

const SQL_CHECK_PREFIX = "__sql_check__/";

/** Minimal projection of a run needed to decide whether to toast it. */
export interface RunFailureCandidate {
  /** Stable per-run key. Composite (``kind:run_id``) so a profiling and a
   *  validation run can never collide on a bare run_id. */
  key: string;
  run_id: string;
  kind: "validation" | "profiling";
  status: string | null;
  source_table_fqn: string;
}

export interface FailureToastDecision {
  /** Runs that should fire a failure toast this pass (empty during seeding). */
  toToast: RunFailureCandidate[];
  /** Updated "already handled" set — every run either toasted or seeded. */
  seen: Set<string>;
}

/**
 * Decide which runs to toast for, given the current listing and the set of
 * run keys already handled this session.
 *
 * Two modes, keyed by ``seeded``:
 *   - **Seeding pass** (`seeded === false`): mark every currently *non-RUNNING*
 *     run as already handled and toast nothing. This is the load-time storm
 *     guard — failures that happened before the app opened are absorbed into
 *     the seen set, never surfaced. RUNNING runs are deliberately left unseen
 *     so we can catch their RUNNING→FAILED transition afterwards.
 *   - **Steady state** (`seeded === true`): toast any FAILED run whose key is
 *     not yet in the seen set (either a run we watched transition from RUNNING,
 *     or a newly-appeared FAILED run), then record it so a re-poll or remount
 *     never re-toasts it.
 *
 * Never mutates its inputs — returns a fresh ``seen`` set.
 */
export function selectFailureToasts(
  runs: readonly RunFailureCandidate[],
  seen: ReadonlySet<string>,
  seeded: boolean,
): FailureToastDecision {
  const next = new Set(seen);

  if (!seeded) {
    for (const run of runs) {
      if (run.status !== "RUNNING") next.add(run.key);
    }
    return { toToast: [], seen: next };
  }

  const toToast: RunFailureCandidate[] = [];
  for (const run of runs) {
    if (run.status === "FAILED" && !next.has(run.key)) {
      toToast.push(run);
      next.add(run.key);
    }
  }
  return { toToast, seen: next };
}

/**
 * Short, human-facing label for a run's target table — the last dotted segment
 * of the FQN (the table name), or the cleaned name for a synthetic cross-table
 * SQL check (``__sql_check__/<name>``).
 */
export function runFailureTableLabel(fqn: string): string {
  const clean = fqn.startsWith(SQL_CHECK_PREFIX) ? fqn.slice(SQL_CHECK_PREFIX.length) : fqn;
  const segments = clean.split(".");
  return segments[segments.length - 1] || clean;
}
