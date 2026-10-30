// Every object a journey creates is named with E2E_PREFIX so leftovers from an
// aborted run can be recognised and swept (see global-setup.ts). Registry rules
// also carry the E2E_TAG free-text tag, which the list endpoint can filter on.

export const E2E_PREFIX = "e2e-";
export const E2E_TAG = "e2e_suite";

/** Unique per `playwright test` invocation; shared by all workers via the environment. */
export function runTag(): string {
  if (!process.env.DQX_E2E_RUN_TAG) {
    const stamp = new Date().toISOString().replace(/\D/g, "").slice(2, 12);
    process.env.DQX_E2E_RUN_TAG = `${stamp}${Math.random().toString(36).slice(2, 6)}`;
  }
  return process.env.DQX_E2E_RUN_TAG;
}

/** A unique, human-readable name, e.g. "e2e-2610291742ab3x-orders-not-null-w0". */
export function e2eName(label: string, workerIndex = 0): string {
  return `${E2E_PREFIX}${runTag()}-${label}-w${workerIndex}`;
}
