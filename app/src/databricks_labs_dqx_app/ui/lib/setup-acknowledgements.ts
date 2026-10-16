/**
 * Setup warnings an administrator has acknowledged in this browser.
 *
 * Warnings such as unverifiable app sharing are re-reported on every unattended
 * startup check, so the acknowledgement is remembered by warning key rather than
 * per session. A warning with a new key is shown again.
 */
const STORAGE_KEY = "dqx-setup-acknowledged-warnings";

export function readAcknowledgedWarnings(): Set<string> {
  try {
    const parsed: unknown = JSON.parse(
      globalThis.localStorage?.getItem(STORAGE_KEY) ?? "[]",
    );
    return new Set(
      Array.isArray(parsed)
        ? parsed.filter((key): key is string => typeof key === "string")
        : [],
    );
  } catch {
    return new Set();
  }
}

export function acknowledgeWarnings(
  current: ReadonlySet<string>,
  keys: readonly string[],
): Set<string> {
  const next = new Set([...current, ...keys]);
  try {
    globalThis.localStorage?.setItem(STORAGE_KEY, JSON.stringify([...next]));
  } catch {
    // Storage unavailable (private mode, quota): the acknowledgement lasts for this page only.
  }
  return next;
}
