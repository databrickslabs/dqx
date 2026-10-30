import { test } from "@playwright/test";

/**
 * Wraps the one assertion that a known Studio bug currently breaks.
 *
 * Unlike test.fail(), only *assertion* is expected to fail — setup and earlier
 * steps still fail the test normally, so a broken setup can't masquerade as
 * "bug still present". While the bug exists the test passes and is annotated
 * "known-bug"; once the assertion starts passing the test fails, telling you to
 * remove the wrapper (the assertion then guards against regressions).
 */
export async function knownBug(description: string, assertion: () => Promise<unknown>): Promise<void> {
  let error: unknown;
  try {
    await assertion();
  } catch (err) {
    error = err;
  }
  if (error === undefined) {
    throw new Error(`Known bug appears fixed — remove the knownBug() wrapper: ${description}`);
  }
  const detail = error instanceof Error ? error.message.split("\n")[0] : String(error);
  test.info().annotations.push({ type: "known-bug", description: `${description} (${detail})` });
  console.log(`KNOWN BUG: ${description}`);
}
