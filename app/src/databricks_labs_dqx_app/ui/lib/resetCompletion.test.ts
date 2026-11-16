import { describe, expect, test } from "bun:test";
import { RESET_POLL_MS, resetJustFinished, resetPollInterval } from "./resetCompletion";

const base = {
  previousState: undefined,
  state: undefined,
  startedAt: "2026-10-30T10:00:00Z",
  awaitingReset: false,
  launchStartedAt: undefined,
};

describe("resetJustFinished", () => {
  test("a reset still running is not finished, so the dialog stays open", () => {
    expect(resetJustFinished({ ...base, previousState: "running", state: "running", awaitingReset: true })).toBe(false);
  });

  test.each(["succeeded", "failed"])("running then %s is finished", (state) => {
    expect(resetJustFinished({ ...base, previousState: "running", state })).toBe(true);
  });

  test("a stale terminal status from an earlier reset is not treated as this one finishing", () => {
    expect(
      resetJustFinished({
        ...base,
        state: "succeeded",
        awaitingReset: true,
        launchStartedAt: "2026-10-30T10:00:00Z",
      }),
    ).toBe(false);
  });

  test("a reset that finishes before the poll sees it running still completes", () => {
    expect(
      resetJustFinished({
        ...base,
        previousState: "succeeded",
        state: "succeeded",
        startedAt: "2026-10-30T11:00:00Z",
        awaitingReset: true,
        launchStartedAt: "2026-10-30T10:00:00Z",
      }),
    ).toBe(true);
  });

  test("on page load a past terminal status never fires completion", () => {
    expect(resetJustFinished({ ...base, state: "succeeded" })).toBe(false);
  });
});

describe("resetPollInterval", () => {
  test("polls while the status reads running", () => {
    expect(resetPollInterval({ state: "running", awaitingReset: false })).toBe(RESET_POLL_MS);
  });

  test("keeps polling while the user waits on a reset, even when the first check failed and left no status", () => {
    expect(resetPollInterval({ state: undefined, awaitingReset: true })).toBe(RESET_POLL_MS);
  });

  test("keeps polling while the user waits on a reset the status hasn't caught up with yet", () => {
    // The last status seen is the previous reset's terminal one.
    expect(resetPollInterval({ state: "succeeded", awaitingReset: true })).toBe(RESET_POLL_MS);
  });

  test.each(["succeeded", "failed", "idle", undefined])("stays idle with nothing to wait on (%s)", (state) => {
    expect(resetPollInterval({ state, awaitingReset: false })).toBe(false);
  });
});

describe("waiting on a reset whose first status check failed", () => {
  test("is released once a later poll returns this reset's terminal status", () => {
    // The check right after launch failed, so the cache still holds the previous
    // run's status; polling continues and the next answer is this reset's outcome.
    const previous = { state: "succeeded", startedAt: "2026-10-30T10:00:00Z" };
    expect(resetPollInterval({ state: previous.state, awaitingReset: true })).toBe(RESET_POLL_MS);
    expect(
      resetJustFinished({
        previousState: previous.state,
        state: "succeeded",
        startedAt: "2026-10-30T11:00:00Z",
        awaitingReset: true,
        launchStartedAt: previous.startedAt,
      }),
    ).toBe(true);
  });
});
