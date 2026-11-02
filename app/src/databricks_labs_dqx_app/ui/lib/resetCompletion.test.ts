import { describe, expect, test } from "bun:test";
import { resetJustFinished } from "./resetCompletion";

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
