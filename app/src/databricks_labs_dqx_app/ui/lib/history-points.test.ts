import { describe, expect, test } from "bun:test";
import {
  NOTHING,
  historyPoints,
  pickerPoints,
  parseRefKey,
  pointTime,
  refKey,
  selectionFor,
  stepRef,
  versionRef,
  type TimelineStep,
} from "./history-points";

// v1 approved, then a draft saved, submitted, approved as v2, then another draft saved.
const versions = [
  { version: 1, createdAt: "2026-10-01T10:00:00Z" },
  { version: 2, createdAt: "2026-10-03T10:00:00Z" },
];
const steps: TimelineStep[] = [
  { action: "created", at: "2026-10-01T09:00:00Z", historyId: 1, hasSnapshot: true },
  { action: "edited", at: "2026-10-02T09:00:00Z", historyId: 2, hasSnapshot: true },
  { action: "submitted", at: "2026-10-02T12:00:00Z", historyId: 3, hasSnapshot: true },
  { action: "edited", at: "2026-10-04T09:00:00Z", historyId: 5, hasSnapshot: true },
  { action: "edited", at: "2026-09-01T09:00:00Z", historyId: 9, hasSnapshot: false },
];
const points = historyPoints(versions, steps);

describe("refs", () => {
  test("round-trip through their keys", () => {
    for (const ref of [versionRef(3), stepRef(12), { kind: "proposed" as const }, NOTHING]) {
      expect(parseRefKey(refKey(ref))).toEqual(ref);
    }
  });
});

describe("historyPoints", () => {
  test("lists the versions and the saved drafts, newest first; other steps aren't points", () => {
    expect(points.map((p) => refKey(p.ref))).toEqual(["s5", "v2", "s2", "v1", "s1"]);
  });

  test("states with the same or an unknown time sort by when they were recorded, newest first", () => {
    const same = "2026-10-01T10:00:00Z";
    const tied = historyPoints(
      [],
      [
        { action: "edited", at: same, historyId: 7, hasSnapshot: true },
        { action: "edited", at: same, historyId: 9, hasSnapshot: true },
        { action: "edited", at: null, historyId: 3, hasSnapshot: true },
        { action: "edited", at: null, historyId: 4, hasSnapshot: true },
      ],
    );
    expect(tied.map((p) => refKey(p.ref))).toEqual(["s9", "s7", "s4", "s3"]);
  });

  test("the created state isn't offered in the pickers", () => {
    expect(pickerPoints(points).map((p) => refKey(p.ref))).toEqual(["s5", "v2", "s2", "v1"]);
  });

  test("the proposed changes are the newest and nothing the oldest", () => {
    expect(pointTime({ kind: "proposed" }, points)).toBe(Number.POSITIVE_INFINITY);
    expect(pointTime(NOTHING, points)).toBe(Number.NEGATIVE_INFINITY);
  });
});

describe("selectionFor", () => {
  const key = (s: ReturnType<typeof selectionFor>) => (s ? `${refKey(s.base)}→${refKey(s.target)}` : null);

  test("a version is shown against nothing", () => {
    expect(key(selectionFor({ kind: "version", version: 2 }, points))).toBe("v0→v2");
  });

  test("an edit shows its draft against nothing", () => {
    expect(key(selectionFor({ kind: "step", ...steps[1] }, points))).toBe("v0→s2");
    expect(key(selectionFor({ kind: "step", ...steps[3] }, points))).toBe("v0→s5");
  });

  test("creating the object shows nothing", () => {
    expect(key(selectionFor({ kind: "step", ...steps[0] }, points))).toBe("v0→v0");
  });

  test("a submit shows the draft under review then", () => {
    expect(key(selectionFor({ kind: "step", ...steps[2] }, points))).toBe("v0→s2");
  });

  test("a step without a recorded state shows the version its cycle produced", () => {
    expect(key(selectionFor({ kind: "step", ...steps[4] }, points))).toBe("v0→v1");
  });

  test("a submit with no edit since creating reviews the created state", () => {
    const created: TimelineStep = { action: "created", at: "2026-09-05T11:06:00Z", historyId: 1, hasSnapshot: true };
    const submit: TimelineStep = { action: "submitted", at: "2026-09-05T11:07:00Z", historyId: 2, hasSnapshot: true };
    const pts = historyPoints([], [created, submit]);
    expect(key(selectionFor({ kind: "step", ...submit }, pts))).toBe("v0→s1");
  });
});
