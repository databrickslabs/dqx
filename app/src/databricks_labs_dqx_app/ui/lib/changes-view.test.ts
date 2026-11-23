import { describe, expect, test } from "bun:test";
import { NOTHING, PROPOSED_REF, versionRef } from "./history-points";
import {
  comparisonToShow,
  rulePendingStatus,
  shouldQueryExplicitChanges,
  unrecordedCollectionNoteKind,
} from "./changes-view";

const defaults = { source: "defaults" };
const explicit = { source: "explicit" };

describe("comparison loading", () => {
  test("the default comparison reuses the default request instead of fetching twice", () => {
    expect(shouldQueryExplicitChanges(true, true, true, PROPOSED_REF)).toBe(false);
    expect(comparisonToShow(defaults, explicit, true, PROPOSED_REF)).toBe(defaults);
  });

  test("a picked comparison uses its explicit request", () => {
    expect(shouldQueryExplicitChanges(true, true, false, versionRef(2))).toBe(true);
    expect(comparisonToShow(defaults, explicit, false, versionRef(2))).toBe(explicit);
  });

  test("an explicit comparison waits for defaults and stays disabled when the caller is disabled", () => {
    expect(shouldQueryExplicitChanges(true, false, false, versionRef(2))).toBe(false);
    expect(shouldQueryExplicitChanges(false, true, false, versionRef(2))).toBe(false);
  });

  test("a Nothing target renders no comparison and needs no explicit request", () => {
    expect(shouldQueryExplicitChanges(true, true, false, NOTHING)).toBe(false);
    expect(comparisonToShow(defaults, explicit, false, NOTHING)).toBeUndefined();
  });
});

describe("unrecorded collection notes", () => {
  const base = { base_recorded: false, base_version: 1, target_version: 2, target_step: null };

  test("there is no note without the currently shown comparison", () => {
    expect(unrecordedCollectionNoteKind(undefined)).toBeNull();
  });

  test("a recorded comparison has no note", () => {
    expect(unrecordedCollectionNoteKind({ ...base, base_recorded: true })).toBeNull();
  });

  test("a proposed target uses the pending explanation", () => {
    expect(unrecordedCollectionNoteKind({ ...base, target_version: null })).toBe("pending");
  });

  test("a step target uses the history explanation even though target_version is null", () => {
    expect(unrecordedCollectionNoteKind({ ...base, target_version: null, target_step: 9 })).toBe("history");
  });

  test("a version target uses the history explanation", () => {
    expect(unrecordedCollectionNoteKind(base)).toBe("history");
  });
});

describe("rule pending title", () => {
  const before = [{ function: "is_not_null" }];
  const after = [{ function: "is_not_empty" }];

  test("an approved rule's title waits for its versions", () => {
    expect(rulePendingStatus(2, true, [], after)).toBeUndefined();
  });

  test("loaded versions distinguish a pending edit from an unchanged rule", () => {
    expect(rulePendingStatus(2, false, before, after)).toBe(true);
    expect(rulePendingStatus(2, false, before, before)).toBe(false);
  });

  test("a never-approved rule has pending changes once loaded", () => {
    expect(rulePendingStatus(0, false, [], after)).toBe(true);
  });

  test("a never-approved rule's title also waits for its versions", () => {
    expect(rulePendingStatus(0, true, [], after)).toBeUndefined();
  });
});
