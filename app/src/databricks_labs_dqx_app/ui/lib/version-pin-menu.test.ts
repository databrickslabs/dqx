import { describe, expect, test } from "bun:test";
import { buildVersionPinMenuModel } from "./version-pin-menu";

// P24 fix: the applied-rule VersionPinDropdown used to only offer "Latest"
// and "v<current> (pinned)" — after a rule reached v3, v1/v2 were
// unreachable even though every version is stored and pinnable. These
// tests cover the pure menu-model derivation that now drives the full
// version history, mirroring MemberVersionPin's shape.
describe("buildVersionPinMenuModel", () => {
  test("follow-latest (no pin): latest entry unchecked list, followLatestChecked true, not stale", () => {
    const model = buildVersionPinMenuModel([3, 2, 1], null, 3);
    expect(model.followLatestChecked).toBe(true);
    expect(model.latestVersion).toBe(3);
    expect(model.entries).toEqual([
      { version: 3, checked: false },
      { version: 2, checked: false },
      { version: 1, checked: false },
    ]);
    expect(model.stale).toBe(false);
  });

  test("pinned to current latest: entry checked, not stale", () => {
    const model = buildVersionPinMenuModel([3, 2, 1], 3, 3);
    expect(model.followLatestChecked).toBe(false);
    expect(model.entries.find((e) => e.version === 3)?.checked).toBe(true);
    expect(model.stale).toBe(false);
  });

  test("pinned to a historical version (v1 of 3): v1 checked, stale amber indicator on", () => {
    const model = buildVersionPinMenuModel([3, 2, 1], 1, 3);
    expect(model.followLatestChecked).toBe(false);
    expect(model.entries).toEqual([
      { version: 3, checked: false },
      { version: 2, checked: false },
      { version: 1, checked: true },
    ]);
    expect(model.stale).toBe(true);
  });

  test("pinned to a middle historical version (v2 of 3): v2 checked, stale", () => {
    const model = buildVersionPinMenuModel([3, 2, 1], 2, 3);
    expect(model.entries.find((e) => e.version === 2)?.checked).toBe(true);
    expect(model.stale).toBe(true);
  });

  test("unsorted/duplicated input versions are deduplicated and sorted descending", () => {
    const model = buildVersionPinMenuModel([1, 3, 2, 3, 1], null, 3);
    expect(model.entries.map((e) => e.version)).toEqual([3, 2, 1]);
  });

  test("single published version: only entry, no staleness possible", () => {
    const model = buildVersionPinMenuModel([1], null, 1);
    expect(model.entries).toEqual([{ version: 1, checked: false }]);
    expect(model.stale).toBe(false);
  });

  test("undefined pinnedVersion is treated the same as null (follow latest)", () => {
    const model = buildVersionPinMenuModel([2, 1], undefined, 2);
    expect(model.followLatestChecked).toBe(true);
    expect(model.stale).toBe(false);
  });
});
