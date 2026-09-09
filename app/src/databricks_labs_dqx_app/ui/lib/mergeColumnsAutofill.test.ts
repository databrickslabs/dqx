import { describe, expect, test } from "bun:test";
import { computeMergeColumnsAutofill } from "./mergeColumnsAutofill";

describe("computeMergeColumnsAutofill", () => {
  test("fills from slot names when field is empty", () => {
    expect(computeMergeColumnsAutofill("", ["id", "region"])).toBe("id, region");
  });
  test("leaves a non-empty field untouched", () => {
    expect(computeMergeColumnsAutofill("id", ["id", "region"])).toBeNull();
  });
  test("treats whitespace-only as empty", () => {
    expect(computeMergeColumnsAutofill("   ", ["id"])).toBe("id");
  });
  test("returns null when there are no slots", () => {
    expect(computeMergeColumnsAutofill("", [])).toBeNull();
  });
});
