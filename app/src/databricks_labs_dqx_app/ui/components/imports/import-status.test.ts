import { describe, expect, test } from "bun:test";
import { isImportStatus, shouldAutoApproveImport } from "./import-status";

describe("shouldAutoApproveImport", () => {
  test("publishes only when the user can approve rules", () => {
    expect(shouldAutoApproveImport("published", true)).toBe(true);
    expect(shouldAutoApproveImport("published", false)).toBe(false);
  });

  test("drafts are never auto-approved", () => {
    expect(shouldAutoApproveImport("draft", true)).toBe(false);
    expect(shouldAutoApproveImport("draft", false)).toBe(false);
  });
});

describe("isImportStatus", () => {
  test("accepts the two statuses and rejects anything else", () => {
    expect(isImportStatus("draft")).toBe(true);
    expect(isImportStatus("published")).toBe(true);
    expect(isImportStatus("approved")).toBe(false);
    expect(isImportStatus("")).toBe(false);
  });
});
