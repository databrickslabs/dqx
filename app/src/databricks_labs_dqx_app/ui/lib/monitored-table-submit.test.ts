import { describe, expect, test } from "bun:test";
import { hasUnapprovedSavedChanges, nothingToSubmit } from "./monitored-table-submit";

describe("nothingToSubmit", () => {
  test("an approved table with no edits has nothing to submit", () => {
    expect(nothingToSubmit({ isDirty: false, status: "approved", unapprovedRuleChanges: false })).toBe(true);
    expect(nothingToSubmit({ isDirty: false, status: "approved" })).toBe(true);
  });

  test("a saved edit that is not approved yet can be submitted", () => {
    expect(nothingToSubmit({ isDirty: false, status: "approved", unapprovedRuleChanges: true })).toBe(false);
  });

  test("unsaved edits can be submitted", () => {
    expect(nothingToSubmit({ isDirty: true, status: "approved", unapprovedRuleChanges: false })).toBe(false);
  });

  test.each(["draft", "rejected", "pending_approval"])("a %s table can be submitted", (status) => {
    expect(nothingToSubmit({ isDirty: false, status, unapprovedRuleChanges: false })).toBe(false);
  });
});

describe("hasUnapprovedSavedChanges", () => {
  test("only an approved table with saved, unapproved edits shows the note", () => {
    expect(hasUnapprovedSavedChanges({ isDirty: false, status: "approved", unapprovedRuleChanges: true })).toBe(true);
    expect(hasUnapprovedSavedChanges({ isDirty: true, status: "approved", unapprovedRuleChanges: true })).toBe(true);
    expect(hasUnapprovedSavedChanges({ isDirty: false, status: "approved", unapprovedRuleChanges: false })).toBe(false);
    expect(hasUnapprovedSavedChanges({ isDirty: false, status: "approved" })).toBe(false);
    expect(hasUnapprovedSavedChanges({ isDirty: false, status: "draft", unapprovedRuleChanges: true })).toBe(false);
  });
});
