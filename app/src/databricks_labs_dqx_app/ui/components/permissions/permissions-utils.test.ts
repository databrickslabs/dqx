import { describe, expect, test } from "bun:test";
import {
  PRIV_ALL,
  PRIV_APPLY,
  PRIV_MODIFY,
  PRIV_SELECT,
  grantsEmptyColSpan,
  hasSavedObject,
  initialGrantInherit,
  isAllPrivileges,
  isOwnerDefaultGrant,
  isUsersGroupGrant,
  privilegeTagLabel,
} from "./permissions-utils";

describe("privilegeTagLabel", () => {
  test("maps ALL_PRIVILEGES to the spaced UC-style label", () => {
    expect(privilegeTagLabel(PRIV_ALL)).toBe("ALL PRIVILEGES");
  });

  test("passes through individual privilege keywords unchanged", () => {
    expect(privilegeTagLabel(PRIV_SELECT)).toBe(PRIV_SELECT);
    expect(privilegeTagLabel(PRIV_MODIFY)).toBe(PRIV_MODIFY);
    expect(privilegeTagLabel(PRIV_APPLY)).toBe(PRIV_APPLY);
  });

  test("passes through unknown values unchanged", () => {
    expect(privilegeTagLabel("SOMETHING_ELSE")).toBe("SOMETHING_ELSE");
  });
});

describe("isAllPrivileges", () => {
  test("true when the explicit ALL_PRIVILEGES marker is present", () => {
    expect(isAllPrivileges([PRIV_ALL])).toBe(true);
  });

  test("true when SELECT + MODIFY + APPLY are all present regardless of order", () => {
    expect(isAllPrivileges([PRIV_APPLY, PRIV_SELECT, PRIV_MODIFY])).toBe(true);
  });

  test("false when any of the three individual privileges is missing", () => {
    expect(isAllPrivileges([PRIV_SELECT, PRIV_APPLY])).toBe(false);
  });

  test("false for an empty privilege list", () => {
    expect(isAllPrivileges([])).toBe(false);
  });
});

describe("isUsersGroupGrant", () => {
  test("true for the literal 'users' principal id", () => {
    expect(isUsersGroupGrant({ principal_id: "users" })).toBe(true);
    expect(isUsersGroupGrant({ principal_id: "users", is_default: true })).toBe(true);
  });

  test("false for the owner default (is_default but not the users group)", () => {
    // The owner/creator default is also is_default but keys on the owner
    // email — it must NOT be classified as the users group.
    expect(isUsersGroupGrant({ principal_id: "creator@x.com", is_default: true })).toBe(false);
  });

  test("false for a regular principal with no default flag", () => {
    expect(isUsersGroupGrant({ principal_id: "alice", is_default: false })).toBe(false);
    expect(isUsersGroupGrant({ principal_id: "alice" })).toBe(false);
  });
});

describe("isOwnerDefaultGrant", () => {
  test("true for an is_default row keyed on a non-users principal (the owner)", () => {
    expect(isOwnerDefaultGrant({ principal_id: "creator@x.com", is_default: true })).toBe(true);
  });

  test("false for the users-group default", () => {
    expect(isOwnerDefaultGrant({ principal_id: "users", is_default: true })).toBe(false);
  });

  test("false for a regular (non-default) grant", () => {
    expect(isOwnerDefaultGrant({ principal_id: "creator@x.com", is_default: false })).toBe(false);
    expect(isOwnerDefaultGrant({ principal_id: "creator@x.com" })).toBe(false);
    expect(isOwnerDefaultGrant({ principal_id: "creator@x.com", is_default: null })).toBe(false);
  });
});

describe("initialGrantInherit", () => {
  test("new grant seeds from the admin default (on)", () => {
    expect(initialGrantInherit(null, true)).toBe(true);
  });

  test("new grant seeds from the admin default (off)", () => {
    expect(initialGrantInherit(null, false)).toBe(false);
  });

  test("editing keeps the grant's stored inherit value over the admin default", () => {
    expect(initialGrantInherit({ inherit: false }, true)).toBe(false);
    expect(initialGrantInherit({ inherit: true }, false)).toBe(true);
  });

  test("editing falls back to the admin default when inherit is null/undefined", () => {
    expect(initialGrantInherit({ inherit: null }, true)).toBe(true);
    expect(initialGrantInherit({}, true)).toBe(true);
  });
});

describe("grantsEmptyColSpan", () => {
  test("rule, no manage: Principal + Privileges + Granted By = 3", () => {
    expect(grantsEmptyColSpan(true, false)).toBe(3);
  });

  test("rule, can manage: +1 for the trailing actions column = 4", () => {
    expect(grantsEmptyColSpan(true, true)).toBe(4);
  });

  test("non-rule, no manage: + Inheritance column = 4", () => {
    expect(grantsEmptyColSpan(false, false)).toBe(4);
  });

  test("non-rule, can manage: Inheritance + actions = 5", () => {
    expect(grantsEmptyColSpan(false, true)).toBe(5);
  });
});

describe("hasSavedObject", () => {
  test("false for an empty object id (not-yet-created object)", () => {
    expect(hasSavedObject("")).toBe(false);
  });

  test("true for any non-empty object id", () => {
    expect(hasSavedObject("00000000-0000-0000-0000-000000000000")).toBe(true);
  });
});
