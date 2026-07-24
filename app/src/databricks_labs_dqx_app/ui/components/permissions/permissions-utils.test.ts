import { describe, expect, test } from "bun:test";
import {
  PRIV_ALL,
  PRIV_APPLY,
  PRIV_EXECUTE,
  PRIV_MODIFY,
  PRIV_SELECT,
  findAllPrivilegesGrantByName,
  forceSelectWhenOthers,
  grantsEmptyColSpan,
  hasSavedObject,
  holdsFullAccess,
  initialGrantInherit,
  isAllPrivileges,
  isOwnerDefaultGrant,
  isUsersGroupGrant,
  overlayStewardPreview,
  privilegeTagLabel,
  stewardPreviewPrivileges,
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

  test("true when SELECT + MODIFY + APPLY + EXECUTE are all present regardless of order", () => {
    expect(isAllPrivileges([PRIV_APPLY, PRIV_SELECT, PRIV_MODIFY, PRIV_EXECUTE])).toBe(true);
  });

  test("false when any of the three individual privileges is missing", () => {
    expect(isAllPrivileges([PRIV_SELECT, PRIV_APPLY])).toBe(false);
  });

  test("false for an empty privilege list", () => {
    expect(isAllPrivileges([])).toBe(false);
  });

  test("EXECUTE counts toward ALL — true when SELECT + MODIFY + APPLY + EXECUTE all present", () => {
    expect(isAllPrivileges([PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY, PRIV_EXECUTE])).toBe(true);
  });

  test("false when EXECUTE is missing from the concrete set", () => {
    expect(isAllPrivileges([PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY])).toBe(false);
  });
});

describe("forceSelectWhenOthers", () => {
  test("checking MODIFY forces SELECT on", () => {
    expect(forceSelectWhenOthers({ view: false, modify: true, apply: false, execute: false }).view).toBe(true);
  });

  test("checking APPLY forces SELECT on", () => {
    expect(forceSelectWhenOthers({ view: false, modify: false, apply: true, execute: false }).view).toBe(true);
  });

  test("checking EXECUTE forces SELECT on", () => {
    expect(forceSelectWhenOthers({ view: false, modify: false, apply: false, execute: true }).view).toBe(true);
  });

  test("no other privilege leaves SELECT as-is (false)", () => {
    expect(forceSelectWhenOthers({ view: false, modify: false, apply: false, execute: false }).view).toBe(false);
  });

  test("no other privilege leaves SELECT as-is (true)", () => {
    expect(forceSelectWhenOthers({ view: true, modify: false, apply: false, execute: false }).view).toBe(true);
  });

  test("does not mutate the original draft", () => {
    const original = { view: false, modify: true, apply: false, execute: false };
    const result = forceSelectWhenOthers(original);
    expect(result).not.toBe(original);
    expect(original.view).toBe(false);
    expect(result.view).toBe(true);
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
  // The "Inheritance" column has been removed for all object types.
  // Base columns are now 3 (Principal · Privileges · Granted by) for both
  // rules and non-rules. isRule is kept as a param for call-site compatibility
  // but no longer affects the result.

  test("rule, no manage: Principal + Privileges + Granted By = 3", () => {
    expect(grantsEmptyColSpan(true, false)).toBe(3);
  });

  test("rule, can manage: +1 for the trailing actions column = 4", () => {
    expect(grantsEmptyColSpan(true, true)).toBe(4);
  });

  test("non-rule, no manage: same 3 columns as rule (Inheritance column removed)", () => {
    expect(grantsEmptyColSpan(false, false)).toBe(3);
  });

  test("non-rule, can manage: 3 + actions = 4", () => {
    expect(grantsEmptyColSpan(false, true)).toBe(4);
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

describe("findAllPrivilegesGrantByName", () => {
  const allPrivsGrant = {
    principal_name: "alice@example.com",
    privileges: [PRIV_ALL],
  };
  const selectOnlyGrant = {
    principal_name: "bob@example.com",
    privileges: [PRIV_SELECT],
  };
  const noNameGrant = {
    principal_name: null,
    privileges: [PRIV_ALL],
  };

  test("returns the grant when the named steward holds ALL_PRIVILEGES", () => {
    expect(
      findAllPrivilegesGrantByName([allPrivsGrant, selectOnlyGrant], "alice@example.com", "data_product"),
    ).toBe(allPrivsGrant);
  });

  test("returns null when the named steward holds only partial privileges", () => {
    expect(
      findAllPrivilegesGrantByName([allPrivsGrant, selectOnlyGrant], "bob@example.com", "data_product"),
    ).toBeNull();
  });

  test("returns null when no grant matches the name", () => {
    expect(
      findAllPrivilegesGrantByName([allPrivsGrant, selectOnlyGrant], "charlie@example.com", "data_product"),
    ).toBeNull();
  });

  test("returns null when the name is empty", () => {
    expect(findAllPrivilegesGrantByName([allPrivsGrant], "", "data_product")).toBeNull();
  });

  test("skips grants with null principal_name", () => {
    expect(
      findAllPrivilegesGrantByName([noNameGrant, allPrivsGrant], "alice@example.com", "data_product"),
    ).toBe(allPrivsGrant);
  });

  test("returns null for an empty grants array", () => {
    expect(findAllPrivilegesGrantByName([], "alice@example.com", "data_product")).toBeNull();
  });

  test("matches on the concrete ALL privileges set (no explicit PRIV_ALL marker)", () => {
    const concreteAllGrant = {
      principal_name: "dave@example.com",
      privileges: [PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY, PRIV_EXECUTE],
    };
    expect(findAllPrivilegesGrantByName([concreteAllGrant], "dave@example.com", "data_product")).toBe(
      concreteAllGrant,
    );
  });

  test("matches on the steward's display name when the grant is keyed by display name", () => {
    // Regression: a steward set via the principal picker stores its grant with
    // the DISPLAY NAME as principal_name (keyed by SCIM id), while `steward`
    // holds the email identity. Matching on the email alone missed it, so the
    // remove-old tickbox never appeared on collections/tables. Passing the
    // display name lets it match.
    const displayNamedGrant = {
      principal_name: "Marcin Wojtyczka",
      privileges: [PRIV_ALL],
    };
    // Email identity alone does not match the display-named grant.
    expect(
      findAllPrivilegesGrantByName([displayNamedGrant], "marcin@example.com", "data_product"),
    ).toBeNull();
    // With the display name supplied, it matches.
    expect(
      findAllPrivilegesGrantByName(
        [displayNamedGrant],
        "marcin@example.com",
        "data_product",
        "Marcin Wojtyczka",
      ),
    ).toBe(displayNamedGrant);
  });

  test("still matches an owner grant keyed by email even when a display name is supplied", () => {
    // The owner default grant is named with the email; passing a display name
    // must not break the email match.
    expect(
      findAllPrivilegesGrantByName([allPrivsGrant], "alice@example.com", "data_product", "Alice Example"),
    ).toBe(allPrivsGrant);
  });

  test("registry_rule: matches a SELECT+MODIFY+APPLY owner grant (EXECUTE never grantable on a rule)", () => {
    // Regression: rules store the owner/steward grant without EXECUTE, so the
    // remove-old tickbox was never offered. holdsFullAccess treats that set as
    // full access for registry_rule.
    const ruleOwnerGrant = {
      principal_name: "erin@example.com",
      privileges: [PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY],
    };
    expect(findAllPrivilegesGrantByName([ruleOwnerGrant], "erin@example.com", "registry_rule")).toBe(
      ruleOwnerGrant,
    );
    // The same partial set is NOT full access on a table/collection.
    expect(
      findAllPrivilegesGrantByName([ruleOwnerGrant], "erin@example.com", "data_product"),
    ).toBeNull();
  });
});

describe("holdsFullAccess", () => {
  test("explicit ALL_PRIVILEGES is full access for any object type", () => {
    expect(holdsFullAccess([PRIV_ALL], "registry_rule")).toBe(true);
    expect(holdsFullAccess([PRIV_ALL], "data_product")).toBe(true);
  });

  test("registry_rule: SELECT+MODIFY+APPLY is full access (no EXECUTE)", () => {
    expect(holdsFullAccess([PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY], "registry_rule")).toBe(true);
  });

  test("registry_rule: missing APPLY is not full access", () => {
    expect(holdsFullAccess([PRIV_SELECT, PRIV_MODIFY], "registry_rule")).toBe(false);
  });

  test("table/collection: SELECT+MODIFY+APPLY without EXECUTE is not full access", () => {
    expect(holdsFullAccess([PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY], "monitored_table")).toBe(false);
    expect(holdsFullAccess([PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY, PRIV_EXECUTE], "monitored_table")).toBe(
      true,
    );
  });
});

describe("stewardPreviewPrivileges", () => {
  test("registry rules get the EXECUTE-stripped full set the save writes", () => {
    expect(stewardPreviewPrivileges("registry_rule")).toEqual([PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY]);
  });

  test("tables and collections get ALL_PRIVILEGES", () => {
    expect(stewardPreviewPrivileges("monitored_table")).toEqual([PRIV_ALL]);
    expect(stewardPreviewPrivileges("data_product")).toEqual([PRIV_ALL]);
  });
});

describe("overlayStewardPreview", () => {
  const g = (id: string, extra: Record<string, unknown> = {}) => ({ principal_id: id, ...extra });

  test("returns the grants unchanged when there is no preview", () => {
    const rows = overlayStewardPreview([g("a"), g("b")], null, null);
    expect(rows.map((r) => r.principal_id)).toEqual(["a", "b"]);
  });

  test("appends the new steward as an ordinary row (brand-new principal)", () => {
    const preview = g("new");
    const rows = overlayStewardPreview([g("a")], preview, null);
    expect(rows).toHaveLength(2);
    expect(rows[1]).toBe(preview);
  });

  test("replaces (not duplicates) when the new steward already has a grant", () => {
    const preview = g("owner", { privileges: [PRIV_ALL] });
    const rows = overlayStewardPreview([g("owner", { privileges: [PRIV_SELECT] }), g("b")], preview, null);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toBe(preview);
    expect(rows[1].principal_id).toBe("b");
  });

  test("drops the old steward row when removal was requested (as if deleted)", () => {
    const preview = g("new");
    const rows = overlayStewardPreview([g("old"), g("b")], preview, "old");
    expect(rows.some((r) => r.principal_id === "old")).toBe(false);
    expect(rows.some((r) => r.principal_id === "b")).toBe(true);
    expect(rows.some((r) => r.principal_id === "new")).toBe(true);
  });

  test("preserves the original row order", () => {
    const rows = overlayStewardPreview([g("a"), g("b"), g("c")], null, null);
    expect(rows.map((r) => r.principal_id)).toEqual(["a", "b", "c"]);
  });
});
