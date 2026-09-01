/**
 * Pure helpers extracted from `PermissionsTab` so they can be exercised by
 * plain bun tests without a component-render harness (none exists in this
 * app yet). Keep these free of React/JSX and API-client imports.
 */

export const PRIV_SELECT = "SELECT";
export const PRIV_MODIFY = "MODIFY";
export const PRIV_APPLY = "APPLY";
export const PRIV_EXECUTE = "EXECUTE";
export const PRIV_MANAGE = "MANAGE";
export const PRIV_ALL = "ALL_PRIVILEGES";

/** Minimal shape needed to detect the synthetic default rows (users-group and
 *  owner/creator) — decoupled from the generated `ObjectGrantOut` API type. */
export interface UsersGroupGrantLike {
  principal_id: string;
  is_default?: boolean | null;
}

// The workspace users-group principal id — the visible, manageable day-one
// default (SELECT + APPLY) shown on every object, mirroring how `account
// users` appears in Unity Catalog grants.
export const USERS_GROUP = "users";

// The synthetic default rows are distinguished by principal, not just the
// `is_default` flag: the users-group default keys on the `"users"` principal,
// while the owner/creator default (also `is_default`) keys on the owner email.
export function isUsersGroupGrant(grant: UsersGroupGrantLike): boolean {
  return grant.principal_id === USERS_GROUP;
}

/** The synthetic owner/creator default row: flagged `is_default` but keyed on
 *  the owner's email rather than the workspace users group. Rendered read-only
 *  with an "owner" label — the creator's implicit ALL PRIVILEGES surfaced for
 *  display parity (enforcement grants it regardless). */
export function isOwnerDefaultGrant(grant: UsersGroupGrantLike): boolean {
  return (grant.is_default ?? false) && grant.principal_id !== USERS_GROUP;
}

/** Initial state of a new/edited grant's "inherit to child objects" toggle.
 *  New grants seed from the cascade default (always ON); an existing grant
 *  keeps its stored value (falling back to that default). */
export function initialGrantInherit(
  editing: { inherit?: boolean | null } | null,
  defaultInherit: boolean,
): boolean {
  if (editing) return editing.inherit ?? defaultInherit;
  return defaultInherit;
}

export function isAllPrivileges(privileges: string[]): boolean {
  if (privileges.includes(PRIV_ALL)) return true;
  return [PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY, PRIV_EXECUTE].every((p) => privileges.includes(p));
}

/**
 * Whether *privileges* amount to full access on an object of *objectType*.
 *
 * Differs from {@link isAllPrivileges} for `registry_rule`: EXECUTE is
 * meaningless on a rule (the backend strips it — rules are never run
 * directly), so a rule's owner grant is stored as
 * `{SELECT, MODIFY, APPLY}` and NEVER carries EXECUTE. That set is the maximum
 * grantable on a rule, i.e. full access. For tables/collections EXECUTE is a
 * real privilege, so full access still requires all four (or ALL_PRIVILEGES) —
 * a SELECT+MODIFY+APPLY grant there is a genuine partial and must not read as
 * full. Used only to decide whether the "remove old owner's grant" tickbox
 * is meaningful; rendering keeps using {@link isAllPrivileges}.
 */
export function holdsFullAccess(privileges: string[], objectType: string): boolean {
  if (privileges.includes(PRIV_ALL)) return true;
  if (objectType === "registry_rule") {
    return [PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY].every((p) => privileges.includes(p));
  }
  return isAllPrivileges(privileges);
}

export interface GrantDraftPrivs {
  view: boolean;
  modify: boolean;
  apply: boolean;
  execute: boolean;
  manage: boolean;
}

/** When any of modify/apply/execute/manage is set, SELECT (view) must be on:
 *  you can't act on what you can't see. Returns the draft with view forced. */
export function forceSelectWhenOthers<T extends GrantDraftPrivs>(draft: T): T {
  const others = draft.modify || draft.apply || draft.execute || draft.manage;
  return others ? { ...draft, view: true } : draft;
}

// Privilege tags render as the canonical Unity-Catalog-style grant keyword
// (SELECT, MODIFY, APPLY, ALL PRIVILEGES) rather than a humanized paraphrase.
export function privilegeTagLabel(p: string): string {
  return p === PRIV_ALL ? "ALL PRIVILEGES" : p;
}

/**
 * Finds the first grant whose principal matches the owner AND holds full
 * access on an object of `objectType`.  Returns that grant (to get its
 * `principal_id`) or `null` when the old owner has no full-access grant —
 * used to decide whether the "remove old owner's grant" tickbox should
 * appear in the OwnerGrantDialog.
 *
 * Matches a grant's `principal_name` against EITHER the owner identity
 * (`name`, usually the email/username) OR the owner's display name
 * (`displayName`). This matters because the old owner's grant may have been
 * stored with the DISPLAY NAME as its `principal_name` (a grant set via the
 * principal picker is keyed by SCIM id and named with the display name), while
 * `owner` holds the email identity — so a name-only match on the email would
 * miss it and the tickbox would never appear. The owner default grant, by
 * contrast, is named with the email, so both forms must be accepted.
 *
 * `objectType` matters because a `registry_rule` owner grant is stored
 * as `{SELECT, MODIFY, APPLY}` (EXECUTE is never grantable on a rule), which is
 * full access there — see {@link holdsFullAccess}. Passing the object type
 * therefore lets the tickbox appear on rules, not just tables/collections.
 */
export function findAllPrivilegesGrantByName<
  T extends { principal_name?: string | null; privileges?: string[] | null },
>(grants: T[], name: string, objectType: string, displayName?: string | null): T | null {
  if (!name && !displayName) return null;
  const candidates = new Set<string>();
  if (name) candidates.add(name);
  if (displayName) candidates.add(displayName);
  return (
    grants.find(
      (g) => candidates.has(g.principal_name ?? "") && holdsFullAccess(g.privileges ?? [], objectType),
    ) ?? null
  );
}

/**
 * Column count for the empty-grants placeholder row. The "Inheritance"
 * column has been removed from the table for all object types, so the base
 * column count is now 3 (Principal · Privileges · Granted by) regardless of
 * object type. An extra column is added when the viewer can manage grants
 * (the trailing actions column).
 */
export function grantsEmptyColSpan(_isRule: boolean, canManage: boolean): number {
  return 3 + (canManage ? 1 : 0);
}

/**
 * Grants require a saved object id (they key on `object_id`, which doesn't
 * exist until the object is first saved). Objects still being created pass
 * an empty `objectId` and should render the "save first" empty shell
 * instead of the grants table.
 */
export function hasSavedObject(objectId: string): boolean {
  return objectId.length > 0;
}

/**
 * The full-access privilege set the caller's save handler actually WRITES for
 * a new owner, keyed by object type — must mirror the real grant write so
 * the optimistic preview row shows exactly what save will persist:
 *  - `registry_rule` → `{SELECT, MODIFY, APPLY}` (EXECUTE is meaningless on a
 *    rule and is stripped; see `RegistryRuleFormDialog.handleSave`).
 *  - tables / collections → `{ALL_PRIVILEGES}` (see `useEditProductState`).
 */
export function ownerPreviewPrivileges(objectType: string): string[] {
  return objectType === "registry_rule"
    ? [PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY]
    : [PRIV_ALL];
}

/**
 * Project a staged owner change onto the real `grants` list so the grants
 * table shows the RESULT of the change immediately — exactly as it will look
 * once saved — the instant the OwnerGrantDialog is confirmed. The write is
 * deferred to object-save time, but the table treats the change as if it has
 * already happened (optimistic UI): no "unsaved" badges, no strike-through.
 *
 * - `previewGrant` (the new owner's full-access row) REPLACES an existing
 *   row with the same `principal_id` (new owner === an existing grantee,
 *   e.g. the owner — no duplicate row), otherwise it is appended as an
 *   ordinary row.
 * - `removeOldPrincipalId`, when set, DROPS the matching existing row (the old
 *   owner's grant the user ticked to revoke) — as if its delete button had
 *   been pressed.
 *
 * Pure and order-preserving so it can be unit-tested without a render harness.
 */
export function overlayOwnerPreview<T extends { principal_id: string }>(
  grants: T[],
  previewGrant: T | null,
  removeOldPrincipalId: string | null,
): T[] {
  const rows: T[] = [];
  let replaced = false;
  for (const grant of grants) {
    if (previewGrant && grant.principal_id === previewGrant.principal_id) {
      rows.push(previewGrant);
      replaced = true;
    } else if (removeOldPrincipalId != null && grant.principal_id === removeOldPrincipalId) {
      // Old owner's grant removed on save — hide it now, as if deleted.
      continue;
    } else {
      rows.push(grant);
    }
  }
  if (previewGrant && !replaced) {
    rows.push(previewGrant);
  }
  return rows;
}
