/**
 * Pure helpers extracted from `PermissionsTab` so they can be exercised by
 * plain bun tests without a component-render harness (none exists in this
 * app yet). Keep these free of React/JSX and API-client imports.
 */

export const PRIV_SELECT = "SELECT";
export const PRIV_MODIFY = "MODIFY";
export const PRIV_APPLY = "APPLY";
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
 *  New grants seed from the admin `permissions_default_inherit` setting; an
 *  existing grant keeps its stored value (falling back to the admin default). */
export function initialGrantInherit(
  editing: { inherit?: boolean | null } | null,
  defaultInherit: boolean,
): boolean {
  if (editing) return editing.inherit ?? defaultInherit;
  return defaultInherit;
}

export function isAllPrivileges(privileges: string[]): boolean {
  if (privileges.includes(PRIV_ALL)) return true;
  return [PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY].every((p) => privileges.includes(p));
}

// Privilege tags render as the canonical Unity-Catalog-style grant keyword
// (SELECT, MODIFY, APPLY, ALL PRIVILEGES) rather than a humanized paraphrase.
export function privilegeTagLabel(p: string): string {
  return p === PRIV_ALL ? "ALL PRIVILEGES" : p;
}

/**
 * Column count for the empty-grants placeholder row. Registry rules omit
 * the "Inheritance" column (there's nothing beneath a rule to inherit a
 * grant to — see `PermissionsTab`'s module doc), so their base column count
 * is one narrower than tables/table spaces. An extra column is added when
 * the viewer can manage grants (the trailing actions column).
 */
export function grantsEmptyColSpan(isRule: boolean, canManage: boolean): number {
  return (isRule ? 3 : 4) + (canManage ? 1 : 0);
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
