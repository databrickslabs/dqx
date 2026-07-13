/**
 * Pure helpers extracted from `PermissionsTab` so they can be exercised by
 * plain bun tests without a component-render harness (none exists in this
 * app yet). Keep these free of React/JSX and API-client imports.
 */

export const PRIV_SELECT = "SELECT";
export const PRIV_MODIFY = "MODIFY";
export const PRIV_APPLY = "APPLY";
export const PRIV_ALL = "ALL_PRIVILEGES";

/** Minimal shape needed to detect the synthetic workspace-users-group row —
 *  decoupled from the generated `ObjectGrantOut` API type. */
export interface UsersGroupGrantLike {
  principal_id: string;
  is_default?: boolean | null;
}

// The workspace users-group principal id — the visible, manageable day-one
// default (SELECT + APPLY) shown on every object, mirroring how `account
// users` appears in Unity Catalog grants.
export const USERS_GROUP = "users";

export function isUsersGroupGrant(grant: UsersGroupGrantLike): boolean {
  return grant.principal_id === USERS_GROUP || (grant.is_default ?? false);
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
