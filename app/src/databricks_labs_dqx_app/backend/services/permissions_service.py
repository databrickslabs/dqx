"""Object-permissions service — UC-style grants CRUD, inheritance resolution,
and enforcement (P22-D item 10).

Stores per-object grants in ``dq_object_grants`` (+ append-only
``dq_object_grants_history`` audit trail). All operations use the app's
service principal executor, mirroring :class:`RoleService`. The privilege
model, hierarchy, baseline, and role-layering rules live in
:mod:`backend.common.permissions`.

Enforcement contract (see :meth:`require`): roles remain the coarse gate
(``require_role`` still guards routes); object grants refine *within* a role.
``ADMIN``/``RULE_APPROVER`` bypass object grants (UC owner/admin convention);
the object creator is owner-equivalent (implicit ``ALL_PRIVILEGES``); the
workspace users group holds
:data:`~backend.common.permissions.DEFAULT_USERS_GROUP_PRIVILEGES` on every
object by default (implicit-unless-overridden) so existing flows keep working
on day one.

Roles are the HARD CEILING (entitlements invariant, item #43). An object
grant can only ever confer a member of the :class:`~backend.common.permissions.Privilege`
vocabulary (``SELECT`` / ``MODIFY`` / ``APPLY``) *on a single object* — it is
purely additive within whatever a route's ``require_role`` guard already
admits, and there is no code path by which a grant satisfies, widens, or
substitutes for that role check. Concretely: (1) the object-grant vocabulary
is disjoint from the role-permission vocabulary (see
:data:`~backend.common.authorization.PERMISSIONS`), so a grant cannot express
a role capability such as ``approve_rules`` or ``manage_roles``; (2) a broad
grant (even ``ALL_PRIVILEGES``) never mutates the caller's resolved
:class:`~backend.common.authorization.UserRole`, so a low-role user hitting a
role-gated route is still rejected by ``require_role`` regardless of any
grant. The ``ADMIN``/``RULE_APPROVER`` bypass is the *upper* boundary of that
ceiling — a deliberate widening for governance roles, never a way for a lower
role to climb. This invariant is pinned by ``tests/test_entitlements_hard_boundary.py``.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime

from fastapi import HTTPException, status

from databricks_labs_dqx_app.backend.common.authorization import UserRole, get_permissions_for_role
from databricks_labs_dqx_app.backend.common.permissions import (
    DEFAULT_USERS_GROUP_PRIVILEGES,
    USERS_GROUP_PRINCIPAL_ID,
    USERS_GROUP_PRINCIPAL_NAME,
    CHILD_TO_PARENT_TYPE,
    ObjectType,
    PrincipalType,
    Privilege,
    expand_privileges,
    is_reserved_principal_id,
    is_users_group,
    normalize_privileges,
    parse_privileges,
    serialize_privileges,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.sql_executor import OltpExecutorProtocol
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_object_id

logger = logging.getLogger(__name__)

# Roles that bypass object grants entirely (UC owner/admin convention). An
# approver is a governance role in this app, so it is trusted the same way an
# admin is for the purpose of object-level mutations.
_ROLE_BYPASS: frozenset[UserRole] = frozenset({UserRole.ADMIN, UserRole.RULE_APPROVER})


@dataclass
class ObjectGrant:
    """A single stored grant row (one principal on one object)."""

    object_type: str
    object_id: str
    principal_id: str
    principal_type: str
    principal_name: str | None
    privileges: set[Privilege]
    inherit: bool
    grantor: str | None = None
    updated_at: datetime | None = None
    # UI-only: set on inherited grants surfaced on a child object. ``None``
    # for direct grants. Holds the parent object's type+id it flowed from.
    inherited_from_type: str | None = None
    inherited_from_id: str | None = None
    # UI-only: True on the synthetic users-group row surfaced when an object
    # has no stored users-group grant (the implicit default). Distinguishes the
    # default from an explicit, materialized users-group grant.
    is_default: bool = False


def _as_bool(value: object) -> bool:
    """Coerce a backend cell (bool or ``"true"``/``"false"`` text) to bool."""
    return str(value).strip().lower() == "true"


class PermissionsService:
    """Manages ``dq_object_grants`` and resolves/enforces object privileges."""

    _ACTION_SET = "set"
    _ACTION_REMOVE = "remove"

    def __init__(self, sql: OltpExecutorProtocol, app_settings: AppSettingsService) -> None:
        self._sql = sql
        self._app_settings = app_settings
        self._table = sql.fqn("dq_object_grants")
        self._history_table = sql.fqn("dq_object_grants_history")
        self._members_table = sql.fqn("dq_data_product_members")

    @staticmethod
    def _validate_object_id(object_id: str) -> None:
        """Reject an ``object_id`` that isn't a well-formed app-minted id.

        Called at every SQL entry boundary of this service (``list_grants``,
        ``get_object_owner``, ``set_grant``, ``remove_grant`` — every other
        public method funnels through one of those before touching SQL).
        ``object_id`` reaches this service as a raw path parameter from any
        authenticated caller; see :func:`validate_object_id` for why this
        matters even though the deployed backend (Lakebase/Postgres) is not
        itself exploitable via this vector.
        """
        try:
            validate_object_id(object_id)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid object id.") from exc

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_grants(self, object_type: str, object_id: str) -> list[ObjectGrant]:
        """Return the direct grants stored on one object (no inheritance)."""
        self._validate_object_id(object_id)
        ot = escape_sql_string(object_type)
        oid = escape_sql_string(object_id)
        sql = (
            "SELECT object_type, object_id, principal_id, principal_type, principal_name, "
            f"privileges, inherit, grantor, {self._sql.ts_text('updated_at')} "
            f"FROM {self._table} "
            f"WHERE object_type = '{ot}' AND object_id = '{oid}' "
            "ORDER BY principal_name, principal_id"
        )
        return [self._row_to_grant(row) for row in self._sql.query(sql)]

    def list_effective_grants(self, object_type: str, object_id: str) -> list[ObjectGrant]:
        """Return direct grants plus inherited grants (flagged) for display.

        Inherited grants carry ``inherited_from_type``/``inherited_from_id``
        so the UI can render them distinctly (greyed + "Inherited from …"),
        mirroring how Unity Catalog surfaces inherited grants.

        The workspace users-group default is surfaced as a real row: when the
        object has no stored users-group grant, a synthetic ``is_default`` row
        (SELECT + APPLY) is prepended so the UI renders it like any grant. Once
        a users-group grant is materialized (narrowed/revoked), that stored row
        shows instead. The users-group grant is per-object and never inherited.

        The object creator is surfaced the same way: a synthetic ``is_default``
        row granting ``ALL_PRIVILEGES`` to the owner's email is prepended when
        the owner is known and has no explicit stored grant — display parity for
        the owner-equivalent privileges the creator already holds at enforcement
        time. If an explicit grant for the owner exists, that stored row is
        authoritative and the synthetic owner row is suppressed.
        """
        direct = self.list_grants(object_type, object_id)
        result = list(direct)
        direct_principals = {g.principal_id for g in direct}
        if USERS_GROUP_PRINCIPAL_ID not in direct_principals:
            result.insert(0, self._default_users_group_grant(object_type, object_id))
        # Synthetic owner/creator default — display parity with the users-group
        # default. The creator holds ALL PRIVILEGES at enforcement time (see
        # ``effective_privileges``), so surface it as an auto-generated,
        # non-materialized row. Suppress it when an explicit grant already
        # targets the owner (that stored row is authoritative — don't double-list).
        owner_email = self.get_object_owner(object_type, object_id)
        if owner_email and not self._owner_has_explicit_grant(owner_email, direct):
            result.insert(0, self._default_owner_grant(object_type, object_id, owner_email))
        for parent_type, parent_id in self._parent_refs(object_type, object_id):
            for g in self.list_grants(parent_type.value, parent_id):
                if not g.inherit:
                    continue
                # The users-group default is intrinsic to each object, not
                # inherited — a child shows its own default, never a parent's.
                if is_users_group(g.principal_id):
                    continue
                # A direct grant on the child object takes precedence over an
                # inherited one for the same principal (UC shows the closest).
                if g.principal_id in direct_principals:
                    continue
                result.append(
                    ObjectGrant(
                        object_type=object_type,
                        object_id=object_id,
                        principal_id=g.principal_id,
                        principal_type=g.principal_type,
                        principal_name=g.principal_name,
                        privileges=set(g.privileges),
                        inherit=g.inherit,
                        grantor=g.grantor,
                        updated_at=g.updated_at,
                        inherited_from_type=parent_type.value,
                        inherited_from_id=parent_id,
                    )
                )
        return result

    def _row_to_grant(self, row: list[str]) -> ObjectGrant:
        return ObjectGrant(
            object_type=row[0],
            object_id=row[1],
            principal_id=row[2],
            principal_type=row[3],
            principal_name=row[4] if row[4] else None,
            privileges=parse_privileges(row[5]),
            inherit=_as_bool(row[6]),
            grantor=row[7] if row[7] else None,
            updated_at=datetime.fromisoformat(row[8]) if row[8] else None,
        )

    @staticmethod
    def _default_users_group_grant(object_type: str, object_id: str) -> ObjectGrant:
        """Build the synthetic (implicit) users-group default grant for display."""
        return ObjectGrant(
            object_type=object_type,
            object_id=object_id,
            principal_id=USERS_GROUP_PRINCIPAL_ID,
            principal_type=PrincipalType.GROUP.value,
            principal_name=USERS_GROUP_PRINCIPAL_NAME,
            privileges=set(DEFAULT_USERS_GROUP_PRIVILEGES),
            inherit=False,
            grantor=None,
            is_default=True,
        )

    @staticmethod
    def _default_owner_grant(object_type: str, object_id: str, owner_email: str) -> ObjectGrant:
        """Build the synthetic (implicit) owner/creator full-privilege grant for display.

        Parallels :meth:`_default_users_group_grant`: the object creator is
        owner-equivalent and holds ``ALL_PRIVILEGES`` at enforcement time (see
        :meth:`effective_privileges`), which is otherwise invisible in the
        Permissions tab. Surface it as an auto-generated, non-materialized
        (``is_default``) row keyed on the owner's email. Display-only — never
        stored, never inherited; enforcement is unchanged.
        """
        return ObjectGrant(
            object_type=object_type,
            object_id=object_id,
            principal_id=owner_email,
            principal_type=PrincipalType.USER.value,
            principal_name=owner_email,
            privileges={Privilege.ALL_PRIVILEGES},
            inherit=False,
            grantor=None,
            is_default=True,
        )

    @staticmethod
    def _owner_has_explicit_grant(owner_email: str, direct: list[ObjectGrant]) -> bool:
        """True when a stored grant already targets the owner (by id or name).

        A stored grant may key the owner by SCIM id or carry the owner's email
        as its principal id/name; match either (case-insensitively) so the
        synthetic owner row is suppressed whenever an authoritative stored grant
        for the owner already exists.
        """
        target = owner_email.strip().lower()
        for grant in direct:
            if (grant.principal_id or "").strip().lower() == target:
                return True
            if (grant.principal_name or "").strip().lower() == target:
                return True
        return False

    def _parent_refs(self, object_type: str, object_id: str) -> list[tuple[ObjectType, str]]:
        """Resolve the parent objects a child inherits grants from.

        Only ``monitored_table`` has parents today: the data products it is a
        member of (``dq_data_product_members``). ``data_product`` is the top
        of the hierarchy and ``registry_rule`` is standalone — both return
        an empty list. Bounded to a single membership query (no N+1).
        """
        try:
            child = ObjectType(object_type)
        except ValueError:
            return []
        if child not in CHILD_TO_PARENT_TYPE:
            return []
        parent_type = CHILD_TO_PARENT_TYPE[child]
        oid = escape_sql_string(object_id)
        sql = f"SELECT product_id FROM {self._members_table} WHERE binding_id = '{oid}'"  # noqa: S608
        try:
            rows = self._sql.query(sql)
        except Exception:
            logger.warning("Failed to resolve permission parents for %s/%s", object_type, object_id, exc_info=True)
            return []
        return [(parent_type, row[0]) for row in rows if row[0]]

    # Maps a securable object type to the (table, id-column) that holds its
    # ``created_by`` owner. Keeps ownership resolution self-contained without
    # injecting the three entity services.
    _OWNER_SOURCE: dict[str, tuple[str, str]] = {
        ObjectType.REGISTRY_RULE.value: ("dq_rules", "rule_id"),
        ObjectType.MONITORED_TABLE.value: ("dq_monitored_tables", "binding_id"),
        ObjectType.DATA_PRODUCT.value: ("dq_data_products", "product_id"),
    }

    def get_object_owner(self, object_type: str, object_id: str) -> str | None:
        """Return the object's ``created_by`` (owner) email, or None if unknown."""
        self._validate_object_id(object_id)
        source = self._OWNER_SOURCE.get(object_type)
        if source is None:
            return None
        table, id_col = source
        fq = self._sql.fqn(table)
        sql = f"SELECT created_by FROM {fq} WHERE {id_col} = '{escape_sql_string(object_id)}'"  # noqa: S608
        try:
            rows = self._sql.query(sql)
        except Exception:
            logger.warning("Owner lookup failed for %s/%s", object_type, object_id, exc_info=True)
            return None
        if rows and rows[0] and rows[0][0]:
            return str(rows[0][0])
        return None

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def effective_privileges(
        self,
        object_type: str,
        object_id: str,
        principal_ids: set[str],
        *,
        owner_email: str | None = None,
        principal_email: str | None = None,
    ) -> set[Privilege]:
        """Resolve the privileges a caller effectively holds on an object.

        Combines: the workspace users-group default (implicit unless the object
        has an explicit users-group grant that narrows/revokes it), direct
        grants matching the caller's principal set (the users-group grant
        matches every caller), and inherited grants (``inherit=True``) on parent
        objects. The object creator (``owner_email``) is treated as holding all
        privileges.

        The users-group default is per-object: if the object has *no* stored
        users-group row it contributes :data:`DEFAULT_USERS_GROUP_PRIVILEGES`;
        if it *does* (even an empty/revoked one) that stored row wins and the
        default is not added. Inherited users-group rows are ignored (the
        default is intrinsic to each object, never inherited).

        Args:
            object_type: The securable object type value.
            object_id: The securable object id.
            principal_ids: The caller's principal ids (own SCIM id + group
                ids/names).
            owner_email: The object's ``created_by`` email, if known.
            principal_email: The caller's email, for the ownership check.

        Returns:
            The set of concrete privileges the caller effectively holds.
        """
        if owner_email and principal_email and owner_email.strip().lower() == principal_email.strip().lower():
            return expand_privileges({Privilege.ALL_PRIVILEGES})

        def _matches(grant: ObjectGrant) -> bool:
            # The users-group grant applies to everyone; other grants match the
            # caller's resolved principal set (own id + group ids/names).
            return is_users_group(grant.principal_id) or grant.principal_id in principal_ids

        priv: set[Privilege] = set()
        direct = self.list_grants(object_type, object_id)
        has_users_group_row = any(is_users_group(g.principal_id) for g in direct)
        for grant in direct:
            if _matches(grant):
                priv |= expand_privileges(grant.privileges)

        for parent_type, parent_id in self._parent_refs(object_type, object_id):
            for grant in self.list_grants(parent_type.value, parent_id):
                # Users-group grants are per-object, never inherited.
                if grant.inherit and not is_users_group(grant.principal_id) and _matches(grant):
                    priv |= expand_privileges(grant.privileges)

        # Implicit users-group default, unless the object overrides it.
        if not has_users_group_row:
            priv |= set(DEFAULT_USERS_GROUP_PRIVILEGES)

        return priv

    def has_privilege(
        self,
        object_type: str,
        object_id: str,
        privilege: Privilege,
        *,
        role: UserRole,
        principal_ids: set[str],
        owner_email: str | None = None,
        principal_email: str | None = None,
    ) -> bool:
        """Return True if the caller may exercise ``privilege`` on the object."""
        if role in _ROLE_BYPASS:
            return True
        eff = self.effective_privileges(
            object_type, object_id, principal_ids, owner_email=owner_email, principal_email=principal_email
        )
        return privilege in eff

    def require(
        self,
        object_type: str,
        object_id: str,
        privilege: Privilege,
        *,
        role: UserRole,
        principal_ids: set[str],
        owner_email: str | None = None,
        principal_email: str | None = None,
    ) -> None:
        """Raise ``403`` unless the caller may exercise ``privilege``.

        The detail is deliberately sanitized (privilege name only, no
        principal ids or grant internals).
        """
        if self.has_privilege(
            object_type,
            object_id,
            privilege,
            role=role,
            principal_ids=principal_ids,
            owner_email=owner_email,
            principal_email=principal_email,
        ):
            return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"You need the {privilege.value} privilege on this object.",
        )

    def require_object(
        self,
        object_type: str,
        object_id: str,
        privilege: Privilege,
        *,
        role: UserRole,
        principal_ids: set[str],
        principal_email: str | None = None,
    ) -> None:
        """Enforce ``privilege`` on an object, resolving its owner internally.

        Convenience wrapper over :meth:`require` for route handlers — one call
        per mutation, one owner-lookup query, honoring direct + inherited
        grants + baseline + role bypass. Raises ``403`` on denial.
        """
        self.require(
            object_type,
            object_id,
            privilege,
            role=role,
            principal_ids=principal_ids,
            owner_email=self.get_object_owner(object_type, object_id),
            principal_email=principal_email,
        )

    def can_manage_grants(
        self,
        object_type: str,
        object_id: str,
        *,
        role: UserRole,
        principal_ids: set[str],
        owner_email: str | None = None,
        principal_email: str | None = None,
    ) -> bool:
        """Return True if the caller may change grants on the object.

        Granting/revoking requires ownership or an admin/approver role — like
        UC, holding ALL PRIVILEGES on an object does NOT by itself let you
        re-grant it (MANAGE is separate).
        """
        if role in _ROLE_BYPASS:
            return True
        if owner_email and principal_email and owner_email.strip().lower() == principal_email.strip().lower():
            return True
        return False

    def can_edit_and_approve(
        self,
        object_type: str,
        object_id: str,
        *,
        role: UserRole,
        principal_ids: set[str],
        owner_email: str | None = None,
        principal_email: str | None = None,
    ) -> bool:
        """Auto-bypass predicate (issue #94): may the caller edit AND approve this object?

        Used by the ``auto_bypass`` approvals mode to decide whether a submit
        can auto-approve within the same call. True when the caller is an
        ``ADMIN``, or holds the ``approve_rules`` role permission *and* ``MODIFY``
        on the object.

        Note: the only roles carrying ``approve_rules`` (``ADMIN`` /
        ``RULE_APPROVER``) are exactly the roles that bypass object grants
        (:data:`_ROLE_BYPASS`), so an approver always satisfies the ``MODIFY``
        check — in the current model this reduces to "the caller may approve".
        Roles are the hard ceiling (object grants can never confer
        ``approve_rules``), so no grant can promote a lower role into
        auto-bypass. The ``MODIFY`` check is kept explicit so the predicate
        stays correct if approve ever becomes grantable per-object.
        """
        if role == UserRole.ADMIN:
            return True
        if "approve_rules" not in get_permissions_for_role(role):
            return False
        return self.has_privilege(
            object_type,
            object_id,
            Privilege.MODIFY,
            role=role,
            principal_ids=principal_ids,
            owner_email=owner_email,
            principal_email=principal_email,
        )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def set_grant(
        self,
        object_type: str,
        object_id: str,
        principal_id: str,
        *,
        principal_type: str,
        principal_name: str | None,
        privileges: set[Privilege],
        inherit: bool,
        grantor: str | None,
    ) -> ObjectGrant:
        """Create or replace the grant for one principal on one object.

        Replace semantics: the principal's full privilege set is overwritten
        (matches the UI's checkbox state).

        Empty privilege set: for a normal principal this is equivalent to
        :meth:`remove_grant` (the row is deleted). For the workspace users
        group it instead materializes an explicit empty-privilege row — the
        per-object "revoked" marker that suppresses the implicit default (so
        the object no longer falls back to SELECT + APPLY for everyone).
        """
        self._validate_object_id(object_id)
        self._reject_reserved_principal(principal_id)
        self._validate_enums(object_type, principal_type)
        norm = normalize_privileges(privileges)
        users_group = is_users_group(principal_id)
        if not norm and not users_group:
            self.remove_grant(object_type, object_id, principal_id, actor=grantor)
            return ObjectGrant(
                object_type=object_type,
                object_id=object_id,
                principal_id=principal_id,
                principal_type=principal_type,
                principal_name=principal_name,
                privileges=set(),
                inherit=inherit,
                grantor=grantor,
            )

        priv_str = serialize_privileges(norm)
        # Replace-in-place: delete any existing row for this principal, then
        # insert a fresh one. Portable across Delta/Postgres and keeps the
        # unique (object_type, object_id, principal_id) invariant.
        self._delete_row(object_type, object_id, principal_id)
        grant_id = uuid.uuid4().hex
        cols = "(grant_id, object_type, object_id, principal_id, principal_type, principal_name, " "privileges, inherit, grantor, created_at, updated_at)"
        vals = (
            f"('{escape_sql_string(grant_id)}', '{escape_sql_string(object_type)}', "
            f"'{escape_sql_string(object_id)}', '{escape_sql_string(principal_id)}', "
            f"'{escape_sql_string(principal_type)}', {self._opt(principal_name)}, "
            f"'{escape_sql_string(priv_str)}', {'TRUE' if inherit else 'FALSE'}, "
            f"{self._opt(grantor)}, now(), now())"
        )
        self._sql.execute(f"INSERT INTO {self._table} {cols} VALUES {vals}")  # noqa: S608
        self._record_history(object_type, object_id, principal_id, principal_name, priv_str, inherit, self._ACTION_SET, grantor)
        logger.info("Set object grant %s on %s/%s", priv_str, object_type, object_id)
        return ObjectGrant(
            object_type=object_type,
            object_id=object_id,
            principal_id=principal_id,
            principal_type=principal_type,
            principal_name=principal_name,
            privileges=norm,
            inherit=inherit,
            grantor=grantor,
        )

    def remove_grant(self, object_type: str, object_id: str, principal_id: str, *, actor: str | None = None) -> None:
        """Remove a principal's grant from an object (no-op if absent)."""
        self._validate_object_id(object_id)
        self._delete_row(object_type, object_id, principal_id)
        self._record_history(object_type, object_id, principal_id, None, None, None, self._ACTION_REMOVE, actor)
        logger.info("Removed object grant on %s/%s", object_type, object_id)

    def _delete_row(self, object_type: str, object_id: str, principal_id: str) -> None:
        sql = (
            f"DELETE FROM {self._table} WHERE object_type = '{escape_sql_string(object_type)}' "  # noqa: S608
            f"AND object_id = '{escape_sql_string(object_id)}' "
            f"AND principal_id = '{escape_sql_string(principal_id)}'"
        )
        self._sql.execute(sql)

    def _reject_reserved_principal(self, principal_id: str) -> None:
        """Reject the legacy all-principals sentinel from the write path.

        The users group is now a first-class principal
        (:data:`~backend.common.permissions.USERS_GROUP_PRINCIPAL_ID`); the old
        ``__all__`` sentinel must never be accepted as a raw principal id.
        """
        if is_reserved_principal_id(principal_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid principal. Grant the workspace users group instead.",
            )

    def _validate_enums(self, object_type: str, principal_type: str) -> None:
        try:
            ObjectType(object_type)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid object type.") from exc
        try:
            PrincipalType(principal_type)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid principal type.") from exc

    def _opt(self, value: str | None) -> str:
        return f"'{escape_sql_string(value)}'" if value else "NULL"

    def _record_history(
        self,
        object_type: str,
        object_id: str,
        principal_id: str,
        principal_name: str | None,
        privileges: str | None,
        inherit: bool | None,
        action: str,
        actor: str | None,
    ) -> None:
        """Append an audit row (best-effort; failures never roll back the grant)."""
        try:
            inherit_sql = "NULL" if inherit is None else ("TRUE" if inherit else "FALSE")
            sql = (
                f"INSERT INTO {self._history_table} "  # noqa: S608
                "(object_type, object_id, principal_id, principal_name, privileges, inherit, action, changed_by, changed_at) "
                f"VALUES ('{escape_sql_string(object_type)}', '{escape_sql_string(object_id)}', "
                f"'{escape_sql_string(principal_id)}', {self._opt(principal_name)}, "
                f"{self._opt(privileges)}, {inherit_sql}, '{escape_sql_string(action)}', "
                f"{self._opt(actor)}, now())"
            )
            self._sql.execute(sql)
        except Exception:
            logger.warning("Failed to record object-grant history for %s/%s (non-fatal)", object_type, object_id, exc_info=True)

    # ------------------------------------------------------------------
    # Admin setting — default inheritance for new grants
    # ------------------------------------------------------------------

    def get_default_inherit(self) -> bool:
        """Return the admin default for the per-grant inheritance toggle."""
        return self._app_settings.get_permissions_default_inherit()

    def set_default_inherit(self, enabled: bool, *, user_email: str | None = None) -> bool:
        """Persist the admin default for the per-grant inheritance toggle."""
        return self._app_settings.save_permissions_default_inherit(enabled, user_email=user_email)
