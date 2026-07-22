"""UC-style object-permission primitives — privileges, securable object types,
and the app-level privilege model layered on top of the coarse role RBAC.

Design (P22-D item 10). This mirrors Unity Catalog's object-permissions model
at the application level:

* **Securable objects** form a hierarchy: ``data_product`` (table space) ->
  ``monitored_table`` -> that table's applied-rule scope. ``registry_rule``
  (the reusable template) is a standalone securable, outside the
  space/table hierarchy.
* **Privileges**: ``SELECT`` (view), ``MODIFY`` (change the object's own
  config — rule logic / table config / space config, and delete),
  ``APPLY`` (attach children — apply a rule to a table; add a table to a
  space), ``EXECUTE`` (run profiling/validation on a table or collection),
  and ``ALL_PRIVILEGES`` (the UC-style superset that expands to the
  concrete set at check-time; the stored form stays ``ALL PRIVILEGES``, not
  its components).
* **Grants** target workspace principals (users/groups, by SCIM id). The
  workspace **users group** (:data:`USERS_GROUP_PRINCIPAL_ID`) is a
  first-class group principal that stands in for "everyone", the way
  ``account users`` appears in Unity Catalog grants.
* **Layering with role RBAC**: roles stay the coarse gate (``require_role``
  still guards every route). Object grants *refine within* what a role
  allows — a ``RULE_AUTHOR`` needs ``MODIFY`` (direct, inherited, or via
  ownership) on rule X to edit X. ``ADMIN`` and ``RULE_APPROVER`` bypass
  object grants entirely, mirroring UC's owner/admin conventions.
* **Stored-row defaults**: the workspace users-group grant
  (:data:`DEFAULT_USERS_GROUP_PRIVILEGES` — ``SELECT`` + ``APPLY`` +
  ``EXECUTE``) and the object owner's ``ALL_PRIVILEGES`` grant are
  materialised as REAL rows in ``dq_object_grants`` by
  :meth:`~backend.services.permissions_service.PermissionsService.seed_default_grants`
  at object-creation time. There is no read-time synthesis of implicit
  defaults and no implicit owner grant — if no row exists for a principal,
  that principal has no access. A separate backfill migration seeds grants
  for pre-existing objects. ``ADMIN``/``RULE_APPROVER`` role bypass is the
  only non-stored access path. Revoking a grant permanently removes the row.
"""

from __future__ import annotations

from enum import Enum


class Privilege(str, Enum):
    """An app-level privilege on a securable object.

    ``ALL_PRIVILEGES`` is a stored superset token, not a concrete grant —
    :func:`expand_privileges` turns it into the concrete set at check-time
    (UC semantics).
    """

    SELECT = "SELECT"
    MODIFY = "MODIFY"
    APPLY = "APPLY"
    EXECUTE = "EXECUTE"
    ALL_PRIVILEGES = "ALL_PRIVILEGES"


class ObjectType(str, Enum):
    """A securable object type in the Rules Registry."""

    REGISTRY_RULE = "registry_rule"
    MONITORED_TABLE = "monitored_table"
    DATA_PRODUCT = "data_product"


class PrincipalType(str, Enum):
    """The kind of principal a grant targets."""

    USER = "user"
    GROUP = "group"


# First-class group principal representing the workspace "users" group — the
# group every workspace user belongs to. It stands in for "everyone" (the way
# ``account users`` appears in Unity Catalog grants) and is stored like any
# other group grant: ``principal_type='group'``, this id/name. A grant against
# this principal matches every caller regardless of the caller's resolved group
# set (see :meth:`PermissionsService.effective_privileges`).
USERS_GROUP_PRINCIPAL_ID = "users"
USERS_GROUP_PRINCIPAL_NAME = "users"

# The legacy internal sentinel the users-group principal replaces. Kept only so
# the public API surface can explicitly reject it (never accept it as a raw
# principal id) — it is no longer written or matched anywhere.
LEGACY_ALL_SENTINEL = "__all__"


def is_users_group(principal_id: str) -> bool:
    """Return True if ``principal_id`` is the workspace users-group principal."""
    return principal_id == USERS_GROUP_PRINCIPAL_ID


def is_reserved_principal_id(principal_id: str) -> bool:
    """Return True if ``principal_id`` is a reserved/rejected id (the legacy sentinel)."""
    return principal_id == LEGACY_ALL_SENTINEL


# The concrete privileges ``ALL_PRIVILEGES`` expands to. Deliberately excludes
# any "manage grants" capability — like UC's ALL PRIVILEGES excluding MANAGE —
# so holding ALL PRIVILEGES on an object does not by itself let you re-grant it
# to others (that requires ownership or an admin/approver role).
_CONCRETE_PRIVILEGES: frozenset[Privilege] = frozenset(
    {Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY, Privilege.EXECUTE}
)

# The default privilege set the workspace users-group holds on objects that
# support EXECUTE (monitored_table, data_product). Confers view + apply + execute
# so existing flows keep working day one; MODIFY is intentionally absent — it is
# the privilege the feature gates. Surfaced in the UI as a real (removable) grant
# row on the users group, not an invisible constant.
DEFAULT_USERS_GROUP_PRIVILEGES: frozenset[Privilege] = frozenset(
    {Privilege.SELECT, Privilege.APPLY, Privilege.EXECUTE}
)


def default_users_group_privileges_for(object_type: str) -> frozenset[Privilege]:
    """Return the default users-group privilege set for *object_type*.

    ``registry_rule`` objects do not support EXECUTE (the privilege means
    "run profiling/validation on a table or collection"; rules are not run
    directly). All other object types (``monitored_table``, ``data_product``)
    include EXECUTE in the default set.

    Args:
        object_type: The securable object type value (e.g.
            ``ObjectType.REGISTRY_RULE.value``).

    Returns:
        The default :data:`Privilege` frozenset for the users-group grant
        seeded at object-creation time.
    """
    if object_type == ObjectType.REGISTRY_RULE.value:
        return frozenset({Privilege.SELECT, Privilege.APPLY})
    return DEFAULT_USERS_GROUP_PRIVILEGES

# Parent object types for inheritance resolution: a grant with ``inherit=True``
# on a parent flows to children of these child types. The parent ids
# themselves are resolved at runtime by the service (membership lookups).
#   data_product --(members)--> monitored_table
# ``registry_rule`` has no parent; ``data_product`` is the top of the tree.
CHILD_TO_PARENT_TYPE: dict[ObjectType, ObjectType] = {
    ObjectType.MONITORED_TABLE: ObjectType.DATA_PRODUCT,
}


def expand_privileges(privileges: set[Privilege]) -> set[Privilege]:
    """Expand ``ALL_PRIVILEGES`` into its concrete component set.

    Args:
        privileges: The raw stored privilege set for a grant.

    Returns:
        A set containing the concrete privileges the grant confers. An
        ``ALL_PRIVILEGES`` token expands to :data:`_CONCRETE_PRIVILEGES`;
        concrete privileges pass through unchanged.
    """
    if Privilege.ALL_PRIVILEGES in privileges:
        return set(_CONCRETE_PRIVILEGES)
    return {p for p in privileges if p in _CONCRETE_PRIVILEGES}


def parse_privileges(raw: str | None) -> set[Privilege]:
    """Parse a comma-joined stored privilege string into a set.

    Unknown tokens are ignored (forward-compatibility with future
    privileges written by a newer deploy).

    Args:
        raw: The comma-joined ``privileges`` column value, or ``None``.

    Returns:
        The parsed set of :class:`Privilege` members.
    """
    if not raw:
        return set()
    out: set[Privilege] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.add(Privilege(token))
        except ValueError:
            continue
    return out


def serialize_privileges(privileges: set[Privilege]) -> str:
    """Serialize a privilege set to the canonical stored string.

    ``ALL_PRIVILEGES`` is stored on its own (UC semantics: the stored form
    is the superset token, not its components). Otherwise the concrete
    privileges are emitted in a stable order.

    Args:
        privileges: The privilege set to serialize.

    Returns:
        A comma-joined, canonically-ordered privilege string.
    """
    if Privilege.ALL_PRIVILEGES in privileges:
        return Privilege.ALL_PRIVILEGES.value
    order = [Privilege.SELECT, Privilege.MODIFY, Privilege.APPLY, Privilege.EXECUTE]
    return ",".join(p.value for p in order if p in privileges)


def normalize_privileges(privileges: set[Privilege]) -> set[Privilege]:
    """Collapse a privilege set to its canonical stored form.

    If the set already covers every concrete privilege it is collapsed to
    ``{ALL_PRIVILEGES}`` so the stored form matches how UC reports a
    full grant.

    Args:
        privileges: The privilege set to normalize.

    Returns:
        The canonical set: either ``{ALL_PRIVILEGES}`` or the concrete subset.
    """
    if Privilege.ALL_PRIVILEGES in privileges or _CONCRETE_PRIVILEGES.issubset(privileges):
        return {Privilege.ALL_PRIVILEGES}
    return {p for p in privileges if p in _CONCRETE_PRIVILEGES}
