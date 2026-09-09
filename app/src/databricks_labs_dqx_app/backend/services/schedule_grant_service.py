"""ScheduleGrantService — grant the schedulers SELECT on scheduled tables (Task 12).

Interactive "Run now" reads the user's table through an OBO-created view, so it
works. A **scheduled** run has no OBO token and reads the source table as a
service principal, so without an explicit grant the scheduled run fails with a
permission error. When a user sets up a schedule on a table (table page) or a
collection (all its tables) we therefore GRANT ``SELECT`` on each source table
— using the caller's OBO token — to the SP identities that scheduled runs read
as:

* the **app SP** — *essential*. For the table-page and collection-page
  schedules the scheduler creates the temp view as the app SP (see
  ``backend/app.py`` — the scheduler's ``ViewService`` is built with SP
  credentials on both legs), so the app SP needs ``SELECT`` on the *source*
  table. Its application id comes from ``DATABRICKS_CLIENT_ID`` (injected into a
  deployed App) and falls back to the SP's own ``current_user.me()``.
* the **task-runner SP** — *best-effort*. Covers the scope-config ``/schedules``
  path where the run reads the raw table as the job's ``run_as`` identity. Its
  application id is not in the app environment; it is derived at runtime from
  ``jobs.get(job_id).settings.run_as.service_principal_name``. If that
  derivation fails or is absent we still grant the app SP and log a warning —
  never fail the whole operation.

All reads and the grant run under the caller's OBO client, so Unity Catalog
enforces exactly what the user is allowed to do. A user who cannot grant (no
``MANAGE`` privilege and not an owner — directly or via a group they belong to)
is hard-blocked, and we surface the users/groups that *do* hold ``MANAGE`` so
the UI can ask one of them to set the schedule up instead.
"""

import asyncio
import logging
import os
import re
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PermissionsChange, Privilege, SecurableType

from databricks_labs_dqx_app.backend.sql_utils import validate_fqn

logger = logging.getLogger(__name__)

# UC securable type is passed to the grants API as a string.
_TABLE_SECURABLE = SecurableType.TABLE.value


class CannotManageError(Exception):
    """Raised when the OBO caller cannot grant on a table (no MANAGE / ownership).

    Carries the ``manage_holders`` list so the route can return it to the UI —
    the users/groups the caller should ask to set the schedule up instead.
    """

    def __init__(self, fqn: str, manage_holders: list[dict[str, str]]) -> None:
        self.fqn = fqn
        self.manage_holders = manage_holders
        super().__init__(f"You do not have permission to grant SELECT on '{fqn}'.")


def manage_block_detail(blocked: list[tuple[str, list[dict[str, str]]]]) -> dict[str, object]:
    """Build the structured 403 detail for a schedule save blocked on MANAGE.

    Kept here (framework-free) so all three schedule-save routes surface an
    identical shape: a machine-readable ``code`` plus the blocked tables and,
    for each, the users/groups that hold MANAGE (whom to ask instead).
    """
    return {
        "code": "cannot_manage_schedule_tables",
        "message": (
            "You do not have permission to grant the scheduler read access to "
            "one or more tables. Ask a user or group with MANAGE to set up the schedule."
        ),
        "tables": [{"fqn": fqn, "manage_holders": holders} for fqn, holders in blocked],
    }


@dataclass
class TablePreflight:
    """Per-table result of the schedule preflight."""

    fqn: str
    can_manage: bool
    manage_holders: list[dict[str, str]]


def _is_real_three_part_fqn(fqn: str) -> bool:
    """True iff *fqn* is a real three-part UC name (not a synthetic sql-check key).

    Synthetic cross-table SQL checks use the ``__sql_check__/<name>`` prefix and
    have no physical home table, so there is nothing to grant on — those are
    skipped by the gate/grant entirely.
    """
    if not fqn or fqn.startswith("__sql_check__/"):
        return False
    return len(fqn.split(".")) == 3


# A service principal appears in UC grants as its application id: a bare UUID.
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)


class ScheduleGrantService:
    """Check grantability and grant SELECT to the scheduler SPs, all via OBO."""

    def __init__(self, obo_ws: WorkspaceClient, sp_ws: WorkspaceClient, job_id: str) -> None:
        self._obo = obo_ws
        self._sp_ws = sp_ws
        self._job_id = (job_id or "").strip()
        # Per-request memoization of the (otherwise per-table) identity lookups.
        # The service is constructed per request (see ``get_schedule_grant_service``),
        # so caching here scopes each identity round-trip to a single request —
        # ``current_user.me()`` / ``jobs.get`` fire once, not once per table.
        self._caller_identity_cache: tuple[str, set[str]] | None = None
        self._app_sp_id_cache: str | None = None
        self._app_sp_id_cached: bool = False
        self._task_runner_sp_id_cache: str | None = None
        self._task_runner_sp_id_cached: bool = False

    # ------------------------------------------------------------------
    # SP identity resolution
    # ------------------------------------------------------------------

    def app_sp_id(self) -> str:
        """Return the app service principal's application (client) id.

        Prefers ``DATABRICKS_CLIENT_ID`` (injected into a deployed Databricks
        App) and falls back to the SP's own SCIM ``me()`` for local dev.
        Memoized per request so the ``me()`` fallback is not re-issued per table.
        """
        if not self._app_sp_id_cached:
            self._app_sp_id_cache = self._resolve_app_sp_id()
            self._app_sp_id_cached = True
        return self._app_sp_id_cache or ""

    def _resolve_app_sp_id(self) -> str:
        env_id = (os.environ.get("DATABRICKS_CLIENT_ID") or "").strip()
        if env_id:
            return env_id
        try:
            me = self._sp_ws.current_user.me()
            return (me.user_name or me.id or "").strip()
        except Exception:  # pragma: no cover - defensive
            logger.warning("Could not resolve app SP identity via current_user.me()", exc_info=True)
            return ""

    def task_runner_sp_id(self) -> str | None:
        """Derive the task-runner SP's application id from the job's ``run_as``.

        Best-effort: returns ``None`` when no job id is configured or the job's
        ``run_as`` cannot be read. Callers must still grant the app SP. Memoized
        per request so ``jobs.get`` fires once, not once per table.
        """
        if not self._task_runner_sp_id_cached:
            self._task_runner_sp_id_cache = self._resolve_task_runner_sp_id()
            self._task_runner_sp_id_cached = True
        return self._task_runner_sp_id_cache

    def _resolve_task_runner_sp_id(self) -> str | None:
        if not self._job_id:
            return None
        try:
            job = self._sp_ws.jobs.get(int(self._job_id))
        except Exception:
            logger.warning("Could not derive task-runner SP id from job id", exc_info=True)
            return None
        run_as = getattr(getattr(job, "settings", None), "run_as", None)
        spn = getattr(run_as, "service_principal_name", None)
        return spn.strip() if spn else None

    def prime_caller_identity(self) -> None:
        """Resolve and cache the OBO caller identity once (before concurrent gates).

        Fanning the per-table ``user_can_manage`` checks out across threads would
        otherwise let several of them race into :meth:`_caller_identity` before
        the cache is populated, re-issuing ``current_user.me()``. Priming it once,
        serially, guarantees exactly one identity round-trip per request.
        """
        self._caller_identity()

    def prime_scheduler_sp_identities(self) -> None:
        """Resolve and cache both scheduler SP identities once (before concurrent grants)."""
        self.app_sp_id()
        self.task_runner_sp_id()

    # ------------------------------------------------------------------
    # Caller identity + ownership
    # ------------------------------------------------------------------

    def _caller_identity(self) -> tuple[str, set[str]]:
        """Return the OBO caller's ``(user_name, principal_set)`` (lowercased).

        Memoized per request — see :meth:`prime_caller_identity`.
        """
        if self._caller_identity_cache is None:
            self._caller_identity_cache = self._resolve_caller_identity()
        return self._caller_identity_cache

    def _resolve_caller_identity(self) -> tuple[str, set[str]]:
        """Compute the OBO caller's ``(user_name, principal_set)`` (lowercased).

        The principal set includes the user name, any registered emails, and
        every group the user belongs to — by both display name and SCIM id — so
        an owner or grant that names any of those identities matches. Group
        membership is what lets group-inherited ``MANAGE`` / ownership count.
        ``user_name`` is returned separately so the effective-privileges lookup
        can filter to the caller specifically. Both are empty on failure.
        """
        principals: set[str] = set()
        try:
            me = self._obo.current_user.me()
        except Exception:
            logger.warning("Could not resolve OBO caller identity for manage check", exc_info=True)
            return "", principals
        user_name = (me.user_name or "").strip().lower()
        if user_name:
            principals.add(user_name)
        for email in me.emails or []:
            if email.value:
                principals.add(email.value.strip().lower())
        for group in me.groups or []:
            if group.display:
                principals.add(group.display.strip().lower())
            if group.value:
                principals.add(group.value.strip().lower())
        return user_name, principals

    def _owners(self, fqn: str) -> list[str]:
        """Return the owners of the table and its parent schema/catalog (lowercased).

        An owner of any containing securable can grant on the table, so we walk
        up the hierarchy. Each read is best-effort — a missing read simply
        contributes no owner rather than failing the whole check.
        """
        catalog, schema, _table = fqn.split(".", 2)
        owners: list[str] = []

        def _add(owner: str | None) -> None:
            if owner:
                owners.append(owner.strip().lower())

        try:
            _add(self._obo.tables.get(fqn).owner)
        except Exception:
            logger.debug("Could not read table owner for manage check", exc_info=True)
        try:
            _add(self._obo.schemas.get(f"{catalog}.{schema}").owner)
        except Exception:
            logger.debug("Could not read schema owner for manage check", exc_info=True)
        try:
            _add(self._obo.catalogs.get(catalog).owner)
        except Exception:
            logger.debug("Could not read catalog owner for manage check", exc_info=True)
        return owners

    # ------------------------------------------------------------------
    # Effective privileges
    # ------------------------------------------------------------------

    def _manage_assignments(self, fqn: str, principal: str | None) -> list:
        """Return effective privilege assignments on *fqn* (best-effort, [] on error).

        With *principal* set the API returns the effective privileges for that
        single principal, inclusive of parent-securable and group inheritance.
        With *principal* ``None`` it returns every principal's effective
        privileges — used to enumerate ``MANAGE`` holders for the warning card.

        Reads **all** pages: on a heavily-granted table a group's ``MANAGE`` (or a
        holder) can land on a later page, and missing it would falsely hard-block
        a legitimate MANAGE-via-group user. Whatever was collected before a
        mid-pagination failure is still returned.
        """
        assignments: list = []
        page_token: str | None = None
        try:
            while True:
                resp = self._obo.grants.get_effective(_TABLE_SECURABLE, fqn, principal=principal, page_token=page_token)
                assignments.extend(resp.privilege_assignments or [])
                page_token = getattr(resp, "next_page_token", None)
                if not page_token:
                    break
        except Exception:
            logger.debug("Could not read effective grants for manage check", exc_info=True)
        return assignments

    @staticmethod
    def _assignment_has_manage(assignment: object) -> bool:
        for priv in getattr(assignment, "privileges", None) or []:
            if getattr(priv, "privilege", None) == Privilege.MANAGE:
                return True
        return False

    @staticmethod
    def _classify_principal(principal: str) -> str:
        """Best-effort principal classification for display in the warning card.

        A service principal holds UC grants under its **application id** — a
        bare UUID with no ``@`` — so an "@"/else split mislabels it as a group
        and the warning card then tells the blocked user to go ask a "group"
        that cannot act on the request. UUID-shaped principals are therefore
        reported as service principals.
        """
        if _UUID_RE.fullmatch(principal.strip()):
            return "service_principal"
        return "user" if "@" in principal else "group"

    # ------------------------------------------------------------------
    # Public checks
    # ------------------------------------------------------------------

    def user_can_manage(self, fqn: str) -> bool:
        """Return whether the OBO caller can GRANT on *fqn*.

        A caller can grant if they own the table (or a containing schema/catalog)
        or hold ``MANAGE`` — directly, inherited from a parent securable, or via
        a group they belong to. ``ALL PRIVILEGES`` alone does *not* confer
        re-grant in Unity Catalog, so only ``MANAGE`` (and ownership) count.
        Synthetic (non-real) FQNs return ``False``.
        """
        validate_fqn(fqn)
        if not _is_real_three_part_fqn(fqn):
            return False

        user_name, principals = self._caller_identity()
        if not principals:
            # Identity unresolved — cannot verify grantability; block (safe default).
            return False

        # Ownership (table, then parent schema/catalog).
        if any(owner in principals for owner in self._owners(fqn)):
            return True

        # MANAGE effective for the caller specifically (folds in parent + group
        # inheritance). The API returns only *user_name*'s privileges, but we
        # assert that explicitly per assignment — defense-in-depth so another
        # principal's MANAGE can never be mis-attributed to the caller.
        if user_name:
            for assignment in self._manage_assignments(fqn, principal=user_name):
                who = (getattr(assignment, "principal", None) or "").strip().lower()
                if who == user_name and self._assignment_has_manage(assignment):
                    return True

        # MANAGE held by any of the caller's groups (enumerate all principals).
        for assignment in self._manage_assignments(fqn, principal=None):
            who = (getattr(assignment, "principal", None) or "").strip().lower()
            if who in principals and self._assignment_has_manage(assignment):
                return True

        return False

    def manage_holders(self, fqn: str) -> list[dict[str, str]]:
        """Return the users/groups that can grant on *fqn*: MANAGE holders + owners.

        Best-effort — reading the full grant list requires elevated privileges,
        so a caller who lacks them may only see the owners we can read. Each
        entry is ``{"principal": <name>, "type": "user"|"group"|"service_principal"}``
        (type is a display heuristic).
        """
        validate_fqn(fqn)
        holders: dict[str, str] = {}
        if not _is_real_three_part_fqn(fqn):
            return []

        for owner in self._owners(fqn):
            holders.setdefault(owner, self._classify_principal(owner))

        for assignment in self._manage_assignments(fqn, principal=None):
            who = getattr(assignment, "principal", None)
            if who and self._assignment_has_manage(assignment):
                holders.setdefault(who.strip(), self._classify_principal(who.strip()))

        return [{"principal": p, "type": t} for p, t in holders.items()]

    def preflight(self, fqns: list[str]) -> list[TablePreflight]:
        """Per-table grantability + manage-holder enumeration for the UI editor."""
        out: list[TablePreflight] = []
        seen: set[str] = set()
        for fqn in fqns:
            if not fqn or fqn in seen:
                continue
            seen.add(fqn)
            try:
                validate_fqn(fqn)
            except ValueError:
                out.append(TablePreflight(fqn=fqn, can_manage=False, manage_holders=[]))
                continue
            if not _is_real_three_part_fqn(fqn):
                # Synthetic cross-table checks need no source-table grant — never block.
                out.append(TablePreflight(fqn=fqn, can_manage=True, manage_holders=[]))
                continue
            can_manage = self.user_can_manage(fqn)
            holders = [] if can_manage else self.manage_holders(fqn)
            out.append(TablePreflight(fqn=fqn, can_manage=can_manage, manage_holders=holders))
        return out

    # ------------------------------------------------------------------
    # Grant
    # ------------------------------------------------------------------

    def _grant_select(self, fqn: str, principal: str) -> None:
        self._obo.grants.update(
            _TABLE_SECURABLE,
            fqn,
            changes=[PermissionsChange(principal=principal, add=[Privilege.SELECT])],
        )

    def grant_select_to_schedulers(self, fqn: str) -> list[str]:
        """Grant ``SELECT`` on *fqn* to the app SP (essential) + task-runner SP.

        Checks grantability first and raises :class:`CannotManageError` when the
        caller cannot grant (the route maps it to a hard block). The app-SP grant
        is essential and any failure propagates; the task-runner grant is
        best-effort. Idempotent — re-granting an existing privilege is a no-op.
        Returns the list of principals granted.
        """
        validate_fqn(fqn)
        if not _is_real_three_part_fqn(fqn):
            # Nothing to grant on a synthetic cross-table key.
            return []

        if not self.user_can_manage(fqn):
            raise CannotManageError(fqn, self.manage_holders(fqn))

        return self._grant_to_schedulers_unchecked(fqn)

    def grant_select_precleared(self, fqn: str) -> list[str]:
        """Grant ``SELECT`` to the scheduler SPs for an already MANAGE-gated *fqn*.

        Public counterpart to :meth:`grant_select_to_schedulers` for callers that
        have *already* established the caller can grant on *fqn* (via
        :meth:`user_can_manage` or :meth:`preflight`). Skipping the redundant
        re-check halves the Unity Catalog round-trips on a multi-table save.

        Args:
            fqn: Fully qualified table name, already MANAGE-gated.

        Returns:
            The principals granted (empty for a synthetic cross-table key).
        """
        return self._grant_to_schedulers_unchecked(fqn)

    def _grant_to_schedulers_unchecked(self, fqn: str) -> list[str]:
        """Grant SELECT to the scheduler SPs **without** re-checking MANAGE.

        The caller must have already gated on :meth:`user_can_manage` for *fqn*
        (the enforce path does this once per table). Splitting the grant from the
        gate is a performance fix — it avoids re-issuing the ownership +
        effective-privilege round-trips that :meth:`user_can_manage` performs. The
        security guarantee is unchanged: every table is still MANAGE-gated before
        any grant is attempted.
        """
        validate_fqn(fqn)
        if not _is_real_three_part_fqn(fqn):
            return []

        app_id = self.app_sp_id()
        if not app_id:
            raise RuntimeError("Could not resolve the app service principal identity to grant SELECT.")

        self._grant_select(fqn, app_id)  # essential — propagate on failure
        granted = [app_id]

        task_id = self.task_runner_sp_id()
        if task_id and task_id != app_id:
            try:
                self._grant_select(fqn, task_id)
                granted.append(task_id)
            except Exception:
                logger.warning("Best-effort SELECT grant to task-runner SP failed", exc_info=True)
        return granted

    # ------------------------------------------------------------------
    # Async wrappers (SDK calls are blocking)
    # ------------------------------------------------------------------

    async def preflight_async(self, fqns: list[str]) -> list[TablePreflight]:
        return await asyncio.to_thread(self.preflight, fqns)

    async def user_can_manage_async(self, fqn: str) -> bool:
        return await asyncio.to_thread(self.user_can_manage, fqn)

    async def manage_holders_async(self, fqn: str) -> list[dict[str, str]]:
        return await asyncio.to_thread(self.manage_holders, fqn)

    async def grant_select_precleared_async(self, fqn: str) -> list[str]:
        """Async wrapper for :meth:`grant_select_precleared` (MANAGE pre-gated)."""
        return await asyncio.to_thread(self.grant_select_precleared, fqn)
