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


class ScheduleGrantService:
    """Check grantability and grant SELECT to the scheduler SPs, all via OBO."""

    def __init__(self, obo_ws: WorkspaceClient, sp_ws: WorkspaceClient, job_id: str) -> None:
        self._obo = obo_ws
        self._sp_ws = sp_ws
        self._job_id = (job_id or "").strip()

    # ------------------------------------------------------------------
    # SP identity resolution
    # ------------------------------------------------------------------

    def app_sp_id(self) -> str:
        """Return the app service principal's application (client) id.

        Prefers ``DATABRICKS_CLIENT_ID`` (injected into a deployed Databricks
        App) and falls back to the SP's own SCIM ``me()`` for local dev.
        """
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
        ``run_as`` cannot be read. Callers must still grant the app SP.
        """
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

    # ------------------------------------------------------------------
    # Caller identity + ownership
    # ------------------------------------------------------------------

    def _caller_principals(self) -> set[str]:
        """Return the OBO caller's identity set (lowercased).

        Includes the user name, any registered emails, and every group the user
        belongs to — by both display name and SCIM id — so an owner or grant
        that names any of those identities matches. Group membership is what
        lets group-inherited ``MANAGE`` / ownership count.
        """
        principals: set[str] = set()
        try:
            me = self._obo.current_user.me()
        except Exception:
            logger.warning("Could not resolve OBO caller identity for manage check", exc_info=True)
            return principals
        if me.user_name:
            principals.add(me.user_name.strip().lower())
        for email in me.emails or []:
            if email.value:
                principals.add(email.value.strip().lower())
        for group in me.groups or []:
            if group.display:
                principals.add(group.display.strip().lower())
            if group.value:
                principals.add(group.value.strip().lower())
        return principals

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
        """
        try:
            resp = self._obo.grants.get_effective(_TABLE_SECURABLE, fqn, principal=principal)
        except Exception:
            logger.debug("Could not read effective grants for manage check", exc_info=True)
            return []
        return list(resp.privilege_assignments or [])

    @staticmethod
    def _assignment_has_manage(assignment: object) -> bool:
        for priv in getattr(assignment, "privileges", None) or []:
            if getattr(priv, "privilege", None) == Privilege.MANAGE:
                return True
        return False

    @staticmethod
    def _classify_principal(principal: str) -> str:
        """Best-effort user/group classification for display in the warning card."""
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

        principals = self._caller_principals()

        # Ownership (table, then parent schema/catalog).
        if principals and any(owner in principals for owner in self._owners(fqn)):
            return True

        # MANAGE effective for the caller specifically (folds in inheritance).
        for assignment in self._manage_assignments(fqn, principal=next(iter(principals), None) or None):
            if self._assignment_has_manage(assignment):
                return True

        # MANAGE held by any of the caller's groups (enumerate all principals).
        if principals:
            for assignment in self._manage_assignments(fqn, principal=None):
                who = (getattr(assignment, "principal", None) or "").strip().lower()
                if who in principals and self._assignment_has_manage(assignment):
                    return True

        return False

    def manage_holders(self, fqn: str) -> list[dict[str, str]]:
        """Return the users/groups that can grant on *fqn*: MANAGE holders + owners.

        Best-effort — reading the full grant list requires elevated privileges,
        so a caller who lacks them may only see the owners we can read. Each
        entry is ``{"principal": <name>, "type": "user"|"group"}`` (type is a
        display heuristic).
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

        Raises :class:`CannotManageError` when the caller cannot grant (the
        route maps it to a hard block). The app-SP grant is essential and any
        failure propagates; the task-runner grant is best-effort. Idempotent —
        re-granting an existing privilege is a no-op. Returns the list of
        principals granted.
        """
        validate_fqn(fqn)
        if not _is_real_three_part_fqn(fqn):
            # Nothing to grant on a synthetic cross-table key.
            return []

        if not self.user_can_manage(fqn):
            raise CannotManageError(fqn, self.manage_holders(fqn))

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

    async def grant_select_to_schedulers_async(self, fqn: str) -> list[str]:
        return await asyncio.to_thread(self.grant_select_to_schedulers, fqn)
