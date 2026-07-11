"""Self-verified user/table entitlement cache + permission-gated failing-rows view.

Phase 4 gives Genie row-level access without granting any user SELECT on
``dq_quarantine_records`` and without mirroring UC ACLs:

- *dq_user_table_entitlements* — a UC Delta table (it MUST be a UC object:
  the dynamic view references it with definer's rights, so Lakebase is not
  an option) recording that a user recently proved they can SELECT a source
  table. SP-written only; NO user grants — users never read it directly.
- *v_dq_failing_rows* — a dynamic view over ``dq_quarantine_records`` whose
  ``WHERE EXISTS`` gate compares ``current_user()`` (the QUERYING user —
  when Genie runs a query OBO, that is the person asking, which is the
  whole point of the gate) against fresh entitlement rows. No fresh row →
  fail-safe empty. Rows from DRAFT runs are visible through the view; that
  is not a leak — every row belongs to a table the caller has verified
  SELECT on, and run-mode filtering is a presentation concern handled in
  Genie's curated SQL (P4.2), not a permission boundary.

Verification is a self-check running BOTH Task 7 gates, in the same order
as the in-app failed-rows endpoint: :meth:`QuarantineSampleService.user_can_select`
(a zero-row probe through the CALLER's OBO SQL executor) first, then
:meth:`QuarantineSampleService.has_fine_grained_access_control` (a metadata
read via the caller's OBO client) — neither needs elevated privilege. An
entitlement is recorded only when SELECT passes AND no fine-grained
controls (row filter / column mask) exist: copied quarantine rows cannot
replicate those policies, the in-app failed-rows path suppresses such
tables, and the Genie view must never serve rows the app itself refuses to
show. Passing verifications are upserted SP-side with a ``verified_at``
timestamp; the view's 24-hour TTL bounds revocation drift — and equally
bounds FGAC drift: a row filter or column mask ADDED to a table after an
entitlement was granted stays exposed for at most the TTL window, until
re-verification runs both gates again.

PII note: the entitlement table stores user emails in a UC table. It is an
SP-only object (no user grants; the dynamic view reads it with definer's
rights), and user emails already flow through the app's logs and the audit
columns of other app tables, so this introduces no new exposure class.
"""

from __future__ import annotations

import asyncio
import logging

from databricks.sdk import WorkspaceClient

from databricks_labs_dqx_app.backend.services.quarantine_sample_service import QuarantineSampleService
from databricks_labs_dqx_app.backend.sql_executor import RawSql, SqlExecutor
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, validate_fqn

logger = logging.getLogger(__name__)

ENTITLEMENTS_TABLE_NAME = "dq_user_table_entitlements"
FAILING_ROWS_VIEW_NAME = "v_dq_failing_rows"

# How long one successful OBO SELECT probe stays valid, both for the
# probe-skip in :meth:`EntitlementService.verify_and_record` and for the
# dynamic view's WHERE gate (the two MUST agree — the skip is only sound
# because a row fresh enough to skip is also fresh enough to open the view).
ENTITLEMENT_TTL_HOURS = 24

# Request cap on POST /api/v1/genie/verify-entitlements. Together with the
# probe semaphore this bounds the worst-case work one request can trigger.
VERIFY_ENTITLEMENTS_MAX_FQNS = 50

# At most this many OBO probes (and their follow-up upserts) in flight per
# verify_and_record call. asyncio.Semaphore + to_thread rather than a thread
# pool: the app has no existing parallel-OBO idiom to mirror, and this keeps
# the fan-out on the event loop where the route already lives.
PROBE_CONCURRENCY = 5

# Per-FQN outcomes of verify_and_record. Deterministic strings — the UI
# treats the endpoint as fire-and-forget but tests (and any curious caller)
# rely on these. "suppressed" mirrors the failed-rows endpoint's semantics:
# SELECT passed, but fine-grained access controls make row-level exposure
# unsafe, so no entitlement is granted.
OUTCOME_VERIFIED = "verified"
OUTCOME_DENIED = "denied"
OUTCOME_SUPPRESSED = "suppressed"
OUTCOME_ERROR = "error"


class EntitlementService:
    """Owns the entitlement cache table + gated view, and the verify flow.

    Constructed over the app SERVICE PRINCIPAL's warehouse executor (the SP
    owns both UC objects). The caller's OBO executor and OBO WorkspaceClient
    are passed per call to :meth:`verify_and_record` — never stored.
    """

    def __init__(self, sql: SqlExecutor) -> None:
        self._sql = sql
        # Quoted per part so hyphenated catalog/schema names stay parseable
        # in object-name positions — same convention as ScoreViewService.
        self._catalog_q = sql.q(sql.catalog)
        self._schema_q = sql.q(sql.schema)

    @property
    def entitlements_table_fqn_quoted(self) -> str:
        return f"{self._catalog_q}.{self._schema_q}.{ENTITLEMENTS_TABLE_NAME}"

    @property
    def failing_rows_view_fqn_quoted(self) -> str:
        return f"{self._catalog_q}.{self._schema_q}.{FAILING_ROWS_VIEW_NAME}"

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def entitlement_table_ddl(self) -> str:
        """CREATE TABLE IF NOT EXISTS statement for *dq_user_table_entitlements*.

        IF NOT EXISTS (not OR REPLACE) — the rows are state, not a definition
        that should ship fresh with every app version. One row per
        (user_email, table_fqn); upserts refresh *verified_at* in place.
        SP-only: the startup grant step deliberately grants users NOTHING on
        this table — the dynamic view reads it with definer's rights.
        """
        return (
            f"CREATE TABLE IF NOT EXISTS {self.entitlements_table_fqn_quoted} (\n"
            "  user_email STRING NOT NULL,\n"
            "  table_fqn STRING NOT NULL,\n"
            "  verified_at TIMESTAMP NOT NULL,\n"
            "  CONSTRAINT pk_dq_user_table_entitlements PRIMARY KEY (user_email, table_fqn) RELY\n"
            ")"
        )

    def failing_rows_view_ddl(self) -> str:
        """CREATE OR REPLACE VIEW statement for *v_dq_failing_rows*.

        The gate is evaluated with the QUERYING user's identity:
        ``current_user()`` inside a definer's-rights view still resolves to
        the caller running the query, so when Genie executes SQL on behalf
        of a user, only tables THAT user self-verified within the TTL window
        contribute rows. No entitlement rows → the view is fail-safe empty.

        Column selection is explicit — ``requesting_user`` (the email of
        whoever triggered the quarantining run) is deliberately excluded so
        the view never exposes another user's identity to every reader.

        Draft-run rows are intentionally NOT filtered here: they belong to
        tables the caller verified SELECT on, so they are not a leak, and
        run-mode filtering happens in Genie's curated SQL (P4.2 — the
        run_id subselect pattern), keeping this view purely a permission
        gate.
        """
        return (
            f"CREATE OR REPLACE VIEW {self.failing_rows_view_fqn_quoted}\n"
            "COMMENT 'Permission-gated failing rows for Ask Genie (OBO). current_user() is the "
            "QUERYING user: rows appear only for source tables that user self-verified SELECT on "
            f"within the last {ENTITLEMENT_TTL_HOURS} hours (dq_user_table_entitlements). "
            "Otherwise the view is empty.'\n"
            "AS\n"
            "SELECT\n"
            "  q.quarantine_id,\n"
            "  q.run_id,\n"
            "  q.source_table_fqn,\n"
            "  q.row_data,\n"
            "  q.errors,\n"
            "  q.warnings,\n"
            "  q.created_at\n"
            f"FROM {self._catalog_q}.{self._schema_q}.dq_quarantine_records q\n"
            "WHERE EXISTS (\n"
            f"  SELECT 1 FROM {self.entitlements_table_fqn_quoted} e\n"
            "  WHERE e.user_email = current_user()\n"
            "    AND e.table_fqn = q.source_table_fqn\n"
            f"    AND e.verified_at > current_timestamp() - INTERVAL {ENTITLEMENT_TTL_HOURS} HOURS\n"
            ")"
        )

    def ensure_objects(self) -> None:
        """Create the entitlement table, then the dependent dynamic view.

        Idempotent; raises on failure — the startup caller decides whether
        that is fatal (it is best-effort, same contract as the score views;
        see *app._ensure_entitlement_objects*).
        """
        logger.info(f"Ensuring entitlement table {ENTITLEMENTS_TABLE_NAME} exists")
        self._sql.execute(self.entitlement_table_ddl())
        logger.info(f"Creating/refreshing gated failing-rows view {FAILING_ROWS_VIEW_NAME}")
        self._sql.execute(self.failing_rows_view_ddl())

    # ------------------------------------------------------------------
    # Cache reads / writes (SP-side)
    # ------------------------------------------------------------------

    def fresh_entitlements(self, user_email: str, table_fqns: list[str]) -> set[str]:
        """FQNs among *table_fqns* whose entitlement row is still fresh.

        One batched SP read. Never raises: the freshness check is only a
        probe-skip optimisation — on any read failure it returns the empty
        set so every FQN falls through to the (authoritative) OBO probe.
        All *table_fqns* must already have passed :func:`validate_fqn`
        (escape_sql_string relies on it having rejected backslashes).
        """
        if not table_fqns:
            return set()
        e_email = escape_sql_string(user_email)
        in_list = ", ".join(f"'{escape_sql_string(fqn)}'" for fqn in table_fqns)
        stmt = (
            f"SELECT table_fqn FROM {self.entitlements_table_fqn_quoted} "  # noqa: S608
            f"WHERE user_email = '{e_email}' AND table_fqn IN ({in_list}) "
            f"AND verified_at > current_timestamp() - INTERVAL {ENTITLEMENT_TTL_HOURS} HOURS"
        )
        try:
            return {row[0] for row in self._sql.query(stmt) if row and row[0]}
        except Exception:
            logger.warning("Entitlement freshness read failed; probing every table", exc_info=True)
            return set()

    def record_entitlement(self, user_email: str, table_fqn: str) -> bool:
        """Best-effort SP-side upsert of one fresh entitlement row.

        MERGE on (user_email, table_fqn) — the same Delta upsert idiom the
        score cache uses — refreshing *verified_at* in place. Never raises:
        the callers (the verify endpoint and the failed-rows piggyback) must
        not fail their own responses over a cache write. Returns whether
        the row was written.
        """
        try:
            validate_fqn(table_fqn)
            self._sql.upsert(
                self.entitlements_table_fqn_quoted,
                {"user_email": user_email, "table_fqn": table_fqn},
                {"verified_at": RawSql("current_timestamp()")},
            )
            return True
        except Exception:
            logger.warning("Could not record table entitlement", exc_info=True)
            return False

    # ------------------------------------------------------------------
    # Verify flow
    # ------------------------------------------------------------------

    async def verify_and_record(
        self, obo_sql: SqlExecutor, obo_ws: WorkspaceClient, user_email: str, table_fqns: list[str]
    ) -> dict[str, str]:
        """Run BOTH permission gates per table AS THE CALLER; cache the passes.

        Per FQN (deduplicated; every input FQN appears exactly once in the
        result):

        1. validate the FQN — malformed names get ``error`` and never touch
           SQL (validate-before-probe);
        2. skip the gates when a fresh cache row exists (one batched SP
           read) — ``verified`` (a fresh row means both gates passed within
           the TTL window);
        3. otherwise run the live *user_can_select* OBO probe (bounded to
           :data:`PROBE_CONCURRENCY` concurrent verifications) — the probe
           fails closed, so any failure is ``denied``;
        4. then — same order as the failed-rows endpoint — check
           *has_fine_grained_access_control* via the caller's OBO client;
           a row filter / column mask (or an unverifiable state — it fails
           closed too) yields ``suppressed`` and NO entitlement, keeping the
           Genie view consistent with the app's own suppression;
        5. upsert the entitlement SP-side only when both gates passed —
           ``verified``, or ``error`` when the row could not be written
           (the view would stay closed, so reporting ``verified`` would
           lie).

        Never raises: every failure mode degrades to a per-FQN outcome.
        """
        outcomes: dict[str, str] = {}
        valid: list[str] = []
        for fqn in dict.fromkeys(table_fqns):
            try:
                validate_fqn(fqn)
                valid.append(fqn)
            except ValueError:
                outcomes[fqn] = OUTCOME_ERROR
        fresh = await asyncio.to_thread(self.fresh_entitlements, user_email, valid)
        to_probe: list[str] = []
        for fqn in valid:
            if fqn in fresh:
                outcomes[fqn] = OUTCOME_VERIFIED
            else:
                to_probe.append(fqn)

        semaphore = asyncio.Semaphore(PROBE_CONCURRENCY)

        async def probe(fqn: str) -> tuple[str, str]:
            async with semaphore:
                # Gate 1 — user_can_select fails closed (returns False on
                # ANY probe failure), so no exception can escape this call.
                allowed = await asyncio.to_thread(QuarantineSampleService.user_can_select, obo_sql, fqn)
                if not allowed:
                    return fqn, OUTCOME_DENIED
                # Gate 2 — fine-grained controls; fails closed too (an
                # unverifiable state reads as "present").
                fgac = await asyncio.to_thread(
                    QuarantineSampleService.has_fine_grained_access_control, obo_ws, fqn
                )
                if fgac:
                    return fqn, OUTCOME_SUPPRESSED
                recorded = await asyncio.to_thread(self.record_entitlement, user_email, fqn)
            return fqn, OUTCOME_VERIFIED if recorded else OUTCOME_ERROR

        for fqn, outcome in await asyncio.gather(*(probe(fqn) for fqn in to_probe)):
            outcomes[fqn] = outcome
        return outcomes
