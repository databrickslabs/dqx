"""Unit tests for ``services.entitlement_service`` + its startup wiring (P4.1).

Pins the security-relevant DDL of the two UC objects (the SP-only
entitlement cache table and the ``current_user()``-gated
``v_dq_failing_rows`` dynamic view), the verify-and-record flow
(validate-before-probe, fresh-row skip, TTL agreement, BOTH Task 7 gates
in the failed-rows order with fail-closed denial/suppression, bounded
probe concurrency, never-raises), and the best-effort startup ensure +
grant steps in ``backend.app``.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, ColumnMask, TableInfo, TableRowFilter

from databricks_labs_dqx_app.backend.services.entitlement_service import (
    ENTITLEMENT_TTL_HOURS,
    ENTITLEMENTS_TABLE_NAME,
    FAILING_ROWS_VIEW_NAME,
    OUTCOME_DENIED,
    OUTCOME_ERROR,
    OUTCOME_SUPPRESSED,
    OUTCOME_VERIFIED,
    PROBE_CONCURRENCY,
    VERIFY_ENTITLEMENTS_MAX_FQNS,
    EntitlementService,
)
from databricks_labs_dqx_app.backend.sql_utils import validate_fqn

FQN = "main.sales.orders"
FQN2 = "main.sales.customers"
EMAIL = "steward@example.com"
PLAIN_TABLE = TableInfo(row_filter=None, columns=[ColumnInfo(name="id"), ColumnInfo(name="amount")])


@pytest.fixture
def svc(sql_executor_mock) -> EntitlementService:
    sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
    sql_executor_mock.query.return_value = []
    return EntitlementService(sql=sql_executor_mock, genie_schema="genie")


@pytest.fixture
def obo_sql_mock():
    """A separate OBO executor mock; SELECT probes succeed by default."""
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

    mock = create_autospec(SqlExecutor, instance=True)
    mock.query.return_value = []
    return mock


@pytest.fixture
def obo_ws_mock():
    """OBO WorkspaceClient mock — no fine-grained controls by default."""
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.tables.get.return_value = PLAIN_TABLE
    return ws


# ---------------------------------------------------------------------------
# Constants + object names
# ---------------------------------------------------------------------------


class TestConstants:
    def test_object_name_constants(self):
        assert ENTITLEMENTS_TABLE_NAME == "dq_user_table_entitlements"
        assert FAILING_ROWS_VIEW_NAME == "v_dq_failing_rows"

    def test_bounds(self):
        # The endpoint cap and the probe semaphore together bound the
        # worst-case OBO work one request can trigger.
        assert VERIFY_ENTITLEMENTS_MAX_FQNS == 50
        assert PROBE_CONCURRENCY == 5
        assert ENTITLEMENT_TTL_HOURS == 24

    def test_quoted_fqns(self, svc):
        assert svc.entitlements_table_fqn_quoted == "`dqx_test`.`dqx_app_test`.dq_user_table_entitlements"
        assert svc.failing_rows_view_fqn_quoted == "`dqx_test`.`genie`.v_dq_failing_rows"


# ---------------------------------------------------------------------------
# DDL — entitlement table
# ---------------------------------------------------------------------------


class TestEntitlementTableDdl:
    def test_is_create_if_not_exists_never_replace(self, svc):
        # Rows are state (verified entitlements), not a definition — a
        # startup must never wipe them.
        ddl = svc.entitlement_table_ddl()
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS ")
        assert "OR REPLACE" not in ddl

    def test_targets_the_quoted_table(self, svc):
        assert f"`dqx_test`.`dqx_app_test`.{ENTITLEMENTS_TABLE_NAME}" in svc.entitlement_table_ddl()

    def test_columns_match_the_plan(self, svc):
        ddl = svc.entitlement_table_ddl()
        assert "user_email STRING NOT NULL" in ddl
        assert "table_fqn STRING NOT NULL" in ddl
        assert "verified_at TIMESTAMP NOT NULL" in ddl

    def test_primary_key_is_user_and_table(self, svc):
        assert "PRIMARY KEY (user_email, table_fqn)" in svc.entitlement_table_ddl()

    def test_is_a_single_statement_with_no_grant(self, svc):
        # The table is SP-only: the DDL must never carry a user grant.
        ddl = svc.entitlement_table_ddl()
        assert ";" not in ddl
        assert "GRANT" not in ddl.upper()


# ---------------------------------------------------------------------------
# DDL — gated dynamic view
# ---------------------------------------------------------------------------


class TestFailingRowsViewDdl:
    def test_is_idempotent_create_or_replace(self, svc):
        assert svc.failing_rows_view_ddl().startswith("CREATE OR REPLACE VIEW ")

    def test_targets_view_and_reads_quarantine_records(self, svc):
        ddl = svc.failing_rows_view_ddl()
        assert f"`dqx_test`.`genie`.{FAILING_ROWS_VIEW_NAME}" in ddl
        assert "`dqx_test`.`dqx_app_test`.dq_quarantine_records" in ddl

    def test_gate_is_current_user_exists_with_ttl(self, svc):
        # The load-bearing predicate: the QUERYING user must hold a fresh
        # entitlement row for the row's source table, else zero rows.
        ddl = svc.failing_rows_view_ddl()
        assert "WHERE EXISTS (" in ddl
        assert f"`dqx_test`.`dqx_app_test`.{ENTITLEMENTS_TABLE_NAME} e" in ddl
        assert "e.user_email = current_user()" in ddl
        assert "e.table_fqn = q.source_table_fqn" in ddl
        assert f"e.verified_at > current_timestamp() - INTERVAL {ENTITLEMENT_TTL_HOURS} HOURS" in ddl

    def test_documents_current_user_semantics_in_the_object_comment(self, svc):
        ddl = svc.failing_rows_view_ddl()
        assert "COMMENT '" in ddl
        assert "QUERYING user" in ddl

    def test_excludes_requesting_user_column(self, svc):
        # The quarantine table records who triggered the run; the shared
        # view must not expose that email to every reader.
        assert "requesting_user" not in svc.failing_rows_view_ddl()

    def test_exposes_the_row_payload_columns(self, svc):
        ddl = svc.failing_rows_view_ddl()
        for col in ("quarantine_id", "run_id", "source_table_fqn", "row_data", "errors", "warnings", "created_at"):
            assert f"q.{col}" in ddl

    def test_is_a_single_statement(self, svc):
        assert ";" not in svc.failing_rows_view_ddl()


class TestEnsureObjects:
    def test_creates_table_then_view(self, svc, sql_executor_mock):
        svc.ensure_objects()
        executed = [call.args[0] for call in sql_executor_mock.execute.call_args_list]
        assert executed == [svc.entitlement_table_ddl(), svc.failing_rows_view_ddl()]


# ---------------------------------------------------------------------------
# Cache reads / writes
# ---------------------------------------------------------------------------


class TestFreshEntitlements:
    def test_batched_read_filters_on_user_ttl_and_fqns(self, svc, sql_executor_mock):
        sql_executor_mock.query.return_value = [[FQN]]
        fresh = svc.fresh_entitlements(EMAIL, [FQN, FQN2])
        assert fresh == {FQN}
        stmt = sql_executor_mock.query.call_args.args[0]
        assert f"user_email = '{EMAIL}'" in stmt
        assert f"'{FQN}', '{FQN2}'" in stmt
        assert f"verified_at > current_timestamp() - INTERVAL {ENTITLEMENT_TTL_HOURS} HOURS" in stmt

    def test_escapes_the_email_literal(self, svc, sql_executor_mock):
        svc.fresh_entitlements("o'brien@example.com", [FQN])
        assert "o''brien@example.com" in sql_executor_mock.query.call_args.args[0]

    def test_empty_input_short_circuits(self, svc, sql_executor_mock):
        assert svc.fresh_entitlements(EMAIL, []) == set()
        sql_executor_mock.query.assert_not_called()

    def test_read_failure_returns_empty_set_never_raises(self, svc, sql_executor_mock):
        # Fail-open here is sound: the freshness read only SKIPS probes;
        # the authoritative gate is the OBO probe itself.
        sql_executor_mock.query.side_effect = RuntimeError("warehouse down")
        assert svc.fresh_entitlements(EMAIL, [FQN]) == set()


class TestRecordEntitlement:
    def test_upserts_on_user_and_table_key(self, svc, sql_executor_mock):
        assert svc.record_entitlement(EMAIL, FQN) is True
        key_cols = sql_executor_mock.upsert.call_args.args[1]
        value_cols = sql_executor_mock.upsert.call_args.args[2]
        assert sql_executor_mock.upsert.call_args.args[0] == svc.entitlements_table_fqn_quoted
        assert key_cols == {"user_email": EMAIL, "table_fqn": FQN}
        assert value_cols["verified_at"].expr == "current_timestamp()"

    def test_rejects_invalid_fqn_without_sql(self, svc, sql_executor_mock):
        assert svc.record_entitlement(EMAIL, "not-a-three-part-name") is False
        sql_executor_mock.upsert.assert_not_called()

    def test_upsert_failure_never_raises(self, svc, sql_executor_mock):
        sql_executor_mock.upsert.side_effect = RuntimeError("merge failed")
        assert svc.record_entitlement(EMAIL, FQN) is False


class TestCanonicalFqnAgreement:
    """The entitlement side must store the EXACT string the runner writes.

    The gated view's predicate is a case-sensitive string equality
    (``e.table_fqn = q.source_table_fqn``), and the runner writes
    ``source_table_fqn`` verbatim from the submitted config
    (``app/tasks/src/dqx_task_runner/runner.py``:
    ``config.get("source_table_fqn", "")``), which the app populates with
    the binding's plain unquoted ``catalog.schema.table`` unchanged
    (``routes/v1/dryrun.py``: ``"source_table_fqn": table_fqn``). So the
    canonical format on BOTH sides is that plain unquoted FQN, case
    preserved exactly as stored on the binding — any transformation here
    (lowercasing, backtick-quoting, trimming) would fail closed: no leak,
    but Genie's failing-rows view silently empty for a verified table.
    """

    def test_validate_and_upsert_preserve_the_runner_written_fqn_verbatim(self, svc, sql_executor_mock):
        # Case + hyphens preserved: exactly what a binding can carry and
        # what the runner therefore writes to dq_quarantine_records.
        runner_written = "Main.Sales-Data.Orders_2024"

        # validate_fqn accepts the plain unquoted form and returns it
        # UNCHANGED (no normalisation the quarantine side wouldn't mirror).
        assert validate_fqn(runner_written) == runner_written

        # record_entitlement stores it byte-for-byte as the merge key —
        # the same string the view will compare against source_table_fqn.
        assert svc.record_entitlement(EMAIL, runner_written) is True
        stored = sql_executor_mock.upsert.call_args.args[1]["table_fqn"]
        assert stored == runner_written  # byte-equal: no lowercase, no quoting, no trim


# ---------------------------------------------------------------------------
# verify_and_record
# ---------------------------------------------------------------------------


class TestVerifyAndRecord:
    async def test_fresh_row_skips_both_gates(self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock):
        # A fresh row means both gates passed within the TTL window —
        # neither the SELECT probe nor the FGAC metadata read runs.
        sql_executor_mock.query.return_value = [[FQN]]
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert outcomes == {FQN: OUTCOME_VERIFIED}
        obo_sql_mock.query.assert_not_called()
        obo_ws_mock.tables.get.assert_not_called()
        sql_executor_mock.upsert.assert_not_called()

    async def test_stale_row_runs_both_gates_and_upserts_on_pass(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        sql_executor_mock.query.return_value = []  # nothing fresh
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert outcomes == {FQN: OUTCOME_VERIFIED}
        # Gate 1 ran as the CALLER (OBO executor), zero-row and quoted.
        probe = obo_sql_mock.query.call_args.args[0]
        assert probe == "SELECT 1 FROM `main`.`sales`.`orders` LIMIT 0"
        # Gate 2 ran as the CALLER too (OBO metadata read).
        obo_ws_mock.tables.get.assert_called_once_with(FQN)
        assert sql_executor_mock.upsert.call_args.args[1] == {"user_email": EMAIL, "table_fqn": FQN}

    async def test_gates_run_in_the_failed_rows_order(self, svc, obo_sql_mock, obo_ws_mock):
        # Same load-bearing order as the failed-rows endpoint: the cheap
        # SELECT self-check first, the FGAC metadata read second.
        order: list[str] = []
        obo_sql_mock.query.side_effect = lambda *a, **k: order.append("obo_select_check") or []
        obo_ws_mock.tables.get.side_effect = lambda *a, **k: order.append("fine_grained_check") or PLAIN_TABLE
        await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert order == ["obo_select_check", "fine_grained_check"]

    async def test_denied_probe_fails_closed_and_skips_the_fgac_gate(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        obo_sql_mock.query.side_effect = PermissionError("no SELECT")
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert outcomes == {FQN: OUTCOME_DENIED}
        obo_ws_mock.tables.get.assert_not_called()
        sql_executor_mock.upsert.assert_not_called()

    async def test_row_filter_suppresses_and_writes_nothing(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        # SELECT passes, but the table carries a row filter: the in-app
        # failed-rows path suppresses such tables, so NO entitlement — the
        # Genie view must never serve rows the app refuses to show.
        obo_ws_mock.tables.get.return_value = TableInfo(
            row_filter=TableRowFilter(function_name="main.sales.f", input_column_names=["region"])
        )
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert outcomes == {FQN: OUTCOME_SUPPRESSED}
        sql_executor_mock.upsert.assert_not_called()

    async def test_column_mask_suppresses_and_writes_nothing(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        obo_ws_mock.tables.get.return_value = TableInfo(
            row_filter=None,
            columns=[ColumnInfo(name="ssn", mask=ColumnMask(function_name="main.sales.m"))],
        )
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert outcomes == {FQN: OUTCOME_SUPPRESSED}
        sql_executor_mock.upsert.assert_not_called()

    async def test_fgac_metadata_failure_fails_closed_to_suppression(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        obo_ws_mock.tables.get.side_effect = RuntimeError("transient UC failure")
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert outcomes == {FQN: OUTCOME_SUPPRESSED}
        sql_executor_mock.upsert.assert_not_called()

    async def test_upsert_failure_reports_error_not_verified(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        # The view stays closed when the row was not written — claiming
        # "verified" would lie to the caller.
        sql_executor_mock.upsert.side_effect = RuntimeError("merge failed")
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert outcomes == {FQN: OUTCOME_ERROR}

    async def test_invalid_fqn_is_error_and_never_probed(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, ["bad name", FQN])
        assert outcomes["bad name"] == OUTCOME_ERROR
        assert outcomes[FQN] == OUTCOME_VERIFIED
        # validate-before-probe: only the valid FQN reached SQL anywhere.
        for call in obo_sql_mock.query.call_args_list:
            assert "bad name" not in call.args[0]
        freshness_stmt = sql_executor_mock.query.call_args.args[0]
        assert "bad name" not in freshness_stmt

    async def test_duplicates_are_verified_once(self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock):
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN, FQN])
        assert outcomes == {FQN: OUTCOME_VERIFIED}
        assert obo_sql_mock.query.call_count == 1

    async def test_freshness_read_failure_degrades_to_probing(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        sql_executor_mock.query.side_effect = RuntimeError("warehouse down")
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, [FQN])
        assert outcomes == {FQN: OUTCOME_VERIFIED}
        obo_sql_mock.query.assert_called_once()

    async def test_every_input_gets_exactly_one_outcome(
        self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock
    ):
        fqns = [f"main.sales.t{i}" for i in range(12)] + ["oops"]
        outcomes = await svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, fqns)
        assert set(outcomes) == set(fqns)
        valid_outcomes = {OUTCOME_VERIFIED, OUTCOME_DENIED, OUTCOME_SUPPRESSED, OUTCOME_ERROR}
        assert all(v in valid_outcomes for v in outcomes.values())

    async def test_probe_concurrency_is_bounded(self, svc, sql_executor_mock, obo_sql_mock, obo_ws_mock):
        """No more than PROBE_CONCURRENCY probes may be in flight at once.

        The probe side_effect blocks every probe thread on an Event; the
        test waits (condition-based, no fixed sleeps) until the semaphore
        is saturated, asserts the in-flight count never exceeded the
        bound, then releases everyone.
        """
        release = threading.Event()
        lock = threading.Lock()
        state = {"in_flight": 0, "max_in_flight": 0, "started": 0}

        def blocking_probe(_stmt: str) -> list[list[str]]:
            with lock:
                state["in_flight"] += 1
                state["started"] += 1
                state["max_in_flight"] = max(state["max_in_flight"], state["in_flight"])
            assert release.wait(timeout=10), "probes were never released"
            with lock:
                state["in_flight"] -= 1
            return []

        obo_sql_mock.query.side_effect = blocking_probe
        fqns = [f"main.sales.t{i}" for i in range(PROBE_CONCURRENCY * 3)]
        task = asyncio.create_task(svc.verify_and_record(obo_sql_mock, obo_ws_mock, EMAIL, fqns))

        async def saturated() -> None:
            while True:
                with lock:
                    if state["started"] >= PROBE_CONCURRENCY:
                        return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(saturated(), timeout=10)
        release.set()
        outcomes = await asyncio.wait_for(task, timeout=10)
        assert state["max_in_flight"] <= PROBE_CONCURRENCY
        assert len(outcomes) == len(fqns)
        assert all(v == OUTCOME_VERIFIED for v in outcomes.values())


# ---------------------------------------------------------------------------
# Startup wiring — backend.app
# ---------------------------------------------------------------------------


class TestStartupWiring:
    """``_ensure_entitlement_objects`` + ``_grant_user_view_access``."""

    def test_ensure_creates_table_then_view(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.app import _ensure_entitlement_objects

        sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
        _ensure_entitlement_objects(sql_executor_mock)
        executed = [call.args[0] for call in sql_executor_mock.execute.call_args_list]
        assert len(executed) == 2
        assert ENTITLEMENTS_TABLE_NAME in executed[0]
        assert FAILING_ROWS_VIEW_NAME in executed[1]

    def test_ensure_is_best_effort_and_never_raises(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.app import _ensure_entitlement_objects

        sql_executor_mock.q.side_effect = lambda ident: "`" + ident.replace("`", "``") + "`"
        sql_executor_mock.execute.side_effect = RuntimeError("no CREATE TABLE privilege")
        _ensure_entitlement_objects(sql_executor_mock)  # must not propagate

    def test_grants_use_schema_plus_select_on_the_five_views(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.app import _grant_user_view_access

        _grant_user_view_access(sql_executor_mock)
        executed = [call.args[0] for call in sql_executor_mock.execute_no_schema.call_args_list]
        assert executed[0] == "GRANT USE SCHEMA ON SCHEMA `dqx_test`.`dqx_app_test` TO `account users`"
        select_grants = executed[1:]
        assert [
            f"GRANT SELECT ON TABLE `dqx_test`.`dqx_app_test`.{name} TO `account users`"
            for name in (
                "mv_dq_scores",
                "v_dq_check_results",
                "v_dq_check_results_asof",
                "v_dq_check_attribution",
                "v_dq_failing_rows",
            )
        ] == select_grants

    def test_the_entitlement_table_gets_no_grant(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.app import _grant_user_view_access

        _grant_user_view_access(sql_executor_mock)
        for call in sql_executor_mock.execute_no_schema.call_args_list:
            assert ENTITLEMENTS_TABLE_NAME not in call.args[0]

    def test_grants_are_individually_best_effort(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.app import _grant_user_view_access

        # First statement fails — the remaining grants must still be issued.
        sql_executor_mock.execute_no_schema.side_effect = [RuntimeError("denied")] + [None] * 5
        _grant_user_view_access(sql_executor_mock)  # must not propagate
        assert sql_executor_mock.execute_no_schema.call_count == 6
