"""Unit tests for :class:`DatabaseResetService` (admin "Reset database").

These pin the destructive-scope guarantees that make the feature safe:

- Exactly the enumerated app-owned tables are cleared — no more, no fewer.
- Only ``DELETE FROM`` is issued (no ``DROP``/``ALTER``/``TRUNCATE`` and no
  arbitrary UC objects), so the customer data tables the app merely monitors
  cannot be touched.
- ``dq_migrations`` (the version tracker) is never touched.
- ``dq_role_mappings`` keeps the ``admin`` role rows so the acting admin is
  not locked out.
"""

from __future__ import annotations

import re

import pytest

from databricks_labs_dqx_app.backend.migrations import (
    ALL_APP_TABLE_NAMES,
    ANALYTICAL_TABLE_NAMES,
    OLTP_TABLE_NAMES,
)
from databricks_labs_dqx_app.backend.services.database_reset_service import DatabaseResetService

_DELETE_RE = re.compile(r"^DELETE FROM cat\.sch\.([a-z_][a-z0-9_]*)")


class _FakeExecutor:
    """Records the SQL it is asked to execute; ``fqn`` is deterministic."""

    def __init__(self, *, fail_on: set[str] | None = None) -> None:
        self.executed: list[str] = []
        self._fail_on = fail_on or set()

    def fqn(self, table: str) -> str:
        return f"cat.sch.{table}"

    def execute(self, sql: str, *, timeout_seconds: int = 120) -> None:
        self.executed.append(sql)
        m = _DELETE_RE.match(sql)
        if m and m.group(1) in self._fail_on:
            raise RuntimeError(f"boom on {m.group(1)}")


def _targeted_tables(executed: list[str]) -> list[str]:
    out: list[str] = []
    for sql in executed:
        m = _DELETE_RE.match(sql)
        assert m is not None, f"unexpected statement (not a scoped DELETE): {sql!r}"
        out.append(m.group(1))
    return out


class TestScope:
    def test_clears_exactly_the_enumerated_app_tables(self):
        delta = _FakeExecutor()
        oltp = _FakeExecutor()
        svc = DatabaseResetService(delta_sql=delta, oltp_sql=oltp)

        result = svc.reset_all_data(performed_by="admin@x.com")

        # The exact set — no more, no fewer — matches the migration registry.
        assert set(result.cleared_tables) == set(ALL_APP_TABLE_NAMES)
        assert not result.failed_tables

    def test_analytical_go_to_delta_oltp_go_to_oltp(self):
        delta = _FakeExecutor()
        oltp = _FakeExecutor()
        DatabaseResetService(delta_sql=delta, oltp_sql=oltp).reset_all_data(performed_by="a@x")

        assert set(_targeted_tables(delta.executed)) == set(ANALYTICAL_TABLE_NAMES)
        assert set(_targeted_tables(oltp.executed)) == set(OLTP_TABLE_NAMES)

    def test_only_delete_statements_no_drop_or_alter(self):
        delta = _FakeExecutor()
        oltp = _FakeExecutor()
        DatabaseResetService(delta_sql=delta, oltp_sql=oltp).reset_all_data(performed_by="a@x")

        for sql in delta.executed + oltp.executed:
            assert sql.startswith("DELETE FROM cat.sch.dq_"), sql
            upper = sql.upper()
            assert "DROP" not in upper
            assert "ALTER" not in upper
            assert "TRUNCATE" not in upper

    def test_never_touches_migration_tracker(self):
        delta = _FakeExecutor()
        oltp = _FakeExecutor()
        DatabaseResetService(delta_sql=delta, oltp_sql=oltp).reset_all_data(performed_by="a@x")

        assert "dq_migrations" not in _targeted_tables(delta.executed + oltp.executed)

    def test_never_touches_a_monitored_customer_table(self):
        # A customer table the app merely monitors must never appear in any
        # statement — the service only ever references its own dq_* tables.
        delta = _FakeExecutor()
        oltp = _FakeExecutor()
        DatabaseResetService(delta_sql=delta, oltp_sql=oltp).reset_all_data(performed_by="a@x")

        for sql in delta.executed + oltp.executed:
            assert "customer_catalog" not in sql
            assert "prod.sales.orders" not in sql


class TestAdminPreservation:
    def test_role_mappings_delete_preserves_admin_rows(self):
        delta = _FakeExecutor()
        oltp = _FakeExecutor()
        DatabaseResetService(delta_sql=delta, oltp_sql=oltp).reset_all_data(performed_by="a@x")

        role_stmts = [s for s, t in zip(oltp.executed, _targeted_tables(oltp.executed)) if t == "dq_role_mappings"]
        assert len(role_stmts) == 1
        # The one role-mappings clear is scoped to keep the admin role.
        assert role_stmts[0] == "DELETE FROM cat.sch.dq_role_mappings WHERE role <> 'admin'"

    def test_every_other_oltp_table_is_fully_cleared(self):
        oltp = _FakeExecutor()
        DatabaseResetService(delta_sql=_FakeExecutor(), oltp_sql=oltp).reset_all_data(performed_by="a@x")

        for sql, table in zip(oltp.executed, _targeted_tables(oltp.executed)):
            if table == "dq_role_mappings":
                continue
            assert "WHERE" not in sql.upper(), f"non-role-mapping clear should be unconditional: {sql!r}"


class TestLakebaseDisabledSharedExecutor:
    def test_no_table_cleared_twice_when_executors_are_the_same(self):
        # When Lakebase is disabled both executors are the same Delta
        # executor; the two table sets are disjoint so nothing double-clears.
        shared = _FakeExecutor()
        result = DatabaseResetService(delta_sql=shared, oltp_sql=shared).reset_all_data(performed_by="a@x")

        targeted = _targeted_tables(shared.executed)
        assert sorted(targeted) == sorted(set(targeted))  # no duplicates
        assert set(targeted) == set(ALL_APP_TABLE_NAMES)
        assert set(result.cleared_tables) == set(ALL_APP_TABLE_NAMES)


class TestBestEffort:
    def test_a_failing_table_is_recorded_and_others_still_clear(self):
        delta = _FakeExecutor(fail_on={"dq_metrics"})
        oltp = _FakeExecutor()
        result = DatabaseResetService(delta_sql=delta, oltp_sql=oltp).reset_all_data(performed_by="a@x")

        assert "dq_metrics" in result.failed_tables
        assert "dq_metrics" not in result.cleared_tables
        # Every other table still cleared.
        assert set(result.cleared_tables) == set(ALL_APP_TABLE_NAMES) - {"dq_metrics"}


class TestAudit:
    def test_result_records_actor_and_timestamp(self):
        result = DatabaseResetService(
            delta_sql=_FakeExecutor(), oltp_sql=_FakeExecutor()
        ).reset_all_data(performed_by="admin@x.com")

        assert result.performed_by == "admin@x.com"
        assert result.performed_at  # ISO-8601 timestamp string
        assert "admin role mappings" in result.preserved_note.lower()


@pytest.mark.parametrize("name", ALL_APP_TABLE_NAMES)
def test_every_registered_table_is_a_dq_prefixed_app_table(name):
    # Sanity pin: the registry must only ever contain app-owned dq_* tables.
    assert name.startswith("dq_")
