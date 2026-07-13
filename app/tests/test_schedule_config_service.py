"""Tests for ``ScheduleConfigService`` name-validation guards.

``save`` is the only user-facing write path for schedule names (see
``routes/v1/schedules.py::save_schedule``). Names are allowed to contain
``:`` (``validate_schedule_name``), which collides with the reserved
``product:<uuid>`` tracker-key namespace ``SchedulerService`` uses for Data
Products schedules (see ``services/scheduler_service.py`` Task 5). This
guards against a user schedule silently hijacking — or being overwritten
by — a Data Product's tracker row in ``dq_schedule_runs``.
"""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.services.schedule_config_service import (
    PRODUCT_SCHEDULE_PREFIX,
    TABLE_SCHEDULE_PREFIX,
    ScheduleConfigService,
)


@pytest.fixture
def schedule_config_service(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.ts_text.side_effect = lambda c: c
    # ``save`` re-reads the row it just wrote via ``get`` — stub a matching
    # row shape (schedule_name, config_json, version, created_by,
    # created_at, updated_by, updated_at) so that re-read doesn't choke on
    # an unconfigured MagicMock row.
    sql_executor_mock.query.return_value = [
        ("main", "{}", 1, "alice@example.com", "2024-01-01", "alice@example.com", "2024-01-01"),
    ]
    return ScheduleConfigService(sql=sql_executor_mock)


class TestSaveRejectsReservedProductPrefix:
    def test_rejects_name_with_product_prefix(self, schedule_config_service):
        with pytest.raises(ValueError, match=PRODUCT_SCHEDULE_PREFIX):
            schedule_config_service.save("product:some-uuid", {}, "alice@example.com")

    def test_rejects_bare_product_prefix(self, schedule_config_service):
        with pytest.raises(ValueError, match=PRODUCT_SCHEDULE_PREFIX):
            schedule_config_service.save("product:", {}, "alice@example.com")

    def test_does_not_write_when_name_is_rejected(self, schedule_config_service, sql_executor_mock):
        with pytest.raises(ValueError):
            schedule_config_service.save("product:some-uuid", {}, "alice@example.com")
        sql_executor_mock.upsert_with_audit.assert_not_called()

    def test_allows_ordinary_name_containing_colon(self, schedule_config_service, sql_executor_mock):
        # Colons are otherwise legal in schedule names — only the reserved
        # ``product:`` prefix must be rejected.
        schedule_config_service.save("team:nightly", {"k": "v"}, "alice@example.com")
        sql_executor_mock.upsert_with_audit.assert_called_once()

    def test_allows_name_containing_product_not_as_prefix(self, schedule_config_service, sql_executor_mock):
        schedule_config_service.save("my-product:x", {"k": "v"}, "alice@example.com")
        sql_executor_mock.upsert_with_audit.assert_called_once()


class TestSaveRejectsReservedTablePrefix:
    """The ``table:<binding_id>`` namespace is reserved for monitored-table
    schedules (P21 item 14), exactly like ``product:``."""

    def test_rejects_name_with_table_prefix(self, schedule_config_service):
        with pytest.raises(ValueError, match=TABLE_SCHEDULE_PREFIX):
            schedule_config_service.save("table:some-binding", {}, "alice@example.com")

    def test_rejects_bare_table_prefix(self, schedule_config_service):
        with pytest.raises(ValueError, match=TABLE_SCHEDULE_PREFIX):
            schedule_config_service.save("table:", {}, "alice@example.com")

    def test_does_not_write_when_name_is_rejected(self, schedule_config_service, sql_executor_mock):
        with pytest.raises(ValueError):
            schedule_config_service.save("table:some-binding", {}, "alice@example.com")
        sql_executor_mock.upsert_with_audit.assert_not_called()

    def test_allows_name_containing_table_not_as_prefix(self, schedule_config_service, sql_executor_mock):
        schedule_config_service.save("my-table:x", {"k": "v"}, "alice@example.com")
        sql_executor_mock.upsert_with_audit.assert_called_once()
