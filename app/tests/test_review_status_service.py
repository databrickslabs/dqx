"""Tests for ``ReviewStatusService`` write paths.

Focus is the audit-history contract: the primary status mutation and the
``dq_run_review_status_history`` INSERT are separate auto-committed
statements, so the history write is best-effort — a failure there must
never propagate after the status change has already committed (mirrors
``RoleService._record_history``).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from databricks_labs_dqx_app.backend.services.review_status_service import ReviewStatusService


@pytest.fixture
def review_status_service(sql_executor_mock):
    # ``fqn`` / ``ts_text`` are passthroughs so the emitted SQL stays
    # inspectable, matching the convention in test_role_service.py.
    sql_executor_mock.fqn.side_effect = lambda t: t
    sql_executor_mock.ts_text.side_effect = lambda c: c
    # No explicit row exists by default, so ``get_effective`` falls back
    # to the catalogue default and ``set_status`` performs an upsert.
    sql_executor_mock.query.return_value = []

    settings = MagicMock(name="AppSettingsService")
    settings.get_run_review_statuses.return_value = [
        {"value": "Pending review", "is_default": True},
        {"value": "Acknowledged"},
    ]
    settings.get_default_run_review_status.return_value = "Pending review"

    svc = ReviewStatusService(sql=sql_executor_mock, settings=settings)
    return svc, sql_executor_mock


def _history_inserts(sql_executor_mock) -> list[str]:
    return [
        call.args[0]
        for call in sql_executor_mock.execute.call_args_list
        if call.args and "INSERT INTO" in call.args[0] and "history" in call.args[0]
    ]


class TestSetStatusHistory:
    def test_set_status_records_history(self, review_status_service):
        svc, sql_executor_mock = review_status_service
        svc.set_status("run-1", "Acknowledged", user_email="alice@example.com")

        sql_executor_mock.upsert.assert_called_once()
        inserts = _history_inserts(sql_executor_mock)
        assert len(inserts) == 1, f"expected one history INSERT, got: {inserts}"
        sql = inserts[0]
        assert "'Acknowledged'" in sql
        assert "'Pending review'" in sql  # previous effective (virtual default)
        assert "'alice@example.com'" in sql
        # Timestamp from the DB, not the app clock.
        assert "now()" in sql

    def test_history_failure_does_not_propagate(self, review_status_service):
        svc, sql_executor_mock = review_status_service
        # In set_status the only execute() call is the history INSERT; a
        # failure there must not surface (the upsert already committed).
        sql_executor_mock.execute.side_effect = RuntimeError("history table unavailable")

        record = svc.set_status("run-1", "Acknowledged", user_email="alice@example.com")

        assert record.status == "Acknowledged"
        assert record.is_default is False
        sql_executor_mock.upsert.assert_called_once()


class TestClearStatusHistory:
    def test_history_failure_does_not_propagate(self, review_status_service):
        svc, sql_executor_mock = review_status_service

        # The DELETE must succeed but the history INSERT fails — assert the
        # revert still returns cleanly (best-effort history).
        def _execute(sql, *_, **__):
            if "history" in sql:
                raise RuntimeError("history table unavailable")
            return None

        sql_executor_mock.execute.side_effect = _execute

        record = svc.clear_status("run-1", user_email="bob@example.com")

        assert record.status == "Pending review"
        assert record.is_default is True
        # Both the DELETE and the (failing) history INSERT were attempted.
        executed = [c.args[0] for c in sql_executor_mock.execute.call_args_list if c.args]
        assert any("DELETE FROM" in s for s in executed)
        assert any("INSERT INTO" in s and "history" in s for s in executed)
