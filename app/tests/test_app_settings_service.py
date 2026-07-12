"""Tests for ``AppSettingsService`` run-review-status seeding semantics.

The key invariant: ``get_run_review_statuses`` is a pure read (no write,
even when unset), and seeding happens only via the explicit
``seed_run_review_statuses_if_absent`` called at startup.
"""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService


@pytest.fixture
def settings_service(sql_executor_mock):
    sql_executor_mock.fqn.side_effect = lambda t: t
    return AppSettingsService(sql=sql_executor_mock), sql_executor_mock


class TestRunReviewStatusReadIsSideEffectFree:
    def test_get_returns_seed_without_writing_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []  # no row → unset

        result = svc.get_run_review_statuses()

        # Seed is returned virtually...
        assert [e["value"] for e in result] == [
            "Pending review",
            "Acknowledged",
            "Resolved",
            "False positive",
        ]
        # ...but nothing was persisted (no upsert / write on the read path).
        sql_executor_mock.upsert.assert_not_called()
        assert not any("INSERT" in str(c) or "UPDATE" in str(c) for c in sql_executor_mock.execute.call_args_list)

    def test_get_default_does_not_write_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_default_run_review_status() == "Pending review"
        sql_executor_mock.upsert.assert_not_called()


class TestSeedRunReviewStatuses:
    def test_seeds_when_absent(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []  # unset

        wrote = svc.seed_run_review_statuses_if_absent()

        assert wrote is True
        sql_executor_mock.upsert.assert_called_once()
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "run_review_statuses_v1"}

    def test_noop_when_present(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["[]"]]  # already has a value

        wrote = svc.seed_run_review_statuses_if_absent()

        assert wrote is False
        sql_executor_mock.upsert.assert_not_called()
