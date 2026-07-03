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


class TestAiGatewaySettings:
    """AI Gateway settings (Rules Registry Phase 4A) — kill-switch, endpoint, rate limit.

    All three default to a safe "AI is off / unconfigured" state when the underlying
    setting row is absent, so a fresh deploy with no AI infra never accidentally calls a
    serving endpoint.
    """

    def test_ai_enabled_defaults_to_false(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_ai_enabled() is False

    def test_save_and_read_ai_enabled(self, settings_service):
        svc, sql_executor_mock = settings_service

        svc.save_ai_enabled(True, user_email="admin@x")

        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "ai_enabled"}
        assert kwargs["value_cols"]["setting_value"] == "true"

        sql_executor_mock.query.return_value = [["true"]]
        assert svc.get_ai_enabled() is True

    def test_ai_endpoint_name_defaults_to_databricks_gpt_5_5(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_ai_endpoint_name() == "databricks-gpt-5-5"
        assert svc.AI_ENDPOINT_NAME_DEFAULT == "databricks-gpt-5-5"

    def test_ai_endpoint_name_respects_explicit_empty_value(self, settings_service):
        """An admin who explicitly clears the endpoint gets '', not the default."""
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [[""]]

        assert svc.get_ai_endpoint_name() == ""

    def test_save_ai_endpoint_name_trims_whitespace(self, settings_service):
        svc, sql_executor_mock = settings_service

        saved = svc.save_ai_endpoint_name("  my-endpoint  ", user_email="admin@x")

        assert saved == "my-endpoint"
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["value_cols"]["setting_value"] == "my-endpoint"

    def test_ai_rate_limit_defaults_to_thirty(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_ai_rate_limit_per_user_per_hour() == 30
        assert svc.AI_RATE_LIMIT_DEFAULT == 30

    def test_save_and_read_ai_rate_limit(self, settings_service):
        svc, sql_executor_mock = settings_service

        svc.save_ai_rate_limit_per_user_per_hour(5, user_email="admin@x")

        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["value_cols"]["setting_value"] == "5"

        sql_executor_mock.query.return_value = [["5"]]
        assert svc.get_ai_rate_limit_per_user_per_hour() == 5

    def test_ai_rate_limit_ignores_unparsable_value(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["not-a-number"]]

        assert svc.get_ai_rate_limit_per_user_per_hour() == 30


class TestVectorSearchSettings:
    """Vector Search / embeddings settings (Rules Registry Phase 4B/4C).

    All three default to empty when unset, so the mapping suggester and
    embedding population are no-ops on any deploy with no Vector Search
    infra provisioned.
    """

    @pytest.mark.parametrize(
        ("getter_name", "key"),
        [
            ("get_embedding_endpoint_name", "embedding_endpoint_name"),
            ("get_vs_endpoint_name", "vs_endpoint_name"),
            ("get_vs_index_name", "vs_index_name"),
        ],
    )
    def test_defaults_to_empty(self, settings_service, getter_name, key):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert getattr(svc, getter_name)() == ""

    @pytest.mark.parametrize(
        ("setter_name", "getter_name", "key"),
        [
            ("save_embedding_endpoint_name", "get_embedding_endpoint_name", "embedding_endpoint_name"),
            ("save_vs_endpoint_name", "get_vs_endpoint_name", "vs_endpoint_name"),
            ("save_vs_index_name", "get_vs_index_name", "vs_index_name"),
        ],
    )
    def test_save_trims_whitespace_and_round_trips(self, settings_service, setter_name, getter_name, key):
        svc, sql_executor_mock = settings_service

        saved = getattr(svc, setter_name)("  my-value  ", user_email="admin@x")

        assert saved == "my-value"
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": key}
        assert kwargs["value_cols"]["setting_value"] == "my-value"

        sql_executor_mock.query.return_value = [["my-value"]]
        assert getattr(svc, getter_name)() == "my-value"
