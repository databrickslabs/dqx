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
            "False positive",
            "Acknowledged",
            "Resolved",
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

    AI is ON by default (per explicit product request) so a fresh deploy is usable
    without an admin opt-in; the endpoint + rate limit default to sensible values.
    Saving ``ai_enabled = false`` is the kill-switch that turns AI off app-wide.
    """

    def test_ai_enabled_defaults_to_true(self, settings_service):
        # AI is ON by default (per explicit product request) so a fresh
        # deploy is usable without an admin opt-in; the kill-switch (an
        # explicit "false") is what turns it off.
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_ai_enabled() is True

    def test_ai_enabled_kill_switch_reads_false(self, settings_service):
        # An explicit "false" (the admin kill-switch) disables AI.
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["false"]]

        assert svc.get_ai_enabled() is False

    def test_save_and_read_ai_enabled(self, settings_service):
        svc, sql_executor_mock = settings_service

        svc.save_ai_enabled(True, user_email="admin@x")

        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "ai_enabled"}
        assert kwargs["value_cols"]["setting_value"] == "true"

        sql_executor_mock.query.return_value = [["true"]]
        assert svc.get_ai_enabled() is True

    def test_ai_endpoint_name_defaults_to_databricks_gpt_5_4_nano(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_ai_endpoint_name() == "databricks-gpt-5-4-nano"
        assert svc.AI_ENDPOINT_NAME_DEFAULT == "databricks-gpt-5-4-nano"

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


class TestGlobalResultsEnabled:
    """Global Results tab gating (issue B2-20) — OFF by default; explicit opt-in only."""

    def test_defaults_to_false_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_global_results_enabled() is False

    def test_explicit_true_reads_on(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["true"]]

        assert svc.get_global_results_enabled() is True

    def test_non_true_value_reads_off(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["false"]]

        assert svc.get_global_results_enabled() is False

    def test_save_and_read_round_trips(self, settings_service):
        svc, sql_executor_mock = settings_service

        svc.save_global_results_enabled(True, user_email="admin@x")

        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "global_results_enabled"}
        assert kwargs["value_cols"]["setting_value"] == "true"

        sql_executor_mock.query.return_value = [["true"]]
        assert svc.get_global_results_enabled() is True


class TestVectorSearchSettings:
    """Vector Search / embeddings settings (Rules Registry Phase 4B/4C, auto-derived since 8B).

    All three auto-derive a sensible default when unset, so the
    rule-mapping suggester's vector store is fully provisioned from just
    the AI enable toggle + serving endpoint (no separate embedding/VS
    fields in the admin UI).
    """

    @pytest.mark.parametrize(
        ("getter_name", "key", "expected"),
        [
            ("get_embedding_endpoint_name", "embedding_endpoint_name", "databricks-gte-large-en"),
            ("get_vs_endpoint_name", "vs_endpoint_name", "dqx_studio_rule_suggester_dqx_test"),
            ("get_vs_index_name", "vs_index_name", "dqx_test.dqx_app_test.dq_rule_embeddings_index"),
        ],
    )
    def test_defaults_to_auto_derived_value(self, settings_service, getter_name, key, expected):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert getattr(svc, getter_name)() == expected

    @pytest.mark.parametrize(
        ("getter_name", "expected"),
        [
            ("get_embedding_endpoint_name", "databricks-gte-large-en"),
            ("get_vs_endpoint_name", "dqx_studio_rule_suggester_dqx_test"),
            ("get_vs_index_name", "dqx_test.dqx_app_test.dq_rule_embeddings_index"),
        ],
    )
    @pytest.mark.parametrize("stored", ["", "   ", "\n\t"])
    def test_empty_or_whitespace_row_falls_back_to_auto_derived_value(
        self, settings_service, getter_name, expected, stored
    ):
        """A row holding an empty/whitespace value must be treated as unset.

        Regression: deployments seeded these keys with empty strings (pre-8B),
        which blocked auto-derive and silently disabled Vector Search
        provisioning + the mapping suggester. Empty must fall back to default.
        """
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [[stored]]

        assert getattr(svc, getter_name)() == expected

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


class TestDefaultAutoUpgrade:
    """``default_auto_upgrade`` (P21-G) — attach-time pin default for new
    rule applications / data-product members. Distinct from
    ``auto_upgrade_without_approval`` (re-approval behaviour, tested
    elsewhere via ``Materializer``)."""

    def test_defaults_to_true_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_default_auto_upgrade() is True

    def test_save_and_round_trip_false(self, settings_service):
        svc, sql_executor_mock = settings_service

        saved = svc.save_default_auto_upgrade(False, user_email="admin@x")

        assert saved is False
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "default_auto_upgrade"}
        assert kwargs["value_cols"]["setting_value"] == "false"

        sql_executor_mock.query.return_value = [["false"]]
        assert svc.get_default_auto_upgrade() is False

    def test_save_and_round_trip_true(self, settings_service):
        svc, sql_executor_mock = settings_service

        svc.save_default_auto_upgrade(True, user_email="admin@x")
        sql_executor_mock.query.return_value = [["true"]]

        assert svc.get_default_auto_upgrade() is True


class TestResolvePinnedVersionForNewAttachment:
    def test_explicit_pin_wins_regardless_of_setting(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["false"]]  # auto-upgrade off

        assert svc.resolve_pinned_version_for_new_attachment(7, 3) == 7

    def test_unspecified_follows_latest_when_auto_upgrade_on(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []  # unset -> defaults True

        assert svc.resolve_pinned_version_for_new_attachment(None, 3) is None

    def test_unspecified_freezes_current_version_when_auto_upgrade_off(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["false"]]

        assert svc.resolve_pinned_version_for_new_attachment(None, 3) == 3


class TestSqlWarehouseSetting:
    """Compute settings (P22-B) — the app-side SQL warehouse override."""

    def test_defaults_to_none_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_sql_warehouse_id() is None

    def test_empty_stored_value_is_treated_as_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["   "]]

        assert svc.get_sql_warehouse_id() is None

    def test_save_and_round_trip(self, settings_service):
        svc, sql_executor_mock = settings_service

        saved = svc.save_sql_warehouse_id("  wh-123  ", user_email="admin@x")

        assert saved == "wh-123"
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "sql_warehouse_id"}
        assert kwargs["value_cols"]["setting_value"] == "wh-123"

        sql_executor_mock.query.return_value = [["wh-123"]]
        assert svc.get_sql_warehouse_id() == "wh-123"


class TestJobsComputeSetting:
    """Compute settings (P22-B) — the task-runner jobs compute selection."""

    def test_defaults_to_serverless_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_jobs_compute() == {"kind": "serverless"}

    def test_malformed_json_defaults_to_serverless(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["not json"]]

        assert svc.get_jobs_compute() == {"kind": "serverless"}

    def test_save_and_round_trip_existing_cluster(self, settings_service):
        svc, sql_executor_mock = settings_service

        saved = svc.save_jobs_compute({"kind": "existing_cluster", "cluster_id": " c-1 "}, user_email="admin@x")

        assert saved == {"kind": "existing_cluster", "cluster_id": "c-1"}
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "jobs_compute_v1"}

        sql_executor_mock.query.return_value = [['{"kind": "existing_cluster", "cluster_id": "c-1"}']]
        assert svc.get_jobs_compute() == {"kind": "existing_cluster", "cluster_id": "c-1"}

    def test_existing_cluster_without_id_collapses_to_serverless(self, settings_service):
        svc, _ = settings_service

        saved = svc.save_jobs_compute({"kind": "existing_cluster"}, user_email="admin@x")

        assert saved == {"kind": "serverless"}

    def test_unknown_kind_collapses_to_serverless(self, settings_service):
        svc, _ = settings_service

        assert svc.save_jobs_compute({"kind": "bogus"}) == {"kind": "serverless"}
