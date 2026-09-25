"""Tests for ``AppSettingsService`` run-review-status seeding semantics.

The key invariant: ``get_run_review_statuses`` is a pure read (no write,
even when unset), and seeding happens only via the explicit
``seed_run_review_statuses_if_absent`` called at startup.
"""

from datetime import datetime, timezone

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
            "Confirmed",
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


def test_record_setup_completion_persists_fixed_sanitized_audit_keys(settings_service) -> None:
    service, sql_executor_mock = settings_service

    service.record_setup_completion(
        job_id=42,
        completed_at=datetime(2026, 9, 1, 14, 30, tzinfo=timezone.utc),
        user_name="admin\nforged@example.com",
    )

    writes = [call.kwargs for call in sql_executor_mock.upsert.call_args_list]
    assert [write["key_cols"]["setting_key"] for write in writes] == [
        "setup_task_runner_job_id",
        "setup_completed_at",
        "setup_completed_by",
    ]
    assert [write["value_cols"]["setting_value"] for write in writes] == [
        "42",
        "2026-09-01T14:30:00+00:00",
        "admin forged@example.com",
    ]
    assert all(write["value_cols"]["updated_by"] == "admin forged@example.com" for write in writes)


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

    def test_ai_endpoint_name_defaults_to_claude_sonnet(self, settings_service):
        # The fresh-deploy default moved from ``databricks-gpt-5-4-nano`` to
        # ``databricks-claude-sonnet-4-5`` after a live golden-set eval showed
        # nano returning zero suggestions on wide tables (blew max_tokens
        # mid-JSON) and taking three of seven planted wrong-column bindings
        # structural post-processing cannot catch. See the constant's docstring
        # for the measured tradeoff.
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_ai_endpoint_name() == "databricks-claude-sonnet-4-5"
        assert svc.AI_ENDPOINT_NAME_DEFAULT == "databricks-claude-sonnet-4-5"

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
    """Global Results tab gating — ON by default; explicit false still disables via API."""

    def test_defaults_to_true_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_global_results_enabled() is True

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


class TestRulesResultsTabEnabled:
    """Per-rule Results tab gating — ON by default; explicit false still disables via API."""

    def test_defaults_to_true_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_rules_results_tab_enabled() is True

    def test_explicit_true_reads_on(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["true"]]

        assert svc.get_rules_results_tab_enabled() is True

    def test_non_true_value_reads_off(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["false"]]

        assert svc.get_rules_results_tab_enabled() is False

    def test_save_and_read_round_trips(self, settings_service):
        svc, sql_executor_mock = settings_service

        svc.save_rules_results_tab_enabled(True, user_email="admin@x")

        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "rules_results_tab_enabled"}
        assert kwargs["value_cols"]["setting_value"] == "true"

        sql_executor_mock.query.return_value = [["true"]]
        assert svc.get_rules_results_tab_enabled() is True


class TestEmbeddingEndpointSettings:
    """Embedding endpoint setting (Rules Registry Phase 4B/4C, auto-derived since 8B).

    Auto-derives a sensible default when unset so cosine rule suggestions
    work from the AI enable toggle + serving endpoint alone.
    """

    def test_defaults_to_auto_derived_value(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []

        assert svc.get_embedding_endpoint_name() == "databricks-gte-large-en"

    @pytest.mark.parametrize("stored", ["", "   ", "\n\t"])
    def test_empty_or_whitespace_row_falls_back_to_auto_derived_value(self, settings_service, stored):
        """A row holding an empty/whitespace value must be treated as unset.

        Regression: deployments seeded this key with empty strings (pre-8B),
        which blocked auto-derive and silently disabled the mapping
        suggester. Empty must fall back to default.
        """
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [[stored]]

        assert svc.get_embedding_endpoint_name() == "databricks-gte-large-en"

    def test_save_trims_whitespace_and_round_trips(self, settings_service):
        svc, sql_executor_mock = settings_service

        saved = svc.save_embedding_endpoint_name("  my-value  ", user_email="admin@x")

        assert saved == "my-value"
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "embedding_endpoint_name"}
        assert kwargs["value_cols"]["setting_value"] == "my-value"

        sql_executor_mock.query.return_value = [["my-value"]]
        assert svc.get_embedding_endpoint_name() == "my-value"


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


class TestTagAutoApply:
    """``tag_auto_apply`` (apply-on-tag) — eager auto-attach of tag-mapped rules; OFF by default."""

    def test_defaults_to_false_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []
        assert svc.get_tag_auto_apply() is False

    def test_explicit_true_reads_on(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["true"]]
        assert svc.get_tag_auto_apply() is True

    def test_non_true_value_reads_off(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["false"]]
        assert svc.get_tag_auto_apply() is False

    def test_save_and_round_trips(self, settings_service):
        svc, sql_executor_mock = settings_service
        saved = svc.save_tag_auto_apply(True, user_email="admin@x")
        assert saved is True
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "tag_auto_apply"}
        assert kwargs["value_cols"]["setting_value"] == "true"
        sql_executor_mock.query.return_value = [["true"]]
        assert svc.get_tag_auto_apply() is True


class TestDefaultPassThreshold:
    """``default_pass_threshold`` — org-wide minimum pass-rate default; 70 when unset."""

    def test_default_pass_threshold_defaults_to_70(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []
        assert svc.get_default_pass_threshold() == 70

    def test_save_and_get_default_pass_threshold(self, settings_service):
        svc, sql_executor_mock = settings_service
        svc.save_default_pass_threshold(85, user_email="a@x")
        sql_executor_mock.query.return_value = [["85"]]
        assert svc.get_default_pass_threshold() == 85

    def test_save_persists_under_expected_key(self, settings_service):
        svc, sql_executor_mock = settings_service
        svc.save_default_pass_threshold(60, user_email="admin@x")
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "default_pass_threshold"}
        assert kwargs["value_cols"]["setting_value"] == "60"

    def test_value_clamped_to_100_on_get(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["150"]]
        assert svc.get_default_pass_threshold() == 100

    def test_value_clamped_to_0_on_get(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["-10"]]
        assert svc.get_default_pass_threshold() == 0

    def test_garbage_value_returns_default(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = [["not-a-number"]]
        assert svc.get_default_pass_threshold() == 70


class TestPassThresholdEnabled:
    """``pass_threshold_enabled`` — master switch for the pass-threshold feature; default ON."""

    def test_defaults_to_true_when_unset(self, settings_service):
        svc, sql_executor_mock = settings_service
        sql_executor_mock.query.return_value = []
        assert svc.get_pass_threshold_enabled() is True

    def test_save_false_then_get_returns_false(self, settings_service):
        svc, sql_executor_mock = settings_service
        svc.save_pass_threshold_enabled(False, user_email="admin@x")
        sql_executor_mock.query.return_value = [["false"]]
        assert svc.get_pass_threshold_enabled() is False

    def test_save_true_then_get_returns_true(self, settings_service):
        svc, sql_executor_mock = settings_service
        svc.save_pass_threshold_enabled(True, user_email="admin@x")
        sql_executor_mock.query.return_value = [["true"]]
        assert svc.get_pass_threshold_enabled() is True

    def test_save_persists_under_expected_key(self, settings_service):
        svc, sql_executor_mock = settings_service
        svc.save_pass_threshold_enabled(False, user_email="admin@x")
        _, kwargs = sql_executor_mock.upsert.call_args
        assert kwargs["key_cols"] == {"setting_key": "pass_threshold_enabled"}
        assert kwargs["value_cols"]["setting_value"] == "false"
