"""Tests for the small Phase 3C building blocks used by the materializer:

* ``registry_models.resolve_criticality`` — severity -> DQX criticality.
* ``RegistryService.get_version`` — fetch an arbitrary historical publish
  snapshot (needed to resolve a ``pinned_version`` older than "latest").
* ``AppSettingsService.get_auto_upgrade_without_approval`` /
  ``save_auto_upgrade_without_approval`` — the admin setting driving
  Behaviour A/B (design spec §5).
"""

from __future__ import annotations

import json

import pytest

from databricks_labs_dqx_app.backend.registry_models import (
    DEFAULT_CRITICALITY,
    resolve_criticality,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


class TestResolveCriticality:
    @pytest.fixture
    def make_settings(self, sql_executor_mock):
        """Build a real AppSettingsService whose stored ``label_definitions`` is *raw*."""

        def _make(raw: str | None) -> AppSettingsService:
            sql_executor_mock.query.return_value = [(raw,)] if raw is not None else []
            return AppSettingsService(sql_executor_mock)

        return _make

    @pytest.mark.parametrize(
        "severity,expected",
        [
            ("Low", "warn"),
            ("Medium", "warn"),
            ("High", "error"),
            ("Critical", "error"),
        ],
    )
    def test_maps_known_severities_via_builtin_defaults_when_unconfigured(self, make_settings, severity, expected):
        # No stored label definitions at all — pre-existing installs keep the
        # historical hardcoded mapping.
        assert resolve_criticality(severity, make_settings(None)) == expected

    @pytest.mark.parametrize(
        "severity,expected",
        [
            ("Low", "warn"),
            ("High", "error"),
        ],
    )
    def test_builtin_defaults_when_stored_definition_has_no_mapping(self, make_settings, severity, expected):
        # A severity definition seeded before value_criticality existed —
        # behaves exactly like the historical hardcoded mapping.
        raw = json.dumps([{"key": "severity", "values": ["Low", "Medium", "High", "Critical"]}])
        assert resolve_criticality(severity, make_settings(raw)) == expected

    def test_reads_admin_edited_mapping(self, make_settings):
        # Admin flipped the default mapping — resolve_criticality must honor it.
        raw = json.dumps(
            [
                {
                    "key": "severity",
                    "values": ["Low", "Critical"],
                    "value_criticality": {"Low": "error", "Critical": "warn"},
                }
            ]
        )
        assert resolve_criticality("Low", make_settings(raw)) == "error"
        assert resolve_criticality("Critical", make_settings(raw)) == "warn"

    def test_falls_back_to_default_for_unmapped_value(self, make_settings):
        assert resolve_criticality("SomeCustomValue", make_settings("[]")) == DEFAULT_CRITICALITY

    def test_none_falls_back_to_default(self, make_settings):
        assert resolve_criticality(None, make_settings("[]")) == DEFAULT_CRITICALITY

    def test_malformed_settings_json_falls_back_to_builtin_defaults(self, make_settings):
        assert resolve_criticality("High", make_settings("{not json")) == "error"
        assert resolve_criticality("Unheard-of", make_settings("{not json")) == DEFAULT_CRITICALITY


class TestRegistryServiceGetVersion:
    @pytest.fixture
    def sql(self, sql_executor_mock):
        sql_executor_mock.dialect = "delta"
        sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
        sql_executor_mock.q.side_effect = lambda i: f"`{i}`"
        sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
        sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
        sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
        sql_executor_mock.query.return_value = []
        return sql_executor_mock

    @pytest.fixture
    def svc(self, sql):
        return RegistryService(sql=sql)

    def test_returns_none_when_missing(self, svc, sql):
        sql.query.return_value = []
        assert svc.get_version("r1", 3) is None

    def test_returns_specific_historical_version(self, svc, sql):
        definition = {
            "body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            "slots": [],
            "parameters": [],
        }
        sql.query.return_value = [
            [
                "r1",
                "2",
                json.dumps(definition),
                None,
                json.dumps({"name": "Not Null Check", "severity": "High"}),
                "alice@x",
                "2026-07-01T00:00:00+00:00",
            ]
        ]
        version = svc.get_version("r1", 2)
        assert version is not None
        assert version.rule_id == "r1"
        assert version.version == 2
        assert version.user_metadata == {"name": "Not Null Check", "severity": "High"}
        query_sql = sql.query.call_args[0][0]
        assert "version = 2" in query_sql
        assert "r1" in query_sql


class TestAutoUpgradeWithoutApprovalSetting:
    @pytest.fixture
    def svc(self, sql_executor_mock):
        return AppSettingsService(sql_executor_mock), sql_executor_mock

    def test_defaults_to_true_when_unset(self, svc):
        s, sql = svc
        sql.query.return_value = []
        assert s.get_auto_upgrade_without_approval() is True

    def test_round_trips_true(self, svc):
        s, sql = svc
        sql.query.return_value = [("true",)]
        assert s.get_auto_upgrade_without_approval() is True

    def test_round_trips_false_explicit(self, svc):
        s, sql = svc
        sql.query.return_value = [("false",)]
        assert s.get_auto_upgrade_without_approval() is False

    def test_save_persists_boolean_string(self, svc):
        s, sql = svc
        s.save_auto_upgrade_without_approval(True, user_email="alice@x")
        sql.upsert.assert_called_once()
        _, kwargs = sql.upsert.call_args
        assert kwargs["value_cols"]["setting_value"] == "true"
