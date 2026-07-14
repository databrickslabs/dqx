"""Tests for the label-definitions admin surface (Rules Registry Phase 1).

Dimensions & severity are reserved TAGS in the existing ``label_definitions``
catalog, not new tables. Three layers exercised:

* ``LabelDefinition`` model — per-value ``color`` (hex) + ``is_builtin`` flag.
* ``AppSettingsService.seed_reserved_label_definitions_if_absent`` — idempotent
  startup seeding of the reserved ``dimension`` / ``severity`` keys.
* ``routes.v1.config.save_label_definitions`` — guards reserved keys against
  deletion/rename while allowing value edits.
"""

from __future__ import annotations

import json

import pytest
from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Task 1.1 — LabelDefinition model
# ---------------------------------------------------------------------------


class TestLabelDefinitionModel:
    @pytest.fixture
    def LabelDefinition(self):
        from databricks_labs_dqx_app.backend.routes.v1.config import LabelDefinition

        return LabelDefinition

    def test_backward_compatible_defaults(self, LabelDefinition):
        d = LabelDefinition(key="team", values=["a", "b"])
        assert d.value_colors is None
        assert d.is_builtin is False

    def test_accepts_valid_hex_value_colors(self, LabelDefinition):
        d = LabelDefinition(
            key="dimension",
            values=["Validity"],
            value_colors={"Validity": "#2563EB"},
            is_builtin=True,
        )
        assert d.value_colors == {"Validity": "#2563EB"}
        assert d.is_builtin is True

    def test_rejects_invalid_hex_value_colors(self, LabelDefinition):
        with pytest.raises(Exception):
            LabelDefinition(key="dimension", values=["Validity"], value_colors={"Validity": "blue"})

    def test_rejects_short_hex_value_colors(self, LabelDefinition):
        with pytest.raises(Exception):
            LabelDefinition(key="dimension", values=["Validity"], value_colors={"Validity": "#FFF"})

    def test_value_criticality_defaults_to_none(self, LabelDefinition):
        d = LabelDefinition(key="severity", values=["Low"])
        assert d.value_criticality is None

    def test_accepts_value_criticality(self, LabelDefinition):
        definition = LabelDefinition(
            key="severity",
            values=["Low", "Critical"],
            value_criticality={"Low": "warn", "Critical": "error"},
            is_builtin=True,
        )
        assert definition.value_criticality == {"Low": "warn", "Critical": "error"}

    def test_rejects_invalid_criticality_value(self, LabelDefinition):
        with pytest.raises(ValueError):
            LabelDefinition(
                key="severity",
                values=["Low"],
                value_criticality={"Low": "not-a-real-criticality"},
            )


# ---------------------------------------------------------------------------
# Task 1.2 — seeding reserved dimension + severity keys
# ---------------------------------------------------------------------------


class TestSeedReservedLabelDefinitions:
    @pytest.fixture
    def svc(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

        return AppSettingsService(sql_executor_mock), sql_executor_mock

    def test_seeds_when_absent(self, svc):
        s, sql = svc
        sql.query.return_value = []

        result = s.seed_reserved_label_definitions_if_absent()

        assert result is True
        assert sql.upsert.called
        kwargs = sql.upsert.call_args.kwargs
        payload = (
            kwargs["value_cols"]["setting_value"]
            if "value_cols" in kwargs
            else sql.upsert.call_args.args[2]["setting_value"]
        )
        saved = json.loads(payload)
        keys = {d["key"]: d for d in saved}
        assert "dimension" in keys
        assert "severity" in keys
        assert keys["dimension"]["is_builtin"] is True
        assert keys["severity"]["is_builtin"] is True
        assert set(keys["dimension"]["values"]) == {
            "Validity",
            "Completeness",
            "Accuracy",
            "Consistency",
            "Uniqueness",
            "Timeliness",
        }
        # Fixed, admin-curated catalog — never author-extensible.
        assert keys["dimension"]["allow_custom_values"] is False
        assert set(keys["severity"]["values"]) == {"Low", "Medium", "High", "Critical"}
        # "Low" severity is grey (gray-500), not green — green wrongly read as a
        # positive/healthy signal for what is still a flagged severity.
        assert keys["severity"]["value_colors"]["Low"] == "#6B7280"
        assert keys["severity"]["value_criticality"] == {
            "Low": "warn",
            "Medium": "warn",
            "High": "error",
            "Critical": "error",
        }
        assert (
            keys["dimension"]["value_descriptions"]["Validity"] == "Whether values match the expected format or rules."
        )

    def test_noop_when_both_present(self, svc):
        s, sql = svc
        existing = [
            {"key": "dimension", "values": ["Validity"], "is_builtin": True},
            {"key": "severity", "values": ["Low"], "is_builtin": True},
        ]
        sql.query.return_value = [(json.dumps(existing),)]

        result = s.seed_reserved_label_definitions_if_absent()

        assert result is False
        assert not sql.upsert.called

    def test_preserves_admin_edited_entry_and_only_adds_missing(self, svc):
        s, sql = svc
        # Admin already customized "dimension" (different values, not builtin
        # marked) but never saved "severity" — seeding must add severity only
        # and must not touch the existing dimension entry.
        existing = [{"key": "dimension", "values": ["Custom1"], "is_builtin": False, "allow_custom_values": False}]
        sql.query.return_value = [(json.dumps(existing),)]

        result = s.seed_reserved_label_definitions_if_absent()

        assert result is True
        kwargs = sql.upsert.call_args.kwargs
        payload = (
            kwargs["value_cols"]["setting_value"]
            if "value_cols" in kwargs
            else sql.upsert.call_args.args[2]["setting_value"]
        )
        saved = json.loads(payload)
        keys = {d["key"]: d for d in saved}
        assert keys["dimension"]["values"] == ["Custom1"]
        assert keys["dimension"]["is_builtin"] is False
        assert "severity" in keys
        assert keys["severity"]["is_builtin"] is True

    def test_idempotent_across_two_calls(self, svc):
        s, sql = svc
        sql.query.return_value = []
        s.seed_reserved_label_definitions_if_absent()
        first_payload_kwargs = sql.upsert.call_args.kwargs
        first_payload = (
            first_payload_kwargs["value_cols"]["setting_value"]
            if "value_cols" in first_payload_kwargs
            else sql.upsert.call_args.args[2]["setting_value"]
        )
        # Simulate the persisted state for the second call.
        sql.query.return_value = [(first_payload,)]
        sql.upsert.reset_mock()

        result = s.seed_reserved_label_definitions_if_absent()

        assert result is False
        assert not sql.upsert.called


# ---------------------------------------------------------------------------
# Task 1.3 — guard reserved keys in save endpoint
# ---------------------------------------------------------------------------


class TestSaveLabelDefinitionsReservedGuard:
    @pytest.fixture
    def route_ctx(self, sql_executor_mock):
        from databricks_labs_dqx_app.backend.routes.v1.config import (
            LabelDefinition,
            LabelDefinitionsIn,
            save_label_definitions,
        )
        from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

        svc = AppSettingsService(sql_executor_mock)
        existing = [
            LabelDefinition(
                key="dimension",
                values=["Validity", "Completeness"],
                allow_custom_values=True,
                is_builtin=True,
                value_colors={"Validity": "#2563EB"},
            ).model_dump(),
            LabelDefinition(key="severity", values=["Low", "High"], is_builtin=True).model_dump(),
            LabelDefinition(key="team", values=["data-eng"], is_builtin=False).model_dump(),
        ]
        sql_executor_mock.query.return_value = [(json.dumps(existing),)]
        return save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql_executor_mock

    def test_deleting_reserved_key_rejected(self, route_ctx):
        save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql = route_ctx
        body = LabelDefinitionsIn(
            definitions=[
                LabelDefinition(key="severity", values=["Low", "High"], is_builtin=True),
                LabelDefinition(key="team", values=["data-eng"]),
            ]
        )
        with pytest.raises(HTTPException) as exc:
            save_label_definitions(body, svc, "admin@example.com")
        assert exc.value.status_code == 400
        assert "dimension" in exc.value.detail

    def test_renaming_reserved_key_rejected(self, route_ctx):
        save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql = route_ctx
        body = LabelDefinitionsIn(
            definitions=[
                LabelDefinition(key="dimension_renamed", values=["Validity"], is_builtin=True),
                LabelDefinition(key="severity", values=["Low", "High"], is_builtin=True),
            ]
        )
        with pytest.raises(HTTPException) as exc:
            save_label_definitions(body, svc, "admin@example.com")
        assert exc.value.status_code == 400
        assert "dimension" in exc.value.detail

    def test_editing_reserved_key_values_and_colors_allowed(self, route_ctx):
        save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql = route_ctx
        body = LabelDefinitionsIn(
            definitions=[
                LabelDefinition(
                    key="dimension",
                    values=["Validity", "Completeness", "Accuracy"],
                    allow_custom_values=True,
                    is_builtin=True,
                    value_colors={"Validity": "#000000", "Accuracy": "#FFFFFF"},
                    value_descriptions={"Validity": "Custom validity blurb"},
                ),
                LabelDefinition(key="severity", values=["Low", "High"], is_builtin=True),
            ]
        )
        result = save_label_definitions(body, svc, "admin@example.com")
        dims = next(d for d in result.definitions if d.key == "dimension")
        assert dims.values == ["Validity", "Completeness", "Accuracy"]
        assert dims.value_colors == {"Validity": "#000000", "Accuracy": "#FFFFFF"}
        assert dims.value_descriptions == {"Validity": "Custom validity blurb"}
        assert dims.is_builtin is True

    def test_dimension_and_severity_always_save_with_custom_values_disallowed(self, route_ctx):
        # Even if a (stale/tampered) client sends allow_custom_values=True for
        # a reserved key, the server forces it back to False on save.
        save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql = route_ctx
        body = LabelDefinitionsIn(
            definitions=[
                LabelDefinition(key="dimension", values=["Validity"], allow_custom_values=True, is_builtin=True),
                LabelDefinition(key="severity", values=["Low"], allow_custom_values=True, is_builtin=True),
                LabelDefinition(key="team", values=["data-eng"], allow_custom_values=True),
            ]
        )
        result = save_label_definitions(body, svc, "admin@example.com")
        by_key = {d.key: d for d in result.definitions}
        assert by_key["dimension"].allow_custom_values is False
        assert by_key["severity"].allow_custom_values is False
        # Non-reserved keys keep their existing (author-controlled) behavior.
        assert by_key["team"].allow_custom_values is True

    def test_client_cannot_unset_is_builtin_on_reserved_key(self, route_ctx):
        save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql = route_ctx
        body = LabelDefinitionsIn(
            definitions=[
                LabelDefinition(key="dimension", values=["Validity"], is_builtin=False),
                LabelDefinition(key="severity", values=["Low"], is_builtin=True),
            ]
        )
        result = save_label_definitions(body, svc, "admin@example.com")
        dims = next(d for d in result.definitions if d.key == "dimension")
        assert dims.is_builtin is True

    def test_adding_new_custom_key_alongside_reserved_keys_allowed(self, route_ctx):
        save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql = route_ctx
        body = LabelDefinitionsIn(
            definitions=[
                LabelDefinition(key="dimension", values=["Validity"], is_builtin=True),
                LabelDefinition(key="severity", values=["Low"], is_builtin=True),
                LabelDefinition(key="new_custom", values=["x"]),
            ]
        )
        result = save_label_definitions(body, svc, "admin@example.com")
        keys = {d.key for d in result.definitions}
        assert keys == {"dimension", "severity", "new_custom"}

    def test_non_reserved_key_can_be_deleted(self, route_ctx):
        save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql = route_ctx
        body = LabelDefinitionsIn(
            definitions=[
                LabelDefinition(key="dimension", values=["Validity"], is_builtin=True),
                LabelDefinition(key="severity", values=["Low"], is_builtin=True),
            ]
        )
        result = save_label_definitions(body, svc, "admin@example.com")
        keys = {d.key for d in result.definitions}
        assert keys == {"dimension", "severity"}

    def test_save_prunes_value_criticality_to_present_values(self, route_ctx):
        # Entries for values no longer in the list are dropped on save,
        # exactly like value_colors/value_descriptions pruning.
        save_label_definitions, LabelDefinition, LabelDefinitionsIn, svc, sql = route_ctx
        body = LabelDefinitionsIn(
            definitions=[
                LabelDefinition(key="dimension", values=["Validity"], is_builtin=True),
                LabelDefinition(
                    key="severity",
                    values=["Low", "High"],
                    is_builtin=True,
                    value_criticality={"Low": "warn", "High": "error", "Removed": "error"},
                ),
            ]
        )
        result = save_label_definitions(body, svc, "admin@example.com")
        sev = next(d for d in result.definitions if d.key == "severity")
        assert sev.value_criticality == {"Low": "warn", "High": "error"}
