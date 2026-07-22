"""Tests for the Phase 3C materializer — the SAFETY-CRITICAL boundary
between the Rules Registry and the unchanged runner.

Two layers are exercised:

* ``render_check`` (pure function) — the exact ``dq_quality_rules.check``
  shape produced for one applied-rule mapping group. The **critical test**
  (``TestRenderCheckMatchesHandAuthoredShape``) asserts this is
  function/arguments/criticality-identical to what
  ``RulesCatalogService.save`` would store for the equivalent hand-authored
  check, so the runner (which only ever reads ``check.function`` /
  ``check.arguments`` / top-level ``criticality``) treats them identically.
* ``Materializer.materialize_binding`` — orchestration: idempotent upsert,
  pin vs follow-latest resolution, severity-override -> criticality,
  auto-upgrade Behaviour A/B, and cleanup of stale/orphaned rows.
"""

from __future__ import annotations

import json
from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.errors import UnsafeSqlQueryError

from databricks_labs_dqx_app.backend.registry_models import (
    AppliedRule,
    MonitoredTable,
    RegistryRule,
    RuleDefinition,
    RuleVersion,
)
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.materializer import MaterializationError, Materializer, render_check
from databricks_labs_dqx_app.backend.services.monitored_table_service import (
    AppliedRuleSummary,
    MonitoredTableDetail,
    MonitoredTableService,
)
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService


# ---------------------------------------------------------------------------
# render_check — pure function, shape parity
# ---------------------------------------------------------------------------


def _app_settings_stub() -> AppSettingsService:
    """AppSettingsService double for direct ``render_check`` calls.

    No stored label definitions, so severity -> criticality resolution uses
    the built-in defaults (``registry_models.SEVERITY_TO_CRITICALITY``).
    Pass-threshold feature is enabled with the default admin threshold (70)
    so existing tests that don't care about thresholds still get a concrete
    value rather than a MagicMock sentinel.
    """
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_label_definitions.return_value = []
    mock.get_pass_threshold_enabled.return_value = True
    mock.get_default_pass_threshold.return_value = 70
    return mock


def _is_not_null_definition(slot_name: str = "column") -> RuleDefinition:
    return RuleDefinition.model_validate(
        {
            "body": {"function": "is_not_null", "arguments": {slot_name: f"{{{{{slot_name}}}}}"}},
            "slots": [{"name": slot_name, "family": "any", "position": 0, "cardinality": "one"}],
            "parameters": [],
        }
    )


class TestRenderCheckMatchesHandAuthoredShape:
    """The CRITICAL TEST: materialized shape must equal a hand-authored check's shape."""

    def test_dqx_native_is_not_null_matches_hand_authored_check(self, sql_executor_mock):
        # -- side A: materialize an applied is_not_null rule on customer_id --
        version = RuleVersion(
            rule_id="r1",
            version=1,
            definition=_is_not_null_definition(),
            polarity=None,
            user_metadata={"name": "Not Null Check", "dimension": "Completeness", "severity": "High"},
        )
        materialized_check, is_tableless = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert is_tableless is False

        # -- side B: what a human would hand-author + RulesCatalogService.save() --
        sql_executor_mock.dialect = "delta"
        sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
        sql_executor_mock.q.side_effect = lambda i: f"`{i}`"
        sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
        sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
        sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
        sql_executor_mock.query.return_value = []
        catalog = RulesCatalogService(sql=sql_executor_mock)
        hand_authored_check = {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "customer_id"}},
        }
        catalog.save("cat.schema.customers", [hand_authored_check], "alice@x")
        inserted_sql = sql_executor_mock.execute.call_args_list[0].args[0]
        # Pull the JSON literal that RulesCatalogService actually persisted
        # for the hand-authored check out of the INSERT statement.
        start = inserted_sql.index("parse_json('") + len("parse_json('")
        end = inserted_sql.index("')", start)
        stored_hand_authored = json.loads(inserted_sql[start:end])

        # The runner only ever reads these three things off a
        # dq_quality_rules row: function, arguments, and top-level
        # criticality. They must match exactly between the registry-
        # materialized row and the equivalent hand-authored one.
        assert materialized_check["check"]["function"] == stored_hand_authored["check"]["function"] == "is_not_null"
        assert (
            materialized_check["check"]["arguments"]
            == stored_hand_authored["check"]["arguments"]
            == {"column": "customer_id"}
        )
        assert materialized_check["criticality"] == stored_hand_authored["criticality"] == "error"

        # The materialized row additionally carries registry provenance +
        # reserved tags in user_metadata (§9) — the hand-authored one does
        # not, and that's fine; the runner aggregates whatever is there.
        assert materialized_check["user_metadata"]["dimension"] == "Completeness"
        assert materialized_check["user_metadata"]["severity"] == "High"
        assert materialized_check["user_metadata"]["registry_rule_id"] == "r1"
        assert materialized_check["user_metadata"]["registry_version"] == "1"
        assert materialized_check["user_metadata"]["applied_rule_id"] == "ar1"
        assert materialized_check["name"] == "Not Null Check"

    def test_missing_slot_mapping_raises(self):
        version = RuleVersion(rule_id="r1", version=1, definition=_is_not_null_definition(), user_metadata={})
        with pytest.raises(ValueError, match="column"):
            render_check(
                mode="dqx_native",
                version=version,
                group={},
                effective_severity="Medium",
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
                app_settings=_app_settings_stub(),
            )

    def test_dqx_native_arbitrary_slot_name_keys_argument_by_param_name(self):
        """Author-renamed slot ('user_email' != function param 'column') must still key

        the rendered argument by the DQX function's real parameter name — only the
        placeholder VALUE is substituted with the mapped column.
        """
        definition = RuleDefinition.model_validate(
            {
                "body": {"function": "is_not_null", "arguments": {"column": "{{user_email}}"}},
                "slots": [{"name": "user_email", "family": "text", "position": 0, "cardinality": "one"}],
                "parameters": [],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"user_email": "email_col"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"] == {"column": "email_col"}

    def test_dqx_native_arbitrary_slot_name_many_cardinality_becomes_list(self):
        definition = RuleDefinition.model_validate(
            {
                "body": {"function": "is_unique", "arguments": {"columns": "{{cols}}"}},
                "slots": [{"name": "cols", "family": "any", "position": 0, "cardinality": "many"}],
                "parameters": [],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"cols": "a, b, c"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"] == {"columns": ["a", "b", "c"]}

    def test_dqx_native_multi_slot_list_argument_substitutes_each_placeholder(self):
        """New multi-column authoring style (fixes #6): instead of ONE
        ``cardinality: "many"`` slot bound to a comma-joined value, the
        author declares MULTIPLE named ``cardinality: "one"`` slots that
        share the same ``arg_key`` — the frozen argument VALUE is then a
        Python LIST of ``{{slot}}`` placeholders (one per slot), rather
        than a single placeholder string. ``_substitute_arguments``/
        ``_substitute_value`` already recurse into list values generically,
        so each element is substituted independently by its own slot's
        mapped column — no separate list-handling branch was needed.
        """
        definition = RuleDefinition.model_validate(
            {
                "body": {
                    "function": "foreign_key",
                    "arguments": {"columns": ["{{column_1}}", "{{column_2}}"]},
                },
                "slots": [
                    {"name": "column_1", "family": "any", "position": 0, "cardinality": "one", "arg_key": "columns"},
                    {"name": "column_2", "family": "any", "position": 1, "cardinality": "one", "arg_key": "columns"},
                ],
                "parameters": [
                    {"name": "ref_columns", "type": "ref_column", "value": ["id", "region"]},
                    {"name": "ref_table", "type": "ref_table", "value": "catalog.schema.customers"},
                ],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, is_tableless = render_check(
            mode="dqx_native",
            version=version,
            group={"column_1": "colA", "column_2": "colB"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert is_tableless is False
        assert check["check"]["arguments"]["columns"] == ["colA", "colB"]
        assert check["check"]["arguments"]["ref_columns"] == ["id", "region"]

    def test_dqx_native_single_slot_list_argument_stays_a_list(self):
        """A list-typed argument with only ONE declared slot (the initial,
        not-yet-expanded state of the new multi-slot group) still renders
        as a one-element LIST, not a bare scalar — ``foreign_key.columns``
        always expects a list regardless of how many columns the author
        has added so far.
        """
        definition = RuleDefinition.model_validate(
            {
                "body": {"function": "foreign_key", "arguments": {"columns": ["{{column_1}}"]}},
                "slots": [
                    {"name": "column_1", "family": "any", "position": 0, "cardinality": "one", "arg_key": "columns"},
                ],
                "parameters": [],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column_1": "colA"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"]["columns"] == ["colA"]

    def test_missing_slot_mapping_raises_arbitrary_slot_name(self):
        definition = RuleDefinition.model_validate(
            {
                "body": {"function": "is_not_null", "arguments": {"column": "{{user_email}}"}},
                "slots": [{"name": "user_email", "family": "text", "position": 0, "cardinality": "one"}],
                "parameters": [],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        with pytest.raises(ValueError, match="user_email"):
            render_check(
                mode="dqx_native",
                version=version,
                group={},
                effective_severity="Medium",
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
                app_settings=_app_settings_stub(),
            )

    def test_severity_maps_to_criticality(self):
        version = RuleVersion(rule_id="r1", version=1, definition=_is_not_null_definition(), user_metadata={})
        for severity, expected in (("Low", "warn"), ("Medium", "warn"), ("High", "error"), ("Critical", "error")):
            check, _ = render_check(
                mode="dqx_native",
                version=version,
                group={"column": "id"},
                effective_severity=severity,
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
                app_settings=_app_settings_stub(),
            )
            assert check["criticality"] == expected


class TestRenderCheckMessageExpr:
    """Phase 7C-a: optional custom failure message (mirrors ``DQRule.message_expr``)."""

    def test_message_expr_present_when_error_message_set(self):
        definition = _is_not_null_definition()
        definition.error_message = "'Column ' || {{column}} || ' failed'"
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["message_expr"] == "'Column ' || {{column}} || ' failed'"

    def test_message_expr_absent_when_error_message_none(self):
        version = RuleVersion(rule_id="r1", version=1, definition=_is_not_null_definition(), user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert "message_expr" not in check

    def test_message_expr_absent_when_error_message_empty_string(self):
        definition = _is_not_null_definition()
        definition.error_message = ""
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert "message_expr" not in check


class TestRenderCheckDefinitionFilter:
    """Task 5: definition.filter is slot-substituted and set as check_dict['filter'].
    The per-binding applied.row_filter is NO LONGER read."""

    def test_definition_filter_slot_substituted(self):
        """{{col_a}} in definition.filter with mapping {col_a: amount} -> 'amount > 0'."""
        definition = RuleDefinition.model_validate({
            "body": {"function": "is_not_null", "arguments": {"column": "{{col_a}}"}},
            "slots": [{"name": "col_a", "family": "any", "position": 0, "cardinality": "one"}],
            "filter": "{{col_a}} > 0",
        })
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"col_a": "amount"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["filter"] == "amount > 0"

    def test_definition_filter_absent_when_none(self):
        """No filter on definition -> no 'filter' key in check_dict."""
        definition = _is_not_null_definition()
        assert definition.filter is None
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert "filter" not in check

    def test_definition_filter_absent_when_empty_string(self):
        """Empty string filter -> no 'filter' key in check_dict."""
        definition = _is_not_null_definition()
        definition.filter = ""
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert "filter" not in check

    def test_row_filter_param_not_read(self):
        """render_check no longer reads the row_filter param — passing it
        must not produce a 'filter' key when definition.filter is None."""
        definition = _is_not_null_definition()
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
            row_filter="old_filter_value IS NOT NULL",
        )
        assert "filter" not in check

    def test_definition_filter_plain_string_no_slots(self):
        """A filter with no slot placeholders is passed through unchanged."""
        definition = RuleDefinition.model_validate({
            "body": {"function": "is_not_null", "arguments": {"column": "{{column}}"}},
            "slots": [{"name": "column", "family": "any", "position": 0}],
            "filter": "region = 'US'",
        })
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["filter"] == "region = 'US'"

    def test_definition_filter_revalidated_after_slot_substitution(self):
        """SECURITY: an injected column_mapping value that turns a safe filter
        template into prohibited SQL after {{slot}} substitution must raise
        UnsafeSqlQueryError.

        The stored filter ``{{col_a}} > 0`` is safe (validated at rule
        create/update time), but the applied rule's column_mapping value is
        free-form and never sanitized. A malicious APPLY-holder can map the
        slot to a value carrying a forbidden statement, so the SUBSTITUTED
        filter must be re-validated with ``is_sql_query_safe`` — exactly as the
        predicate/sql_query paths already do — before it reaches DQRule.filter.
        """
        definition = RuleDefinition.model_validate({
            "body": {"function": "is_not_null", "arguments": {"column": "{{col_a}}"}},
            "slots": [{"name": "col_a", "family": "any", "position": 0, "cardinality": "one"}],
            "filter": "{{col_a}} > 0",
        })
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        with pytest.raises(UnsafeSqlQueryError):
            render_check(
                mode="dqx_native",
                version=version,
                # Injected value: after substitution the filter becomes
                # "amount); DROP TABLE dq_quality_rules; -- > 0" (contains DROP).
                group={"col_a": "amount); DROP TABLE dq_quality_rules; --"},
                effective_severity="Medium",
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
                app_settings=_app_settings_stub(),
            )


class TestRenderCheckNativeNegate:
    """Item 11 — a native check's PASS/FAIL polarity injects ``negate``."""

    def _regex_definition(self) -> RuleDefinition:
        return RuleDefinition.model_validate(
            {
                "body": {"function": "regex_match", "arguments": {"column": "{{column}}"}},
                "slots": [{"name": "column", "family": "text", "position": 0, "cardinality": "one"}],
                "parameters": [{"name": "regex", "type": "string", "value": "^[0-9]+$"}],
            }
        )

    def test_pass_polarity_injects_negate_false(self):
        version = RuleVersion(
            rule_id="r1", version=1, definition=self._regex_definition(), polarity="pass", user_metadata={}
        )
        check, is_tableless = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "code"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert is_tableless is False
        assert check["check"]["function"] == "regex_match"
        assert check["check"]["arguments"]["negate"] is False
        assert check["check"]["arguments"]["regex"] == "^[0-9]+$"

    def test_fail_polarity_injects_negate_true(self):
        version = RuleVersion(
            rule_id="r1", version=1, definition=self._regex_definition(), polarity="fail", user_metadata={}
        )
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "code"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"]["negate"] is True

    def test_no_polarity_leaves_negate_absent(self):
        """A native check WITHOUT negate support (polarity None) never gets a
        spurious ``negate`` key — the runner would reject it as an unknown arg."""
        version = RuleVersion(
            rule_id="r1", version=1, definition=_is_not_null_definition(), polarity=None, user_metadata={}
        )
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert "negate" not in check["check"]["arguments"]


class TestRenderCheckSqlMode:
    def _sql_definition(self, body: dict, slot_name: str = "column") -> RuleDefinition:
        return RuleDefinition.model_validate(
            {
                "body": body,
                "slots": [{"name": slot_name, "family": "any", "position": 0, "cardinality": "one"}],
                "parameters": [],
            }
        )

    def test_pass_polarity_does_not_negate(self):
        definition = self._sql_definition({"predicate": "{{column}} IS NOT NULL"})
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, is_tableless = render_check(
            mode="sql",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert is_tableless is False
        assert check["check"]["function"] == "sql_expression"
        assert check["check"]["arguments"]["expression"] == "customer_id IS NOT NULL"
        assert check["check"]["arguments"]["negate"] is False
        # The mapped column is surfaced for DQX reporting + results by-column
        # attribution (sql_expression accepts a ``columns`` arg).
        assert check["check"]["arguments"]["columns"] == ["customer_id"]

    def test_fail_polarity_negates(self):
        definition = self._sql_definition({"predicate": "{{column}} IS NULL"})
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="fail", user_metadata={})
        check, _ = render_check(
            mode="sql",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"]["negate"] is True

    def test_dataset_sql_query_without_slots_is_tableless(self):
        definition = RuleDefinition.model_validate(
            {"body": {"sql_query": "SELECT COUNT(*) > 100 AS condition FROM t"}, "slots": [], "parameters": []}
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="fail", user_metadata={})
        check, is_tableless = render_check(
            mode="sql",
            version=version,
            group={},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert is_tableless is True
        assert check["check"]["function"] == "sql_query"
        # A tableless (no-slot) query has no mapped columns, so no ``columns``
        # key is added — the check dict stays unchanged for this case.
        assert "columns" not in check["check"]["arguments"]

    def test_sql_expression_surfaces_deduped_mapped_columns_in_slot_order(self):
        # A multi-slot SQL predicate reports every referenced column once, in
        # slot-declaration order, so the results by-column breakdown attributes
        # the check to each involved column (was empty before: SQL checks
        # carried no ``arguments.columns``).
        definition = RuleDefinition.model_validate(
            {
                "body": {"predicate": "{{start}} <= {{end}} AND {{start}} IS NOT NULL"},
                "slots": [
                    {"name": "start", "family": "any", "position": 0, "cardinality": "one"},
                    {"name": "end", "family": "any", "position": 1, "cardinality": "one"},
                ],
                "parameters": [],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, _ = render_check(
            mode="sql",
            version=version,
            group={"start": "shipped_at", "end": "delivered_at"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"]["columns"] == ["shipped_at", "delivered_at"]

    def test_unsafe_sql_raises(self):
        definition = self._sql_definition({"predicate": "1=1; DROP TABLE {{column}}"})
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        with pytest.raises(UnsafeSqlQueryError):
            render_check(
                mode="sql",
                version=version,
                group={"column": "t"},
                effective_severity="Medium",
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
                app_settings=_app_settings_stub(),
            )


class TestRenderCheckLowcodeMode:
    """Low-code rules flow through the SAME sql-mode materialization path.

    The Low-Code builder compiles its structured AST into either a simple
    ``predicate`` (no joins/group-by) or a full ``sql_query`` + ``merge_columns``
    (joins/group-by), stored in ``body`` alongside the re-editable
    ``lowcode_ast``. ``render_check`` treats mode ``lowcode`` exactly like
    ``sql`` — these tests pin the AST -> SQL -> materialized-check byte shape.
    """

    def _lowcode_definition(self, body: dict, slot_name: str = "column") -> RuleDefinition:
        return RuleDefinition.model_validate(
            {
                "body": body,
                "slots": [{"name": slot_name, "family": "any", "position": 0, "cardinality": "one"}],
                "parameters": [],
            }
        )

    def test_simple_predicate_renders_as_sql_expression(self):
        # Simple row stack: body carries the compiled predicate (the pass
        # condition) plus the re-editable AST; renders like an sql-mode rule.
        body = {
            "lowcode_ast": {
                "rows": [
                    {
                        "kind": "row",
                        "combinator": None,
                        "column_ref": "column",
                        "operator": "is not null",
                        "value": None,
                    }
                ],
                "joins": [],
            },
            "predicate": "{{column}} IS NOT NULL",
        }
        definition = self._lowcode_definition(body)
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, is_tableless = render_check(
            mode="lowcode",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert is_tableless is False
        assert check["check"]["function"] == "sql_expression"
        assert check["check"]["arguments"]["expression"] == "customer_id IS NOT NULL"
        assert check["check"]["arguments"]["negate"] is False
        assert "merge_columns" not in check["check"]["arguments"]

    def test_fail_polarity_negates_lowcode_predicate(self):
        body = {"lowcode_ast": {"rows": [], "joins": []}, "predicate": "{{column}} IS NOT NULL"}
        definition = self._lowcode_definition(body)
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="fail", user_metadata={})
        check, _ = render_check(
            mode="lowcode",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"]["negate"] is True

    def test_group_by_renders_sql_query_with_substituted_merge_columns(self):
        # Advanced (group-by): body carries a full sql_query referencing
        # {{input_view}} plus merge_columns as {{slot}} refs — both the query
        # and each merge column get the slot substituted with the real column.
        body = {
            "lowcode_ast": {"rows": [], "joins": []},
            "group_by": "{{column}}",
            "sql_query": "SELECT {{column}}, (NOT (COUNT(*) > 1)) AS condition FROM {{input_view}} GROUP BY {{column}}",
            "merge_columns": ["{{column}}"],
        }
        definition = self._lowcode_definition(body)
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, is_tableless = render_check(
            mode="lowcode",
            version=version,
            group={"column": "region"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        args = check["check"]["arguments"]
        assert check["check"]["function"] == "sql_query"
        # {{input_view}} is NOT a declared slot — it survives substitution and
        # is resolved by DQX's sql_query check at run time.
        assert args["query"] == "SELECT region, (NOT (COUNT(*) > 1)) AS condition FROM {{input_view}} GROUP BY region"
        assert args["merge_columns"] == ["region"]
        # sql_query does NOT declare a ``columns`` parameter — DQX's validator
        # rejects unknown args, so columns must NOT be in arguments.
        assert "columns" not in args
        assert is_tableless is False

    def test_joins_only_renders_row_level_sql_query_with_join_key_merge_columns(self):
        # A joins-only rule (no group-by) must run ROW-LEVEL: the compiler emits
        # merge_columns = the input-side join keys so DQX joins the per-row
        # result back onto the monitored table. Without merge_columns DQX would
        # route it to the dataset-level "must return exactly one row" path and
        # fail at run time for every such rule.
        body = {
            "lowcode_ast": {
                "rows": [
                    {
                        "kind": "row",
                        "combinator": None,
                        "column_ref": "column",
                        "operator": "is not null",
                        "value": None,
                    }
                ],
                "joins": [
                    {
                        "join_type": "LEFT",
                        "target_table": "c.s.dim",
                        "keys": [{"joined_column": "id", "column_ref": "column"}],
                    }
                ],
            },
            "sql_query": (
                "SELECT {{column}}, (NOT ({{column}} IS NOT NULL)) AS condition "
                "FROM {{input_view}} LEFT JOIN c.s.dim ON c.s.dim.id = {{column}}"
            ),
            "merge_columns": ["{{column}}"],
        }
        definition = self._lowcode_definition(body)
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, is_tableless = render_check(
            mode="lowcode",
            version=version,
            group={"column": "customer_id"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        args = check["check"]["arguments"]
        assert check["check"]["function"] == "sql_query"
        assert args["merge_columns"] == ["customer_id"]
        assert "c.s.dim.id = customer_id" in args["query"]
        assert is_tableless is False

    def test_expression_group_by_renders_sql_query_with_substituted_merge_columns(self):
        # Advanced group-by with an expression grouping key: the compiler splits
        # the group-by at top-level commas only, so the COALESCE's inner comma
        # is NOT shredded into invalid merge-column fragments. Each token — plain
        # slot ref AND expression — has its slots substituted by render_check.
        definition = RuleDefinition.model_validate(
            {
                "body": {
                    "lowcode_ast": {"rows": [], "joins": []},
                    "group_by": "{{region}}, COALESCE({{country}}, 'XX')",
                    "sql_query": (
                        "SELECT {{region}}, COALESCE({{country}}, 'XX'), (NOT (COUNT(*) > 1)) AS condition "
                        "FROM {{input_view}} GROUP BY {{region}}, COALESCE({{country}}, 'XX')"
                    ),
                    "merge_columns": ["{{region}}", "COALESCE({{country}}, 'XX')"],
                },
                "slots": [
                    {"name": "region", "family": "text", "position": 0, "cardinality": "one"},
                    {"name": "country", "family": "text", "position": 1, "cardinality": "one"},
                ],
                "parameters": [],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, _ = render_check(
            mode="lowcode",
            version=version,
            group={"region": "sales_region", "country": "country_code"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        args = check["check"]["arguments"]
        assert check["check"]["function"] == "sql_query"
        assert args["merge_columns"] == ["sales_region", "COALESCE(country_code, 'XX')"]
        assert args["query"] == (
            "SELECT sales_region, COALESCE(country_code, 'XX'), (NOT (COUNT(*) > 1)) AS condition "
            "FROM {{input_view}} GROUP BY sales_region, COALESCE(country_code, 'XX')"
        )

    def test_unsafe_compiled_sql_query_raises(self):
        body = {
            "lowcode_ast": {"rows": [], "joins": []},
            "sql_query": "SELECT 1 AS condition FROM {{input_view}}; DROP TABLE {{column}}",
        }
        definition = self._lowcode_definition(body)
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        with pytest.raises(UnsafeSqlQueryError):
            render_check(
                mode="lowcode",
                version=version,
                group={"column": "t"},
                effective_severity="Medium",
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
                app_settings=_app_settings_stub(),
            )

    def test_group_by_sql_query_has_no_columns_arg_but_metadata_carries_them(self):
        # sql_query does NOT declare a `columns` parameter — DQX's validator rejects
        # any unknown arg, so `columns` must NOT be in arguments. The mapped columns
        # instead ride in user_metadata['mapped_columns'] (JSON), which the
        # attribution view reads so by-column still populates for sql_query rules.
        body = {
            "lowcode_ast": {"rows": [], "joins": []},
            "group_by": "{{column}}",
            "sql_query": "SELECT {{column}}, (NOT (COUNT(*) > 1)) AS condition FROM {{input_view}} GROUP BY {{column}}",
            "merge_columns": ["{{column}}"],
        }
        definition = self._lowcode_definition(body)
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, _ = render_check(
            mode="lowcode",
            version=version,
            group={"column": "region"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["function"] == "sql_query"
        assert "columns" not in check["check"]["arguments"]
        assert check["check"]["arguments"]["merge_columns"] == ["region"]
        assert json.loads(check["user_metadata"]["mapped_columns"]) == ["region"]

    def test_sql_expression_keeps_columns_arg_and_metadata(self):
        # sql_expression DECLARES a columns param — keep arguments.columns AND also
        # stamp the metadata carrier (uniform across modes).
        definition = RuleDefinition.model_validate(
            {
                "body": {"predicate": "{{start}} <= {{end}}"},
                "slots": [
                    {"name": "start", "family": "any", "position": 0, "cardinality": "one"},
                    {"name": "end", "family": "any", "position": 1, "cardinality": "one"},
                ],
                "parameters": [],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
        check, _ = render_check(
            mode="sql",
            version=version,
            group={"start": "shipped_at", "end": "delivered_at"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"]["columns"] == ["shipped_at", "delivered_at"]
        assert json.loads(check["user_metadata"]["mapped_columns"]) == ["shipped_at", "delivered_at"]

    def test_dqx_native_stamps_mapped_columns_metadata(self):
        # dqx_native single-column check: arguments.column unchanged, metadata carrier added.
        version = RuleVersion(
            rule_id="r1", version=1, definition=_is_not_null_definition(), user_metadata={"name": "x"}
        )
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert check["check"]["arguments"]["column"] == "customer_id"
        assert json.loads(check["user_metadata"]["mapped_columns"]) == ["customer_id"]

    def test_tableless_sql_query_no_mapped_columns_metadata(self):
        definition = RuleDefinition.model_validate(
            {"body": {"sql_query": "SELECT COUNT(*) > 100 AS condition FROM t"}, "slots": [], "parameters": []}
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="fail", user_metadata={})
        check, is_tableless = render_check(
            mode="sql",
            version=version,
            group={},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert is_tableless is True
        assert "columns" not in check["check"]["arguments"]
        assert "mapped_columns" not in check["user_metadata"]

    def test_sql_query_columns_recoverable_by_attribution_contract(self):
        """Guarantees sql_query columns survive rendering for results attribution.

        The attribution view relies on columns being recoverable via
        user_metadata['mapped_columns'] (JSON), NOT arguments (which DQX validator
        rejects as unknown). This test pins the contract: a rendered sql_query check
        exposes its columns through the metadata carrier and omits them from arguments.
        """
        definition = RuleDefinition.model_validate(
            {
                "body": {
                    "sql_query": "SELECT {{col}}, (NOT (COUNT({{col}}) = 1)) AS condition FROM {{input_view}} GROUP BY {{col}}",
                    "merge_columns": ["{{col}}"],
                },
                "slots": [{"name": "col", "family": "any", "position": 0, "cardinality": "one"}],
                "parameters": [],
            }
        )
        version = RuleVersion(
            rule_id="r1",
            version=1,
            definition=definition,
            polarity="pass",
            user_metadata={"name": "Unique value check"},
        )
        check, _ = render_check(
            mode="lowcode",
            version=version,
            group={"col": "city"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        # 1. No columns arg (would fail DQX validation)
        assert "columns" not in check["check"]["arguments"]
        # 2. Columns are recoverable via the metadata carrier the attribution view reads
        recovered = json.loads(check["user_metadata"]["mapped_columns"])
        assert recovered == ["city"]
        # 3. The underlying rule name is available for display
        assert check["user_metadata"]["name"] == "Unique value check"


def test_dqx_rejects_columns_arg_on_sql_query():
    # Documents the reason Task 2 removed arguments.columns from sql_query: DQX's
    # ChecksValidator rejects any argument absent from the function signature.
    import inspect

    from databricks.labs.dqx import check_funcs
    from databricks.labs.dqx.checks_validator import ChecksValidator

    check = {
        "criticality": "error",
        "check": {"function": "sql_query", "arguments": {"query": "SELECT 1 AS condition", "columns": ["city"]}},
    }
    errors = ChecksValidator._validate_func_args(
        check["check"]["arguments"],
        check_funcs.sql_query,
        check,
        inspect.signature(check_funcs.sql_query).parameters,
    )
    assert any("Unexpected argument 'columns'" in e for e in errors)


# ---------------------------------------------------------------------------
# Materializer.materialize_binding — orchestration
# ---------------------------------------------------------------------------


@pytest.fixture
def sql(sql_executor_mock):
    sql_executor_mock.dialect = "delta"
    sql_executor_mock.fqn.side_effect = lambda t: f"dqx_test.dqx_app_test.{t}"
    sql_executor_mock.q.side_effect = lambda i: f"`{i}`"
    sql_executor_mock.json_literal_expr.side_effect = lambda j: f"parse_json('{j}')"
    sql_executor_mock.select_json_text.side_effect = lambda c: f"to_json({c})"
    sql_executor_mock.ts_text.side_effect = lambda c: f"CAST({c} AS STRING)"
    sql_executor_mock.query.return_value = []
    return sql_executor_mock


@pytest.fixture
def registry():
    return create_autospec(RegistryService, instance=True)


@pytest.fixture
def monitored_tables():
    return create_autospec(MonitoredTableService, instance=True)


@pytest.fixture
def app_settings():
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_auto_upgrade_without_approval.return_value = False
    mock.get_label_definitions.return_value = []
    mock.get_pass_threshold_enabled.return_value = True
    mock.get_default_pass_threshold.return_value = 70
    return mock


@pytest.fixture
def materializer(sql, registry, monitored_tables, app_settings):
    return Materializer(sql=sql, registry=registry, monitored_tables=monitored_tables, app_settings=app_settings)


def _detail(applied: AppliedRule, table_fqn: str = "cat.schema.customers") -> MonitoredTableDetail:
    table = MonitoredTable(binding_id="b1", table_fqn=table_fqn, status="draft")
    return MonitoredTableDetail(table=table, applied_rules=[AppliedRuleSummary(applied_rule=applied)])


def _published_rule(rule_id: str = "r1", version: int = 1) -> RegistryRule:
    return RegistryRule(
        rule_id=rule_id,
        mode="dqx_native",
        status="approved",
        version=version,
        definition=_is_not_null_definition(),
        user_metadata={"name": "Not Null Check", "severity": "High"},
    )


def _version_snapshot(rule_id: str = "r1", version: int = 1, severity: str = "High") -> RuleVersion:
    return RuleVersion(
        rule_id=rule_id,
        version=version,
        definition=_is_not_null_definition(),
        user_metadata={"name": "Not Null Check", "severity": severity},
    )


class TestMaterializeBindingBasics:
    def test_raises_for_missing_binding(self, materializer, monitored_tables):
        monitored_tables.get.return_value = None
        with pytest.raises(MaterializationError):
            materializer.materialize_binding("missing")

    def test_writes_new_row_as_draft(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()
        sql.query.return_value = []  # no existing materialized row, no orphans

        written = materializer.materialize_binding("b1")
        assert written == ["ar1-0"]
        insert_sql = sql.execute.call_args_list[0].args[0]
        assert "INSERT INTO dqx_test.dqx_app_test.dq_quality_rules" in insert_sql
        assert "'draft'" in insert_sql
        assert "'ar1-0'" in insert_sql
        assert "'registry'" in insert_sql

    def test_uses_pinned_version_when_set(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            pinned_version=2,
            column_mapping=[{"column": "customer_id"}],
            mapping_hash="h",
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=5)  # latest published is v5
        registry.get_version.return_value = _version_snapshot(version=2)
        sql.query.return_value = []

        materializer.materialize_binding("b1")
        registry.get_version.assert_called_once_with("r1", 2)

    def test_follows_latest_when_unpinned(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=5)
        registry.get_version.return_value = _version_snapshot(version=5)
        sql.query.return_value = []

        materializer.materialize_binding("b1")
        registry.get_version.assert_called_once_with("r1", 5)

    def test_severity_override_changes_criticality(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            severity_override="Low",
            column_mapping=[{"column": "customer_id"}],
            mapping_hash="h",
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()  # rule's own tag is High
        registry.get_version.return_value = _version_snapshot(severity="High")
        sql.query.return_value = []

        materializer.materialize_binding("b1")
        insert_sql = sql.execute.call_args_list[0].args[0]
        stored = _extract_json_literal(insert_sql)
        assert stored["criticality"] == "warn"  # Low overrides the rule's High tag
        assert stored["user_metadata"]["severity"] == "Low"


class TestMaterializeBindingIdempotency:
    def test_rerun_with_unchanged_content_updates_without_status_change(
        self, materializer, sql, registry, monitored_tables
    ):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        check, _ = render_check(
            mode="dqx_native",
            version=_version_snapshot(),
            group={"column": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        existing_check_json = json.dumps(check, sort_keys=True)

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return [["approved", 1, existing_check_json]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")

        update_calls = [c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE")]
        assert len(update_calls) == 1
        assert "status = 'approved'" in update_calls[0]
        insert_calls = [c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("INSERT")]
        assert not insert_calls

    def test_rerun_does_not_duplicate_rows(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return [["draft", 1, "{}"]]
            return []

        sql.query.side_effect = fake_query
        first = materializer.materialize_binding("b1")
        second = materializer.materialize_binding("b1")
        assert first == second == ["ar1-0"]


class TestAutoUpgradeBehaviour:
    def test_behaviour_b_resets_approved_to_pending_when_content_changes(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        app_settings.get_auto_upgrade_without_approval.return_value = False
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)  # republished
        registry.get_version.return_value = _version_snapshot(version=2, severity="Critical")  # content differs

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                # Was materialized against v1; resolved version now moves to v2.
                return [["approved", 1, json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'pending_approval'" in update_sql

    def test_behaviour_a_keeps_approved_when_version_moves(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        app_settings.get_auto_upgrade_without_approval.return_value = True
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = _version_snapshot(version=2, severity="Critical")

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                # Was materialized against v1; an unpinned follower now resolves
                # to the freshly published v2 — a genuine VERSION move, so the
                # auto-upgrade shortcut may keep it approved.
                return [["approved", 1, json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'approved'" in update_sql

    def test_unpinned_severity_edit_requires_review_even_with_auto_upgrade(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        # B2-67: a DIRECT severity-override edit on an approved, UNPINNED rule
        # leaves the resolved version unchanged, so it must return to review
        # even when auto-upgrade is on — the auto-upgrade shortcut is only for
        # genuine version moves, not deliberate edits. Before the fix this was
        # silently kept approved (no submit-for-review, no banner).
        app_settings.get_auto_upgrade_without_approval.return_value = True
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            severity_override="Critical",  # direct edit changes content, version stays v1
            column_mapping=[{"column": "customer_id"}],
            mapping_hash="h",
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=1)
        registry.get_version.return_value = _version_snapshot(version=1, severity="High")

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                # Already materialized against the SAME resolved version (v1).
                return [["approved", 1, json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'pending_approval'" in update_sql

    def test_pinned_row_content_change_always_goes_to_pending_approval(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        app_settings.get_auto_upgrade_without_approval.return_value = True  # even with auto-upgrade ON
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            pinned_version=1,
            severity_override="Critical",  # direct edit changes content
            column_mapping=[{"column": "customer_id"}],
            mapping_hash="h",
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=1)
        registry.get_version.return_value = _version_snapshot(version=1, severity="High")

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return [["approved", 1, json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'pending_approval'" in update_sql


class TestCleanup:
    def test_shrinking_mapping_deletes_stale_group_row(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        def fake_query(sql_text: str):
            if "SELECT status" in sql_text:
                return []  # ar1-0 is new
            if "SELECT rule_id FROM" in sql_text and "applied_rule_id = " in sql_text:
                return [["ar1-1"]]  # a stale second group from a previous, wider mapping
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        delete_calls = [c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("DELETE")]
        assert any("'ar1-1'" in c for c in delete_calls)

    def test_removed_application_orphan_is_deleted(self, materializer, sql, registry, monitored_tables):
        # binding now has NO applied rules at all (the application was
        # removed directly from dq_applied_rules), but a stale
        # dq_quality_rules row from it still exists.
        table = MonitoredTable(binding_id="b1", table_fqn="cat.schema.customers", status="draft")
        monitored_tables.get.return_value = MonitoredTableDetail(table=table, applied_rules=[])

        materializer.materialize_binding("b1")
        # No applied rules -> no orphan cleanup query needed (nothing to scope to).
        sql.execute.assert_not_called()


class TestAllGroupsFailRenderIsNonDestructive:
    """DATA-LOSS GUARD (B2-27): when an update/auto-upgrade re-renders a
    follower against a new version whose slots no longer match the follower's
    stored ``column_mapping``, EVERY group fails to render and
    ``_iter_rendered_checks`` returns a non-None EMPTY list. That must NOT be
    treated as delete-all — the existing approved ``dq_quality_rules`` rows
    must survive intact (they keep serving; the mismatch surfaces for
    re-review) rather than being wiped.
    """

    @staticmethod
    def _mismatched_version(version: int = 2) -> RuleVersion:
        # New version exposes slot "email"; the follower's mapping binds
        # "column" -> render_check can't satisfy slot "email" and raises.
        return RuleVersion(
            rule_id="r1",
            version=version,
            definition=_is_not_null_definition("email"),
            user_metadata={"name": "Not Null Check", "severity": "High"},
        )

    @pytest.mark.parametrize("auto_upgrade", [False, True])
    def test_preserves_existing_rows_when_every_group_fails(
        self, materializer, sql, registry, monitored_tables, app_settings, auto_upgrade
    ):
        app_settings.get_auto_upgrade_without_approval.return_value = auto_upgrade
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = self._mismatched_version(version=2)

        def fake_query(sql_text: str):
            # The application still has one materialized approved row.
            if "SELECT rule_id FROM" in sql_text and "applied_rule_id = " in sql_text:
                return [["ar1-0"]]
            return []

        sql.query.side_effect = fake_query

        written = materializer.materialize_binding("b1")

        # The existing row is retained (counted as "written"/expected)...
        assert written == ["ar1-0"]
        # ...and NOTHING is deleted or rewritten — no data loss.
        sql.execute.assert_not_called()

    def test_legitimately_empty_mapping_still_cleans_up_stale_rows(self, materializer, sql, registry, monitored_tables):
        # An application with NO mapping groups genuinely materializes nothing;
        # the guard must be narrow enough to still delete a leftover stale row.
        applied = AppliedRule(id="ar1", binding_id="b1", rule_id="r1", column_mapping=[], mapping_hash="h")
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        def fake_query(sql_text: str):
            if "SELECT rule_id FROM" in sql_text and "applied_rule_id = " in sql_text:
                return [["ar1-0"]]  # stale row from a previously-wider mapping
            return []

        sql.query.side_effect = fake_query
        materializer.materialize_binding("b1")
        delete_calls = [c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("DELETE")]
        assert any("'ar1-0'" in c for c in delete_calls)


class TestRematerializeForRule:
    """``rematerialize_for_rule`` — the "publish -> propagate to followers" entry
    point (design spec §5). Wired into ``approve_registry_rule`` so publishing a
    new registry-rule version re-materializes every FOLLOWING (unpinned)
    application; PINNED applications are excluded from the query entirely and
    only ever change via a direct edit (see ``TestAutoUpgradeBehaviour``).
    """

    def test_queries_only_following_unpinned_applications(self, materializer, sql, monitored_tables, registry):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = _version_snapshot(version=2)

        def fake_query(sql_text: str):
            if "dq_applied_rules" in sql_text:
                return [["b1"]]
            return []

        sql.query.side_effect = fake_query
        result = materializer.rematerialize_for_rule("r1")

        assert result == ["b1"]
        applied_rules_query = next(c.args[0] for c in sql.query.call_args_list if "dq_applied_rules" in c.args[0])
        assert "pinned_version IS NULL" in applied_rules_query
        assert "'r1'" in applied_rules_query
        monitored_tables.get.assert_called_once_with("b1")

    def test_rematerializes_every_distinct_binding(self, materializer, sql, monitored_tables, registry):
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = _version_snapshot(version=2)

        def get_detail(binding_id: str):
            applied = AppliedRule(
                id=f"ar-{binding_id}",
                binding_id=binding_id,
                rule_id="r1",
                column_mapping=[{"column": "customer_id"}],
                mapping_hash="h",
            )
            return _detail(applied, table_fqn=f"cat.schema.{binding_id}")

        monitored_tables.get.side_effect = get_detail

        def fake_query(sql_text: str):
            if "dq_applied_rules" in sql_text:
                return [["b1"], ["b2"]]
            return []

        sql.query.side_effect = fake_query
        result = materializer.rematerialize_for_rule("r1")

        assert result == ["b1", "b2"]
        assert monitored_tables.get.call_args_list == [(("b1",),), (("b2",),)]

    def test_skips_binding_that_no_longer_exists(self, materializer, sql, monitored_tables):
        monitored_tables.get.return_value = None  # binding was deleted after publish

        def fake_query(sql_text: str):
            if "dq_applied_rules" in sql_text:
                return [["gone"]]
            return []

        sql.query.side_effect = fake_query
        result = materializer.rematerialize_for_rule("r1")

        # The binding_id is still reported (caller-visible), but nothing raises.
        assert result == ["gone"]

    def test_no_following_applications_is_a_noop(self, materializer, sql, monitored_tables):
        sql.query.return_value = []
        result = materializer.rematerialize_for_rule("r1")
        assert result == []
        monitored_tables.get.assert_not_called()

    def test_respects_auto_upgrade_behaviour_b_via_materialize_binding(
        self, materializer, sql, registry, monitored_tables, app_settings
    ):
        """Re-materializing through the publish path still honours Behaviour B
        (auto-upgrade OFF): a previously-approved following row whose content
        changed is pushed back to pending_approval, same as a direct
        ``materialize_binding`` call."""
        app_settings.get_auto_upgrade_without_approval.return_value = False
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule(version=2)
        registry.get_version.return_value = _version_snapshot(version=2, severity="Critical")

        def fake_query(sql_text: str):
            if "dq_applied_rules" in sql_text:
                return [["b1"]]
            if "SELECT status" in sql_text:
                return [["approved", 1, json.dumps({"different": "content"})]]
            return []

        sql.query.side_effect = fake_query
        materializer.rematerialize_for_rule("r1")

        update_sql = next(c.args[0] for c in sql.execute.call_args_list if c.args[0].startswith("UPDATE"))
        assert "status = 'pending_approval'" in update_sql


class TestRenderCheckNativeCrossTable:
    """Phase 7C-a: confirm DQX Native cross-table rules materialize correctly.

    ``foreign_key`` is a dataset-level check with reference-table arguments
    (``ref_table``/``ref_columns``) that are NOT column slots on the
    monitored table — they are frozen ``RuleParameter`` values on the
    registry rule's definition (see ``registry_seed_map.py``:
    ``ref_table``/``ref_columns`` kinds map to the ``ref_table``/``ref_column``
    ``ParamType``s). This asserts a ``dqx_native`` ``foreign_key`` rule
    materializes with those ref-table args intact, alongside its own
    column slot substitution — no materializer change was needed for this;
    ``render_check``'s existing ``dqx_native`` branch already fills in both
    slots and non-``None`` parameters generically regardless of function
    name (see ``_substitute_arguments``).
    """

    def test_foreign_key_native_rule_materializes_with_ref_table_args(self):
        definition = RuleDefinition.model_validate(
            {
                "body": {
                    "function": "foreign_key",
                    "arguments": {"columns": "{{columns}}"},
                },
                "slots": [{"name": "columns", "family": "any", "position": 0, "cardinality": "many"}],
                "parameters": [
                    {"name": "ref_columns", "type": "ref_column", "value": ["id"]},
                    {"name": "ref_table", "type": "ref_table", "value": "catalog.schema.customers"},
                ],
            }
        )
        version = RuleVersion(
            rule_id="r1",
            version=1,
            definition=definition,
            user_metadata={"name": "Orders FK", "severity": "High"},
        )
        check, is_tableless = render_check(
            mode="dqx_native",
            version=version,
            group={"columns": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )

        # foreign_key is per-table (it validates the monitored table's own
        # rows against a reference table), not the tableless __sql_check__
        # convention reserved for genuinely table-less cross-table SQL.
        assert is_tableless is False
        assert check["check"]["function"] == "foreign_key"
        assert check["check"]["arguments"]["columns"] == ["customer_id"]
        assert check["check"]["arguments"]["ref_columns"] == ["id"]
        assert check["check"]["arguments"]["ref_table"] == "catalog.schema.customers"
        assert check["criticality"] == "error"


class TestNativeArityBinding:
    """Task 6: arity-aware column binding in the native materializer path (B3 backend).

    For list-arity funcs (arg_key shared across multiple slots, e.g. is_unique's
    ``columns``), ALL declared slots appear in function args EXCEPT those whose
    ``{{slot}}`` placeholder is referenced in ``definition.filter`` (filter-only
    columns).  For single-arity funcs (one slot per arg_key, e.g. is_not_null's
    ``column``), extra slots that are only referenced in the filter never appear
    in function args — the template already encodes this and the materializer
    must not inject them.
    """

    @staticmethod
    def _list_arity_definition(
        slot_names: list[str],
        filter_text: str | None = None,
    ) -> RuleDefinition:
        """is_unique-style definition: N slots all bound to the same ``columns`` list arg."""
        slots = [
            {
                "name": s,
                "family": "any",
                "position": i,
                "cardinality": "one",
                "arg_key": "columns",
            }
            for i, s in enumerate(slot_names)
        ]
        # Template lists all slot placeholders — the materializer must strip
        # filter-only ones from the rendered function arg.
        arguments = {"columns": [f"{{{{{s}}}}}" for s in slot_names]}
        defn: dict = {"body": {"function": "is_unique", "arguments": arguments}, "slots": slots, "parameters": []}
        if filter_text is not None:
            defn["filter"] = filter_text
        return RuleDefinition.model_validate(defn)

    @staticmethod
    def _single_arity_definition(
        first_slot: str,
        extra_slot: str,
        filter_text: str | None = None,
    ) -> RuleDefinition:
        """is_not_null-style definition: 2 declared slots, only the first in function args.

        The template only references ``{{first_slot}}`` in body.arguments; the
        extra slot exists solely for the advanced filter condition.
        """
        slots = [
            {"name": first_slot, "family": "any", "position": 0, "cardinality": "one", "arg_key": "column"},
            {"name": extra_slot, "family": "any", "position": 1, "cardinality": "one", "arg_key": "column"},
        ]
        arguments = {"column": f"{{{{{first_slot}}}}}"}
        defn: dict = {
            "body": {"function": "is_not_null", "arguments": arguments},
            "slots": slots,
            "parameters": [],
        }
        if filter_text is not None:
            defn["filter"] = filter_text
        return RuleDefinition.model_validate(defn)

    def _render(self, definition: RuleDefinition, group: dict) -> dict:
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group=group,
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        return check

    def test_list_arity_all_three_slots_in_function_args_when_no_filter(self):
        """(a) 3 declared slots, none in filter → function args include all 3."""
        definition = self._list_arity_definition(["col_1", "col_2", "col_3"])
        check = self._render(definition, {"col_1": "a", "col_2": "b", "col_3": "c"})
        assert check["check"]["arguments"]["columns"] == ["a", "b", "c"]

    def test_list_arity_filter_referenced_slot_excluded_from_function_args(self):
        """(b) 3 declared slots, col_3 referenced in definition.filter →
        function args include only the other 2 (col_3 is filter-only)."""
        definition = self._list_arity_definition(
            ["col_1", "col_2", "col_3"],
            filter_text="{{col_3}} > 100",
        )
        check = self._render(definition, {"col_1": "a", "col_2": "b", "col_3": "c"})
        # Filter-only col_3 must NOT appear in function args.
        assert check["check"]["arguments"]["columns"] == ["a", "b"]
        # The filter itself IS rendered with col_3 substituted.
        assert check["filter"] == "c > 100"

    def test_single_arity_second_slot_absent_from_function_args(self):
        """(c) Single-arity func, 2 declared slots → function arg = first slot only;
        second slot is used only in the filter and must not appear in function args."""
        definition = self._single_arity_definition(
            "col_1",
            "col_2",
            filter_text="{{col_2}} IS NOT NULL",
        )
        check = self._render(definition, {"col_1": "amount", "col_2": "status"})
        # Only the first slot's column in function args.
        assert check["check"]["arguments"]["column"] == "amount"
        assert "col_2" not in check["check"]["arguments"]
        assert "status" not in str(check["check"]["arguments"])
        # The filter is rendered with col_2 substituted.
        assert check["filter"] == "status IS NOT NULL"


def _extract_json_literal(sql_text: str) -> dict:
    start = sql_text.index("parse_json('") + len("parse_json('")
    end = sql_text.index("')", start)
    return json.loads(sql_text[start:end])


class TestRenderBindingChecks:
    """``render_binding_checks`` — the read-only draft-run source (design spec §4.1).

    Renders the binding's CURRENT persisted applied-rules state through the
    SAME path materialization uses, but writes NOTHING to
    ``dq_quality_rules``. For an approved binding with no pending edits the
    render equals the materialized output.
    """

    def test_render_equals_materialized_check_and_writes_nothing(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        checks = materializer.render_binding_checks("b1")

        # Byte-identical to what render_check (hence materialization) produces.
        expected, _ = render_check(
            mode="dqx_native",
            version=_version_snapshot(),
            group={"column": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert checks == [expected]
        # Read-only: no INSERT/UPDATE/DELETE against dq_quality_rules.
        sql.execute.assert_not_called()

    def test_renders_every_mapping_group_across_applied_rules(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            column_mapping=[{"column": "a"}, {"column": "b"}],
            mapping_hash="h",
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        checks = materializer.render_binding_checks("b1")
        assert [c["check"]["arguments"]["column"] for c in checks] == ["a", "b"]
        # Multi-column rule: each per-column check gets a DISTINCT name (pinned
        # name suffixed with its column) so metrics/attribution can tell the
        # columns apart — otherwise both would be "Not Null Check" and collapse
        # to one column with pooled failure counts.
        assert [c["name"] for c in checks] == ["Not Null Check (a)", "Not Null Check (b)"]
        sql.execute.assert_not_called()

    def test_single_column_rule_keeps_plain_pinned_name(self, materializer, sql, registry, monitored_tables):
        # A single-column application is NOT suffixed — the check keeps the
        # rule's plain pinned name (no disambiguation needed).
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()

        checks = materializer.render_binding_checks("b1")
        assert [c["name"] for c in checks] == ["Not Null Check"]

    def test_raises_for_missing_binding(self, materializer, monitored_tables):
        monitored_tables.get.return_value = None
        with pytest.raises(MaterializationError):
            materializer.render_binding_checks("missing")

    def test_unresolvable_applied_rule_contributes_nothing(self, materializer, sql, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "customer_id"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = None  # registry rule vanished
        assert materializer.render_binding_checks("b1") == []
        sql.execute.assert_not_called()

    def test_render_uses_frozen_snapshot_mode_not_live_mode(self, materializer, registry, monitored_tables):
        """A follower still serving vN renders vN's FROZEN mode, even after the
        live approved rule's mode was switched in place (P20 major fix): the
        frozen native snapshot must render as a native check, not be
        reinterpreted under the live rule's now-``sql`` mode."""
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "amount"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        live = _published_rule()
        live.mode = "sql"  # edited in place, unpublished — must NOT drive rendering of vN
        registry.get_rule.return_value = live
        registry.get_version.return_value = RuleVersion(
            rule_id="r1",
            version=1,
            mode="dqx_native",
            definition=_is_not_null_definition(),
            user_metadata={"name": "Not Null Check", "severity": "High"},
        )
        checks = materializer.render_binding_checks("b1")
        assert checks[0]["check"]["function"] == "is_not_null"

    def test_render_falls_back_to_live_mode_for_legacy_null_snapshot(self, materializer, registry, monitored_tables):
        """A legacy snapshot written before mode was frozen (``mode=None``)
        falls back to the live rule's mode so it still renders."""
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", column_mapping=[{"column": "amount"}], mapping_hash="h"
        )
        monitored_tables.get.return_value = _detail(applied)
        registry.get_rule.return_value = _published_rule()  # live mode dqx_native
        registry.get_version.return_value = _version_snapshot()  # mode defaults to None
        checks = materializer.render_binding_checks("b1")
        assert checks[0]["check"]["function"] == "is_not_null"


class TestRenderAppliedChecks:
    """``render_applied_checks`` — reference resolution for frozen version snapshots.

    Renders a GIVEN list of applied-rule references (each pinning an explicit
    registry version) against the immutable ``dq_rule_versions`` snapshots,
    reproducing the runner payload without a stored copy of the rule set. Used
    by :class:`MonitoredTableVersionService.get_checks`.
    """

    def test_renders_refs_against_pinned_versions(self, materializer, sql, registry):
        applied = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            pinned_version=1,  # the RESOLVED version frozen into the reference
            column_mapping=[{"column": "customer_id"}],
            mapping_hash="h",
        )
        registry.get_rules_many.return_value = {"r1": _published_rule()}
        registry.get_versions_many.return_value = {("r1", 1): _version_snapshot()}

        checks = materializer.render_applied_checks("cat.schema.customers", [applied])

        expected, _ = render_check(
            mode="dqx_native",
            version=_version_snapshot(),
            group={"column": "customer_id"},
            effective_severity="High",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
        )
        assert checks == [expected]
        # Batch path: resolves via grouped queries, never the per-id lookups.
        registry.get_rule.assert_not_called()
        registry.get_version.assert_not_called()
        registry.get_versions_many.assert_called_once_with({("r1", 1)})
        # Read-only: renders without writing to dq_quality_rules.
        sql.execute.assert_not_called()

    def test_empty_and_idless_refs_render_nothing(self, materializer, registry):
        idless = AppliedRule(id=None, binding_id="b1", rule_id="r1", column_mapping=[{"column": "c"}])
        assert materializer.render_applied_checks("cat.schema.t", []) == []
        assert materializer.render_applied_checks("cat.schema.t", [idless]) == []
        registry.get_rules_many.assert_not_called()

    def test_unresolvable_ref_contributes_nothing(self, materializer, registry):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="r1", pinned_version=1, column_mapping=[{"column": "c"}], mapping_hash="h"
        )
        registry.get_rules_many.return_value = {}  # registry rule vanished
        registry.get_versions_many.return_value = {}
        assert materializer.render_applied_checks("cat.schema.t", [applied]) == []


class TestRenderBindingChecksCountsMany:
    """``render_binding_checks_counts_many`` — the BATCHED draft check-count path (B2-141).

    Backs the monitored-tables list load's never-approved count column. The
    per-binding :meth:`render_binding_checks` version fanned out to
    ``~3N + 2·ΣR`` sequential OLTP round-trips (N never-approved bindings, R
    applied rules each); this collapses that to a CONSTANT handful of grouped
    queries. These tests pin BOTH properties: query-count boundedness
    (independent of N) AND count parity with the old per-binding render.
    """

    def test_count_parity_with_per_binding_render_including_for_each_column(
        self, materializer, registry, monitored_tables
    ):
        # A for_each_column / multi-slot rule renders ONE check per mapping
        # group, so a 3-group application must count 3 — exactly what a
        # per-binding render_binding_checks would return.
        multi = AppliedRule(
            id="ar1",
            binding_id="b1",
            rule_id="r1",
            column_mapping=[{"column": "a"}, {"column": "b"}, {"column": "c"}],
            mapping_hash="h",
        )
        single = AppliedRule(
            id="ar2", binding_id="b2", rule_id="r1", column_mapping=[{"column": "x"}], mapping_hash="h"
        )
        monitored_tables.list_applied_rules_many.return_value = {"b1": [multi], "b2": [single]}
        registry.get_rules_many.return_value = {"r1": _published_rule()}
        registry.get_versions_many.return_value = {("r1", 1): _version_snapshot()}

        counts = materializer.render_binding_checks_counts_many([("b1", "cat.schema.t1"), ("b2", "cat.schema.t2")])

        assert counts == {"b1": 3, "b2": 1}

        # Parity cross-check: the per-binding render of each binding yields the
        # same length the batched count reports.
        monitored_tables.get.return_value = _detail(multi, table_fqn="cat.schema.t1")
        registry.get_rule.return_value = _published_rule()
        registry.get_version.return_value = _version_snapshot()
        assert len(materializer.render_binding_checks("b1")) == counts["b1"]

    def test_query_count_is_bounded_and_independent_of_binding_count(self, materializer, registry, monitored_tables):
        """The win: the batched path issues a CONSTANT number of registry/
        applied-rule queries regardless of how many bindings it counts, and
        NEVER the per-binding ``get_rule`` / ``get_version`` round-trips the
        old loop paid. Verified for 1 vs 5 bindings — the call counts don't
        scale with N."""

        def _run(n: int) -> None:
            bindings = [(f"b{i}", f"cat.schema.t{i}") for i in range(n)]
            applied_by_binding = {
                f"b{i}": [
                    AppliedRule(
                        id=f"ar{i}",
                        binding_id=f"b{i}",
                        rule_id="r1",
                        column_mapping=[{"column": "a"}, {"column": "b"}],
                        mapping_hash="h",
                    )
                ]
                for i in range(n)
            }
            registry.reset_mock()
            monitored_tables.reset_mock()
            monitored_tables.list_applied_rules_many.return_value = applied_by_binding
            registry.get_rules_many.return_value = {"r1": _published_rule()}
            registry.get_versions_many.return_value = {("r1", 1): _version_snapshot()}

            counts = materializer.render_binding_checks_counts_many(bindings)

            assert counts == {f"b{i}": 2 for i in range(n)}
            # ONE grouped query each for applied rules, rules, and versions —
            # regardless of N.
            assert monitored_tables.list_applied_rules_many.call_count == 1
            assert registry.get_rules_many.call_count == 1
            assert registry.get_versions_many.call_count == 1
            # The per-binding round-trips the old loop paid are NEVER issued.
            registry.get_rule.assert_not_called()
            registry.get_version.assert_not_called()
            monitored_tables.get.assert_not_called()

        _run(1)
        _run(5)  # 5x the bindings, SAME (constant) query-call counts

    def test_empty_input_issues_no_queries(self, materializer, registry, monitored_tables):
        assert materializer.render_binding_checks_counts_many([]) == {}
        monitored_tables.list_applied_rules_many.assert_not_called()
        registry.get_rules_many.assert_not_called()
        registry.get_versions_many.assert_not_called()

    def test_unresolvable_binding_counts_zero(self, materializer, registry, monitored_tables):
        applied = AppliedRule(
            id="ar1", binding_id="b1", rule_id="gone", column_mapping=[{"column": "a"}], mapping_hash="h"
        )
        monitored_tables.list_applied_rules_many.return_value = {"b1": [applied]}
        registry.get_rules_many.return_value = {}  # registry rule vanished
        registry.get_versions_many.return_value = {}
        counts = materializer.render_binding_checks_counts_many([("b1", "cat.schema.t1")])
        assert counts == {"b1": 0}


# ---------------------------------------------------------------------------
# render_check — pass-threshold resolution
# ---------------------------------------------------------------------------


def _app_settings_with_threshold(*, enabled: bool = True, default: int = 70) -> AppSettingsService:
    """AppSettingsService stub with configurable threshold settings."""
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_label_definitions.return_value = []
    mock.get_pass_threshold_enabled.return_value = enabled
    mock.get_default_pass_threshold.return_value = default
    return mock


class TestRenderCheckPassThreshold:
    """render_check always emits the resolved effective threshold when enabled;
    emits nothing when the feature is disabled."""

    def _version(self, registry_default: int | None = None) -> RuleVersion:
        user_metadata: dict = {}
        if registry_default is not None:
            from databricks_labs_dqx_app.backend.registry_models import RESERVED_PASS_THRESHOLD_KEY

            user_metadata[RESERVED_PASS_THRESHOLD_KEY] = registry_default
        return RuleVersion(
            rule_id="r1",
            version=1,
            definition=_is_not_null_definition(),
            user_metadata=user_metadata,
        )

    def _render(
        self,
        *,
        app_settings: AppSettingsService,
        pass_threshold: int | None = None,
        per_application_tags: dict | None = None,
        version: "RuleVersion | None" = None,
    ) -> dict:
        v = version or self._version()
        check, _ = render_check(
            mode="dqx_native",
            version=v,
            group={"column": "col1"},
            effective_severity="Medium",
            per_application_tags=per_application_tags or {},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=app_settings,
            pass_threshold=pass_threshold,
        )
        return check

    def test_enabled_no_overrides_emits_admin_default(self):
        """Enabled + no per-rule/column/registry override → admin default."""
        app_settings = _app_settings_with_threshold(enabled=True, default=70)
        check = self._render(app_settings=app_settings)
        assert check["user_metadata"]["pass_threshold"] == "70"

    def test_enabled_per_rule_override_wins_over_admin_default(self):
        """Enabled + per-rule override → that value (not admin default)."""
        app_settings = _app_settings_with_threshold(enabled=True, default=70)
        check = self._render(app_settings=app_settings, pass_threshold=85)
        assert check["user_metadata"]["pass_threshold"] == "85"

    def test_enabled_registry_default_wins_over_admin_default(self):
        """Enabled + registry-rule default, no per-rule/column → registry value."""
        app_settings = _app_settings_with_threshold(enabled=True, default=70)
        v = self._version(registry_default=60)
        check = self._render(app_settings=app_settings, version=v)
        assert check["user_metadata"]["pass_threshold"] == "60"

    def test_enabled_per_column_override_wins_over_per_rule(self):
        """Enabled + per-column override on the group's column → that value (strictest)."""
        from databricks_labs_dqx_app.backend.registry_models import RESERVED_COLUMN_PASS_THRESHOLDS_KEY

        app_settings = _app_settings_with_threshold(enabled=True, default=70)
        per_application_tags = {RESERVED_COLUMN_PASS_THRESHOLDS_KEY: {"col1": 95}}
        check = self._render(
            app_settings=app_settings,
            pass_threshold=80,  # per-rule override, overridden by per-column
            per_application_tags=per_application_tags,
        )
        assert check["user_metadata"]["pass_threshold"] == "95"

    def test_disabled_pass_threshold_absent_from_user_metadata(self):
        """Feature disabled → pass_threshold key must NOT appear in user_metadata."""
        app_settings = _app_settings_with_threshold(enabled=False, default=70)
        check = self._render(app_settings=app_settings, pass_threshold=85)
        assert "pass_threshold" not in check["user_metadata"]

    def test_enabled_zero_threshold_emits_zero(self):
        """Zero is a real threshold value — must NOT be treated as falsy."""
        app_settings = _app_settings_with_threshold(enabled=True, default=0)
        check = self._render(app_settings=app_settings)
        assert check["user_metadata"]["pass_threshold"] == "0"
