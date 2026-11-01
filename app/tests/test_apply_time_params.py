"""Apply-time parameter validation, overlay, and leftover-placeholder guards."""

from unittest.mock import create_autospec

import pytest

from databricks_labs_dqx_app.backend.apply_time_params import (
    InvalidParameterValueError,
    UnboundParameterError,
    assert_no_placeholders,
    coerce_parameter_value,
    is_sql_param_placeholder_name,
    mapping_group_key,
    overlay_parameters,
    substitute_param_placeholders,
    validate_applied_parameter_values,
    validate_group_parameter_values,
)
from databricks_labs_dqx_app.backend.registry_models import RuleParameter


def _param(name: str, type_: str, value=None) -> RuleParameter:
    return RuleParameter(name=name, type=type_, value=value)


class TestCoerceParameterValue:
    def test_max_length_requires_positive_int(self):
        param = _param("column_max_length", "number")
        assert coerce_parameter_value(param, "32") == 32
        with pytest.raises(InvalidParameterValueError, match="positive whole number"):
            coerce_parameter_value(param, 0)
        with pytest.raises(InvalidParameterValueError, match="whole number"):
            coerce_parameter_value(param, "1.5")
        with pytest.raises(InvalidParameterValueError, match="required"):
            coerce_parameter_value(param, "")
        with pytest.raises(InvalidParameterValueError, match="required"):
            coerce_parameter_value(param, "{{column_max_length}}")

    def test_range_min_not_greater_than_max(self):
        params = [_param("min_limit", "number"), _param("max_limit", "number")]
        validate_group_parameter_values(params, {"min_limit": 1, "max_limit": 10}, require_all=True)
        with pytest.raises(InvalidParameterValueError, match="must not be greater"):
            validate_group_parameter_values(params, {"min_limit": 10, "max_limit": 1}, require_all=True)


class TestSqlPlaceholderClassification:
    def test_column_max_length_is_a_parameter_not_a_slot(self):
        assert is_sql_param_placeholder_name("column_max_length") is True
        assert is_sql_param_placeholder_name("limit") is True
        assert is_sql_param_placeholder_name("threshold") is True
        assert is_sql_param_placeholder_name("column") is False
        assert is_sql_param_placeholder_name("order_amount") is False


class TestOverlayAndSubstitute:
    def test_overlay_prefers_apply_time_value(self):
        params = [_param("limit", "number", "{{column_max_length}}")]
        overlaid = overlay_parameters(params, {"limit": 40})
        assert overlaid[0].value == 40

    def test_substitute_replaces_whole_placeholder_with_number(self):
        assert substitute_param_placeholders("{{column_max_length}}", {"column_max_length": 12}) == 12
        assert (
            substitute_param_placeholders("length(col) <= {{column_max_length}}", {"column_max_length": 12})
            == "length(col) <= 12"
        )

    def test_leftover_placeholder_fails_closed(self):
        with pytest.raises(UnboundParameterError, match="column_max_length"):
            assert_no_placeholders({"limit": "{{column_max_length}}"})
        assert_no_placeholders({"query": "SELECT * FROM {{input_view}}"})

    def test_per_column_values_are_keyed_by_mapping(self):
        params = [_param("limit", "number")]
        stored = {
            mapping_group_key({"column": "email"}): {"limit": 50},
            mapping_group_key({"column": "name"}): {"limit": 100},
        }
        out = validate_applied_parameter_values(params, [{"column": "email"}, {"column": "name"}], stored)
        assert out[mapping_group_key({"column": "email"})]["limit"] == 50
        assert out[mapping_group_key({"column": "name"})]["limit"] == 100


_LEGACY_SCALE_DEFINITION = {
    "body": {"predicate": "length(split(CAST({{column}} AS STRING), '[.]')[1]) > {{allowed_scale}}"},
    "slots": [
        {"name": "column", "family": "any", "position": 0, "cardinality": "one", "arg_key": None},
        {"name": "allowed_scale", "family": "any", "position": 1, "cardinality": "one", "arg_key": None},
    ],
    "parameters": [],
}


class TestLegacySqlParameterSlots:
    def test_value_named_sql_slot_is_promoted_to_a_parameter(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleDefinition

        definition = RuleDefinition.model_validate(_LEGACY_SCALE_DEFINITION)
        assert [s.name for s in definition.slots] == ["column"]
        assert [(p.name, p.type, p.value) for p in definition.parameters] == [("allowed_scale", "integer", None)]

    def test_declared_parameters_keep_slots_as_written(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleDefinition

        definition = RuleDefinition.model_validate(
            {
                "body": {"predicate": "{{pattern_id}} > {{threshold}}"},
                "slots": [{"name": "pattern_id", "family": "numeric", "position": 0}],
                "parameters": [{"name": "threshold", "type": "decimal"}],
            }
        )
        assert [s.name for s in definition.slots] == ["pattern_id"]
        assert [p.name for p in definition.parameters] == ["threshold"]

    def test_native_slots_are_left_alone(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleDefinition

        definition = RuleDefinition.model_validate(
            {
                "body": {"function": "is_in_list", "arguments": {"column": "{{allowed}}"}},
                "slots": [{"name": "allowed", "family": "any", "position": 0, "cardinality": "one"}],
            }
        )
        assert [s.name for s in definition.slots] == ["allowed"]
        assert definition.parameters == []

    def test_stale_mapping_key_is_dropped_and_values_rekeyed(self):
        from databricks_labs_dqx_app.backend.apply_time_params import prune_parameter_mapping
        from databricks_labs_dqx_app.backend.registry_models import RuleDefinition

        definition = RuleDefinition.model_validate(_LEGACY_SCALE_DEFINITION)
        legacy_group = {"column": "amount", "allowed_scale": "review"}
        mapping, stored = prune_parameter_mapping(
            [legacy_group],
            definition.parameters,
            definition.slots,
            {mapping_group_key(legacy_group): {"allowed_scale": "2"}},
        )
        assert mapping == [{"column": "amount"}]
        assert stored == {mapping_group_key({"column": "amount"}): {"allowed_scale": "2"}}
        validated = validate_applied_parameter_values(definition.parameters, mapping, stored)
        assert validated[mapping_group_key({"column": "amount"})] == {"allowed_scale": 2}

    def test_legacy_sql_rule_renders_with_apply_time_scale(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleDefinition, RuleVersion
        from databricks_labs_dqx_app.backend.services.materializer import render_check

        definition = RuleDefinition.model_validate(_LEGACY_SCALE_DEFINITION)
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="sql",
            version=version,
            group={"column": "amount"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
            apply_parameter_values={"allowed_scale": 2},
        )
        rendered = str(check)
        assert "{{" not in rendered
        assert "> 2" in rendered
        assert "amount" in rendered
        assert set(check["check"]["arguments"]) == {"expression", "negate", "columns"}


def _app_settings_stub():
    from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_label_definitions.return_value = []
    mock.get_pass_threshold_enabled.return_value = False
    mock.get_default_pass_threshold.return_value = 70
    return mock


class TestRenderCheckApplyTimeParams:
    def test_native_limit_placeholder_is_replaced_per_column(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleDefinition, RuleVersion
        from databricks_labs_dqx_app.backend.services.materializer import render_check

        definition = RuleDefinition.model_validate(
            {
                "body": {
                    "function": "is_not_greater_than",
                    "arguments": {"column": "{{column}}", "limit": "{{column_max_length}}"},
                },
                "slots": [{"name": "column", "family": "text", "position": 0, "cardinality": "one"}],
                "parameters": [{"name": "limit", "type": "number", "value": "{{column_max_length}}"}],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        check, _ = render_check(
            mode="dqx_native",
            version=version,
            group={"column": "email"},
            effective_severity="Medium",
            per_application_tags={},
            registry_rule_id="r1",
            registry_version=1,
            applied_rule_id="ar1",
            app_settings=_app_settings_stub(),
            apply_parameter_values={"limit": 40},
        )
        assert check["check"]["arguments"] == {"column": "email", "limit": 40}

    def test_unbound_placeholder_never_reaches_the_check(self):
        from databricks_labs_dqx_app.backend.registry_models import RuleDefinition, RuleVersion
        from databricks_labs_dqx_app.backend.services.materializer import render_check

        definition = RuleDefinition.model_validate(
            {
                "body": {
                    "function": "is_not_greater_than",
                    "arguments": {"column": "{{column}}", "limit": "{{column_max_length}}"},
                },
                "slots": [{"name": "column", "family": "text", "position": 0, "cardinality": "one"}],
                "parameters": [{"name": "limit", "type": "number", "value": "{{column_max_length}}"}],
            }
        )
        version = RuleVersion(rule_id="r1", version=1, definition=definition, user_metadata={})
        with pytest.raises(UnboundParameterError, match="limit"):
            render_check(
                mode="dqx_native",
                version=version,
                group={"column": "email"},
                effective_severity="Medium",
                per_application_tags={},
                registry_rule_id="r1",
                registry_version=1,
                applied_rule_id="ar1",
                app_settings=_app_settings_stub(),
            )


class TestTypedParameters:
    def test_integer_decimal_and_date_coercion(self):
        assert coerce_parameter_value(_param("offset", "integer"), "-3") == -3
        with pytest.raises(InvalidParameterValueError, match="whole number"):
            coerce_parameter_value(_param("offset", "integer"), "2.5")
        assert coerce_parameter_value(_param("threshold", "decimal"), "0.25") == 0.25
        with pytest.raises(InvalidParameterValueError, match="must be a number"):
            coerce_parameter_value(_param("threshold", "decimal"), "abc")
        assert coerce_parameter_value(_param("launch_date", "date"), "2026-03-01") == "2026-03-01"
        with pytest.raises(InvalidParameterValueError, match="YYYY-MM-DD"):
            coerce_parameter_value(_param("launch_date", "date"), "01/03/2026")

    def test_sql_rules_require_every_parameter(self):
        params = [_param("status", "string")]
        assert validate_group_parameter_values(params, {}, require_all=True) == {}
        with pytest.raises(InvalidParameterValueError, match="status is required"):
            validate_group_parameter_values(params, {}, require_all=True, bind_all=True)

    def test_declarations_reject_collisions_and_bad_names(self):
        from databricks_labs_dqx_app.backend.apply_time_params import validate_parameter_declarations
        from databricks_labs_dqx_app.backend.registry_models import RuleSlot

        column = RuleSlot(name="amount", family="numeric")
        validate_parameter_declarations([_param("threshold", "decimal")], [column])
        with pytest.raises(ValueError, match="both a column and a parameter"):
            validate_parameter_declarations([_param("amount", "decimal")], [column])
        with pytest.raises(ValueError, match="more than once"):
            validate_parameter_declarations([_param("x", "integer"), _param("x", "date")], [])
        with pytest.raises(ValueError, match="must start with a letter"):
            validate_parameter_declarations([_param("1x", "integer")], [])
        with pytest.raises(ValueError, match="reserved"):
            validate_parameter_declarations([_param("input_view", "string")], [])


class TestSqlParameterSubstitution:
    def _sub(self, text: str, *params: RuleParameter) -> str:
        from databricks_labs_dqx_app.backend.apply_time_params import substitute_sql_parameters

        return substitute_sql_parameters(text, list(params))

    def test_numbers_are_bare(self):
        assert self._sub("x > {{t}}", _param("t", "decimal", 0.5)) == "x > 0.5"
        assert self._sub("x > {{n}}", _param("n", "integer", 3)) == "x > 3"

    def test_strings_dates_and_lists_become_literals(self):
        assert self._sub("s = {{v}}", _param("v", "string", "it's")) == "s = 'it\\'s'"
        assert self._sub("d >= {{d}}", _param("d", "date", "2026-03-01")) == "d >= DATE'2026-03-01'"
        assert self._sub("s IN ({{a}})", _param("a", "list", ["A", "B"])) == "s IN ('A', 'B')"
        assert self._sub("b = {{f}}", _param("f", "boolean", True)) == "b = TRUE"

    def test_author_quoted_tokens_get_escaped_content_only(self):
        assert self._sub("s rlike '{{r}}'", _param("r", "regex", "^\\d+$")) == "s rlike '^\\\\d+$'"
        assert self._sub("d >= '{{d}}'", _param("d", "date", "2026-03-01")) == "d >= '2026-03-01'"

    def test_unbound_parameters_are_left_for_the_leftover_guard(self):
        assert self._sub("x > {{t}}", _param("t", "decimal")) == "x > {{t}}"
