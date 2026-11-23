import json
import re
from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.checks_schema import build_checks_json_schema
from databricks.labs.dqx.geo import check_funcs as geo_check_funcs
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY

_SCHEMA_FILE = Path(__file__).resolve().parents[2] / "schemas" / "checks.schema.json"
_RESOURCES = Path(__file__).resolve().parents[1] / "resources"
_CHECK_RESOURCES = [
    "all_row_checks.yaml",
    "all_dataset_checks.yaml",
    "all_row_geo_checks.yaml",
    "all_dateset_geo_checks.yaml",
]


@pytest.fixture(scope="module")
def schema() -> dict:
    return build_checks_json_schema()


def _function_rule(checks_schema: dict, function: str) -> dict:
    rules = checks_schema["definitions"]["CheckBlock"]["allOf"]
    return next(rule["then"] for rule in rules if rule["if"]["properties"]["function"]["const"] == function)


def _arguments(checks_schema: dict, function: str) -> dict:
    return _function_rule(checks_schema, function)["properties"]["arguments"]


def test_committed_schema_matches_generated(schema):
    """The committed schema must match the check functions in the code.

    If a check function was added or changed, regenerate the file with *make checks-schema* and review the diff.
    """
    committed = json.loads(_SCHEMA_FILE.read_text(encoding="utf-8"))
    assert committed == schema, (
        f"{_SCHEMA_FILE.name} is out of date with the check functions. Regenerate it with "
        "'make checks-schema' and review the diff."
    )


def test_schema_describes_a_list_of_checks(schema):
    assert schema["$schema"] == "http://json-schema.org/draft-07/schema#"
    assert schema["type"] == "array"
    assert schema["items"] == {"$ref": "#/definitions/CheckSpec"}
    check_spec = schema["definitions"]["CheckSpec"]
    assert check_spec["required"] == ["check"]
    assert check_spec["properties"]["criticality"]["enum"] == ["warn", "error"]


def test_every_built_in_check_function_has_a_rule(schema):
    built_in = sorted(
        name for name in CHECK_FUNC_REGISTRY if hasattr(check_funcs, name) or hasattr(geo_check_funcs, name)
    )
    rules = schema["definitions"]["CheckBlock"]["allOf"]
    assert [rule["if"]["properties"]["function"]["const"] for rule in rules] == built_in
    offered = schema["definitions"]["CheckBlock"]["properties"]["function"]["anyOf"]
    assert [option["const"] for option in offered if "const" in option] == built_in


def test_custom_function_names_are_accepted(schema):
    offered = schema["definitions"]["CheckBlock"]["properties"]["function"]["anyOf"]
    assert offered[-1] == {"type": "string"}


def test_arguments_follow_the_function_signature(schema):
    arguments = _arguments(schema, "is_in_range")
    assert arguments["additionalProperties"] is False
    assert set(arguments["properties"]) == {"column", "min_limit", "max_limit", "allow_nulls"}
    assert arguments["properties"]["allow_nulls"]["type"] == "boolean"
    assert {"type": "string"} in arguments["properties"]["min_limit"]["anyOf"]
    assert arguments["properties"]["min_limit"]["description"]


@pytest.mark.parametrize(
    "function, argument, expected",
    [
        ("is_unique", "columns", {"type": "array", "items": {"type": "string"}}),
        ("is_not_null_and_is_in_list", "allowed", {"type": "array"}),
        ("has_valid_json_schema", "schema", {"type": "string"}),
        ("is_aggr_not_greater_than", "aggr_params", {"anyOf": [{"type": "object"}, {"type": "null"}]}),
    ],
)
def test_argument_types(schema, function: str, argument: str, expected: dict):
    actual = dict(_arguments(schema, function)["properties"][argument])
    actual.pop("description", None)
    assert actual == expected


def test_column_is_required_unless_for_each_column_is_set(schema):
    rule = _function_rule(schema, "is_not_null")
    assert rule["anyOf"] == [
        {"required": ["for_each_column"]},
        {"required": ["arguments"], "properties": {"arguments": {"required": ["column"]}}},
    ]
    assert "required" not in rule["properties"]["arguments"]


def test_other_required_arguments_are_always_required(schema):
    rule = _function_rule(schema, "is_older_than_n_days")
    assert rule["properties"]["arguments"]["required"] == ["days"]
    assert rule["required"] == ["arguments"]


def _as_editor_sees_it(value: object) -> object:
    """PyYAML loads unquoted dates as date objects; editors validate them as the strings they are written as."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, list):
        return [_as_editor_sees_it(item) for item in value]
    if isinstance(value, dict):
        return {key: _as_editor_sees_it(item) for key, item in value.items()}
    return value


@pytest.fixture(scope="module")
def validator(schema):
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.Draft7Validator.check_schema(schema)
    return jsonschema.Draft7Validator(schema)


@pytest.mark.parametrize("resource", _CHECK_RESOURCES)
def test_shipped_check_examples_are_valid(validator, resource: str):
    checks = _as_editor_sees_it(yaml.safe_load((_RESOURCES / resource).read_text(encoding="utf-8")))
    errors = [f"{list(error.absolute_path)}: {error.message}" for error in validator.iter_errors(checks)]
    assert not errors


@pytest.mark.parametrize(
    "check",
    [
        {"check": {"function": "is_not_null", "arguments": {"colum": "a"}}},
        {"check": {"function": "is_not_null", "arguments": {}}},
        {"check": {"function": "is_older_than_n_days", "arguments": {"column": "a"}}},
        {"check": {"function": "is_in_range", "arguments": {"column": "a", "min_limit": [1]}}},
        {"criticality": "fatal", "check": {"function": "is_not_null", "arguments": {"column": "a"}}},
        {"criticality": "error"},
    ],
    ids=["unknown argument", "missing column", "missing required argument", "wrong type", "criticality", "no check"],
)
def test_invalid_checks_are_rejected(validator, check: dict):
    assert list(validator.iter_errors([check]))


@pytest.mark.parametrize(
    "check",
    [
        {"check": {"function": "is_not_null", "for_each_column": ["a", "b"]}},
        {"check": {"function": "my_custom_check", "arguments": {"anything": 1}}},
        {
            "name": "x",
            "filter": "a > 1",
            "user_metadata": {"k": 1},
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        },
    ],
    ids=["for_each_column", "custom function", "optional fields"],
)
def test_valid_checks_are_accepted(validator, check: dict):
    assert not list(validator.iter_errors([check]))


def test_descriptions_are_user_facing(schema):
    definitions = schema["definitions"]
    assert definitions["CheckSpec"]["description"] == "A data quality check."
    assert "pydantic" not in json.dumps(definitions).lower()


def _descriptions(node: object) -> list[str]:
    if isinstance(node, dict):
        own = [node["description"]] if isinstance(node.get("description"), str) else []
        return own + [text for value in node.values() for text in _descriptions(value)]
    if isinstance(node, list):
        return [text for item in node for text in _descriptions(item)]
    return []


def test_descriptions_have_no_docstring_markup(schema):
    marked = [text for text in _descriptions(schema) if re.search(r"(?:^|\s)\*{1,2}\S", text)]
    assert not marked


def test_literal_asterisks_are_kept(schema):
    column = _arguments(schema, "aggr_matches_dataset")["properties"]["column"]["description"]
    assert "count(*)" in column
