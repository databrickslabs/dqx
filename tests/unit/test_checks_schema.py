import json
from pathlib import Path

import pytest

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.checks_schema import build_checks_json_schema
from databricks.labs.dqx.geo import check_funcs as geo_check_funcs
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY

_SCHEMA_FILE = Path(__file__).resolve().parents[2] / "schemas" / "checks.schema.json"


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

    If a check function was added or changed, regenerate the file and review the diff:

        python -c "import json; from databricks.labs.dqx.checks_schema import build_checks_json_schema as b; \\
        open('schemas/checks.schema.json', 'w', encoding='utf-8').write(json.dumps(b(), indent=2) + '\\n')"
    """
    committed = json.loads(_SCHEMA_FILE.read_text(encoding="utf-8"))
    assert committed == schema, (
        f"{_SCHEMA_FILE.name} is out of date with the check functions. Regenerate it (see the docstring) "
        "and review the diff."
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
