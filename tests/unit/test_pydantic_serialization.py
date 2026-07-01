"""Tests for the Pydantic-driven serialization/deserialization migration (issue #1285).

Covers:
- Fingerprint byte-parity: compute_rule_fingerprint output must be identical before/after refactor.
- YAML + JSON round-trip including the __decimal__ wire format.
- ChecksValidator structural validation (Pydantic path) + signature-arg validation.
- ChecksDeserializer dispatch to DQRowRule, DQDatasetRule, and for_each_column.
- validate_custom_check_functions=False tolerance for unknown functions.
- DataFrameConverter row<->dict round-trip.
- checks_formats consolidation: FILE_SERIALIZERS/FILE_DESERIALIZERS are derived from SerializerFactory.
"""

import json
from decimal import Decimal
from io import StringIO
from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError
from pyspark.sql import SparkSession

from databricks.labs.dqx.check_funcs import is_not_null
from databricks.labs.dqx.checks_formats import FILE_DESERIALIZERS, FILE_SERIALIZERS
from databricks.labs.dqx.checks_serializer import (
    ChecksDeserializer,
    ChecksNormalizer,
    ChecksSerializer,
    SerializerFactory,
    deserialize_checks,
    project_to_check_schema,
    serialize_checks,
)
from databricks.labs.dqx.checks_storage import DataFrameConverter
from databricks.labs.dqx.checks_validator import ChecksValidator, CheckSpec
from databricks.labs.dqx.errors import InvalidCheckError
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule, compute_rule_fingerprint
from databricks.labs.dqx.rule_fingerprint import compute_rule_set_fingerprint_by_metadata


# ---------------------------------------------------------------------------
# Fingerprint byte-parity corpus
# These SHA-256 hex strings were captured from the codebase BEFORE the Pydantic
# migration, and must remain byte-identical afterwards.
# ---------------------------------------------------------------------------

_FINGERPRINT_CORPUS: list[tuple[dict, str]] = [
    # 0 — row rule with explicit column
    (
        {
            "name": "id_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
        "c7cdc9ef8525e0a21cfd5bee89d9db592b826040da386db8f4886d759e674ab2",
    ),
    # 1 — dataset-level rule
    (
        {
            "name": "row_count_check",
            "criticality": "warn",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"limit": 100, "aggr_type": "count"},
            },
        },
        "f0f3fee77e26b1536f715fe93f381ba88a9821daf62da1c3a3e73c6baf91172b",
    ),
    # 2 — for_each_column compact rule
    (
        {
            "name": "fec_rule",
            "criticality": "warn",
            "check": {
                "function": "is_not_null",
                "arguments": {},
                "for_each_column": ["a", "b", "c"],
            },
        },
        "299b365ba4d89751b25c54981eb4848a7d1a236c20f8443b8ed853bdb1090eb7",
    ),
    # 3 — rule with filter
    (
        {
            "name": "with_filter",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "filter": "id > 0",
        },
        "48f39b1855fe6076c3711bbfde743fe223c646c19f293510bf83404835d33342",
    ),
    # 4 — rule with user_metadata (excluded from fingerprint)
    (
        {
            "name": "with_meta",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "user_metadata": {"team": "eng"},
        },
        "d17a63634dcc00f35335db33daf55decd11b005c64346f91ad801b194e0273ba",
    ),
    # 5 — rule with __decimal__ markers in arguments (Decimal values serialized to disk)
    (
        {
            "name": "range_check",
            "criticality": "warn",
            "check": {
                "function": "is_in_range",
                "arguments": {
                    "column": "price",
                    "min_limit": {"__decimal__": "1.5"},
                    "max_limit": {"__decimal__": "99.99"},
                },
            },
        },
        "1c1aa9f8948412f50b0b35eace615821a60aca30d514243ef1812ff6e39a62f5",
    ),
    # 6 — rule with message_expr
    (
        {
            "name": "with_msg",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "message_expr": "concat('id is null: ', cast(id as string))",
        },
        "8a605a7e1f40528dab91b6bf77b28ef6376071380df9d6504bff3cdb0e2aba62",
    ),
]


@pytest.mark.parametrize(
    "check_dict,expected_fp", _FINGERPRINT_CORPUS, ids=[f"corpus[{i}]" for i in range(len(_FINGERPRINT_CORPUS))]
)
def test_fingerprint_byte_parity(check_dict: dict, expected_fp: str):
    """compute_rule_fingerprint must produce byte-identical SHA-256 hashes after migration."""
    assert compute_rule_fingerprint(check_dict) == expected_fp


def test_dq_row_rule_fingerprint_matches_corpus():
    """DQRowRule.rule_fingerprint must match the pre-migration baseline for the same logical rule."""
    rule = DQRowRule(name="id_not_null", criticality="error", check_func=is_not_null, column="id")
    baseline = "c7cdc9ef8525e0a21cfd5bee89d9db592b826040da386db8f4886d759e674ab2"
    assert rule.rule_fingerprint == baseline


def test_rule_set_fingerprint_corpus_single_rule():
    """compute_rule_set_fingerprint_by_metadata baseline for a single-rule set."""
    checks = [_FINGERPRINT_CORPUS[0][0]]
    baseline = "f2fd4c82ed44f7db513f5ee0b392ce9dcf27ce5ec08ccdc8dc1d71f8cf6be826"
    assert compute_rule_set_fingerprint_by_metadata(checks) == baseline


# ---------------------------------------------------------------------------
# YAML + JSON round-trip including __decimal__ wire format
# ---------------------------------------------------------------------------

_DECIMAL_CHECK: dict = {
    "criticality": "warn",
    "check": {
        "function": "is_in_range",
        "arguments": {
            "column": "price",
            "min_limit": {"__decimal__": "1.5"},
            "max_limit": {"__decimal__": "9.99"},
        },
    },
}


def test_yaml_roundtrip_with_decimal_marker():
    """YAML round-trip must preserve the __decimal__ wire format unchanged."""
    serializer = SerializerFactory.create_serializer(".yaml")
    checks = [_DECIMAL_CHECK]
    serialized = serializer.serialize(checks)
    assert "__decimal__" in serialized

    deserialized = serializer.deserialize(StringIO(serialized))
    assert deserialized == checks


def test_json_roundtrip_with_decimal_marker():
    """JSON round-trip must preserve the __decimal__ wire format unchanged."""
    serializer = SerializerFactory.create_serializer(".json")
    checks = [_DECIMAL_CHECK]
    serialized = serializer.serialize(checks)
    assert "__decimal__" in serialized

    deserialized = serializer.deserialize(StringIO(serialized))
    assert deserialized == checks


def test_normalize_converts_decimal_to_marker():
    """ChecksNormalizer.normalize must convert Decimal to __decimal__ marker."""
    checks = [
        {
            "criticality": "warn",
            "check": {
                "function": "is_in_range",
                "arguments": {"column": "price", "min_limit": Decimal("1.5"), "max_limit": Decimal("9.99")},
            },
        }
    ]
    normalized = ChecksNormalizer.normalize(checks)
    args = normalized[0]["check"]["arguments"]
    assert args["min_limit"] == {"__decimal__": "1.5"}
    assert args["max_limit"] == {"__decimal__": "9.99"}


def test_denormalize_converts_marker_to_decimal():
    """ChecksNormalizer.denormalize must convert __decimal__ markers back to Decimal."""
    checks = [_DECIMAL_CHECK]
    denormalized = ChecksNormalizer.denormalize(checks)
    args = denormalized[0]["check"]["arguments"]
    assert args["min_limit"] == Decimal("1.5")
    assert args["max_limit"] == Decimal("9.99")


def test_serialize_to_bytes_yaml_with_decimal():
    """ChecksSerializer.serialize_to_bytes must produce UTF-8 YAML with __decimal__ marker."""
    checks_as_dicts = [_DECIMAL_CHECK]
    raw = ChecksSerializer.serialize_to_bytes(checks_as_dicts, ".yaml")
    assert b"__decimal__" in raw


def test_deserialize_from_file_yaml_with_decimal():
    """ChecksDeserializer.deserialize_from_file must denormalize __decimal__ back to Decimal."""
    serialized = SerializerFactory.create_serializer(".yaml").serialize([_DECIMAL_CHECK])
    result = ChecksDeserializer.deserialize_from_file(".yaml", StringIO(serialized))
    args = result[0]["check"]["arguments"]
    assert args["min_limit"] == Decimal("1.5")
    assert args["max_limit"] == Decimal("9.99")


# ---------------------------------------------------------------------------
# ChecksValidator structural validation (Pydantic path)
# ---------------------------------------------------------------------------


def test_pydantic_validates_missing_check_field():
    """CheckSpec.model_validate must reject a dict without a 'check' key."""
    with pytest.raises(ValidationError):
        CheckSpec.model_validate({"criticality": "error"})


def test_pydantic_validates_invalid_criticality():
    """CheckSpec must reject criticality values outside {warn, error}."""
    with pytest.raises(ValidationError):
        CheckSpec.model_validate({"criticality": "invalid", "check": {"function": "is_not_null"}})


def test_pydantic_ignores_extra_top_level_field():
    """CheckSpec must tolerate (ignore, not reject) unknown top-level fields.

    This preserves the pre-migration behaviour: the hand-rolled validator never rejected unknown
    top-level keys, and storage backends persist extra columns (run_config_name, fingerprints,
    timestamps) alongside the check. The extra key must be silently dropped, not raise.
    """
    spec = CheckSpec.model_validate(
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "rule_fingerprint": "abc123",
            "unknown_field": "bad",
        }
    )
    assert not hasattr(spec, "unknown_field")
    assert not hasattr(spec, "rule_fingerprint")


def test_pydantic_validates_for_each_column_must_be_list():
    """CheckSpec must reject for_each_column that is not a list."""
    with pytest.raises(ValidationError):
        CheckSpec.model_validate(
            {
                "check": {"function": "is_not_null", "for_each_column": "not_a_list"},
            }
        )


def test_pydantic_validates_for_each_column_must_not_be_empty():
    """CheckSpec must reject an empty for_each_column list."""
    with pytest.raises(ValidationError):
        CheckSpec.model_validate(
            {
                "check": {"function": "is_not_null", "for_each_column": []},
            }
        )


def test_checks_validator_bad_criticality_produces_correct_message():
    """ChecksValidator must translate invalid criticality into the existing error message format."""
    check = {
        "criticality": "invalid",
        "check": {"function": "is_not_null", "arguments": {"column": "a"}},
    }
    status = ChecksValidator.validate_checks([check])
    assert status.has_errors
    assert any("Invalid 'criticality' value: 'invalid'" in e for e in status.errors)
    assert any("Expected 'warn' or 'error'" in e for e in status.errors)


def test_checks_validator_missing_check_field():
    """ChecksValidator must report missing 'check' field."""
    status = ChecksValidator.validate_checks([{"criticality": "error"}])
    assert status.has_errors
    assert any("'check' field is missing" in e for e in status.errors)


def test_checks_validator_check_not_dict():
    """ChecksValidator must report when 'check' is not a dict."""
    status = ChecksValidator.validate_checks([{"criticality": "error", "check": "not_a_dict"}])
    assert status.has_errors
    assert any("'check' field should be a dictionary" in e for e in status.errors)


def test_checks_validator_missing_function():
    """ChecksValidator must report missing 'function' in check block."""
    status = ChecksValidator.validate_checks([{"criticality": "error", "check": {"arguments": {"column": "a"}}}])
    assert status.has_errors
    assert any("'function' field is missing" in e for e in status.errors)


def test_checks_validator_missing_check_reports_missing_not_extra():
    """A check whose 'check' key is misspelled reports the missing 'check', tolerating the stray key.

    Unknown top-level keys are ignored (extra="ignore"); the actionable error is the missing 'check'.
    """
    check = {
        "criticality": "warn",
        "check_invalid_field": {"function": "is_not_null", "arguments": {"column": "b"}},
    }
    status = ChecksValidator.validate_checks([check])
    assert status.has_errors
    assert any("'check' field is missing" in e for e in status.errors)


def test_checks_validator_tolerates_storage_columns_on_valid_check():
    """Regression: a valid check carrying storage-internal columns must NOT raise.

    Checks loaded from a Delta or Lakebase table arrive with storage-only columns
    (run_config_name, created_at, rule_fingerprint, rule_set_fingerprint). The load -> apply
    round-trip re-validates them; extra="ignore" must let them through so applying stored checks
    keeps working. See LakebaseChecksStorageHandler._project_row_to_check.
    """
    stored_check = {
        "name": "id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        "filter": None,
        "run_config_name": "default",
        "user_metadata": None,
        "created_at": "2026-01-01T00:00:00Z",
        "rule_fingerprint": "abc123",
        "rule_set_fingerprint": "def456",
    }
    status = ChecksValidator.validate_checks([stored_check])
    assert not status.has_errors


def test_project_to_check_schema_strips_storage_columns():
    """project_to_check_schema keeps only logical check keys, dropping storage-only columns."""
    row = {
        "name": "id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        "filter": None,
        "user_metadata": None,
        "run_config_name": "default",
        "created_at": "2026-01-01T00:00:00Z",
        "rule_fingerprint": "abc123",
        "rule_set_fingerprint": "def456",
    }
    projected = project_to_check_schema(row)
    assert set(projected) == {"name", "criticality", "check", "filter", "user_metadata"}
    assert not ChecksValidator.validate_checks([projected]).has_errors


# ---------------------------------------------------------------------------
# Signature-based argument validation (retained from hand-rolled code)
# ---------------------------------------------------------------------------


def test_checks_validator_missing_required_arg():
    """Missing required argument for a known check function must be reported."""
    check = {"check": {"function": "regex_match", "arguments": {"negate": False, "regex": "^a"}}}
    status = ChecksValidator.validate_checks([check])
    assert status.has_errors
    assert any("Missing required argument 'column'" in e for e in status.errors)


def test_checks_validator_unknown_argument():
    """Unknown argument for a known check function must be reported."""
    check = {
        "check": {"function": "is_not_null", "arguments": {"column": "a", "bad_arg": "x"}},
    }
    status = ChecksValidator.validate_checks([check])
    assert status.has_errors
    assert any("Unexpected argument 'bad_arg'" in e for e in status.errors)


def test_checks_validator_unknown_function_tolerated_when_disabled():
    """validate_custom_check_functions=False must tolerate unknown function names."""
    checks = [{"check": {"function": "totally_unknown_func", "arguments": {}}}]
    status = ChecksValidator.validate_checks(checks, validate_custom_check_functions=False)
    assert not status.has_errors


def test_checks_validator_unknown_function_reported_when_enabled():
    """Unknown function is reported when validate_custom_check_functions=True (default)."""
    checks = [{"check": {"function": "totally_unknown_func", "arguments": {}}}]
    status = ChecksValidator.validate_checks(checks)
    assert status.has_errors
    assert any("is not defined" in e for e in status.errors)


# ---------------------------------------------------------------------------
# ChecksDeserializer dispatch to DQRowRule, DQDatasetRule, for_each_column
# ---------------------------------------------------------------------------


def test_deserialize_dispatches_to_dq_row_rule():
    """deserialize_checks must produce a DQRowRule for a row-level function."""
    checks = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
    rules = deserialize_checks(checks)
    assert len(rules) == 1
    assert isinstance(rules[0], DQRowRule)
    assert rules[0].criticality == "error"


def test_deserialize_dispatches_to_dq_dataset_rule():
    """deserialize_checks must produce a DQDatasetRule for a dataset-level function."""
    checks = [
        {
            "criticality": "warn",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "*", "limit": 100, "aggr_type": "count"},
            },
        }
    ]
    rules = deserialize_checks(checks)
    assert len(rules) == 1
    assert isinstance(rules[0], DQDatasetRule)


def test_deserialize_expands_for_each_column():
    """deserialize_checks must expand for_each_column into one DQRule per column."""
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {}, "for_each_column": ["a", "b", "c"]},
        }
    ]
    rules = deserialize_checks(checks)
    assert len(rules) == 3
    assert all(isinstance(r, DQRowRule) for r in rules)


def test_deserialize_raises_invalid_check_error_on_bad_input():
    """deserialize_checks must raise InvalidCheckError, never a raw Pydantic ValidationError."""
    checks = [{"criticality": "bad_value", "check": {"function": "is_not_null", "arguments": {"column": "a"}}}]
    with pytest.raises(InvalidCheckError):
        deserialize_checks(checks)


def test_deserialize_preserves_name_and_filter():
    """deserialize_checks must preserve name, criticality, and filter on the produced rule."""
    checks = [
        {
            "name": "my_rule",
            "criticality": "warn",
            "filter": "id > 0",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        }
    ]
    rules = deserialize_checks(checks)
    rule = rules[0]
    assert rule.name == "my_rule"
    assert rule.criticality == "warn"
    assert rule.filter == "id > 0"


def test_serialize_checks_round_trips_via_deserialize():
    """serialize_checks followed by deserialize_checks must produce equivalent rules."""
    original = [
        DQRowRule(name="id_not_null", criticality="error", check_func=is_not_null, column="id"),
    ]
    dicts = serialize_checks(original)
    restored = deserialize_checks(dicts)
    assert len(restored) == 1
    assert restored[0].name == "id_not_null"
    assert restored[0].criticality == "error"


def test_serialize_checks_raises_for_non_dq_rule():
    """serialize_checks must raise InvalidCheckError when passed a non-DQRule object."""
    with pytest.raises(InvalidCheckError):
        serialize_checks([{"not": "a rule"}])  # type: ignore[list-item]


# ---------------------------------------------------------------------------
# Delta DataFrameConverter row<->dict round-trip + fingerprint parity
# ---------------------------------------------------------------------------

_SIMPLE_CHECK = {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
_SIMPLE_CHECK_WITH_FILTER = {
    "criticality": "warn",
    "filter": "id > 0",
    "check": {"function": "is_not_null", "arguments": {"column": "id"}},
}
_FEC_CHECK = {
    "criticality": "warn",
    "check": {"function": "is_not_null", "arguments": {}, "for_each_column": ["a", "b"]},
}


def test_to_dataframe_row_structure():
    """DataFrameConverter.to_dataframe must pass a list of rows to spark.createDataFrame."""
    spark = create_autospec(SparkSession)
    DataFrameConverter.to_dataframe(spark, [_SIMPLE_CHECK])
    rows = spark.createDataFrame.call_args.args[0]
    assert len(rows) == 1
    row = rows[0]
    # name=0, criticality=1, check_struct=2, filter=3, run_config_name=4, user_metadata=5,
    # created_at=6, rule_fingerprint=7, rule_set_fingerprint=8
    assert row[1] == "error"
    assert row[2]["function"] == "is_not_null"


def test_to_dataframe_arguments_are_json_strings():
    """DataFrameConverter must json.dumps each argument value for MAP<STRING, STRING>."""
    spark = create_autospec(SparkSession)
    DataFrameConverter.to_dataframe(spark, [_SIMPLE_CHECK])
    rows = spark.createDataFrame.call_args.args[0]
    check_struct = rows[0][2]
    # arguments must be a dict of str -> str (json.dumps'd)
    for arg_value in check_struct["arguments"].values():
        assert isinstance(arg_value, str)


def test_to_dataframe_with_filter():
    """DataFrameConverter must include the filter in the row."""
    spark = create_autospec(SparkSession)
    DataFrameConverter.to_dataframe(spark, [_SIMPLE_CHECK_WITH_FILTER])
    rows = spark.createDataFrame.call_args.args[0]
    assert rows[0][3] == "id > 0"


def test_to_dataframe_fec_compact_row():
    """DataFrameConverter must store for_each_column rules in compact (unexpanded) form."""
    spark = create_autospec(SparkSession)
    DataFrameConverter.to_dataframe(spark, [_FEC_CHECK])
    rows = spark.createDataFrame.call_args.args[0]
    # A compact for_each_column check produces a single row.
    assert len(rows) == 1
    assert rows[0][2]["for_each_column"] == ["a", "b"]


def test_to_dataframe_rule_fingerprint_parity():
    """Rule fingerprint in the DataFrame row must match compute_rule_fingerprint(rule.to_dict())."""
    spark = create_autospec(SparkSession)
    DataFrameConverter.to_dataframe(spark, [_SIMPLE_CHECK])
    rows = spark.createDataFrame.call_args.args[0]
    stored_fp = rows[0][7]

    # Deserialize to get the autogenerated name, then fingerprint
    rule = deserialize_checks([_SIMPLE_CHECK])[0]
    expected_fp = rule.rule_fingerprint
    assert stored_fp == expected_fp


def test_to_dataframe_rule_set_fingerprint_deterministic():
    """Rule set fingerprint must be the same across two identical calls."""
    spark = create_autospec(SparkSession)
    DataFrameConverter.to_dataframe(spark, [_SIMPLE_CHECK])
    fp1 = spark.createDataFrame.call_args.args[0][0][8]
    DataFrameConverter.to_dataframe(spark, [_SIMPLE_CHECK])
    fp2 = spark.createDataFrame.call_args.args[0][0][8]
    assert fp1 == fp2


# ---------------------------------------------------------------------------
# checks_formats consolidation: FILE_SERIALIZERS/FILE_DESERIALIZERS derived from SerializerFactory
# ---------------------------------------------------------------------------


def test_file_serializers_keys_match_serializer_factory():
    """FILE_SERIALIZERS must cover exactly the same extensions as SerializerFactory."""
    assert set(FILE_SERIALIZERS.keys()) == set(SerializerFactory.get_supported_extensions())


def test_file_deserializers_keys_match_serializer_factory():
    """FILE_DESERIALIZERS must cover exactly the same extensions as SerializerFactory."""
    assert set(FILE_DESERIALIZERS.keys()) == set(SerializerFactory.get_supported_extensions())


def test_file_serializers_json_output_matches_serializer_factory():
    """FILE_SERIALIZERS['.json'] and SerializerFactory JSON serialize must produce identical output."""
    checks = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "a"}}}]
    via_map = FILE_SERIALIZERS[".json"](checks)
    via_factory = SerializerFactory.create_serializer(".json").serialize(checks)
    assert via_map == via_factory


def test_file_deserializers_json_output_matches_serializer_factory():
    """FILE_DESERIALIZERS['.json'] must produce the same result as SerializerFactory JSON deserialize."""
    checks = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "a"}}}]
    serialized = json.dumps(checks)
    via_map = FILE_DESERIALIZERS[".json"](StringIO(serialized))
    via_factory = SerializerFactory.create_serializer(".json").deserialize(StringIO(serialized))
    assert via_map == via_factory
