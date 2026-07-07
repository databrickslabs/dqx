"""Tests for the Pydantic-driven serialization/deserialization migration (issue #1285).

Covers:
- Fingerprint byte-parity: compute_rule_fingerprint output must be identical before/after refactor.
- YAML + JSON round-trip including the __decimal__ wire format.
- ChecksValidator structural validation (Pydantic path) + signature-arg validation.
- ChecksDeserializer dispatch to DQRowRule, DQDatasetRule, and for_each_column.
- validate_custom_check_functions=False tolerance for unknown functions.
- DataFrameConverter row<->dict round-trip.
"""

from decimal import Decimal
from io import StringIO
from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError
from pydantic_core import ErrorDetails
from pyspark.sql import SparkSession

from databricks.labs.dqx.check_funcs import is_not_null
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


def test_checks_validator_reports_criticality_and_argument_errors_together():
    """A bad criticality must not suppress signature errors — both surface in one pass.

    Regression: the pre-migration validator reported the criticality error alongside the
    missing-argument error. Structural validation must only short-circuit on a malformed
    'check' block, not on sibling-field errors like 'criticality'.
    """
    check = {
        "criticality": "bogus",
        "check": {"function": "is_not_null", "arguments": {}},  # missing required 'column'
    }
    status = ChecksValidator.validate_checks([check])
    assert status.has_errors
    assert any("Invalid 'criticality' value: 'bogus'" in e for e in status.errors)
    assert any("No arguments provided for function 'is_not_null'" in e for e in status.errors)


def test_checks_validator_reports_criticality_and_malformed_check_block_together():
    """A bad criticality must surface even when the 'check' block itself is malformed.

    Regression for the parity gap where a field-level failure (here, a missing 'function') skips
    the model validator that checks criticality, so only the check-block error was reported. The
    pre-migration validator always reported criticality regardless of the check block.
    """
    check = {
        "criticality": "bogus",
        "check": {"arguments": {"column": "a"}},  # missing required 'function'
    }
    status = ChecksValidator.validate_checks([check])
    assert status.has_errors
    assert any("Invalid 'criticality' value: 'bogus'" in e for e in status.errors)
    assert any("'function' field is missing in the 'check' block" in e for e in status.errors)


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


@pytest.mark.parametrize(
    ("check", "expected_field"),
    [
        ({"check": {"function": 123, "arguments": {"column": "a"}}}, "check -> function"),
        ({"check": {"function": "is_not_null", "arguments": {"column": "a"}}, "name": 42}, "name"),
        ({"check": {"function": "is_not_null", "arguments": {"column": "a"}}, "filter": 1}, "filter"),
        ({"check": {"function": "is_not_null", "arguments": {"column": "a"}}, "message_expr": 1}, "message_expr"),
    ],
)
def test_checks_validator_field_type_error_names_offending_field(check, expected_field):
    """A wrong-typed field must be reported with its location, not a generic message on the whole dict.

    Regression: the initial Pydantic-error translation dropped the field location, yielding
    "Input should be a valid string: {check}" with no indication of which field was wrong.
    """
    status = ChecksValidator.validate_checks([check])
    assert status.has_errors
    assert any(f"Invalid value for '{expected_field}'" in e for e in status.errors), status.errors


def test_checks_validator_criticality_wrong_type_reports_field():
    """A wrong *type* for criticality (not a bad enum value) must name the field and not be malformed.

    Regression: the criticality branch previously matched any error whose location contained
    'criticality' and stripped a "Value error, " prefix that is absent from type errors, producing
    the malformed "Input should be a valid string Check details: {check}".
    """
    check = {"check": {"function": "is_not_null", "arguments": {"column": "a"}}, "criticality": None}
    status = ChecksValidator.validate_checks([check])
    assert status.has_errors
    assert any("Invalid value for 'criticality'" in e for e in status.errors), status.errors


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
    keeps working. See checks_serializer.project_to_check_schema.
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
# Pydantic error-type pinning
#
# The error translator (ChecksValidator._translate_pydantic_error) branches on Pydantic's
# internal error *type* codes to reproduce the legacy message text. A Pydantic upgrade that
# renamed one of these codes would silently change which branch fires and degrade the message —
# a change the message-string tests above might not catch cleanly. This test asserts the exact
# type codes the translator depends on, so such a rename fails loudly and points at the shim.
# ---------------------------------------------------------------------------


def _first_error_for_loc(check: dict, loc: tuple) -> ErrorDetails:
    """Return the first Pydantic error whose loc matches, validating with the standard context."""
    try:
        CheckSpec.model_validate(check, context={"raw_check": check})
    except ValidationError as exc:
        for err in exc.errors():
            if err["loc"] == loc:
                return err
    raise AssertionError(f"No error at loc {loc} for {check}")


@pytest.mark.parametrize(
    ("check", "loc", "expected_type"),
    [
        # missing 'check' key
        ({"criticality": "error"}, ("check",), "missing"),
        # 'check' is not a mapping
        ({"check": "not_a_dict"}, ("check",), "model_type"),
        # missing 'function' inside the check block
        ({"check": {"arguments": {"column": "a"}}}, ("check", "function"), "missing"),
        # for_each_column not a list
        (
            {"check": {"function": "is_not_null", "for_each_column": "x", "arguments": {"column": "a"}}},
            ("check", "for_each_column"),
            "list_type",
        ),
        # for_each_column empty (field validator raises ValueError -> value_error)
        (
            {"check": {"function": "is_not_null", "for_each_column": [], "arguments": {"column": "a"}}},
            ("check", "for_each_column"),
            "value_error",
        ),
        # arguments not a dict
        ({"check": {"function": "is_not_null", "arguments": "nope"}}, ("check", "arguments"), "dict_type"),
        # semantic errors (criticality value, function resolution, argument validation) surface as a
        # single model-level value_error with an empty loc
        (
            {"criticality": "bogus", "check": {"function": "is_not_null", "arguments": {"column": "a"}}},
            (),
            "value_error",
        ),
    ],
)
def test_translator_relies_on_stable_pydantic_error_types(check, loc, expected_type):
    """Pin the Pydantic error *type* codes the translation shim branches on (finding #4)."""
    error = _first_error_for_loc(check, loc)
    assert error["type"] == expected_type, error


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


def test_checks_validator_argument_type_check_is_strict():
    """Argument type validation must be strict — a stringy int must not be coerced through.

    The TypeAdapter-based check runs in strict mode, so "5" does not silently satisfy an int
    parameter (which lax pydantic coercion would allow); a real int does.
    """
    bad = [{"check": {"function": "is_older_than_n_days", "arguments": {"column": "a", "days": "5"}}}]
    status = ChecksValidator.validate_checks(bad)
    assert status.has_errors
    assert any("Argument 'days' should be of type 'int'" in e for e in status.errors), status.errors

    good = [{"check": {"function": "is_older_than_n_days", "arguments": {"column": "a", "days": 5}}}]
    assert not ChecksValidator.validate_checks(good).has_errors


def test_check_spec_validate_check_binds_tolerance_context():
    """CheckSpec.validate_check must bind the semantic-validation context.

    Regression for the footgun where a bare *model_validate* ignored the tolerance flag and
    rejected every unknown function. Going through *validate_check* honours it.
    """
    check = {"check": {"function": "totally_unknown_func", "arguments": {}}}
    # Tolerated when the context flag routes through validate_check.
    spec = CheckSpec.validate_check(check, None, False)
    assert spec.check.function == "totally_unknown_func"
    # Enforced by default.
    with pytest.raises(ValidationError):
        CheckSpec.validate_check(check)


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


@pytest.mark.parametrize(
    "user_metadata",
    [
        {"confidence": 0.95},  # float
        {"attempts": 3},  # int
        {"enabled": True},  # bool
        {"threshold": Decimal("1.5")},  # Decimal
        {"owner": "team-a", "confidence": 0.95},  # mixed str + non-str
    ],
)
def test_deserialize_preserves_non_string_user_metadata(user_metadata):
    """user_metadata with non-string values must load without InvalidCheckError (regression).

    The pre-Pydantic-migration validator never inspected user_metadata, so checks carrying
    non-string values loaded fine. Narrowing the field to dict[str, str] regressed that; the
    field is kept dict[str, Any] to preserve released behaviour.
    """
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "user_metadata": user_metadata,
        }
    ]

    # Structural validation must not flag the non-string values.
    status = ChecksValidator.validate_checks(checks)
    assert not status.has_errors, status.errors

    # Deserialization must preserve the values verbatim (no coercion to str).
    rules = deserialize_checks(checks)
    assert rules[0].user_metadata == user_metadata


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
