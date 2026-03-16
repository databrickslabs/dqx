"""Unit tests for checks file format serializers and deserializers."""

from io import StringIO

from databricks.labs.dqx.checks_formats import FILE_DESERIALIZERS, FILE_SERIALIZERS
from databricks.labs.dqx.checks_serializer import (
    ChecksDeserializer,
    ChecksNormalizer,
    ChecksSerializer,
    SerializerFactory,
)


def test_file_serializers_keys():
    """FILE_SERIALIZERS supports .json, .yml, .yaml."""
    assert set(FILE_SERIALIZERS.keys()) == {".json", ".yml", ".yaml"}


def test_file_deserializers_keys():
    """FILE_DESERIALIZERS supports .json, .yml, .yaml."""
    assert set(FILE_DESERIALIZERS.keys()) == {".json", ".yml", ".yaml"}


def test_json_roundtrip():
    """Serialize and deserialize checks list with .json."""
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "a"}}},
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}}},
    ]
    serialized = FILE_SERIALIZERS[".json"](checks)
    assert isinstance(serialized, str)
    deserialized = FILE_DESERIALIZERS[".json"](StringIO(serialized))
    assert deserialized == checks


def test_yaml_roundtrip():
    """Serialize and deserialize checks list with .yaml."""
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "a"}}},
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}}},
    ]
    serialized = FILE_SERIALIZERS[".yaml"](checks)
    assert isinstance(serialized, str)
    deserialized = FILE_DESERIALIZERS[".yaml"](serialized)
    assert deserialized == checks


def test_yml_roundtrip():
    """Serialize and deserialize checks list with .yml."""
    checks = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "a"}}},
    ]
    serialized = FILE_SERIALIZERS[".yml"](checks)
    assert isinstance(serialized, str)
    deserialized = FILE_DESERIALIZERS[".yml"](serialized)
    assert deserialized == checks


def test_empty_list_roundtrip():
    """Empty checks list round-trips for all formats."""
    checks = []
    for (ext, serialize_func), (_, deserialize_func) in zip(FILE_SERIALIZERS.items(), FILE_DESERIALIZERS.items()):
        serialized = serialize_func(checks)
        if ext == ".json":
            deserialized = deserialize_func(StringIO(serialized))
        else:
            deserialized = deserialize_func(serialized)
        assert deserialized == checks


def test_yaml_is_in_list_allowed_yes_no_roundtrip_strings():
    """YAML round-trip preserves 'Yes'/'No' as strings; unquoted they are parsed as booleans."""
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "exception", "allowed": ["Yes", "No", "N/A"]},
            },
            "name": "exception_other_value",
        },
    ]
    normalized = ChecksNormalizer.normalize(checks)
    serializer = SerializerFactory.create_serializer(".yaml")
    serialized = serializer.serialize(normalized)
    assert '"Yes"' in serialized or "'Yes'" in serialized
    assert '"No"' in serialized or "'No'" in serialized
    deserialized_raw = serializer.deserialize(StringIO(serialized))
    deserialized = ChecksNormalizer.denormalize(deserialized_raw)
    allowed = deserialized[0]["check"]["arguments"]["allowed"]
    assert allowed == ["Yes", "No", "N/A"], "allowed must round-trip as strings, not [True, False, 'N/A']"
    assert [type(x).__name__ for x in allowed] == ["str", "str", "str"]


def test_checks_serializer_yaml_roundtrip_yes_no():
    """Full ChecksSerializer path: save and load preserves allowed as strings."""
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "exception", "allowed": ["Yes", "No"]},
            },
            "name": "exception_other_value",
        },
    ]
    normalized = ChecksNormalizer.normalize(checks)
    payload = ChecksSerializer.serialize_to_bytes(normalized, ".yaml")
    assert isinstance(payload, bytes)
    loaded = ChecksDeserializer.deserialize_from_file(".yaml", StringIO(payload.decode("utf-8")))
    assert len(loaded) == 1
    args = loaded[0].get("check", {}).get("arguments", {})
    allowed = args.get("allowed")
    assert allowed == ["Yes", "No"], "allowed must be strings after YAML round-trip"
    assert all(isinstance(x, str) for x in allowed)
