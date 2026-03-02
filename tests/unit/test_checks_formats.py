"""Unit tests for checks file format serializers and deserializers."""

from io import StringIO

from databricks.labs.dqx.checks_formats import FILE_DESERIALIZERS, FILE_SERIALIZERS


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
