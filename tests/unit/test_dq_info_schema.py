"""Unit tests for dq_info_schema: registry and schema"""

from collections.abc import Generator

import pytest
from pyspark.sql.types import DoubleType, StringType, StructType

import databricks.labs.dqx.schema.dq_info_schema as dq_info_schema_module
from databricks.labs.dqx.schema.dq_info_schema import (
    dq_info_item_schema,
    register_dq_info_field,
)

_TEST_FIELD_PREFIX = "_test_dq_info_schema_"
_REGISTRY_KEY = "_DQ_INFO_FIELDS"


def _get_registry():
    return getattr(dq_info_schema_module, _REGISTRY_KEY)


@pytest.fixture
def cleaned_test_fields() -> Generator[list[str], None, None]:
    """Yield a list; append any test field names you register. They are unregistered in teardown."""
    to_clean: list[str] = []
    yield to_clean
    registry = _get_registry()
    for name in to_clean:
        registry.pop(name, None)


def test_register_dq_info_field_adds_new_field(cleaned_test_fields):
    """Registering a new name adds it to the schema (first registration wins)."""
    name = _TEST_FIELD_PREFIX + "placeholder"
    cleaned_test_fields.append(name)

    register_dq_info_field(name, DoubleType())

    schema = dq_info_item_schema()
    field_names = [f.name for f in schema.fields]
    assert name in field_names


def test_register_dq_info_field_duplicate_ignored(cleaned_test_fields):
    """Registering the same name again does not add a duplicate (first wins)."""
    name = _TEST_FIELD_PREFIX + "dup"
    cleaned_test_fields.append(name)

    register_dq_info_field(name, DoubleType())
    count_before = len(_get_registry())
    register_dq_info_field(name, DoubleType())

    count_after = len(_get_registry())
    assert count_after == count_before


def test_dq_info_item_schema_returns_struct_type(cleaned_test_fields):
    """dq_info_item_schema returns a StructType; fields are nullable."""
    name = _TEST_FIELD_PREFIX + "struct_type"
    cleaned_test_fields.append(name)

    register_dq_info_field(name, DoubleType())

    schema = dq_info_item_schema()
    assert isinstance(schema, StructType)
    assert len(schema.fields) >= 1
    for f in schema.fields:
        assert f.nullable


def test_dq_info_item_schema_field_order_preserved(cleaned_test_fields):
    """Registered fields appear in insertion order in the schema."""
    field_a = _TEST_FIELD_PREFIX + "a"
    field_b = _TEST_FIELD_PREFIX + "b"
    cleaned_test_fields.append(field_a)
    cleaned_test_fields.append(field_b)

    register_dq_info_field(field_a, DoubleType())
    register_dq_info_field(field_b, StringType())

    schema = dq_info_item_schema()
    names = [f.name for f in schema.fields]
    assert names.index(field_a) < names.index(field_b)
