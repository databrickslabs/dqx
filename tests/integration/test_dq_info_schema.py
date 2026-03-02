"""Integration tests for dq_info_schema: build_dq_info_struct with Spark DataFrames."""

import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType

import databricks.labs.dqx.schema.dq_info_schema as dq_info_schema_module
from databricks.labs.dqx.schema.dq_info_schema import (
    build_dq_info_struct,
    dq_info_item_schema,
)

_TEST_FIELD_A = "_test_dq_info_integration_a"
_TEST_FIELD_B = "_test_dq_info_integration_b"
_REGISTRY_KEY = "_DQ_INFO_FIELDS"


def _get_registry():
    return getattr(dq_info_schema_module, _REGISTRY_KEY)


@pytest.fixture
def register_dq_info_test_fields():
    """Isolate registry to only test fields for the test; restore after."""
    registry = _get_registry()
    saved = dict(registry)
    registry.clear()
    registry[_TEST_FIELD_A] = DoubleType()
    registry[_TEST_FIELD_B] = StringType()
    yield
    registry.clear()
    registry.update(saved)


def test_build_dq_info_struct_column_has_correct_schema(spark, register_dq_info_test_fields):
    """build_dq_info_struct() produces a column whose schema matches dq_info_item_schema()."""
    info_col = build_dq_info_struct()
    df = spark.createDataFrame([(1,)], "id int").withColumn("info", info_col)
    actual = df.schema["info"].dataType
    expected = dq_info_item_schema()
    assert actual == expected


def test_build_dq_info_struct_all_nulls_without_kwargs(spark, register_dq_info_test_fields):
    """With no kwargs, struct fields are null."""
    info_col = build_dq_info_struct()
    df = spark.createDataFrame([(1,)], "id int").withColumn("info", info_col)
    row = df.select("info").first()
    assert row is not None
    info = row["info"]
    assert info is not None
    assert info[_TEST_FIELD_A] is None
    assert info[_TEST_FIELD_B] is None


def test_build_dq_info_struct_with_kwarg_fills_field(spark, register_dq_info_test_fields):
    """Passing a kwarg fills the corresponding field in the struct."""
    info_col = build_dq_info_struct(**{_TEST_FIELD_A: F.lit(1.5), _TEST_FIELD_B: F.lit("x")})
    df = spark.createDataFrame([(1,)], "id int").withColumn("info", info_col)
    row = df.select("info").first()
    assert row is not None
    assert row["info"][_TEST_FIELD_A] == 1.5
    assert row["info"][_TEST_FIELD_B] == "x"


def test_build_dq_info_struct_schema_matches_registry(register_dq_info_test_fields):
    """dq_info_item_schema() field names match the registry."""
    schema = dq_info_item_schema()
    assert schema.fieldNames() == list(_get_registry().keys())
