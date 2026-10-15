"""Unit coverage for generated anomaly feature names under Spark's column resolver."""

from unittest.mock import create_autospec

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.transformers import ColumnTypeInfo
from databricks.labs.dqx.anomaly.validation import validate_generated_feature_names
from databricks.labs.dqx.errors import InvalidParameterError


def _fake_df(*, case_sensitive: bool, schema: dict[str, T.DataType] | None = None) -> DataFrame:
    """Return the minimal DataFrame shape the validation boundary reads, without Spark."""
    df = create_autospec(DataFrame, instance=True)
    schema = schema or {
        "amount": T.DoubleType(),
        "AMOUNT_REL_BASELINE": T.DoubleType(),
        "region": T.StringType(),
    }
    df.schema = T.StructType([T.StructField(name, data_type, True) for name, data_type in schema.items()])
    df.columns = [field.name for field in df.schema.fields]
    df.sparkSession.conf.get.return_value = str(case_sensitive).lower()
    return df


def _column_infos() -> list[ColumnTypeInfo]:
    return [
        ColumnTypeInfo("amount", T.DoubleType(), "numeric", null_count=0),
        ColumnTypeInfo("AMOUNT_REL_BASELINE", T.DoubleType(), "numeric", null_count=0),
    ]


def test_case_insensitive_resolver_refuses_a_case_only_generated_feature_collision() -> None:
    with pytest.raises(InvalidParameterError, match="overwrite existing columns"):
        validate_generated_feature_names(
            _fake_df(case_sensitive=False),
            ["amount", "AMOUNT_REL_BASELINE"],
            ["region"],
            None,
            column_infos=_column_infos(),
        )


def test_case_sensitive_resolver_keeps_case_distinct_generated_features_supported() -> None:
    validate_generated_feature_names(
        _fake_df(case_sensitive=True),
        ["amount", "AMOUNT_REL_BASELINE"],
        ["region"],
        None,
        column_infos=_column_infos(),
    )


def test_case_insensitive_resolver_does_not_expand_unicode_identifiers() -> None:
    """Spark does not treat a single sharp-s as the two-letter string ``SS``."""
    df = _fake_df(
        case_sensitive=False,
        schema={
            "Straße": T.DoubleType(),
            "STRASSE_REL_BASELINE": T.DoubleType(),
            "region": T.StringType(),
        },
    )

    validate_generated_feature_names(
        df,
        ["Straße", "STRASSE_REL_BASELINE"],
        ["region"],
        None,
        column_infos=[
            ColumnTypeInfo("Straße", T.DoubleType(), "numeric", null_count=0),
            ColumnTypeInfo("STRASSE_REL_BASELINE", T.DoubleType(), "numeric", null_count=0),
        ],
    )
