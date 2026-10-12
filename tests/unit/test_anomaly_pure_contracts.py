"""Anomaly contracts that need neither Spark execution nor a workspace."""

import pytest
from pyspark.sql.types import DoubleType, MapType, StringType

from databricks.labs.dqx.anomaly.explainability import format_contributions_map
from databricks.labs.dqx.anomaly.scoring_utils import create_udf_schema


@pytest.mark.parametrize("contributions", [False, True])
def test_udf_schema_includes_typed_contribution_maps(contributions: bool) -> None:
    schema = create_udf_schema(contributions)
    assert schema["anomaly_score"].dataType == DoubleType()
    expected = ["anomaly_score"]
    if contributions:
        expected.extend(["anomaly_contributions", "anomaly_basis_contributions"])
        for name in expected[1:]:
            assert schema[name].dataType == MapType(StringType(), DoubleType())
    assert schema.fieldNames() == expected


@pytest.mark.parametrize(
    "values,expected",
    [
        (None, ""),
        ({}, ""),
        ({"a": None, "b": 0.0}, ""),
        ({"a": 70.0, "b": None, "c": 20.0, "d": 10.0}, "a (70%), c (20%)"),
    ],
)
def test_format_contributions_reports_only_the_largest_positive_shares(
    values: dict[str, float | None] | None, expected: str
) -> None:
    assert format_contributions_map(values, 2) == expected
