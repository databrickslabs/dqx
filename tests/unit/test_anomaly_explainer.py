"""Pure validation for the contributor-message API; Spark output lives in integration tests."""

from unittest.mock import create_autospec

import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.explainability import add_top_contributors_to_message
from databricks.labs.dqx.errors import InvalidParameterError


@pytest.mark.parametrize("columns", [[], ["anomaly_contributions", "other"]])
def test_add_top_contributors_requires_a_public_severity_source(columns: list[str]) -> None:
    df = create_autospec(DataFrame, instance=True)
    df.columns = columns
    with pytest.raises(InvalidParameterError, match="severity_percentile is required"):
        add_top_contributors_to_message(df, threshold=60.0)
