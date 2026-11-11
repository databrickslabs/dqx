from unittest.mock import PropertyMock, create_autospec

import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.reporting_columns import merge_info_columns


@pytest.mark.parametrize("info_col_names", [None, []])
def test_merge_info_columns_without_info_columns_does_not_read_schema(info_col_names):
    """With nothing to merge the DataFrame is returned as is, without a schema analysis (an RPC on Spark Connect)."""
    df_mock = create_autospec(DataFrame)
    columns = PropertyMock(return_value=["id", "_dq_info"])
    type(df_mock).columns = columns

    result = merge_info_columns("_dq_info", df_mock, info_col_names=info_col_names)

    assert result is df_mock
    columns.assert_not_called()
    df_mock.withColumn.assert_not_called()
