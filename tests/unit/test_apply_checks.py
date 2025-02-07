from unittest.mock import MagicMock

from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.col_functions import is_not_null_and_not_empty
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRule
from databricks.sdk import WorkspaceClient


def test_apply_checks(spark_local):
    ws = MagicMock(spec=WorkspaceClient, **{"catalogs.list.return_value": []})

    schema = "a: int, b: int, c: int"
    expected_schema = schema + ", _errors: map<string,string>, _warnings: map<string,string>"
    test_df = spark_local.createDataFrame([[1, None, 3]], schema)

    checks = [
        DQRule(name="col_a_is_null_or_empty", criticality="warn", check=is_not_null_and_not_empty("a")),
        DQRule(name="col_b_is_null_or_empty", criticality="error", check=is_not_null_and_not_empty("b")),
    ]

    dq_engine = DQEngine(ws)
    df = dq_engine.apply_checks(test_df, checks)

    expected_df = spark_local.createDataFrame(
        [[1, None, 3, {"col_b_is_null_or_empty": "Column b is null or empty"}, None]], expected_schema
    )
    assert_df_equality(df, expected_df)
