from unittest.mock import MagicMock

from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.col_functions import is_not_null_and_not_empty
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRule
from databricks.sdk import WorkspaceClient


def test_apply_checks(spark_local):
    ws = MagicMock(spec=WorkspaceClient, **{"catalogs.list.return_value": []})
    dq_engine = DQEngine(ws)

    schema = "a: int"
    test_df = spark_local.createDataFrame([[1], [None]], schema)

    checks = [
        DQRule(name="col_a_is_null_or_empty", criticality="error", check=is_not_null_and_not_empty("a")),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected_schema = schema + ", _errors: map<string,string>, _warnings: map<string,string>"
    expected = spark_local.createDataFrame(
        [
            [1, None, None],
            [None, {"col_a_is_null_or_empty": "Column a is null or empty"}, None],
        ],
        expected_schema,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)
