from unittest.mock import MagicMock

from datetime import datetime
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.col_functions import is_not_null_and_not_empty
from databricks.labs.dqx.engine import DQEngine, ExtraParams, DQRule
from databricks.labs.dqx.schema import dq_result_schema
from databricks.sdk import WorkspaceClient


def test_apply_checks(spark_local):
    ws = MagicMock(spec=WorkspaceClient, **{"catalogs.list.return_value": []})

    schema = "x: int, y: int, z: int"
    expected_schema = (
        schema + f", _errors: {dq_result_schema.simpleString()}, _warnings: {dq_result_schema.simpleString()}"
    )
    test_df = spark_local.createDataFrame([[1, None, 3]], schema)

    checks = [
        DQRule(
            name="col_x_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty,
            col_name="x",
        ),
        DQRule(
            name="col_y_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            col_name="y",
        ),
    ]

    dq_engine = DQEngine(workspace_client=ws, extra_params=ExtraParams(run_time=datetime(2025, 1, 1, 0, 0, 0, 0)))

    df = dq_engine.apply_checks(test_df, checks)
    expected_df = spark_local.createDataFrame(
        [
            [
                1,
                None,
                3,
                [
                    {
                        "name": "col_y_is_null_or_empty",
                        "message": "Column y is null or empty",
                        "col_name": "y",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": datetime(2025, 1, 1, 0, 0, 0, 0),
                    }
                ],
                None,
            ]
        ],
        expected_schema,
    )
    assert_df_equality(df, expected_df, ignore_nullable=True)
