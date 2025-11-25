# Databricks notebook source

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")
%pip install 'databricks-labs-dqx @ {dbutils.widgets.get("test_library_ref")}' chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from unittest.mock import MagicMock
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from chispa import assert_df_equality
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,test_apply_checks_foreachbatch_with_mock
def test_apply_checks_foreachbatch_with_mock():
    ws = MagicMock(spec=WorkspaceClient)
    engine = DQEngine(workspace_client=ws, spark=spark)
    input_df = spark.readStream.format("rate").load()
    checks = [
        DQRowRule(  # 'rate' source has a non-null 'value' column of type 'long'
            name="value_is_null_or_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="value",
        ),
    ]

    def batch_handler_function(batch_df: DataFrame, _: int) -> None:
        expected = batch_df.select("*", lit(None).alias("_warnings"), lit(None).alias("_errors"))
        actual = engine.apply_checks(batch_df, checks)
        assert_df_equality(actual, expected)

    input_df.writeStream.trigger(availableNow=True).foreachBatch(batch_handler_function).start()


test_apply_checks_foreachbatch_with_mock()
