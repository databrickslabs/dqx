from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
import dbldatagen as dg
from datetime import datetime
import yaml
from databricks.labs.dqx.rule import (
    ExtraParams,
)
from pathlib import Path
from pyspark.sql.types import _parse_datatype_string

SCHEMA = "col1: int, col2: int, col3: int, col4: array<int>, col5: date, col6: timestamp, col7: map<string, int>, col8: struct<field1: int>, col9: string"
RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0)
EXTRA_PARAMS = ExtraParams(run_time=RUN_TIME)

def test_apply_checks_all_row_checks(ws, spark, generate_table_name, all_row_checks, generated_df):

    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    table_name = generate_table_name()
    df = generated_df()
    row_checks = all_row_checks()
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    checked_df = dq_engine.apply_checks_by_metadata(df, row_checks)

def test_apply_checks_all_row_checks_with_streaming(ws, spark, generate_table_name, all_row_checks, generated_df):

    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    table_name = generate_table_name()
    df = generated_df()
    row_checks = all_row_checks()
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    streaming_test_df = spark.readStream.table(table_name)
    streaming_checked_df = dq_engine.apply_checks_by_metadata(streaming_test_df, row_checks)
