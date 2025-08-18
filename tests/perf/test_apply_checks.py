from databricks.labs.dqx.engine import DQEngine
from datetime import datetime
from databricks.labs.dqx.rule import (
    ExtraParams,
)

SCHEMA = "col1: int, col2: int, col3: int, col4: array<int>, col5: date, col6: timestamp, col7: map<string, int>, col8: struct<field1: int>, col9: string"
RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0)
EXTRA_PARAMS = ExtraParams(run_time=RUN_TIME)


def get_checked_df(ws, all_row_checks, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checked_df = dq_engine.apply_checks_by_metadata(generated_df, all_row_checks)
    return checked_df

def test_benchmark_apply_checks_all_row_checks(benchmark, ws, all_row_checks, generated_df):
    checked_df = benchmark(get_checked_df, ws, all_row_checks, generated_df)
    assert checked_df.count() == 10_000_000, "Row count after applying checks does not match expected count"

# def test_apply_checks_all_row_checks_with_streaming(ws, spark, table_name, all_row_checks, generated_df):

#     dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
#     row_checks = all_row_checks()
#     # df.write.format("delta").mode("overwrite").saveAsTable(table_name)

#     streaming_test_df = spark.readStream.table(table_name)
#     streaming_checked_df = dq_engine.apply_checks_by_metadata(streaming_test_df, row_checks)
#     assert streaming_checked_df.count() == 10_000_000, "Row count after applying checks does not match expected count"
