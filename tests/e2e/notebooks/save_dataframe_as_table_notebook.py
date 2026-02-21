# Databricks notebook source

# MAGIC %md
# MAGIC We need to implement streaming tests for cluster_by using as e2e tests in a notebook since spark connect which used by to run integration tests does not support setting the spark property 'spark.databricks.delta.liquid.eagerClustering.streaming.enabled'

# COMMAND ----------
dbutils.widgets.text("test_library_ref", "", "Test Library Ref")
%pip install 'databricks-labs-dqx @ {dbutils.widgets.get("test_library_ref")}' chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("test_catalog", "", "Test Catalog")
dbutils.widgets.text("test_schema", "", "Test Schema")
dbutils.widgets.text("test_volume", "", "Test Volume")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")
test_volume = dbutils.widgets.get("test_volume")

# COMMAND ----------

import io
import logging

from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.io import save_dataframe_as_table
from chispa import assert_df_equality

# COMMAND ----------

logger = logging.getLogger("databricks.labs.dqx.io")

def test_save_streaming_dataframe_in_table_with_cluster_by_missing_spark_config():
    input_table_name = f"{test_catalog}.{test_schema}.test_input_table_001"
    output_table_name = f"{test_catalog}.{test_schema}.test_output_table_001"
    options = {"checkpointLocation": f"{test_volume}/_checkpoint_001"}
    trigger = {"availableNow": True}
    output_config = OutputConfig(location=output_table_name, options=options, trigger=trigger, cluster_by=["a"])

    data_schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], data_schema)
    input_df.write.format("delta").mode("overwrite").saveAsTable(input_table_name)
    streaming_input_df = spark.readStream.table(input_table_name)

    spark.conf.set("spark.databricks.delta.liquid.eagerClustering.streaming.enabled", "false")
    # NOTE: Streaming writes with `cluster_by` will generate warnings via logger; Using log handling in this test to
    # ensure that the generated logs and ensure the appropriate warnings are written
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    logger.addHandler(handler)

    try:
        save_dataframe_as_table(streaming_input_df, output_config).awaitTermination()
        handler.flush()
        assert "Ignoring 'cluster_by' for streaming writes; Set spark.conf.set('spark.databricks.delta.liquid.eagerClustering.streaming.enabled', 'true') to enable liquid clustering" in log_stream.getvalue()
    finally:
        logger.removeHandler(handler)

    result_df = spark.table(output_table_name)
    assert_df_equality(input_df, result_df)

test_save_streaming_dataframe_in_table_with_cluster_by_missing_spark_config()

# COMMAND ----------

def test_save_streaming_dataframe_in_table_with_cluster_by():
    input_table_name = f"{test_catalog}.{test_schema}.test_input_table_002"
    output_table_name = f"{test_catalog}.{test_schema}.test_output_table_002"
    options = {"checkpointLocation": f"{test_volume}/_checkpoint_002"}
    trigger = {"availableNow": True}
    output_config = OutputConfig(location=output_table_name, options=options, trigger=trigger, cluster_by=["a"])

    data_schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], data_schema)
    input_df.write.format("delta").mode("overwrite").saveAsTable(input_table_name)
    streaming_input_df = spark.readStream.table(input_table_name)

    spark.conf.set("spark.databricks.delta.liquid.eagerClustering.streaming.enabled", "true")
    save_dataframe_as_table(streaming_input_df, output_config).awaitTermination()

    result_df = spark.table(output_table_name)
    assert_df_equality(input_df, result_df)

    table_detail = spark.sql(f"DESCRIBE DETAIL {output_table_name}").collect()[0]
    assert table_detail["clusteringColumns"] == ["a"]

test_save_streaming_dataframe_in_table_with_cluster_by()
