from pyspark.sql.types import Row
import pytest
from databricks.labs.dqx.utils import read_input_data


@pytest.fixture(scope="module")
def setup(spark):
    schema = "col1: str, col2: int"
    input_df = spark.createDataFrame([["k1", 1]], schema)

    # write dataframe to catalog, create a catalog if it is not there
    spark.sql("CREATE CATALOG IF NOT EXISTS dqx_catalog")
    spark.sql("CREATE DATABASE IF NOT EXISTS dqx_catalog.dqx_db")
    input_df.write.format("delta").saveAsTable("dqx_catalog.dqx_db.dqx_table")

    # write dataframe to file
    input_df.write.format("delta").save("/tmp/dqx_table")


def test_read_input_data_unity_catalog_table(setup, spark):
    input_location = "dqx_catalog.dqx_db.dqx_table"
    input_format = None

    result = read_input_data(spark, input_location, input_format)
    assert result.collect() == [Row(col1='k1', col2=1)]


def test_read_input_data_workspace_file(setup, spark):
    input_location = "/tmp/dqx_table"
    input_format = "delta"

    result = read_input_data(spark, input_location, input_format)
    assert result.collect() == [Row(col1='k1', col2=1)]
