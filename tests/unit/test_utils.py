import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx.utils import get_column_name


def test_get_column_name(spark_session: SparkSession):
    col = F.col("a")
    actual = get_column_name(col)
    assert actual == "a"


def test_get_col_name_alias(spark_session: SparkSession):
    col = F.col("a").alias("b")
    actual = get_column_name(col)
    assert actual == "b"


def test_get_col_name_multiple_alias(spark_session: SparkSession):
    col = F.col("a").alias("b").alias("c")
    actual = get_column_name(col)
    assert actual == "c"


def test_get_col_name_longer(spark_session: SparkSession):
    col = F.col("local")
    actual = get_column_name(col)
    assert actual == "local"
