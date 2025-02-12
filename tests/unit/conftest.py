from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark_local():
    return SparkSession.builder.appName("DQX Test").remote("sc://localhost").getOrCreate()
