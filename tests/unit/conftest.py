from unittest.mock import Mock
from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark_mock():
    return Mock(spec=SparkSession)
