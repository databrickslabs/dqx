from unittest.mock import Mock
from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark():
    return Mock(spec=SparkSession)
