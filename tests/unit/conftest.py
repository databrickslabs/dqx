import os
from pathlib import Path
from unittest.mock import Mock
from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark_session_mock():
    return Mock(spec=SparkSession)


@pytest.fixture
def temp_yml_file():
    base_path = str(Path(__file__).resolve().parent.parent)
    file_path = base_path + "/test_data/checks-local-test.yml"
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)
