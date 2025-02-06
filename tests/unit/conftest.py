import os
from pathlib import Path
from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark_session():
    return SparkSession.builder.appName("DQX Test").remote("sc://localhost").getOrCreate()


@pytest.fixture
def temp_yml_file():
    base_path = str(Path(__file__).resolve().parent.parent)
    file_path = base_path + "/test_data/checks-local-test.yml"
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)
