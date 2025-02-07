import os
from unittest.mock import Mock
from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark_session_mock():
    return Mock(spec=SparkSession)


@pytest.fixture
def checks_yml_content():
    return """
        - criticality: error
          check:
            function: is_not_null
            arguments:
              col_names:
              - col1
              - col2
        - name: col_col3_is_null_or_empty
          criticality: error
          check:
            function: is_not_null_and_not_empty
            arguments:
              col_name: col3
              trim_strings: true
        - criticality: warn
          check:
            function: value_is_in_list
            arguments:
              col_name: col4
              allowed:
              - 1
              - 2
    """


@pytest.fixture
def checks_json_content():
    return """
    [
        {
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {
                    "col_names": ["col1", "col2"]
                }
            }
        },
        {
            "name": "col_col3_is_null_or_empty",
            "criticality": "error",
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {
                    "col_name": "col3",
                    "trim_strings": true
                }
            }
        },
        {
            "criticality": "warn",
            "check": {
                "function": "value_is_in_list",
                "arguments": {
                    "col_name": "col4",
                    "allowed": [1, 2]
                }
            }
        }
    ]
    """


@pytest.fixture
def make_local_check_file_as_yml(checks_yml_content):
    file_path = 'checks.yml'
    with open(file_path, 'w', encoding="utf-8") as f:
        f.write(checks_yml_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_local_check_file_as_json(checks_json_content):
    file_path = 'checks.json'
    with open(file_path, 'w', encoding="utf-8") as f:
        f.write(checks_json_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)
