import logging
import os
from datetime import datetime
from pathlib import Path
import pytest
import yaml
import dbldatagen as dg
from pyspark.sql.types import _parse_datatype_string

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import ExtraParams

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)

ROWS = 10_000_000
PARTITIONS = 8

SCHEMA_STR = (
    "col1: int, col2: int, col3: int, col4: array<int>, "
    "col5: date, col6: timestamp, col7: map<string, int>, "
    'col8: struct<field1: int>, col9: string'
)
RUN_TIME = datetime(2025, 1, 1, 0, 0, 0)


@pytest.fixture
def extra_params():
    return ExtraParams(run_time=RUN_TIME)

@pytest.fixture
def dq_engine(ws, extra_params):
    return DQEngine(workspace_client=ws, extra_params=extra_params)

@pytest.fixture
def table_schema():
    return _parse_datatype_string(SCHEMA_STR)

@pytest.fixture
def all_row_checks():
    file_path = Path(__file__).parent / "resources" / "all_row_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

@pytest.fixture
def all_dataset_checks():
    file_path = Path(__file__).parent / "resources" / "all_dataset_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

@pytest.fixture
def generate_table_name(make_schema, make_random):
    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    return f"{catalog}.{schema}.{make_random(6).lower()}"

@pytest.fixture
def generated_df(spark, table_schema):
    spec = (
        dg.DataGenerator(spark, rows=ROWS, partitions=PARTITIONS)
        .withSchema(table_schema)
        .withColumnSpec("col1", percentNulls=0.20)
        .withColumnSpec("col2")
        .withColumnSpec("col3")
        .withColumnSpec("col4")
        .withColumnSpec("col5", begin="1900-01-01", end="2025-12-31", interval="1 second")
        .withColumnSpec("col6", begin="1900-01-01 01:00:00", end="2025-12-31 23:59:00", interval="1 second")
        .withColumnSpec("col7")
        .withColumnSpec("col8")
        .withColumnSpec("col9", template=r"\n.\n.\n.\n")
    )
    return spec.build()
