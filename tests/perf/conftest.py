import logging
from datetime import datetime, timezone
from pathlib import Path
import pytest
import yaml
import dbldatagen as dg  # type: ignore[import-untyped]
from pyspark.sql.types import _parse_datatype_string

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import ExtraParams

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)

DEFAULT_ROWS = 100_000_000  # 100 million rows
DEFAULT_PARTITIONS = 10
DEFAULT_COLUMNS = 4
SCHEMA_STR = (
    "col1: int, col2: int, col3: int, col4: array<int>, "
    "col5: date, col6: timestamp, col7: map<string, int>, "
    'col8: struct<field1: int>, col9: string, col10: string'
)
DEFAULT_BEGIN_DATE = "1900-01-01"
DEFAULT_END_DATE = "2025-12-31"
DEFAULT_BEGIN_TIMESTAMP = "1900-01-01 00:00:00"
DEFAULT_END_TIMESTAMP = "2025-12-31 23:59:59"
DEFAULT_INTERVAL = "1 second"

REF_SCHEMA_STR = "ref_col1: int, ref_col2: int, ref_col3: int"

RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)


def make_data_gen(
    spark,
    n_rows: int = DEFAULT_ROWS,
    n_columns: int = DEFAULT_COLUMNS,
    partitions: int = DEFAULT_PARTITIONS,
):
    col_names = [f"col{i+1}" for i in range(n_columns)]
    gen = dg.DataGenerator(spark, rows=n_rows, partitions=partitions)
    return col_names, gen


@pytest.fixture
def extra_params():
    return ExtraParams(run_time=RUN_TIME.isoformat())


@pytest.fixture
def dq_engine(ws, extra_params):
    return DQEngine(workspace_client=ws, extra_params=extra_params)


@pytest.fixture
def table_schema():
    return _parse_datatype_string(SCHEMA_STR)


@pytest.fixture
def all_row_checks():
    file_path = Path(__file__).parent.parent / "resources" / "all_row_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def all_dataset_checks():
    file_path = Path(__file__).parent.parent / "resources" / "all_dataset_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def table_name(make_schema, make_random):
    catalog = "main"
    schema = make_schema(catalog_name=catalog).name
    return f"{catalog}.{schema}.{make_random(6).lower()}"


@pytest.fixture
def generated_df(spark, rows=DEFAULT_ROWS):
    schema = _parse_datatype_string(SCHEMA_STR)
    spec = (
        dg.DataGenerator(spark, rows=rows, partitions=DEFAULT_PARTITIONS)
        .withSchema(schema)
        .withColumnSpec("col1", percentNulls=0.20)
        .withColumnSpec("col2")
        .withColumnSpec("col3")
        .withColumnSpec("col4", expr="array(col1, col2)")
        .withColumnSpec("col5", begin=DEFAULT_BEGIN_DATE, end=DEFAULT_END_DATE, interval=DEFAULT_INTERVAL)
        .withColumnSpec("col6", begin=DEFAULT_BEGIN_TIMESTAMP, end=DEFAULT_END_TIMESTAMP, interval=DEFAULT_INTERVAL)
        .withColumnSpec("col7", expr="map('key', col2)")
        .withColumnSpec("col8", expr="named_struct('col8', col1)")
        .withColumnSpec("col9", template=r"\n.\n.\n.\n")
        .withColumnSpec("col10", template="XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX")
    )
    return spec.build()


@pytest.fixture
def make_ref_df(spark, n_rows=DEFAULT_ROWS):
    schema = _parse_datatype_string(REF_SCHEMA_STR)
    spec = (
        dg.DataGenerator(spark, rows=n_rows, partitions=DEFAULT_PARTITIONS)
        .withSchema(schema)
        .withColumnSpec("ref_col1")
        .withColumnSpec("ref_col2")
        .withColumnSpec("ref_col3")
    )
    return spec.build()


@pytest.fixture
def generated_string_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    template = params.get("template", None)
    opts = params.get("opts", {})

    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        if template is None:
            data_gen = data_gen.withColumn(col, "string", **opts)
        else:
            data_gen = data_gen.withColumn(col, template=template, **opts)
    return col_names, data_gen.build(), n_rows


@pytest.fixture
def generated_integer_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    opts = params.get("opts", {})

    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        data_gen = data_gen.withColumn(col, "int", **opts)
    return col_names, data_gen.build(), n_rows


@pytest.fixture
def generated_array_string_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    array_length = params.get("array_length", 2)
    opts = params.get("opts", {})
    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        data_gen = data_gen.withColumn(col, "string", template=r'\\w.\\w@\\w.com',
                 numFeatures=(1, array_length), structType="array", **opts)
    return col_names, data_gen.build(), n_rows


@pytest.fixture
def generated_date_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    begin = params.get("begin", DEFAULT_BEGIN_DATE)
    end = params.get("end", DEFAULT_END_DATE)
    interval = params.get("interval", DEFAULT_INTERVAL)
    opts = params.get("opts", {})
    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        data_gen = data_gen.withColumn(col, "date", begin=begin, end=end, interval=interval, **opts)
    return col_names, data_gen.build(), n_rows


@pytest.fixture
def generated_timestamp_df(request, spark):
    params = getattr(request, "param", {}) or {}
    n_rows = params.get("n_rows", DEFAULT_ROWS)
    n_columns = params.get("n_columns", DEFAULT_COLUMNS)
    begin = params.get("begin", DEFAULT_BEGIN_TIMESTAMP)
    end = params.get("end", DEFAULT_END_TIMESTAMP)
    interval = params.get("interval", DEFAULT_INTERVAL)
    opts = params.get("opts", {})

    col_names, data_gen = make_data_gen(spark, n_rows=n_rows, n_columns=n_columns)
    for col in col_names:
        data_gen = data_gen.withColumn(col, "timestamp", begin=begin, end=end, interval=interval, **opts)
    return col_names, data_gen.build(), n_rows
