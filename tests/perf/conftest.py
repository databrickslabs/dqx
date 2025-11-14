import logging
from datetime import datetime, timezone
from pathlib import Path
import pytest
import yaml
import dbldatagen as dg  # type: ignore[import-untyped]
from pyspark.sql.types import _parse_datatype_string

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import ExtraParams

from tests.conftest import TEST_CATALOG


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)

DEFAULT_ROWS = 100_000_000  # 100 million rows
DEFAULT_PARTITIONS = 10
DEFAULT_COLUMNS = 4
DEFAULT_BEGIN_DATE = "1900-01-01"
DEFAULT_END_DATE = "2025-12-31"
DEFAULT_BEGIN_TIMESTAMP = "1900-01-01 00:00:00"
DEFAULT_END_TIMESTAMP = "2025-12-31 23:59:59"
DEFAULT_INTERVAL = "1 second"
DEFAULT_EMAIL_TEMPLATE = r'\\w.\\w@\\w.com'

REF_SCHEMA_STR = "ref_col1: int, ref_col2: int, ref_col3: int"
SCHEMA_STR = (
    "col1: int, col2: int, col3: int, col4: array<int>, "
    "col5: date, col6: timestamp, col7: map<string, int>, "
    "col8: struct<field1: int>, col10: int, col_ipv4: string, col_ipv6: string"
)

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
    catalog = TEST_CATALOG
    schema = make_schema(catalog_name=catalog).name
    return f"{catalog}.{schema}.{make_random(10).lower()}"


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
        .withColumnSpec("col10")
        .withColumnSpec("col_ipv4", template=r"\n.\n.\n.\n")
        .withColumnSpec("col_ipv6", template="XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX")
    )
    return spec.build()


@pytest.fixture
def generated_ipv4_df(spark):
    ipv4_schema_str = (
        "col1_ipv4_standard: string, "
        "col2_ipv4_with_leading_zeros: string, "
        "col3_ipv4_partial: string, "
        "col4_ipv4_mixed: string"
    )
    schema = _parse_datatype_string(ipv4_schema_str)

    ipv4_templates = {
        "col1_ipv4_standard": r"\n.\n.\n.\n",
        "col2_ipv4_with_leading_zeros": r"000.\n.\n.\n",
        "col3_ipv4_partial": r"\n.\n.",
        "col4_ipv4_mixed": r"192.168.\n.\n",
    }

    _, gen = make_data_gen(spark, n_rows=DEFAULT_ROWS, n_columns=len(ipv4_templates), partitions=DEFAULT_PARTITIONS)
    gen = gen.withSchema(schema)
    for col, template in ipv4_templates.items():
        gen = gen.withColumnSpec(col, template=template)

    return gen.build()


@pytest.fixture
def generated_ipv6_df(spark):
    ipv6_schema_str = (
        "col1_ipv6_u_upper: string, "
        "col2_ipv6_u_lower: string, "
        "col3_ipv6_c_min1: string, "
        "col4_ipv6_c_r3: string, "
        "col5_ipv6_c_l3: string, "
        "col6_ipv6_c_mid1: string, "
        "col7_ipv6_c_mid4: string, "
        "col8_ipv6_u_prefix: string"
    )
    schema = _parse_datatype_string(ipv6_schema_str)

    ipv6_templates = {
        "col1_ipv6_u_upper": r"XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX",
        "col2_ipv6_u_lower": r"xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx",
        "col3_ipv6_c_min1": "::",
        "col4_ipv6_c_r3": "::XXXX:XXXX:XXXX",
        "col5_ipv6_c_l3": "XXXX:XXXX:XXXX::",
        "col6_ipv6_c_mid1": "XXXX::XXXX",
        "col7_ipv6_c_mid4": "XXXX:XXXX::XXXX",
        "col8_ipv6_u_prefix": "2001:0DB8:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX",
    }

    _, gen = make_data_gen(spark, n_rows=DEFAULT_ROWS, n_columns=len(ipv6_templates), partitions=DEFAULT_PARTITIONS)
    gen = gen.withSchema(schema)
    for col, template in ipv6_templates.items():
        gen = gen.withColumnSpec(col, template=template)

    return gen.build()


@pytest.fixture
def generated_geo_df(spark):
    geo_schema_str = (
        "num_col: int, point_geom: string, linestring_geom: string, polygon_geom: string, multipoint_geom: string, "
        "multilinestring_geom: string, multipolygon_geom: string, geometrycollection_geom: string"
    )
    schema = _parse_datatype_string(geo_schema_str)

    geo_templates = {
        "num_col": "int",
        "point_geom": "POINT(x x)",
        "linestring_geom": "LINESTRING(x x, x x)",
        "polygon_geom": "POLYGON((x x, x x, x x, x x))",
        "multipoint_geom": "MULTIPOINT(x x, x x)",
        "multilinestring_geom": "MULTILINESSTRING((x x, x x))",
        "multipolygon_geom": "MULTIPOLYGON(((x x, x x, x x, x x))",
        "geometrycollection_geom": "GEOMETRYCOLLECTION(POINT(x x), LINESTRING(x x, x x), POLYGON((x x, x x, x x, x x)))",
    }

    _, gen = make_data_gen(spark, n_rows=DEFAULT_ROWS, n_columns=len(geo_schema_str), partitions=DEFAULT_PARTITIONS)
    gen = gen.withSchema(schema)
    for col, template in geo_templates.items():
        gen = gen.withColumnSpec(col, template=template)
    return gen.build()


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
        data_gen = data_gen.withColumn(
            col, "string", template=DEFAULT_EMAIL_TEMPLATE, numFeatures=(1, array_length), structType="array", **opts
        )
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
