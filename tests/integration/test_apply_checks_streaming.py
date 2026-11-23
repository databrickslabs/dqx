import pyspark.sql.functions as F
import pytest

from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidCheckError
from tests.constants import TEST_CATALOG


def test_apply_checks_rejects_streaming_unsupported_dataset_checks(ws, make_schema, make_random, spark):
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    input_table_name = f"{TEST_CATALOG}.{schema_name}.t{make_random(10).lower()}"
    spark.createDataFrame([[1, "a"], [1, "b"]], "id: int, name: string").write.saveAsTable(input_table_name)
    checks = [{"criticality": "error", "check": {"function": "is_unique", "arguments": {"columns": ["id"]}}}]

    with pytest.raises(InvalidCheckError, match=r"not supported on streaming DataFrames.*\(is_unique\)"):
        DQEngine(ws).apply_checks_by_metadata(spark.readStream.table(input_table_name), checks)


def test_apply_checks_supported_dataset_checks_with_streaming(ws, make_schema, make_random, make_volume, spark):
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    input_table_name = f"{TEST_CATALOG}.{schema_name}.t{make_random(10).lower()}"
    output_table_name = f"{TEST_CATALOG}.{schema_name}.t{make_random(10).lower()}"
    volume = make_volume(catalog_name=TEST_CATALOG, schema_name=schema_name)
    spark.createDataFrame([[1, "a"], [2, "b"], [3, "c"]], "id: int, name: string").write.saveAsTable(input_table_name)
    ref_df = spark.createDataFrame([[1], [2]], "ref_id: int")
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "foreign_key",
                "arguments": {"columns": ["id"], "ref_columns": ["ref_id"], "ref_df_name": "ref_ids"},
            },
        },
        {
            "criticality": "warn",
            "check": {"function": "has_valid_schema", "arguments": {"expected_schema": "id INT, name STRING"}},
        },
    ]

    dq_engine = DQEngine(ws)
    checked_df = dq_engine.apply_checks_by_metadata(
        spark.readStream.table(input_table_name), checks, ref_dfs={"ref_ids": ref_df}
    )
    dq_engine.save_results_in_table(
        output_df=checked_df,
        output_config=OutputConfig(
            location=output_table_name,
            mode="append",
            trigger={"availableNow": True},
            options={
                "checkpointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{make_random(10).lower()}"
            },
        ),
    )

    result_df = spark.table(output_table_name)
    assert result_df.count() == 3
    assert {row["id"] for row in result_df.where(F.col("_errors").isNotNull()).collect()} == {3}
    assert result_df.where(F.col("_warnings").isNotNull()).count() == 0
