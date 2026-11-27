import dataclasses
import uuid

from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.engine import DQEngine
from tests.constants import TEST_CATALOG
from tests.integration.conftest import EXTRA_PARAMS

SCHEMA = "a: int, b: int, c: int"


def test_ref_table_reads_supplied_dataframe_when_opted_in(ws, spark):
    extra_params = dataclasses.replace(EXTRA_PARAMS, resolve_ref_tables_from_ref_dfs=True)
    dq_engine = DQEngine(workspace_client=ws, extra_params=extra_params)
    src_df = spark.createDataFrame([[1, 2, 3], [4, 5, 6]], SCHEMA)
    ref_df = spark.createDataFrame([[1]], "a: int")
    # The table does not exist, so the check can only pass if it reads the supplied DataFrame.
    ref_table = f"{TEST_CATALOG}.no_such_schema_{uuid.uuid4().hex[:8]}.parents"
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "foreign_key",
                "arguments": {"columns": ["a"], "ref_columns": ["a"], "ref_table": ref_table},
            },
        }
    ]

    _, invalid_df = dq_engine.apply_checks_by_metadata_and_split(src_df, checks, ref_dfs={ref_table: ref_df})

    rows = invalid_df.collect()
    assert [row.a for row in rows] == [4]
    # The check is not rewritten, so its fingerprint is the one stored for the ref_table check.
    assert rows[0]["_errors"][0]["rule_fingerprint"] == deserialize_checks(checks)[0].rule_fingerprint
