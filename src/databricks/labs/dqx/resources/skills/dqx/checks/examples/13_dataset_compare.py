# based on checks from dqx/checks/dataset-level/SKILL.md:45-49
# On Databricks notebooks, `spark` is already available — skip the next 2 lines.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.createDataFrame(
    [(1, "Alice", 100), (2, "Bob", 200), (3, "Carol", 300)],
    ["id", "name", "amount"],
)
ref_df = spark.createDataFrame(
    [(1, "Alice", 100), (2, "Bob", 250)],
    ["id", "name", "amount"],
)

checks = yaml.safe_load("""
- criticality: error
  check:
    function: compare_datasets
    arguments:
      columns: [id]
      ref_columns: [id]
      ref_df_name: reference
      check_missing_records: true
""")

dq = DQEngine(WorkspaceClient(), spark=spark)
dq.apply_checks_by_metadata(df, checks, ref_dfs={"reference": ref_df}).show(truncate=False)
